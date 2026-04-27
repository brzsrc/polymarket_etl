"""
Append-only WAL writer for raw WebSocket messages.

One ``WALWriter`` instance owns a directory tree under ``data_dir``:

    {data_dir}/wal/{YYYY-MM-DD}/{prefix}.jsonl

where ``prefix`` is a 2-character shard derived from the first asset_id
mentioned in a message. The shard exists to keep individual files small —
Polymarket has ~46k active binary tokens; if every asset wrote to one file
per day, replays would be IO-bound on uninteresting messages. Sharding by
asset_id prefix means a researcher querying "give me asset X for date D"
only opens one small file.

Why prefix is the *first* 2 chars (not last, not hash): asset_id is a
decimal uint256 string. Empirically the first 2 chars are well-distributed
across the active token population; using them avoids any computation
(no hashing) and is human-inspectable (you can look at "01.jsonl" and know
which assets are inside).

Multi-asset messages
--------------------
``price_change`` events can mention multiple asset_ids in one frame. We
write the message to **only one shard** (the first asset_id's prefix) and
include the full ``asset_ids`` list in the record. This avoids duplicating
the same bytes across N files. Replay code that wants "all messages for
asset X" must scan the relevant shard for any record where ``asset_ids``
contains X.

Concurrency
-----------
A single WALWriter is intended to be shared by multiple WSConnections (as
will be the case once a Pool feeds it). All public async methods are
serialized through a single lock — fine because:
- write() does at most one in-memory buffer append + occasional file open
- the slow part (fsync, gzip) is in background tasks not holding the lock

Lifecycle
---------
Use as an async context manager:

    async with WALWriter(data_dir=Path("data")) as wal:
        # plug into a WSConnection:
        async def on_msg(asset_ids, raw, parsed, ts_recv_ns, conn_id):
            await wal.write(asset_ids, raw, ts_recv_ns)
        conn = WSConnection(0, on_msg)
        ...

On exit, all handles are flushed + fsynced + closed. The fsync background
task is also started on enter / cancelled on exit.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Self

import msgspec

logger = logging.getLogger(__name__)

# How often to fsync all open files. fsync is expensive (1-10ms typically),
# so we batch — at the cost of losing up to this many seconds of data on
# crash. 1s is the standard Postgres-style tradeoff.
DEFAULT_FSYNC_INTERVAL_SEC = 1.0

# Default shard prefix length. 2 means 100 buckets max (00-99) for decimal
# asset_ids — small enough to keep open file count manageable, large enough
# to keep individual files reasonable on busy days.
SHARD_PREFIX_LEN = 2


class WALWriter:
    """
    Append-only sharded JSONL writer.

    Methods:
        write(asset_ids, raw_str, ts_recv_ns) — record one message
        flush() — flush+fsync all open handles right now
        stats() — current counters (for monitoring)

    Use as async context manager (handles the periodic fsync task).
    """

    def __init__(
        self,
        data_dir: Path,
        fsync_interval_sec: float = DEFAULT_FSYNC_INTERVAL_SEC,
        shard_prefix_len: int = SHARD_PREFIX_LEN,
    ) -> None:
        if fsync_interval_sec <= 0:
            raise ValueError("fsync_interval_sec must be > 0")
        if shard_prefix_len < 1 or shard_prefix_len > 4:
            raise ValueError("shard_prefix_len must be 1..4")

        self._root = Path(data_dir) / "wal"
        self._fsync_interval = fsync_interval_sec
        self._shard_prefix_len = shard_prefix_len

        self._encoder = msgspec.json.Encoder()

        # Background fsync task; started in __aenter__.
        self._fsync_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        # All public methods serialise through this. Held only briefly.
        self._lock = asyncio.Lock()

        # (date_str, prefix) -> open file handle (binary append mode)
        self._handles: dict[tuple[str, str], IO[bytes]] = {}
        # Same key — set when handle was last written to. Drives cleanup
        # for files that are no longer being written (e.g. yesterday's).
        self._handle_dirty: dict[tuple[str, str], bool] = {}

        # Counters for stats() / metrics integration
        self._n_messages = 0
        self._n_bytes = 0
        # Messages we couldn't route (no asset_ids extracted) go to a
        # special "unknown" shard. Tracked separately for visibility.
        self._n_no_route = 0




    async def __aenter__(self) -> Self:
        self._root.mkdir(parents=True, exist_ok=True)
        self._stop_event.clear()
        self._fsync_task = asyncio.create_task(
            self._fsync_loop(), name="wal-fsync-loop"
        )
        logger.info("WAL writer started (root=%s)", self._root)
        return self


    async def __aexit__(self, *_exc) -> None:
        # Stop the background task
        self._stop_event.set()
        if self._fsync_task is not None:
            try:
                await self._fsync_task
            except asyncio.CancelledError:
                pass
            self._fsync_task = None

        # Final flush + close everything
        async with self._lock:
            for key, fh in list(self._handles.items()):
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                except Exception:
                    logger.exception("WAL: error during final fsync of %s", key)
                try:
                    fh.close()
                except Exception:
                    pass
            self._handles.clear()
            self._handle_dirty.clear()
        logger.info(
            "WAL writer stopped (msgs=%d, bytes=%d, no_route=%d)",
            self._n_messages, self._n_bytes, self._n_no_route,
        )


    async def write(
        self,
        asset_ids: tuple[str, ...],
        raw_str: str,
        ts_recv_ns: int,
    ) -> None:
        """
        Persist one message.

        Args:
            asset_ids: tuple of asset_ids the message pertains to. May be
                empty (we couldn't route) — in which case it goes to the
                "unknown" shard.
            raw_str: original message body as received from the WS (Polymarket
                sends Text frames, so this is str). We embed it verbatim
                into our JSONL wrapper as a string field — no parse / re-
                serialize round-trip, no information loss.
            ts_recv_ns: nanoseconds since epoch when WE received the message
                (set as close to wire arrival as possible by the WSConnection).
        """
        # Compute everything OUTSIDE the lock that we can — encoding,
        # date derivation, key calculation. Only the actual file handle
        # cache lookup + write happens under the lock.

        date_str = self._date_str_from_ns(ts_recv_ns)
        prefix = self._shard_prefix(asset_ids)

        # The wrapper. Two timestamps:
        # - ts_recv_ns: machine-readable, monotonic-ish, what code uses
        # - ts_recv_iso: human-readable, what people grep for
        # Both derive from the same source so they can't disagree.
        wrapper = {
            "ts_recv_ns": ts_recv_ns,
            "ts_recv_iso": self._iso_from_ns(ts_recv_ns),
            "asset_ids": list(asset_ids),
            "raw": raw_str,  # raw embedded as string — JSON escapes \\" etc.
        }
        line = self._encoder.encode(wrapper) + b"\n"

        async with self._lock:
            fh = self._get_handle_locked(date_str, prefix)
            fh.write(line)
            self._handle_dirty[(date_str, prefix)] = True
            self._n_messages += 1
            self._n_bytes += len(line)
            if not asset_ids:
                self._n_no_route += 1

    def _shard_prefix(self, asset_ids: tuple[str, ...]) -> str:
        """First N chars of the first asset_id, or "unknown" if empty."""
        if not asset_ids:
            return "unknown"
        first = asset_ids[0]
        if len(first) < self._shard_prefix_len:
            # Asset_ids should be ~78 chars; if shorter, just use as-is
            # padded so it's still a valid filename.
            return first.ljust(self._shard_prefix_len, "_")
        return first[: self._shard_prefix_len]

    def _shard_path(self, date_str: str, prefix: str) -> Path:
        return self._root / date_str / f"{prefix}.jsonl"


    def _get_handle_locked(self, date_str: str, prefix: str) -> IO[bytes]:
        """
        Get-or-create the file handle for (date, prefix).

        Caller must hold self._lock.

        When we cross a UTC midnight, the (date_str, prefix) key changes,
        so we naturally start a new file. Old handles for previous dates
        stay open until __aexit__ — they may still receive late writes
        (e.g. if a message has ts_recv_ns from yesterday for some reason),
        which is the correct behavior. They're flushed periodically and
        closed cleanly on shutdown.
        """
        key = (date_str, prefix)
        fh = self._handles.get(key)
        if fh is not None:
            return fh

        path = self._shard_path(date_str, prefix)
        path.parent.mkdir(parents=True, exist_ok=True)
        # Binary append. The "ab" mode is the right one even though our
        # content is text JSON — see Phase 2 discussion: binary mode
        # avoids platform-dependent line-ending munging and accepts bytes
        # from msgspec.json.Encoder directly.
        fh = path.open("ab")
        self._handles[key] = fh
        self._handle_dirty[key] = False
        logger.info("WAL: opened %s", path)
        return fh

    def _flush_dirty_locked(self) -> None:
        """Flush + fsync any handle that was written to since last flush."""
        for key, dirty in list(self._handle_dirty.items()):
            if not dirty:
                continue
            fh = self._handles.get(key)
            assert fh is not None
            try:
                fh.flush()
                os.fsync(fh.fileno())
            except Exception:
                logger.exception("WAL: error flushing %s", key)
            self._handle_dirty[key] = False


    async def _fsync_loop(self) -> None:
        """Background: every fsync_interval, flush dirty handles."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), self._fsync_interval
                )
                # If wait returned without timeout, stop was requested
                return
            except asyncio.TimeoutError:
                pass  # interval elapsed, do work

            async with self._lock:
                self._flush_dirty_locked()


    @staticmethod
    def _date_str_from_ns(ts_ns: int) -> str:
        # Always UTC so dates are stable across deployments / timezones.
        # Hot path: avoid datetime() if possible? Empirically datetime is
        # fast enough (~1 µs) and clearer than manual arithmetic.
        dt = datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d")

    @staticmethod
    def _iso_from_ns(ts_ns: int) -> str:
        # ISO 8601 with nanosecond precision. Python's isoformat() goes
        # only to microseconds, so we tack on the remaining 3 digits.
        # Example output: "2026-04-27T15:23:01.123456789Z"
        sec, ns_remainder = divmod(ts_ns, 1_000_000_000)
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        # Format: YYYY-MM-DDTHH:MM:SS, then 9-digit fraction, then Z
        return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{ns_remainder:09d}Z"

    def stats(self) -> dict:
        """Snapshot of writer state. Cheap (no lock — eventually consistent)."""
        return {
            "messages": self._n_messages,
            "bytes": self._n_bytes,
            "no_route": self._n_no_route,
            "open_handles": len(self._handles),
            "root": str(self._root),
        }

    async def stats_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            logger.info("wal stats: %s", self.stats())

    async def flush(self) -> None:
        """Force-flush + fsync all currently open handles."""
        async with self._lock:
            self._flush_dirty_locked()