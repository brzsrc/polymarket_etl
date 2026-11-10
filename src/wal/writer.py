"""
Append-only WAL writer for raw WebSocket messages, Gamma snapshots,
connection events and metrics samples (segment layout).

Four streams, same layout and same lifecycle, under ``data_dir``::

    wal/{YYYY-MM-DD}/000000.jsonl.gz       WS messages, sealed (immutable, gzip)
    wal/{YYYY-MM-DD}/000001.jsonl          WS messages, active
    markets/{YYYY-MM-DD}/000000.jsonl.gz   one line per market per discovery cycle
    events/{YYYY-MM-DD}/000000.jsonl.gz    WS connection events (connect / disconnect / ...)
    metrics/{YYYY-MM-DD}/000000.jsonl.gz   orchestrator metrics, one sample per second

Each (stream, date) has at most one active segment. The date is the UTC
date of ``ts_recv_ns``. Every WS message is written once, with its full
``asset_ids`` list. There is no sharding by asset: readers scan all
segments of a day and filter.

Segment lifecycle
-----------------
1. ``write()`` appends one JSON line to the active segment (hot path, the
   lock is held only for the in-memory write).
2. Every ``fsync_interval_sec`` the background loop fsyncs dirty files, so a
   crash loses at most that much data.
3. Every ``check_interval_sec`` the background loop checks the active
   segment size. Past ``segment_max_bytes`` it is closed and the number is
   bumped; the next write opens the next file. The same pass closes every
   segment of an earlier UTC date once ``day_close_grace_sec`` has passed
   since midnight, so yesterday's files are sealed and uploaded without
   waiting for an exit. A late record for a closed date opens the next
   number (e.g. ``000001.jsonl``), which the next pass seals too.
4. A closed segment is sealed in a worker thread::

       000001.jsonl -> 000001.jsonl.gz.tmp -> fsync -> rename -> fsync dir
                    -> delete 000001.jsonl

   Readers therefore never see a partial ``.gz``. A torn last line (no
   trailing newline, left by a crash) is dropped during sealing.

On a clean exit (``seal_on_exit=True``, the default) the active segments are
closed and sealed too, so nothing is left as ``.jsonl``.

Crash recovery
--------------
On start, orphan ``*.gz.tmp`` files are removed and every ``NNNNNN.jsonl``
left from a previous run is sealed. New writes never resume into an old
file: a date's first segment number is (max number on disk, counting both
``.jsonl`` and ``.jsonl.gz``) + 1, so an archived number is never reused.

Legacy ``gen*/`` folders, ``archive_*.zip`` files and the old one-file-per-day
``markets/{date}.jsonl`` are ignored (not sealed, not deleted).

Usage::

    async with Walwriter(data_dir=Path("data")) as wal:
        async def on_msg(rec, parsed, conn_id):
            await wal.write(rec)
        ...
"""

from __future__ import annotations

import asyncio
import gzip
import logging
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Callable, Self

import msgspec

from src import config
from src.common.utilities import Rec

logger = logging.getLogger(__name__)

# Segment file names: 000123.jsonl (active or pending seal), 000123.jsonl.gz (sealed),
# 000123.jsonl.gz.uploaded (left by the uploader after it deletes the .gz). The marker
# counts when picking the next number, so an uploaded number is never reused
# (reuse would overwrite the earlier file at the destination).
_SEG_RE = re.compile(r"^(\d{6})\.jsonl(?:\.gz(?:\.uploaded)?)?$")

_GZIP_LEVEL = 6
_COPY_CHUNK = 1 << 20

# Stream names double as directory names under data_dir.
WAL = "wal"
MARKETS = "markets"
EVENTS = "events"
METRICS = "metrics"
_STREAMS = (WAL, MARKETS, EVENTS, METRICS)

# (stream, date), e.g. ("wal", "2026-11-08")
_Key = tuple[str, str]


@dataclass
class _Seg:
    """State of one (stream, date)'s current segment."""
    n: int                        # segment number, i.e. the NNNNNN in the file name
    fh: IO[bytes] | None = None   # open file; None after rotation until the next write
    nbytes: int = 0               # bytes in the current file, drives size-based rotation
    dirty: bool = False           # written since the last fsync


class Walwriter:
    """
    Append-only segmented JSONL writer.

    Public methods:
        write(rec)                     record one WS message
        write_market_snapshot(ts, recs) record one Gamma discovery cycle
        write_event(ts, event)         record one WS connection event
        write_metrics(ts, sample)      record one metrics sample
        flush()                        fsync all dirty files now
        stats()                        counters for monitoring
        check_now() / wait_seals()     run one maintenance pass / wait for seals (tests)

    Use as an async context manager: it starts and stops the background loops.
    """

    def __init__(
        self,
        data_dir: Path,
        fsync_interval_sec: float = config.FSYNC_INTERVAL_SEC,
        segment_max_bytes: int | None = config.SEGMENT_MAX_BYTES,
        check_interval_sec: float = config.CHECK_INTERVAL_SEC,
        seal_on_exit: bool = True,
        day_close_grace_sec: float = config.DAY_CLOSE_GRACE_SEC,
        clock_ns: Callable[[], int] | None = None,
    ) -> None:
        if fsync_interval_sec <= 0:
            raise ValueError("fsync_interval_sec must be > 0")
        if segment_max_bytes is not None and segment_max_bytes <= 0:
            raise ValueError("segment_max_bytes must be > 0 or None")
        if check_interval_sec <= 0:
            raise ValueError("check_interval_sec must be > 0")
        if day_close_grace_sec < 0:
            raise ValueError("day_close_grace_sec must be >= 0")

        self._data_dir = Path(data_dir)
        self._fsync_interval = fsync_interval_sec
        self._segment_max_bytes = segment_max_bytes
        self._check_interval = check_interval_sec
        # Seal active segments on clean exit. Needed wherever local disk does
        # not outlive the process (e.g. a Databricks serverless job): there is
        # no "next start" to seal them. False keeps them as .jsonl until the
        # next start.
        self._seal_on_exit = seal_on_exit
        self._day_close_grace_ns = int(day_close_grace_sec * 1e9)
        # Wall clock in ns; injectable so tests can cross midnight.
        self._clock_ns = clock_ns or time.time_ns

        self._encoder = msgspec.json.Encoder()

        # Background tasks, started in __aenter__.
        self._fsync_task: asyncio.Task | None = None
        self._check_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        # Serialises all file access. Held only for in-memory writes,
        # fsync and close; compression runs outside it.
        self._lock = asyncio.Lock()

        # (stream, date) -> its current segment. Usually one entry per stream;
        # two around UTC midnight while late records for the previous day arrive.
        self._segs: dict[_Key, _Seg] = {}

        # Seal tasks, kept so __aexit__ can wait for them.
        self._seal_tasks: set[asyncio.Task] = set()
        # Files currently being sealed, so one file is never sealed twice.
        self._sealing: set[Path] = set()

        # Counters
        self._n_messages = 0
        self._n_bytes = 0
        self._n_no_route = 0          # messages without asset_ids (still written)
        self._n_market_records = 0
        self._n_market_bytes = 0
        self._n_events = 0
        self._n_event_bytes = 0
        self._n_metrics = 0
        self._n_sealed = 0
        self._n_sealed_bytes = 0      # compressed bytes produced
        self._n_torn_bytes = 0        # torn trailing bytes dropped while sealing

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        for stream in _STREAMS:
            (self._data_dir / stream).mkdir(parents=True, exist_ok=True)
        self._stop_event.clear()

        self._cleanup_orphan_tmps()
        # Seal whatever the previous run left behind right away.
        self._seal_dangling_segments()

        self._fsync_task = asyncio.create_task(self._fsync_loop(), name="wal-fsync-loop")
        # Always runs: besides size rotation it retries failed seals.
        self._check_task = asyncio.create_task(self._check_loop(), name="wal-check-loop")
        logger.info(
            "WAL writer started (data_dir=%s, segment_max=%s)",
            self._data_dir,
            f"{self._segment_max_bytes / 1e9:.2f}GB" if self._segment_max_bytes else "unlimited",
        )
        return self

    async def __aexit__(self, *_exc) -> None:
        self._stop_event.set()
        for attr in ("_fsync_task", "_check_task"):
            t = getattr(self, attr)
            if t is not None:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                setattr(self, attr, None)

        # Let in-flight seals finish; cancelling would only leave .gz.tmp files.
        if self._seal_tasks:
            logger.info("WAL: waiting for %d in-flight seals", len(self._seal_tasks))
            await self.wait_seals()

        async with self._lock:
            for key, seg in self._segs.items():
                if seg.fh is not None:
                    self._close_fh(seg.fh, "/".join(key))
                    seg.fh = None
            self._segs.clear()

        if self._seal_on_exit:
            # Nothing is active any more, so this seals every NNNNNN.jsonl:
            # the segments just closed plus any earlier seal that failed.
            self._seal_dangling_segments()
            if self._seal_tasks:
                logger.info("WAL: sealing %d segments on exit", len(self._seal_tasks))
                await self.wait_seals()
        # Otherwise active segments stay as .jsonl and are sealed on the next start.

        logger.info(
            "WAL writer stopped (msgs=%d, bytes=%d, no_route=%d, sealed=%d, "
            "market_records=%d, market_bytes=%d, events=%d, metrics=%d)",
            self._n_messages, self._n_bytes, self._n_no_route, self._n_sealed,
            self._n_market_records, self._n_market_bytes, self._n_events, self._n_metrics,
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    async def write(self, rec: Rec) -> None:
        """
        Persist one WS message.

        rec.ts_recv_ns: when we received it (ns since epoch); picks the date.
        rec.asset_ids:  assets it mentions. May be empty: still written,
                        counted in ``no_route``.
        rec.raw:        message text exactly as received, embedded as a
                        string field (no parse / re-serialise).

        On-disk line = the three Rec fields + ``ts_recv_iso`` (human-readable
        only; the reader's Rec decoder ignores it).
        """
        # Everything that doesn't touch files happens outside the lock.
        date_str = self._date_str_from_ns(rec.ts_recv_ns)
        wrapper = {
            "ts_recv_ns": rec.ts_recv_ns,
            "ts_recv_iso": self._iso_from_ns(rec.ts_recv_ns),
            "asset_ids": rec.asset_ids,
            "raw": rec.raw,
        }
        line = self._encoder.encode(wrapper) + b"\n"

        async with self._lock:
            self._append_locked((WAL, date_str), line)
            self._n_messages += 1
            self._n_bytes += len(line)
            if not rec.asset_ids:
                self._n_no_route += 1

    async def write_market_snapshot(self, ts_recv_ns: int, records: list[dict]) -> None:
        """
        Persist one Gamma discovery cycle: one line per record.

        Stored in its own stream, ``{data_dir}/markets/{date}/NNNNNN.jsonl``,
        because the records have a different shape (Gamma dicts, not WS text).
        Rotation and sealing work exactly as for the WS stream.

        Each line is ``{"ts_recv_ns": <int>, **record}``. The writer does not
        look inside ``record``; the orchestrator writes
        ``{"market_id", "source", "rank", "sub": {...}, "raw": <Gamma dict>}``.
        The timestamp is shared by all records of a cycle so downstream code
        can group "what Gamma said, and what we did, at moment T".
        """
        if not records:
            return
        date_str = self._date_str_from_ns(ts_recv_ns)
        lines = b"".join(
            self._encoder.encode({"ts_recv_ns": ts_recv_ns, **r}) + b"\n"
            for r in records
        )
        async with self._lock:
            self._append_locked((MARKETS, date_str), lines)
            self._n_market_records += len(records)
            self._n_market_bytes += len(lines)

    async def write_event(self, ts_ns: int, event: dict) -> None:
        """
        Persist one WS connection event to ``{data_dir}/events/{date}/NNNNNN.jsonl``.

        Each line is ``{"ts_ns": <int>, **event}``; the orchestrator writes
        ``{"conn_id", "event", "extra"}``. Events are rare (connect,
        disconnect, ...), so this is not on the hot path.
        """
        date_str = self._date_str_from_ns(ts_ns)
        line = self._encoder.encode({"ts_ns": ts_ns, **event}) + b"\n"
        async with self._lock:
            self._append_locked((EVENTS, date_str), line)
            self._n_events += 1
            self._n_event_bytes += len(line)

    async def write_metrics(self, ts_ns: int, sample: dict) -> None:
        """
        Persist one metrics sample to ``{data_dir}/metrics/{date}/NNNNNN.jsonl``.

        Each line is ``{"ts_ns": <int>, **sample}``; the orchestrator writes
        one sample per second. Observability only, not on the hot path.
        """
        date_str = self._date_str_from_ns(ts_ns)
        line = self._encoder.encode({"ts_ns": ts_ns, **sample}) + b"\n"
        async with self._lock:
            self._append_locked((METRICS, date_str), line)
            self._n_metrics += 1

    async def flush(self) -> None:
        """fsync every dirty file now."""
        async with self._lock:
            self._flush_dirty_locked()

    # ------------------------------------------------------------------
    # Segment files
    # ------------------------------------------------------------------

    def _segment_path(self, key: _Key, n: int) -> Path:
        stream, date_str = key
        return self._data_dir / stream / date_str / f"{n:06d}.jsonl"

    def _next_seg_number(self, key: _Key) -> int:
        """Highest segment number on disk for (stream, date), .jsonl or .jsonl.gz, + 1; 0 if none."""
        d = self._data_dir / key[0] / key[1]
        if not d.is_dir():
            return 0
        nums = [int(m.group(1)) for p in d.iterdir() if (m := _SEG_RE.match(p.name))]
        return max(nums) + 1 if nums else 0

    def _active_seg_locked(self, key: _Key) -> _Seg:
        """
        The segment currently written for (stream, date), opening its file
        if needed. Caller must hold ``self._lock``.

        First write for a key: number = _next_seg_number(). After a rotation
        ``fh`` is None and ``n`` was already bumped, so this opens the next file.
        """
        seg = self._segs.get(key)
        if seg is None:
            seg = self._segs[key] = _Seg(n=self._next_seg_number(key))
        if seg.fh is None:
            path = self._segment_path(key, seg.n)
            path.parent.mkdir(parents=True, exist_ok=True)
            # Binary append: no newline translation, takes msgspec bytes directly.
            seg.fh = path.open("ab")
            seg.nbytes = os.fstat(seg.fh.fileno()).st_size
            seg.dirty = False
            logger.info("WAL: opened %s", path)
        return seg

    def _append_locked(self, key: _Key, data: bytes) -> None:
        """Append to the active segment of (stream, date). Caller must hold ``self._lock``."""
        seg = self._active_seg_locked(key)
        seg.fh.write(data)
        seg.nbytes += len(data)
        seg.dirty = True

    def _flush_dirty_locked(self) -> None:
        """flush + fsync every file written since the last call."""
        for key, seg in self._segs.items():
            if seg.fh is None or not seg.dirty:
                continue
            try:
                seg.fh.flush()
                os.fsync(seg.fh.fileno())
            except Exception:
                logger.exception("WAL: error flushing %s", "/".join(key))
            seg.dirty = False

    @staticmethod
    def _close_fh(fh: IO[bytes], what: str) -> None:
        try:
            fh.flush()
            os.fsync(fh.fileno())
        except Exception:
            logger.exception("WAL: error during final fsync of %s", what)
        try:
            fh.close()
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _fsync_loop(self) -> None:
        while not await self._sleep_or_stop(self._fsync_interval):
            async with self._lock:
                self._flush_dirty_locked()

    async def _check_loop(self) -> None:
        while not await self._sleep_or_stop(self._check_interval):
            try:
                await self.check_now()
            except Exception:
                logger.exception("WAL: check loop error")

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Wait ``seconds``; return True if stop was requested meanwhile."""
        try:
            await asyncio.wait_for(self._stop_event.wait(), seconds)
            return True
        except asyncio.TimeoutError:
            return False

    async def check_now(self) -> None:
        """One maintenance pass: rotate oversized segments, close past days, seal closed ones."""
        await self._maybe_rotate_oversized()
        await self._close_past_days()
        self._seal_dangling_segments()

    async def wait_seals(self) -> None:
        """Wait until every in-flight seal has finished."""
        # Wait only for tasks that are not done yet. A finished task stays in
        # the set until its discard callback runs on the next loop iteration;
        # waiting on it again would not yield (gather of done futures returns
        # an already-done future since Python 3.12), so the callback would
        # never run and this loop would spin forever.
        while pending := [t for t in self._seal_tasks if not t.done()]:
            await asyncio.wait(pending)

    # ------------------------------------------------------------------
    # Rotation and sealing
    # ------------------------------------------------------------------

    async def _maybe_rotate_oversized(self) -> None:
        if self._segment_max_bytes is None:
            return
        for key, seg in list(self._segs.items()):
            if seg.fh is not None and seg.nbytes >= self._segment_max_bytes:
                closed = await self._rotate_segment(key)
                if closed is not None:
                    self._spawn_seal(closed)

    async def _rotate_segment(self, key: _Key) -> Path | None:
        """Close the current segment of (stream, date) and bump its number. Returns the closed file."""
        async with self._lock:
            seg = self._segs.get(key)
            if seg is None or seg.fh is None:
                return None
            closed = self._segment_path(key, seg.n)
            self._close_fh(seg.fh, str(closed))
            seg.fh = None
            seg.n += 1
            seg.nbytes = 0
            seg.dirty = False
            next_n = seg.n
        logger.info("WAL: rotated %s, next segment %06d", closed, next_n)
        return closed

    async def _close_past_days(self) -> None:
        """
        Close the active segments of every date before today (UTC, minus the
        grace period). The key is dropped, so a late write for that date picks
        the next number from disk and never reopens a file being sealed. The
        closed files are sealed by the ``_seal_dangling_segments`` call that follows.
        """
        cutoff = self._date_str_from_ns(self._clock_ns() - self._day_close_grace_ns)
        closed: list[Path] = []
        # Pop and close under one lock hold: otherwise a write in between
        # could reopen the key and its file handle would be lost.
        async with self._lock:
            for key in [k for k in self._segs if k[1] < cutoff]:   # YYYY-MM-DD sorts as text
                seg = self._segs.pop(key)
                if seg.fh is not None:
                    path = self._segment_path(key, seg.n)
                    self._close_fh(seg.fh, str(path))
                    closed.append(path)
        for p in closed:
            logger.info("WAL: day over, closed %s", p)

    def _cleanup_orphan_tmps(self) -> None:
        """Remove .gz.tmp files left by a crash mid-seal. Their .jsonl is intact and gets resealed."""
        for stream in _STREAMS:
            for tmp in (self._data_dir / stream).glob("*/*.jsonl.gz.tmp"):
                try:
                    tmp.unlink()
                    logger.warning("WAL: removed orphan tmp %s", tmp)
                except OSError:
                    pass

    def _seal_dangling_segments(self) -> None:
        """
        Seal every NNNNNN.jsonl that is not an active segment: left over from
        a previous run, closed by a rotation, or a seal that failed earlier.
        """
        active = {
            self._segment_path(k, s.n) for k, s in self._segs.items() if s.fh is not None
        }
        # Exactly {stream}/{date}/NNNNNN.jsonl: legacy gen*/ folders and the old
        # markets/{date}.jsonl files never match.
        for stream in _STREAMS:
            for p in sorted((self._data_dir / stream).glob("*/*.jsonl")):
                if _SEG_RE.match(p.name) and p not in active:
                    self._spawn_seal(p)

    def _spawn_seal(self, src: Path) -> None:
        if src in self._sealing:
            return
        # Mark before the task runs, so the next scan cannot schedule it again.
        self._sealing.add(src)
        task = asyncio.create_task(self._seal_segment(src), name=f"wal-seal-{src}")
        self._seal_tasks.add(task)
        task.add_done_callback(self._seal_tasks.discard)

    async def _seal_segment(self, src: Path) -> None:
        try:
            result = await asyncio.to_thread(_seal_file, src)
        except Exception:
            logger.exception("WAL: seal failed for %s (retried next pass)", src)
            return
        finally:
            self._sealing.discard(src)
        if result is None:
            return
        raw_size, gz_size, torn = result
        self._n_sealed += 1
        self._n_sealed_bytes += gz_size
        self._n_torn_bytes += torn
        if torn:
            logger.warning("WAL: %s had %d torn trailing bytes, dropped", src, torn)
        logger.info(
            "WAL: sealed %s (%.1f MB -> %.1f MB)", src, raw_size / 1e6, gz_size / 1e6,
        )

    # ------------------------------------------------------------------
    # Helpers and stats
    # ------------------------------------------------------------------

    @staticmethod
    def _date_str_from_ns(ts_ns: int) -> str:
        # Always UTC so dates are stable across machines and timezones.
        return datetime.fromtimestamp(ts_ns / 1_000_000_000, tz=timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _iso_from_ns(ts_ns: int) -> str:
        # ISO 8601 with nanoseconds, e.g. "2026-04-27T15:23:01.123456789Z".
        sec, ns_remainder = divmod(ts_ns, 1_000_000_000)
        dt = datetime.fromtimestamp(sec, tz=timezone.utc)
        return f"{dt.strftime('%Y-%m-%dT%H:%M:%S')}.{ns_remainder:09d}Z"

    def stats(self) -> dict:
        """Snapshot of writer state. No lock: eventually consistent."""
        return {
            "messages": self._n_messages,
            "bytes": self._n_bytes,
            "no_route": self._n_no_route,
            "market_records": self._n_market_records,
            "market_bytes": self._n_market_bytes,
            "events": self._n_events,
            "event_bytes": self._n_event_bytes,
            "metrics": self._n_metrics,
            "sealed": self._n_sealed,
            "sealed_bytes": self._n_sealed_bytes,
            "torn_bytes": self._n_torn_bytes,
            "current_segment": {"/".join(k): s.n for k, s in self._segs.items()},
            "segment_bytes": {"/".join(k): s.nbytes for k, s in self._segs.items()},
            "sealing": len(self._sealing),
            "open_handles": sum(s.fh is not None for s in self._segs.values()),
            "data_dir": str(self._data_dir),
        }

    async def stats_loop(self) -> None:
        while True:
            await asyncio.sleep(5)
            logger.info("wal stats: %s", self.stats())


# ----------------------------------------------------------------------
# Sealing (runs in a worker thread, touches no writer state)
# ----------------------------------------------------------------------

def _seal_file(src: Path) -> tuple[int, int, int] | None:
    """
    Compress ``NNNNNN.jsonl`` into ``NNNNNN.jsonl.gz`` and delete the source.

    Order matters for crash safety: data fsynced, then atomic rename, then
    the directory fsynced, and only then the source deleted. At every point
    either the full .gz exists or the source is still there.

    Returns (raw bytes, gz bytes, torn bytes dropped), or None if the target
    already exists (the source is then left untouched).
    """
    final = src.with_name(src.name + ".gz")
    tmp = src.with_name(src.name + ".gz.tmp")
    if final.exists():
        # Numbering should make this impossible. Never delete data to resolve it.
        logger.error("WAL: %s already exists, leaving %s in place", final, src)
        return None

    size = src.stat().st_size
    valid = _bytes_through_last_newline(src, size)
    try:
        with open(src, "rb") as fin, open(tmp, "wb") as fout:
            # mtime=0: no timestamp in the header, same input -> same bytes.
            with gzip.GzipFile(fileobj=fout, mode="wb", compresslevel=_GZIP_LEVEL, mtime=0) as gz:
                remaining = valid
                while remaining:
                    chunk = fin.read(min(remaining, _COPY_CHUNK))
                    if not chunk:
                        break
                    gz.write(chunk)
                    remaining -= len(chunk)
            fout.flush()
            os.fsync(fout.fileno())
            # Size taken before the rename: once final exists the uploader may
            # upload and delete it at any moment.
            gz_size = os.fstat(fout.fileno()).st_size
        os.replace(tmp, final)
        _fsync_dir(src.parent)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    src.unlink()
    _fsync_dir(src.parent)
    return size, gz_size, size - valid


def _bytes_through_last_newline(path: Path, size: int) -> int:
    """Length of the file up to and including its last newline (drops a torn tail)."""
    with open(path, "rb") as f:
        pos = size
        while pos > 0:
            step = min(1 << 16, pos)
            pos -= step
            f.seek(pos)
            i = f.read(step).rfind(b"\n")
            if i != -1:
                return pos + i + 1
    return 0


def _fsync_dir(d: Path) -> None:
    """Persist directory entries (renames, deletes) to disk."""
    fd = os.open(d, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
