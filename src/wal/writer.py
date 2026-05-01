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
from shutil import rmtree as shutil_rmtree
from datetime import datetime, timezone
from pathlib import Path
from typing import IO, Self

import msgspec

logger = logging.getLogger(__name__)

# How often to fsync all open files. fsync is expensive (1-10ms typically),
# so we batch — at the cost of losing up to this many seconds of data on
# crash. 1s is the standard Postgres-style tradeoff.
DEFAULT_FSYNC_INTERVAL_SEC = 1.0

# Archival: when the current generation's folder exceeds this size, rotate.
# "Rotate" = (1) close all current-gen handles, bump generation counter,
# new writes go into a new gen{N+1}/ subdir, and (2) background-zip the
# now-frozen previous gen folder into archive_NNN.zip.
DEFAULT_ARCHIVE_THRESHOLD_BYTES = 3 * 1024 * 1024 * 1024  # 8 GiB
# How often the background task checks folder sizes. 30s is a balance:
# it caps how much over-threshold the folder gets between checks (at
# ~3 MB/s ingest, ~90 MB), and it's cheap (one `du` of a directory).
DEFAULT_ARCHIVE_CHECK_INTERVAL_SEC = 30.0

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
        archive_threshold_bytes: int | None = DEFAULT_ARCHIVE_THRESHOLD_BYTES,
        archive_check_interval_sec: float = DEFAULT_ARCHIVE_CHECK_INTERVAL_SEC,
    ) -> None:
        if fsync_interval_sec <= 0:
            raise ValueError("fsync_interval_sec must be > 0")
        if shard_prefix_len < 1 or shard_prefix_len > 4:
            raise ValueError("shard_prefix_len must be 1..4")
        if archive_threshold_bytes is not None and archive_threshold_bytes <= 0:
            raise ValueError("archive_threshold_bytes must be > 0 or None")
        if archive_check_interval_sec <= 0:
            raise ValueError("archive_check_interval_sec must be > 0")

        self._root = Path(data_dir) / "wal"
        self._fsync_interval = fsync_interval_sec
        self._shard_prefix_len = shard_prefix_len
        self._archive_threshold = archive_threshold_bytes
        self._archive_check_interval = archive_check_interval_sec

        self._encoder = msgspec.json.Encoder()

        # Background tasks; started in __aenter__.
        self._fsync_task: asyncio.Task | None = None
        self._archive_task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

        # All public methods serialise through this. Held only briefly.
        self._lock = asyncio.Lock()

        # (date_str, prefix) -> open file handle (binary append mode)
        # Path is wal/{date}/gen{N}/{prefix}.jsonl where N is the current
        # generation for that date. Generation starts at 0; bumps when
        # the folder exceeds the archive threshold.
        self._handles: dict[tuple[str, str], IO[bytes]] = {}
        # Same key — set when handle was last written to. Drives cleanup
        # for files that are no longer being written (e.g. yesterday's).
        self._handle_dirty: dict[tuple[str, str], bool] = {}

        # date_str -> current generation number. New entries default to 0.
        # Only mutated under self._lock during rotation.
        self._current_gen: dict[str, int] = {}

        # Background compress jobs we've kicked off — kept so __aexit__
        # waits for in-flight ones rather than abandoning .tmp files.
        self._compress_tasks: set[asyncio.Task] = set()

        # Counters for stats() / metrics integration
        self._n_messages = 0
        self._n_bytes = 0
        # Messages we couldn't route (no asset_ids extracted) go to a
        # special "unknown" shard. Tracked separately for visibility.
        self._n_no_route = 0
        # Gamma discovery snapshot counters — separate stream, see
        # write_market_snapshot().
        self._n_market_records = 0
        self._n_market_bytes = 0
        # Archive counters: how many rotations triggered, total bytes
        # produced. Useful for ops monitoring.
        self._n_archives_created = 0
        self._n_archive_bytes = 0

        # Cache of the per-date markets file handle. Keyed by date only
        # (no shard) since the markets stream isn't sharded. Kept in a
        # separate dict from self._handles because the keying shape
        # differs and mixing them would force a tagged-key scheme.
        self._market_handles: dict[str, IO[bytes]] = {}




    async def __aenter__(self) -> Self:
        self._root.mkdir(parents=True, exist_ok=True)
        self._stop_event.clear()

        # Recover state from any prior run:
        #  (1) discover existing gen{N} dirs per date and resume from max+1
        #      to avoid overwriting a folder that's already being archived.
        #  (2) clean up orphan archive_*.zip.tmp files (crashed compressions).
        self._recover_generations()
        self._cleanup_orphan_tmps()

        self._fsync_task = asyncio.create_task(
            self._fsync_loop(), name="wal-fsync-loop"
        )
        if self._archive_threshold is not None:
            self._archive_task = asyncio.create_task(
                self._archive_loop(), name="wal-archive-loop"
            )
        logger.info(
            "WAL writer started (root=%s, archive_threshold=%s)",
            self._root,
            f"{self._archive_threshold/1e9:.1f}GB" if self._archive_threshold else "disabled",
        )
        return self


    async def __aexit__(self, *_exc) -> None:
        # Stop the background tasks
        self._stop_event.set()
        for attr in ("_fsync_task", "_archive_task"):
            t = getattr(self, attr)
            if t is not None:
                try:
                    await t
                except asyncio.CancelledError:
                    pass
                setattr(self, attr, None)

        # Wait for any in-flight compress jobs. Don't cancel them — half-
        # compressed .zip.tmp files are bad. Let them finish naturally.
        if self._compress_tasks:
            logger.info(
                "WAL: waiting for %d in-flight compressions to finish",
                len(self._compress_tasks),
            )
            await asyncio.gather(*self._compress_tasks, return_exceptions=True)
            self._compress_tasks.clear()

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
            for date_str, fh in list(self._market_handles.items()):
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                except Exception:
                    logger.exception("WAL: error during final fsync of markets/%s", date_str)
                try:
                    fh.close()
                except Exception:
                    pass
            self._handles.clear()
            self._market_handles.clear()
            self._handle_dirty.clear()
        logger.info(
            "WAL writer stopped (msgs=%d, bytes=%d, no_route=%d, market_records=%d, market_bytes=%d)",
            self._n_messages, self._n_bytes, self._n_no_route,
            self._n_market_records, self._n_market_bytes,
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

    async def write_market_snapshot(
        self,
        ts_ns: int,
        raws: list[dict],
    ) -> None:
        """
        Persist a Gamma discovery snapshot — every raw market record from
        one cycle, one JSON object per line.

        Storage layout is **separate** from the WS WAL tree:
            {data_dir}/markets/{YYYY-MM-DD}.jsonl

        That deliberate split exists because:
        - the records have a different shape (Gamma market dicts, not WS
          message strings)
        - readers want different access patterns (full scan over time vs.
          shard lookup by asset_id)
        - mixing them in the WS WAL would break the asset_id-prefix
          shard contract.

        We still piggyback on this writer's lock + handle cache + fsync
        loop, so all WAL I/O is centralized.

        Each line:
            {"ts_ns": <int>, "raw": <market dict>}

        ts_ns is shared across every record from the same cycle so
        downstream code can group "what Gamma told us at moment T".
        """
        if not raws:
            return

        date_str = self._date_str_from_ns(ts_ns)
        # Encode the whole batch outside the lock — only file IO runs
        # under it, same as write().
        lines = b"".join(
            self._encoder.encode({"ts_ns": ts_ns, "raw": r}) + b"\n"
            for r in raws
        )

        async with self._lock:
            fh = self._get_market_handle_locked(date_str)
            fh.write(lines)
            self._handle_dirty[("markets", date_str)] = True
            self._n_market_records += len(raws)
            self._n_market_bytes += len(lines)

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
        gen = self._current_gen.get(date_str, 0)
        return self._root / date_str / f"gen{gen}" / f"{prefix}.jsonl"


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

        Path layout:  wal/{date}/gen{N}/{prefix}.jsonl

        The gen{N} subdir is the rotation unit — when the date folder
        crosses the archive threshold, we close all handles, bump
        _current_gen[date] to N+1, and a background task zips the now-
        frozen gen{N}/ folder into archive_NNN.zip.
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

    def _get_market_handle_locked(self, date_str: str) -> IO[bytes]:
        """
        Get-or-create the markets snapshot file handle for `date_str`.

        Caller must hold self._lock. Lives in a sibling tree to the WAL
        shards: {root.parent}/markets/{date}.jsonl
        """
        fh = self._market_handles.get(date_str)
        if fh is not None:
            return fh

        # self._root is .../wal — markets is its sibling.
        path = self._root.parent / "markets" / f"{date_str}.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        fh = path.open("ab")
        self._market_handles[date_str] = fh
        self._handle_dirty[("markets", date_str)] = False
        logger.info("WAL: opened markets snapshot %s", path)
        return fh

    def _flush_dirty_locked(self) -> None:
        """Flush + fsync any handle that was written to since last flush."""
        for key, dirty in list(self._handle_dirty.items()):
            if not dirty:
                continue
            # Two key shapes share this dirty map:
            #   ("markets", date_str)        → markets snapshot stream
            #   (date_str, prefix)           → WAL message shards
            # Look up in the right handle dict.
            if isinstance(key, tuple) and key[0] == "markets":
                fh = self._market_handles.get(key[1])
            else:
                fh = self._handles.get(key)
            if fh is None:
                continue
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


    # =====================================================================
    # Archive rotation
    #
    # When `wal/{date}/gen{N}/` total size > archive_threshold:
    #   1. (under lock, fast) close all gen{N} handles → bump
    #      _current_gen[date] to N+1. New writes immediately go to
    #      gen{N+1}/, no blocking the hot path beyond a few close()s.
    #   2. (background) zip gen{N}/ into archive_NNN.zip.tmp, atomic
    #      rename to archive_NNN.zip, then `rm -rf gen{N}/`.
    #
    # Crash safety:
    #   - If we crash before (1): nothing to do, gen{N}/ keeps growing
    #     across the threshold, next __aenter__ resumes writing to it.
    #   - If we crash between (1) and (2): gen{N}/ is intact and frozen
    #     (no handles), gen{N+1}/ is new and being written. Next start
    #     will detect gen{N}/ has no compress task and re-trigger.
    #   - If we crash mid-zip: archive_NNN.zip.tmp is orphan, gen{N}/
    #     is intact. Cleanup deletes the .tmp; next archive_loop pass
    #     re-zips gen{N}/.
    # =====================================================================

    def _recover_generations(self) -> None:
        """
        Scan wal/{date}/gen*/ to set _current_gen[date] = max(N) + 1 if any
        gen{N}/ already exists. This way, after a restart we never overwrite
        a folder that may already be archived (or in-flight).

        Special case: if gen{max}/ exists and there's NO archive_max.zip(.tmp),
        then it's still 'live' — resume into it. We only bump to max+1 when
        the existing gen{max}/ is already archived (zip exists).
        """
        if not self._root.exists():
            return
        for date_dir in self._root.iterdir():
            if not date_dir.is_dir():
                continue
            gen_nums = []
            for sub in date_dir.iterdir():
                if sub.is_dir() and sub.name.startswith("gen"):
                    try:
                        gen_nums.append(int(sub.name[3:]))
                    except ValueError:
                        continue
            if not gen_nums:
                continue
            max_gen = max(gen_nums)
            # Is gen{max} already archived?
            archive = date_dir / f"archive_{max_gen:03d}.zip"
            if archive.exists():
                # Already archived; somehow gen{max}/ wasn't cleaned up.
                # Skip past it.
                self._current_gen[date_dir.name] = max_gen + 1
            else:
                # Resume into the live gen{max}.
                self._current_gen[date_dir.name] = max_gen
            logger.info(
                "WAL: recovered date=%s, resuming at gen%d",
                date_dir.name, self._current_gen[date_dir.name],
            )

    def _cleanup_orphan_tmps(self) -> None:
        """Delete any archive_*.zip.tmp left over from a crashed compression."""
        if not self._root.exists():
            return
        for tmp in self._root.glob("*/archive_*.zip.tmp"):
            try:
                tmp.unlink()
                logger.warning("WAL: removed orphan archive tmp %s", tmp)
            except OSError:
                pass

    async def _archive_loop(self) -> None:
        """Background: every archive_check_interval, check + rotate as needed."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), self._archive_check_interval
                )
                return  # stop requested
            except asyncio.TimeoutError:
                pass

            try:
                # First pick up any unfinished work from a crash.
                self._cleanup_orphan_tmps()
                await self._maybe_rotate_oversized_dates()
                # Also: any frozen gen{N}/ without a corresponding archive
                # (i.e. we rotated handles but compress didn't run / crashed)
                # should be archived now.
                await self._archive_dangling_frozen_gens()
            except Exception:
                logger.exception("WAL: archive loop error")

    async def _maybe_rotate_oversized_dates(self) -> None:
        """
        For each (date) with at least one open handle, check the size of
        its current gen folder. If over threshold, do a fast handle-rotation
        and spawn a background compress for the now-frozen folder.
        """
        if self._archive_threshold is None or not self._root.exists():
            return

        # Collect candidates outside the lock — read-only ops.
        # Snapshot dates with open handles (the only "live" ones).
        live_dates: set[str] = {key[0] for key in self._handles.keys()}
        for date_str in live_dates:
            gen = self._current_gen.get(date_str, 0)
            gen_dir = self._root / date_str / f"gen{gen}"
            if not gen_dir.exists():
                continue
            try:
                total = sum(
                    f.stat().st_size for f in gen_dir.rglob("*") if f.is_file()
                )
            except OSError:
                continue
            if total < self._archive_threshold:
                continue

            logger.info(
                "WAL: %s/gen%d hit %.2f GB — rotating",
                date_str, gen, total / 1e9,
            )
            frozen_dir = await self._rotate_generation_for(date_str)
            if frozen_dir is not None:
                self._spawn_compress(frozen_dir)

    async def _rotate_generation_for(self, date_str: str) -> Path | None:
        """
        FAST handle rotation: close all open handles for `date_str`, bump
        its generation. Returns the now-frozen gen{N}/ path so the caller
        can spawn a background compress for it.

        Holds the lock — but the work inside is just `close()` calls
        (~microseconds each, ~ms in aggregate for 100 shards). Other
        writers will briefly block waiting for the lock, which is the
        intended back-pressure: we're rotating because the folder is
        oversized, a tiny pause is fine.
        """
        async with self._lock:
            old_gen = self._current_gen.get(date_str, 0)
            frozen_dir = self._root / date_str / f"gen{old_gen}"

            # Flush + close every open handle for this date.
            keys_to_close = [k for k in list(self._handles) if k[0] == date_str]
            for key in keys_to_close:
                fh = self._handles.pop(key)
                self._handle_dirty.pop(key, None)
                try:
                    fh.flush()
                    os.fsync(fh.fileno())
                except Exception:
                    logger.exception("WAL: error during rotate fsync %s", key)
                try:
                    fh.close()
                except Exception:
                    pass

            # Bump generation. Next call to _get_handle_locked() will open
            # files in gen{old_gen+1}/.
            self._current_gen[date_str] = old_gen + 1
            logger.info(
                "WAL: rotated %s gen%d → gen%d (%d handles closed)",
                date_str, old_gen, old_gen + 1, len(keys_to_close),
            )

        return frozen_dir if frozen_dir.exists() else None

    async def _archive_dangling_frozen_gens(self) -> None:
        """
        Find any gen{N}/ that's NOT the current generation for its date
        AND has no corresponding archive_NNN.zip — it's frozen (no
        handles writing to it) but never got compressed. Schedule it.
        """
        if not self._root.exists():
            return
        for date_dir in self._root.iterdir():
            if not date_dir.is_dir():
                continue
            current_gen = self._current_gen.get(date_dir.name, 0)
            for sub in date_dir.iterdir():
                if not sub.is_dir() or not sub.name.startswith("gen"):
                    continue
                try:
                    gen_n = int(sub.name[3:])
                except ValueError:
                    continue
                if gen_n >= current_gen:
                    continue  # current or future (shouldn't happen)
                archive = date_dir / f"archive_{gen_n:03d}.zip"
                if archive.exists():
                    continue
                # Already being compressed?
                already = any(
                    str(sub) in (t.get_name() or "") for t in self._compress_tasks
                )
                if already:
                    continue
                logger.info("WAL: scheduling missed archive for %s", sub)
                self._spawn_compress(sub)

    def _spawn_compress(self, frozen_dir: Path) -> None:
        """Kick off a background compress task; track it for shutdown."""
        task = asyncio.create_task(
            self._compress_folder(frozen_dir),
            name=f"wal-compress-{frozen_dir}",
        )
        self._compress_tasks.add(task)
        task.add_done_callback(self._compress_tasks.discard)

    async def _compress_folder(self, frozen_dir: Path) -> None:
        """
        Zip `frozen_dir` (a gen{N}/) into a sibling archive_NNN.zip,
        atomic-rename, then rm -rf the original folder.

        Done in `to_thread` because zipfile is sync + CPU-bound; we don't
        want to block the event loop for ~minutes on an 8GB compress.
        """
        try:
            gen_n = int(frozen_dir.name[3:])
        except ValueError:
            logger.error("WAL: bad frozen dir name %s", frozen_dir)
            return

        date_dir = frozen_dir.parent
        archive_final = date_dir / f"archive_{gen_n:03d}.zip"
        archive_tmp = date_dir / f"archive_{gen_n:03d}.zip.tmp"

        if archive_final.exists():
            logger.warning(
                "WAL: archive %s already exists, removing frozen dir %s",
                archive_final, frozen_dir,
            )
            await asyncio.to_thread(shutil_rmtree, frozen_dir, True)
            return

        size_before = sum(
            f.stat().st_size for f in frozen_dir.rglob("*") if f.is_file()
        )
        logger.info(
            "WAL: compressing %s (%.2f GB) → %s",
            frozen_dir, size_before / 1e9, archive_final.name,
        )

        def _do_zip() -> int | None:
            import zipfile
            try:
                # Create the .tmp; zipfile's default is ZIP_STORED — we
                # explicitly want ZIP_DEFLATED for the 7-8x ratio on JSONL.
                with zipfile.ZipFile(
                    archive_tmp, "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=6,
                ) as zf:
                    for fpath in sorted(frozen_dir.rglob("*")):
                        if not fpath.is_file():
                            continue
                        arcname = fpath.relative_to(frozen_dir)
                        zf.write(fpath, arcname)
            except Exception:
                logger.exception("WAL: zip failed for %s", frozen_dir)
                if archive_tmp.exists():
                    try: archive_tmp.unlink()
                    except OSError: pass
                return None

            # Atomic rename
            archive_tmp.rename(archive_final)
            # Remove original folder (only after the rename succeeded —
            # if rename failed, frozen folder is still our source of truth)
            shutil_rmtree(frozen_dir, True)
            return archive_final.stat().st_size

        size_after = await asyncio.to_thread(_do_zip)
        if size_after is not None:
            self._n_archives_created += 1
            self._n_archive_bytes += size_after
            ratio = size_before / size_after if size_after else 0
            logger.info(
                "WAL: archived %s (%.2f GB → %.2f GB, ratio %.1fx)",
                archive_final.name,
                size_before / 1e9, size_after / 1e9, ratio,
            )


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
            "market_records": self._n_market_records,
            "market_bytes": self._n_market_bytes,
            "archives_created": self._n_archives_created,
            "archive_bytes": self._n_archive_bytes,
            "current_gen": dict(self._current_gen),
            "compress_in_flight": len(self._compress_tasks),
            "open_handles": len(self._handles) + len(self._market_handles),
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