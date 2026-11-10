"""
Ship sealed WAL segments from the local data dir to a destination directory.

Local side (written by ``Walwriter``)::

    {data_dir}/wal/{date}/000003.jsonl.gz        sealed, waiting for upload
    {data_dir}/markets/{date}/000001.jsonl.gz
    {data_dir}/events/{date}/000000.jsonl.gz
    {data_dir}/wal/{date}/000002.jsonl.gz.uploaded   marker left after upload

Destination (``dest_root``)::

    ws/{date}/{source_id}-{date}-000003.jsonl.gz
    markets/{date}/{source_id}-{date}-000001.jsonl.gz
    events/{date}/{source_id}-{date}-000000.jsonl.gz
    _staging/                                     in-flight copies, normally empty

``dest_root`` is a UC volume path (``/Volumes/...``, only on Databricks
compute) or any local directory (local end-to-end runs and tests).

Two pieces:

- ``put_file``: puts ONE file at ``dest_root/rel`` atomically (copy into
  ``_staging``, check size, rename). Readers such as Auto Loader never see a
  partial file. Raises on failure; no retries, no state.
- ``Uploader``: decides WHICH files to send and WHEN, names them, marks them
  done, deletes local copies, retries failures on the next pass and drains
  what is left on exit. Calls ``put`` once per file.

``source_id`` (``{host_id}-{8 hex}``) is created once per data dir and kept in
``{data_dir}/.source_id``. A persistent data dir keeps its id, so a re-upload
after a crash hits the same name with the same bytes (idempotent). A fresh
data dir (every Databricks job run) gets a new id, so runs never collide even
though each starts numbering at 000000.

Nest it OUTSIDE the writer so the segments sealed on writer exit get uploaded::

    async with Uploader(data_dir, put, host_id="dbx") as up:
        async with Walwriter(data_dir) as wal:
            ...
"""

from __future__ import annotations

import asyncio
import logging
import os
import re
import secrets
import shutil
import time
from collections.abc import Callable
from pathlib import Path, PurePosixPath
from typing import Self

from src import config

logger = logging.getLogger(__name__)

# put(local_file, remote_rel_path). Sync; the uploader runs it in a worker thread.
PutFn = Callable[[Path, str], None]

_SEALED_RE = re.compile(r"^(\d{6})\.jsonl\.gz$")
_MARKER = ".uploaded"
# local stream dir -> destination dir
_REMOTE_STREAM = {"wal": "ws", "markets": "markets", "events": "events"}


# ----------------------------------------------------------------------
# put_file
# ----------------------------------------------------------------------

# In-flight copies go here, next to (not under) ws/ markets/ events/, so a
# reader watching those folders never sees a partial file.
STAGING_DIR = "_staging"


def check_rel(rel: str) -> None:
    """Reject remote paths that are empty, absolute or contain '..'."""
    p = PurePosixPath(rel)
    if not rel or p.is_absolute() or ".." in p.parts:
        raise ValueError(f"bad remote path: {rel!r}")


def staging_name(rel: str) -> str:
    # Derived from rel and deterministic: ws/ and markets/ files with the same
    # basename don't collide, and a .tmp left by a crash is overwritten by the
    # retry instead of piling up.
    return rel.replace("/", "__") + ".tmp"


def put_file(local: Path, rel: str, *, dest_root: Path) -> None:
    """
    Put ``local`` at ``dest_root/rel`` atomically: copy to
    ``dest_root/_staging/``, check the size, then rename into place.

    An existing target is overwritten: for a given source the same name always
    carries the same bytes, so re-uploads are safe. Raises on failure and
    leaves no partial file behind; the caller decides whether to retry.
    """
    check_rel(rel)
    dest_root = Path(dest_root)
    final = dest_root / rel
    tmp = dest_root / STAGING_DIR / staging_name(rel)
    tmp.parent.mkdir(parents=True, exist_ok=True)
    final.parent.mkdir(parents=True, exist_ok=True)
    try:
        shutil.copyfile(local, tmp)  # sequential write: supported on UC volumes
        if tmp.stat().st_size != local.stat().st_size:
            raise OSError(f"size mismatch copying {local} -> {tmp}")
        os.replace(tmp, final)  # atomic publish
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


# ----------------------------------------------------------------------
# Uploader
# ----------------------------------------------------------------------

class Uploader:
    """
    Upload sealed ``{wal,markets,events}/{date}/NNNNNN.jsonl.gz`` files with ``put``.

    - Only complete ``.jsonl.gz`` files are picked up. The writer creates them
      by rename, so they are never partial; ``.jsonl`` and ``.tmp`` are ignored.
    - After a successful put: create ``NNNNNN.jsonl.gz.uploaded``, then delete
      the ``.gz`` unless ``keep_local``. The marker stays so the writer never
      reuses that number.
    - A failed put stops the pass; the next pass starts over. Files without a
      marker are always retried, and retries are idempotent.
    - On exit: stop the loop, then keep uploading until nothing is pending or
      ``exit_timeout_sec`` runs out.
    """

    def __init__(
        self,
        data_dir: Path,
        put: PutFn,
        host_id: str,
        *,
        scan_interval_sec: float = config.UPLOAD_SCAN_INTERVAL_SEC,
        keep_local: bool = False,
        exit_timeout_sec: float = config.UPLOAD_EXIT_TIMEOUT_SEC,
    ) -> None:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", host_id):
            raise ValueError("host_id must match [A-Za-z0-9_-]+")
        if scan_interval_sec <= 0:
            raise ValueError("scan_interval_sec must be > 0")
        if exit_timeout_sec < 0:
            raise ValueError("exit_timeout_sec must be >= 0")

        self._data_dir = Path(data_dir)
        self._put = put
        self._host_id = host_id
        self._scan_interval = scan_interval_sec
        self._keep_local = keep_local
        self._exit_timeout = exit_timeout_sec

        self._source_id: str | None = None   # set in __aenter__
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        # One pass at a time: the background loop and the exit drain must not
        # upload the same file concurrently.
        self._scan_lock = asyncio.Lock()

        self._n_uploaded = 0
        self._n_bytes = 0
        self._n_failures = 0
        self._last_error: str | None = None

    @property
    def source_id(self) -> str:
        if self._source_id is None:
            raise RuntimeError("Uploader not started")
        return self._source_id

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def __aenter__(self) -> Self:
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._source_id = self._load_or_create_source_id()
        self._stop.clear()
        self._task = asyncio.create_task(self._loop(), name="uploader-loop")
        logger.info("uploader started (data_dir=%s, source_id=%s)", self._data_dir, self._source_id)
        return self

    async def __aexit__(self, *_exc) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

        # Final drain. The writer has exited by now (it is nested inside), so
        # its last sealed segments are on disk.
        deadline = time.monotonic() + self._exit_timeout
        while True:
            await self.scan_once()
            left = self._pending()
            if not left:
                break
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # On Databricks the local disk is about to disappear: these are lost.
                logger.error(
                    "uploader: %d files NOT uploaded at exit (last error: %s): %s",
                    len(left), self._last_error, [str(p) for p in left[:10]],
                )
                break
            await asyncio.sleep(min(5.0, remaining))
        logger.info("uploader stopped: %s", self.stats())

    async def _loop(self) -> None:
        while True:
            try:
                await self.scan_once()
            except Exception:
                logger.exception("uploader: scan failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self._scan_interval)
                return
            except asyncio.TimeoutError:
                pass

    # ------------------------------------------------------------------
    # One pass
    # ------------------------------------------------------------------

    async def scan_once(self) -> int:
        """
        Upload every pending file, oldest first. Stops at the first failure
        (the destination is probably down; the next pass retries). Returns
        how many files were uploaded.
        """
        async with self._scan_lock:
            n = 0
            for p in self._pending():
                rel = self.remote_rel(p)
                try:
                    size = p.stat().st_size
                    await asyncio.to_thread(self._put, p, rel)
                except Exception as e:
                    self._n_failures += 1
                    self._last_error = f"{p.name}: {e!r}"
                    logger.warning("uploader: put %s -> %s failed: %r (retry next pass)", p, rel, e)
                    break
                self._mark_done(p)
                self._n_uploaded += 1
                self._n_bytes += size
                n += 1
                logger.info("uploader: %s -> %s (%.1f MB)", p, rel, size / 1e6)
            return n

    def _pending(self) -> list[Path]:
        """Sealed files without an upload marker, sorted by (stream, date, number)."""
        out: list[Path] = []
        for stream in _REMOTE_STREAM:
            for p in (self._data_dir / stream).glob("*/*.jsonl.gz"):
                if _SEALED_RE.match(p.name) and not p.with_name(p.name + _MARKER).exists():
                    out.append(p)
        return sorted(out)

    def remote_rel(self, p: Path) -> str:
        """Destination path for a local sealed segment, relative to dest_root."""
        stream, date = p.parent.parent.name, p.parent.name
        m = _SEALED_RE.match(p.name)
        if stream not in _REMOTE_STREAM or m is None:
            raise ValueError(f"not a sealed segment: {p}")
        return f"{_REMOTE_STREAM[stream]}/{date}/{self.source_id}-{date}-{m.group(1)}.jsonl.gz"

    def _mark_done(self, p: Path) -> None:
        # Marker first, then delete. A crash in between leaves a marked .gz,
        # which is skipped (not re-uploaded).
        p.with_name(p.name + _MARKER).touch()
        if not self._keep_local:
            p.unlink(missing_ok=True)

    def _load_or_create_source_id(self) -> str:
        f = self._data_dir / ".source_id"
        if f.exists():
            sid = f.read_text().strip()
            if sid:
                return sid
        sid = f"{self._host_id}-{secrets.token_hex(4)}"
        tmp = f.with_name(".source_id.tmp")
        tmp.write_text(sid)
        os.replace(tmp, f)
        return sid

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def stats(self) -> dict:
        return {
            "source_id": self._source_id,
            "uploaded": self._n_uploaded,
            "uploaded_bytes": self._n_bytes,
            "failures": self._n_failures,
            "pending": len(self._pending()),
            "last_error": self._last_error,
        }
