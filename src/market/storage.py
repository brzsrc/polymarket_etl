"""
On-disk persistence for raw Gamma market records.

Separated from `models.py` so the schema/parser layer doesn't need to know
about file IO. If you only want to parse markets in-memory, you don't need
this module.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import msgspec


class MarketsJsonlWriter:
    """
    Append-only JSONL writer for market metadata.

    Not thread-safe; intended to be called from a single asyncio task. We
    keep a single file handle open for the lifetime of the writer (one cycle
    typically writes thousands of lines, no point reopening).

    Use as a context manager so the file gets flushed and fsynced on exit.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None
        # msgspec encoder is reusable and faster than json.dumps.
        self._encoder = msgspec.json.Encoder()

    def __enter__(self) -> "MarketsJsonlWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Append mode in binary so msgspec.encode (returns bytes) writes
        # without an extra encoding step.
        self._fh = self._path.open("ab")
        return self

    def __exit__(self, *_exc) -> None:
        if self._fh is not None:
            self._fh.flush()
            # fsync ensures the kernel actually flushes to disk. For a
            # discovery cycle that runs every 5-30 min, the cost (~ms) is
            # noise. For a 60s WAL writer in Phase 3 we'll be more careful.
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None

    def write(self, ts_recv_ns: int, raw_record: dict[str, Any]) -> None:
        if self._fh is None:
            raise RuntimeError("Writer not opened (use as context manager)")
        wrapper = {
            "ts_recv_ns": ts_recv_ns,
            "raw": raw_record,
        }
        self._fh.write(self._encoder.encode(wrapper))
        self._fh.write(b"\n")
