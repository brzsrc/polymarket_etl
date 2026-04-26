"""
Market discovery: full pull + persistence of metadata.

Runs a complete sweep of ``/markets?active=true&closed=false``, filters to
tradeable binary markets, and appends the raw records to ``markets.jsonl``.

Why we persist the *raw* record (not our parsed ``Market``):

1. Forensics. If a researcher months from now wants to know e.g. what
   ``volume24hr`` was at the time we discovered the market, it's there.
2. Schema drift. Gamma adds fields. If we only saved our typed projection,
   we'd lose anything we didn't know to look for.
3. Reproducibility. Re-running our parse logic against historical raw is
   cheap and deterministic; rebuilding raw from a typed projection is not.

Each line in ``markets.jsonl`` is a small wrapper:

    {"ts_recv": "<iso8601>", "raw": {...full Gamma record...}}

The discovery cycle appends one line per market per cycle. So one market
will appear many times over the file's life — that's intentional, it gives
us the history of metadata changes (e.g. someone editing the question text,
endDate getting pushed back, etc.).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import msgspec

from .gamma_client import GammaClient, GammaError
from .market_filter import extract_token_ids, is_tradeable_binary_market
from .models import Market

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DiscoveryResult:
    """What one discovery cycle produced."""

    markets: list[Market]
    """All tradeable binary markets seen this cycle."""

    token_ids: set[str]
    """Flattened set of token_ids (2 per market)."""

    cycle_started_at: datetime
    cycle_finished_at: datetime
    raw_records_seen: int
    """Total Gamma records before filtering — includes non-binary, non-tradeable."""

    @property
    def duration_seconds(self) -> float:
        return (self.cycle_finished_at - self.cycle_started_at).total_seconds()


class MarketsJsonlWriter:
    """
    Append-only JSONL writer for market metadata.

    Not thread-safe; intended to be called from a single asyncio task. We
    keep a single file handle open for the lifetime of the writer (one cycle
    typically writes ~30k lines, no point reopening).

    Use as a context manager so the file gets closed and fsynced on exit.
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
            # discovery cycle that runs every 5-10 min, the cost (~ms) is
            # noise. For a 60s WAL writer in Phase 3 we'll be more careful.
            import os
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None

    def write(self, ts_recv: datetime, raw_record: dict) -> None:
        if self._fh is None:
            raise RuntimeError("Writer not opened (use as context manager)")
        # Wrap the raw record so consumers always have a receive timestamp
        # even if Gamma's record changes shape over time.
        wrapper = {
            "ts_recv": ts_recv.isoformat().replace("+00:00", "Z"),
            "raw": raw_record,
        }
        self._fh.write(self._encoder.encode(wrapper))
        self._fh.write(b"\n")


async def fetch_all_active_binary_markets(
    client: GammaClient,
    markets_jsonl_path: Path | None = None,
) -> DiscoveryResult:
    """
    One full discovery cycle.

    Pulls every page of ``active=true&closed=false`` from Gamma, filters to
    tradeable binary markets, and (optionally) appends every raw record we
    saw to ``markets.jsonl``.

    Why we write *all* tradeable binary markets to the JSONL each cycle, not
    just newly-seen ones: the JSONL is meant to be a history of what we
    knew, when. A delta-based representation would lose information about
    changes within a record (price ticks, volume, question edits).

    Returns a ``DiscoveryResult`` even on partial failures? No — if Gamma
    fails mid-iteration we raise ``GammaError``. The caller (the discovery
    loop) treats that as "skip this cycle" and tries again later. Half-baked
    results are worse than no results because they'd wrongly trigger
    `to_remove` for markets that just happened to be on later pages.

    Args:
        client: an entered ``GammaClient`` (use as ``async with``).
        markets_jsonl_path: if provided, raw records are appended here. If
            ``None``, this becomes a pure read (useful for tests / dry-run).

    Returns:
        ``DiscoveryResult`` with the parsed markets and flattened token_id
        set ready to feed into the WS subscription manager.
    """
    started = datetime.now(timezone.utc)
    raw_records_seen = 0
    parsed_markets: list[Market] = []

    writer_ctx = (
        MarketsJsonlWriter(markets_jsonl_path)
        if markets_jsonl_path is not None
        else _NullWriter()
    )

    with writer_ctx as writer:
        async for market, raw in client.iter_markets(active=True, closed=False):
            raw_records_seen += 1
            if not is_tradeable_binary_market(market):
                continue
            parsed_markets.append(market)
            # ts_recv is the discovery cycle's start time. We deliberately
            # use a single timestamp for the whole cycle rather than
            # re-stamping each record — Gamma data within one cycle is "as
            # of cycle start" semantically, and it makes downstream
            # de-duplication ("give me each market's most recent record")
            # trivially work via cycle timestamp.
            writer.write(started, raw)

    finished = datetime.now(timezone.utc)
    token_ids = extract_token_ids(parsed_markets)

    logger.info(
        "discovery cycle: saw %d raw, kept %d binary tradeable, %d unique tokens, took %.1fs",
        raw_records_seen,
        len(parsed_markets),
        len(token_ids),
        (finished - started).total_seconds(),
    )

    return DiscoveryResult(
        markets=parsed_markets,
        token_ids=token_ids,
        cycle_started_at=started,
        cycle_finished_at=finished,
        raw_records_seen=raw_records_seen,
    )


# Internal: null writer for the optional path case. Keeps the with-block
# uniform without sprinkling None checks everywhere.
class _NullWriter:
    def __enter__(self) -> "_NullWriter":
        return self

    def __exit__(self, *_exc) -> None:
        pass

    def write(self, ts_recv: datetime, raw_record: dict) -> None:
        pass
