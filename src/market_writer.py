from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import msgspec

from src.gamma_client import GammaClient
from src.models import Market
from src.utilities import now_ns


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

    def write(self, ts_recv_ns: int, raw_record: dict) -> None:
        if self._fh is None:
            raise RuntimeError("Writer not opened (use as context manager)")
        wrapper = {
            "ts_recv_ns": ts_recv_ns,
            "raw": raw_record,
        }
        self._fh.write(self._encoder.encode(wrapper))
        self._fh.write(b"\n")



async def fetch_all_active_binary_markets(
    client: GammaClient,
    markets_jsonl_path: Path | None = None,
):
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
    raw_records_seen = 0
    parsed_markets: list[Market] = []

    assert markets_jsonl_path is not None

    writer_ctx = MarketsJsonlWriter(markets_jsonl_path)

    with writer_ctx as writer:
        async for market, raw in client.iter_markets():
            # raw_records_seen += 1
            # duoble check all markets are valid
            # if not is_tradeable_binary_market(market):
            #     continue
            parsed_markets.append(market)
            # ts_recv is the discovery cycle's start time. We deliberately
            # use a single timestamp for the whole cycle rather than
            # re-stamping each record — Gamma data within one cycle is "as
            # of cycle start" semantically, and it makes downstream
            # de-duplication ("give me each market's most recent record")
            # trivially work via cycle timestamp.
            writer.write(now_ns(), raw)
    print("len(parsed_markets): ", len(parsed_markets))
    return


async def main():
    async with GammaClient() as client:
        await fetch_all_active_binary_markets(
            client,
            markets_jsonl_path=Path("../data/markets2.jsonl"),
        )

if __name__ == "__main__":
    asyncio.run(main())