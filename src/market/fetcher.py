from __future__ import annotations

import asyncio
from pathlib import Path

from src.market.gamma_client import GammaClient
from src.market.models import Market, MarketsJsonlWriter
from src.utilities import now_ns


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
        async for market, raw in client.iter_all_markets():
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
            markets_jsonl_path=Path("../../data/markets2.jsonl"),
        )

if __name__ == "__main__":
    asyncio.run(main())