"""
Live smoke test — actually hits gamma-api.polymarket.com.

Run this manually:
    python tests/smoke_test_live.py

Not part of the pytest suite because it requires network and is slow (~30s
for a full discovery cycle). Useful for sanity-checking before deployment
or after schema changes on Gamma's side.
"""

import asyncio
import logging
import tempfile
from pathlib import Path

from polymarket_wal.gamma_client import GammaClient
from polymarket_wal.market_discovery import fetch_all_active_binary_markets

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "markets.jsonl"
        async with GammaClient() as gamma:
            result = await fetch_all_active_binary_markets(
                gamma, markets_jsonl_path=path
            )
        print(f"\n=== discovery cycle complete ===")
        print(f"raw records seen:    {result.raw_records_seen}")
        print(f"tradeable binary:    {len(result.markets)}")
        print(f"unique token_ids:    {len(result.token_ids)}")
        print(f"duration:            {result.duration_seconds:.1f}s")
        print(f"jsonl size:          {path.stat().st_size / 1024:.1f} KB")
        print(f"jsonl lines:         {sum(1 for _ in path.open())}")
        # Sanity: a sample market
        if result.markets:
            sample = result.markets[0]
            print(f"\nsample market:")
            print(f"  question:   {sample.question[:80]}")
            print(f"  token_ids:  {sample.token_ids[0][:30]}.../{sample.token_ids[1][:30]}...")


if __name__ == "__main__":
    asyncio.run(main())
