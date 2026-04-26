"""
Live Phase 2 smoke test — connect to real Polymarket WS, subscribe to a
small set of real assets, count messages for ~20s.

Usage:
    python tests/smoke_test_ws.py

Verifies the full pipeline end-to-end:
- Gamma fetch -> token_ids
- WSPool starts
- WSConnection connects with custom_feature_enabled=true
- Real messages arrive
- on_message callback receives raw bytes + asset_ids + ts_recv
"""

import asyncio
import logging
from collections import Counter
from datetime import datetime

from polymarket_wal.discovery_loop import DiscoveryLoop
from polymarket_wal.gamma_client import GammaClient
from polymarket_wal.ws.pool import WSPool

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("smoke")


async def main() -> None:
    msg_counter = Counter()
    asset_counter = Counter()
    raw_bytes_total = 0

    async def on_msg(asset_ids, raw_bytes, ts_recv: datetime, conn_id: int):
        nonlocal raw_bytes_total
        # We can cheaply peek event_type without full parse for stats
        try:
            import json
            data = json.loads(raw_bytes)
            if isinstance(data, list):
                for d in data:
                    msg_counter[d.get("event_type", "?")] += 1
            else:
                msg_counter[data.get("event_type", "?")] += 1
        except Exception:
            msg_counter["<malformed>"] += 1
        for a in asset_ids:
            asset_counter[a] += 1
        raw_bytes_total += len(raw_bytes)

    async def on_event(conn_id, event, extra):
        log.info("CONN %d %s %s", conn_id, event.value, extra)

    # Use small max_per_connection so we exercise sharding even with few assets
    pool = WSPool(on_message=on_msg, on_event=on_event, max_per_connection=5)
    await pool.start()

    # Pick ~20 active assets to subscribe to
    async with GammaClient() as gamma:
        markets_seen = 0
        token_ids: list[str] = []
        async for market, _raw in gamma.iter_markets():
            token_ids.extend(market.token_ids)
            markets_seen += 1
            if len(token_ids) >= 20:
                break
        log.info("subscribing to %d token_ids from %d markets", len(token_ids), markets_seen)

    await pool.add_subscriptions(token_ids)

    # Run for 20s and report
    log.info("running for 20s...")
    await asyncio.sleep(20)

    stats = pool.stats()
    print("\n========= SMOKE TEST RESULTS =========")
    print(f"pool stats: {stats}")
    print(f"total messages received: {sum(msg_counter.values())}")
    print(f"by event_type: {dict(msg_counter)}")
    print(f"unique assets seen messages for: {len(asset_counter)}/{len(token_ids)}")
    print(f"raw bytes received: {raw_bytes_total / 1024:.1f} KB")
    if asset_counter:
        top = asset_counter.most_common(3)
        print(f"top 3 chatty assets: {[(a[:20]+'...', c) for a, c in top]}")

    await pool.stop()
    print("clean shutdown ✓")


if __name__ == "__main__":
    asyncio.run(main())
