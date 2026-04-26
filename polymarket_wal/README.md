# polymarket-wal

Phase 1 + Phase 2 of the Polymarket WS message archive.

## What this does

Maintains real-time WebSocket subscriptions to **all active binary markets**
on Polymarket and dispatches every received message to a user-supplied
callback. Designed as the foundation of a "WAL" (Write-Ahead Log) service
that captures complete tick-level history for research replay.

This package handles:

- **Discovery** — periodic Gamma REST poll to find all active binary markets
  (~46k at any given time)
- **Filtering** — only `acceptingOrders=true` and `enableOrderBook=true`
  binary (2-outcome) markets
- **WS subscriptions** — pool of WS connections, sharded ~200 assets each
  (Polymarket's hard cap is 500)
- **Resilience** — auto-reconnect with exponential backoff, data-flow watchdog
  for "silent freeze" failure mode, full re-subscribe after each reconnect
- **`new_market` events** — sets `custom_feature_enabled: true` so we get
  push notification of newly created markets within seconds (vs. 10-min
  Gamma poll)
- **Strike-based removal** — an asset must be missing for 3 consecutive
  Gamma cycles before we unsubscribe, protecting against pagination races

## What it does NOT do (yet)

- **Persistence** — `on_message` is a callback. Phase 3 will provide
  the WAL writer that lands bytes to JSONL on disk.
- **Monitoring** — no Prometheus metrics yet
- **OrderBook reconstruction** — out of scope; the WAL stores raw events,
  research-time replay reconstructs books

## Install

```bash
pip install -e ".[dev]"
```

Requires Python 3.11+.

## Quick start

```python
import asyncio
from datetime import datetime
from pathlib import Path
from polymarket_wal import GammaClient, WSPool, DiscoveryLoop

async def on_msg(asset_ids, raw_bytes, ts_recv: datetime, conn_id: int):
    # asset_ids: tuple[str, ...]   — assets this message pertains to
    #                                (price_change can have multiple)
    # raw_bytes: bytes             — the original WS frame, unmodified
    # ts_recv:   datetime (UTC)    — when WE received it
    # conn_id:   int               — which pool connection delivered it
    print(f"[{ts_recv}] {len(raw_bytes)} bytes for {len(asset_ids)} assets")

async def main():
    pool = WSPool(on_message=on_msg)
    await pool.start()

    async with GammaClient() as gamma:
        loop = DiscoveryLoop(
            gamma=gamma,
            pool=pool,
            markets_jsonl_path=Path("data/markets.jsonl"),
        )
        await loop.start()
        try:
            await asyncio.sleep(3600)  # run for an hour
        finally:
            await loop.stop()
            await pool.stop()

asyncio.run(main())
```

## Testing

```bash
# Unit + integration tests (mock WS server, mock Gamma)
pytest -q

# Live smoke tests (hit real Polymarket)
python tests/smoke_test_live.py   # Gamma discovery cycle
python tests/smoke_test_ws.py     # WS subscribe + receive
```

## Architecture

```
                    ┌─────────────────┐
                    │  GammaClient    │  REST /markets
                    └────────┬────────┘
                             │
                    ┌────────▼─────────┐
                    │ DiscoveryLoop    │  poll every 10min,
                    │  - diff vs pool  │  3-strike removal
                    │  - persist meta  │
                    └────────┬─────────┘
                             │ add/remove subs
                    ┌────────▼─────────┐
                    │     WSPool       │  ref-counted,
                    │  - shard 200/conn│  first-fit sharding
                    │  - dispatch msgs │
                    └────────┬─────────┘
                             │
                ┌────────────┼────────────┐
                ▼            ▼            ▼
          WSConnection WSConnection  WSConnection
           - PING 10s   - watchdog    - reconnect
                             │
                             ▼
                       on_message(...)
                             │
                       (Phase 3: WAL)
```

## File layout

```
src/polymarket_wal/
├── models.py              Market dataclass + parse_market()
├── gamma_client.py        Async REST client w/ pagination & 429 handling
├── market_filter.py       is_tradeable_binary_market()
├── market_discovery.py    Single-cycle fetch + markets.jsonl writer
├── discovery_loop.py      Periodic discovery + diff vs pool
└── ws/
    ├── connection.py      Single managed WS connection
    └── pool.py            Connection pool, sharding, ref-count

tests/
├── fixtures/              Real & synthetic Gamma response samples
├── test_models.py
├── test_market_filter.py
├── test_gamma_client.py
├── test_market_discovery.py
├── test_ws_extract.py     extract_asset_ids() unit tests
├── test_ws_connection.py  Integration via real local WS server
├── test_ws_pool.py        Pool logic w/ mock WSConnection
├── test_discovery_loop.py Diff & strike logic w/ mock Gamma + pool
├── smoke_test_live.py     Hit real Gamma API
└── smoke_test_ws.py       Hit real Polymarket WS
```

## Key design decisions

1. **Two-track output from `iter_markets`** — yields `(Market, raw_dict)`.
   The `Market` is for in-memory filtering; the `raw_dict` is what we
   persist (preserves Gamma fields we don't know about).

2. **`custom_feature_enabled: true` in WS subscribe** — required to
   receive `new_market` and `best_bid_ask` events. Nautilus's adapter
   doesn't set this; we do.

3. **First-fit sharding (not consistent hashing)** — simpler,
   matches nautilus's approach. The "asset can shift connections after
   remove+add" downside is irrelevant since we don't care about
   connection affinity.

4. **Dispatch forwards everything, including unknown assets** — `new_market`
   arrives with an asset_id we never subscribed to. We must record it.
   Filtering is the consumer's job.

5. **Data-flow watchdog separate from PING** — Polymarket has a known
   failure mode where the connection stays OPEN, PING/PONG works, but no
   data flows. 60s without ANY data message forces reconnect.

6. **3-strike removal** — Gamma's offset pagination can briefly drop an
   asset between pages. Wait for 3 consecutive misses before unsubscribing
   to avoid flapping.

7. **Markets.jsonl stores raw, not Market dataclass** — Gamma adds fields
   over time; only raw guarantees we don't lose data.
