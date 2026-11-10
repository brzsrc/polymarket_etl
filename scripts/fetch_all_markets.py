from __future__ import annotations
import asyncio
import json
from collections import defaultdict
import time

import websockets  # pip install websockets
import httpx

WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
GAMMA_BASE = "https://gamma-api.polymarket.com"
GAMMA_URL = "https://gamma-api.polymarket.com/markets"
TRACE_DURATION_SEC = 30


async def find_active_market() -> tuple[list[str], dict]:
    """从 Gamma 找一个 bid/ask 不在边缘的活跃市场"""
    async with httpx.AsyncClient(headers={"User-Agent": "ws-trace/0.1"}, timeout=15) as c:
        r = await c.get(f"{GAMMA_BASE}/markets", params={
            "limit": 30, "active": "true", "closed": "false",
            "order": "volume24hr", "ascending": "false",
        })
    for m in r.json():
        bid, ask = m.get("bestBid"), m.get("bestAsk")
        if bid and ask and 0.05 < float(bid) < 0.95:
            tokens = json.loads(m["clobTokenIds"])
            return tokens, m
    raise RuntimeError("找不到合适的活跃市场")


async def fetch_all_active_token_ids(min_volume_24h: float = 10_000) -> list[str]:
    """从 Gamma 拉取活跃市场的 token id 列表."""
    token_ids = []
    offset = 0
    async with httpx.AsyncClient(timeout=15.0) as c:
        while True:
            r = await c.get(f"{GAMMA_BASE}/markets", params={
                "limit": 500,
                "offset": offset,
                "active": "true",
                "closed": "false",
                "volume_min": min_volume_24h,
            })
            r.raise_for_status()
            batch = r.json()
            if not batch:
                break
            for m in batch:
                if m.get("clobTokenIds"):
                    ids = json.loads(m["clobTokenIds"])
                    # print(ids, m)
                    token_ids.extend(ids)
            if len(batch) < 500:
                break
            offset += 500
    return token_ids

# 启用 custom feature 订阅
sub_new_market = {
    "type": "market",
    "assets_ids": [],   # 可以空, 想接 global new_market 事件
    "custom_feature_enabled": True,
}

async def trace(tokens, market: dict | None):
    # print(f"  Market: {market['question'][:60]}")
    print(f"  YES token: {tokens[0][:40]}...")
    print(f"  NO  token: {tokens[1][:40]}...")

    async with websockets.connect(WS_URL, ping_interval=None) as ws:
        sub_msg = {"assets_ids": tokens, "type": "market"}

        await ws.send(json.dumps(sub_msg))

        # PING 协程
        async def ping_loop():
            while True:
                await asyncio.sleep(10)
                print(f"\n  [client → server] 'PING'")
                await ws.send("PING")

        ping_task = asyncio.create_task(ping_loop())

        end_at = time.time() + TRACE_DURATION_SEC
        type_counts: dict[str, int] = {}
        msg_count = 0

        try:
            while time.time() < end_at:
                try:
                    raw = await asyncio.wait_for(ws.recv(), timeout=2)
                except asyncio.TimeoutError:
                    continue
                msg_count += 1

                if raw == "PONG":
                    print(f"  [server → client] 'PONG'")
                    type_counts["PONG"] = type_counts.get("PONG", 0) + 1
                    continue

                try:
                    msg = json.loads(raw)
                except json.JSONDecodeError:
                    print(f"  [server → client] (non-JSON, skipping)")
                    continue

                # 服务端推送可能是 list (批量) 或 dict (单条)
                events = msg if isinstance(msg, list) else [msg]
                for ev in events:
                    print('*'*20)
                    print(ev)
                    print(type(ev))
                    print('*' * 20)
                    et = ev.get("event_type", "(no event_type)")
                    type_counts[et] = type_counts.get(et, 0) + 1

                    if type_counts[et] <= 2:
                        # 完整打印前 2 条 (简化巨长字段)
                        print(f"\n  [server → client] event_type='{et}'")
                        ev_display = dict(ev)
                        if "asset_id" in ev_display:
                            ev_display["asset_id"] = ev_display["asset_id"][:20] + "..."
                        if "bids" in ev_display and len(ev_display["bids"]) > 3:
                            ev_display["bids"] = (
                                    ev_display["bids"][:3]
                                    + [f"... +{len(ev_display['bids']) - 3} more levels"]
                            )
                        if "asks" in ev_display and len(ev_display["asks"]) > 3:
                            ev_display["asks"] = (
                                    ev_display["asks"][:3]
                                    + [f"... +{len(ev_display['asks']) - 3} more levels"]
                            )
                        for line in json.dumps(ev_display, indent=2).split("\n")[:30]:
                            print(f"  {line}")
                    else:
                        print(f"  [server → client] event_type='{et}' (suppressed)")
        finally:
            ping_task.cancel()




if __name__ == "__main__":
    # try:
    #     # tokens, market = await find_active_market()
    #     print("========")
    #     with open("active_token_ids.json", "r") as f:
    #         tokens = json.load(f)
    #     asyncio.run(trace(tokens[:2], None))
    # except KeyboardInterrupt:
    #     print("\n中断退出")


    token_ids = asyncio.run(fetch_all_active_token_ids())
    print(len(token_ids))
    #
    # with open("active_token_ids.json", "w") as f:
    #     json.dump(token_ids, f)