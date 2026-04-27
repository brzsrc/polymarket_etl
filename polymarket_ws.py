"""
polymarket_ws.py - Polymarket 双 WebSocket 客户端 (Market + RTDS).

订阅:
- CLOB Market 频道: 订单簿和成交 (订阅你指定的 outcome token)
- RTDS 频道: 加密货币现货价 (Binance / Chainlink)

完全替代 REST polling - 数据是真正的实时推送 (~100ms 延迟).

启动:
    # 一次性: 装依赖
    pip install websockets httpx

    # 跑 demo (会先 REST 拉一个活跃市场, 再订阅它的两个 token)
    python polymarket_ws.py

    # 也可以指定 token IDs (空格分隔)
    python polymarket_ws.py 8501... 2527...

设计要点 (面试可讲):
1. 两个 WS 协议完全不同 (订阅格式 / ping 间隔), 各自独立封装
2. ping 用单独协程, 避免被消息处理 block 导致连接超时被踢
3. 自动重连 + 指数退避, backoff 上限 30s
4. 用 asyncio.Queue 做事件解耦, 多个 source 共享一个 sink
5. 优雅关闭: Ctrl+C → 取消任务 → 关 WS → 清空队列
"""
from __future__ import annotations

import asyncio
import json
import signal
import sys
import time
from dataclasses import dataclass
from typing import Any, AsyncIterator
import httpx
import websockets
from websockets.exceptions import ConnectionClosed



# ============================================================
# 事件类型 (统一表示从两个 WS 来的所有消息)
# ============================================================

@dataclass
class Event:
    source: str             # "market" | "rtds"
    event_type: str         # "book" | "price_change" | "last_trade_price" | "crypto_price" | ...
    received_at_ns: int     # 本地接收时间戳 (用于算端到端延迟)
    payload: dict           # 原始消息


# ============================================================
# Market WebSocket (CLOB)
# ============================================================

class MarketWSClient:
    """订阅 Polymarket CLOB Market 频道.

    协议:
    - URL: wss://ws-subscriptions-clob.polymarket.com/ws/market
    - 订阅: {"assets_ids": [...], "type": "market"}
    - 心跳: 客户端每 10s 发字符串 "PING", 服务端回 "PONG"
    - 消息可能是 dict (单事件) 或 list (批量事件)
    - event_type 字段标识类型: book / price_change / last_trade_price / tick_size_change
    """

    URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
    PING_INTERVAL = 10

    def __init__(self, asset_ids: list[str], event_queue: asyncio.Queue[Event]):
        self.asset_ids = asset_ids
        self.queue = event_queue
        self._stopping = False

    async def run(self):
        """主循环: 连接 → 订阅 → 收消息 → 断开自动重连"""
        backoff = 1
        while not self._stopping:
            try:
                await self._session()
                backoff = 1  # 成功连接后重置
            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"[market] disconnected: {type(e).__name__}: {e}, "
                      f"reconnect in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def _session(self):
        async with websockets.connect(
            self.URL,
            ping_interval=None,  # 禁用 websockets 库默认 ping, 我们自己发 "PING" 字符串
            ping_timeout=None,
            close_timeout=5,
        ) as ws:
            # 订阅
            sub = {"assets_ids": self.asset_ids, "type": "market"}
            await ws.send(json.dumps(sub))
            print(f"[market] subscribed to {len(self.asset_ids)} assets")

            # 启动 ping 协程, 跟消息处理并行
            ping_task = asyncio.create_task(self._ping_loop(ws))

            try:
                async for raw in ws:
                    await self._handle_message(raw)
            finally:
                ping_task.cancel()
                try:
                    await ping_task
                except asyncio.CancelledError:
                    pass

    async def _ping_loop(self, ws):
        """每 10s 发 PING. 注意: 是字符串 "PING", 不是 WebSocket 协议层 ping frame."""
        try:
            while True:
                await asyncio.sleep(self.PING_INTERVAL)
                await ws.send("PING")
        except (asyncio.CancelledError, ConnectionClosed):
            pass

    async def _handle_message(self, raw: str):
        recv_ts = time.time_ns()

        # PONG 是字符串响应, 不是 JSON. 跳过.
        if raw == "PONG":
            return

        try:
            print(raw)
            msg = json.loads(raw)
        except json.JSONDecodeError:
            print(f"[market] non-json: {raw[:100]}")
            return

        # 消息可能是 list (批量) 或 dict (单条)
        events = msg if isinstance(msg, list) else [msg]
        for ev in events:
            print("ev now" +" ============" * 10)
            print(ev)
            print("ev end" +" ============" * 10)
            event_type = ev.get("event_type", "unknown")
            await self.queue.put(Event(
                source="market",
                event_type=event_type,
                received_at_ns=recv_ts,
                payload=ev,
            ))

    def stop(self):
        self._stopping = True



# ============================================================
# 引导: 用 REST 找一个真实活跃市场, 拿它的 token IDs
# ============================================================

async def find_active_market_tokens() -> tuple[list[str], str]:
    """REST 调用: 找一个有 bid/ask 的活跃市场, 返回 [yes_token, no_token] 和市场名"""
    async with httpx.AsyncClient(
        headers={"User-Agent": "polymarket-ws-demo/0.1"},
        timeout=15,
    ) as c:
        r = await c.get(
            "https://gamma-api.polymarket.com/markets",
            params={
                "limit": 30, "active": "true", "closed": "false",
                "order": "volume24hr", "ascending": "false",
            },
        )
        r.raise_for_status()
        markets = r.json()

    for m in markets:
        bid, ask = m.get("bestBid"), m.get("bestAsk")
        if bid and ask and 0.05 < float(bid) < 0.95:
            tokens = json.loads(m["clobTokenIds"])
            return tokens, m["question"]

    raise RuntimeError("没找到合适的活跃市场")


# ============================================================
# 主程序
# ============================================================

async def main():
    # 1. 拿 token IDs (从命令行或自动找)
    if len(sys.argv) > 1:
        asset_ids = sys.argv[1:]
        market_name = "(user-provided)"
    else:
        print("自动查找活跃市场...")
        asset_ids, market_name = await find_active_market_tokens()

    print(f"目标市场: {market_name}")
    print(f"YES token: {asset_ids[0][:30]}...")
    if len(asset_ids) > 1:
        print(f"NO  token: {asset_ids[1][:30]}...")
    print()

    # 2. 共享事件队列
    queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=10000)

    # token_id → "YES" / "NO" 标签, 让日志更易读
    token_labels = {}
    if len(asset_ids) >= 1:
        token_labels[asset_ids[0]] = "YES"
    if len(asset_ids) >= 2:
        token_labels[asset_ids[1]] = "NO"

    # 3. 三个客户端: Market WS / RTDS WS / Renderer
    market = MarketWSClient(asset_ids=asset_ids, event_queue=queue)


    # 4. 启动. 用 gather 让任意一个挂掉时整体退出
    tasks = [
        asyncio.create_task(market.run(), name="market"),
        # asyncio.create_task(rtds.run(), name="rtds"),
        # asyncio.create_task(renderer.run(), name="renderer"),
    ]

    # 5. 优雅关闭: SIGINT/SIGTERM → 取消所有任务
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, lambda: [t.cancel() for t in tasks])
        except NotImplementedError:
            pass  # Windows 不支持 add_signal_handler


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
