"""
polymarket_api.py - 调用 Polymarket 三个公开 API 的客户端.

本文件中所有的 endpoint 和字段格式都经过实测验证 (2026-04-25):
- Gamma API:  GET https://gamma-api.polymarket.com/markets
- CLOB API:   GET https://clob.polymarket.com/{book,price,midpoint,spread}
- Data API:   GET https://data-api.polymarket.com/{trades,holders,positions}

所有这些都不需要认证.

实测踩到的几个坑 (面试时如果讲到, 加分):
1. Gamma 返回的 outcomes / outcomePrices / clobTokenIds 是 "字符串形式的 JSON 数组",
   不是直接的 list. 需要二次 json.loads().
2. volume / liquidity 这些数字字段也是字符串.
3. 必须带 User-Agent, 不然某些边缘网络会被 Cloudflare 拦.
4. /markets 端点带 deprecation 警告, 官方建议迁移到 /markets/keyset
   (但 /markets 目前还能用, 2026-05-01 才 sunset).
5. /book 偶尔返回过期快照 ("ghost market" 0.01/0.99), 做市需要走 WebSocket.
"""
from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any
import httpx


# ============================================================
# 数据模型 (只保留实际有用的字段, 不照搬 80+ 字段全量)
# ============================================================

@dataclass
class Market:
    """从 Gamma API /markets 返回的市场对象, 只取关心的字段"""
    id: str
    question: str
    condition_id: str          # 0x... hex, Data API 用这个
    slug: str
    yes_token_id: str          # CLOB API 用这个 (YES outcome)
    no_token_id: str           # CLOB API 用这个 (NO outcome)
    yes_price: float           # 当前 YES 价格 (= 市场认为发生的概率)
    no_price: float
    volume: float              # 累计成交量 (USDC)
    liquidity: float           # 当前流动性
    volume_24h: float
    end_date: str | None
    enable_orderbook: bool
    accepting_orders: bool

    @classmethod
    def from_gamma(cls, raw: dict) -> "Market | None":
        """从 Gamma API 原始响应构造. 字段名映射 + 类型转换 + 容错."""
        try:
            # 这三个字段是字符串形式的 JSON 数组, 需要二次解析
            outcomes = json.loads(raw.get("outcomes", "[]"))
            prices = json.loads(raw.get("outcomePrices", "[]"))
            tokens = json.loads(raw.get("clobTokenIds", "[]"))

            if len(outcomes) != 2 or len(tokens) != 2:
                # 不是二元市场 (multi-outcome), 我们暂时跳过
                return None

            return cls(
                id=str(raw.get("id", "")),
                question=raw.get("question", ""),
                condition_id=raw.get("conditionId", ""),
                slug=raw.get("slug", ""),
                yes_token_id=tokens[0],
                no_token_id=tokens[1],
                yes_price=float(prices[0]) if len(prices) > 0 else 0.0,
                no_price=float(prices[1]) if len(prices) > 1 else 0.0,
                # volume / liquidity 是字符串, 偶尔是空字符串
                volume=float(raw.get("volume") or 0),
                liquidity=float(raw.get("liquidity") or 0),
                volume_24h=float(raw.get("volume24hr") or 0),
                end_date=raw.get("endDate"),
                enable_orderbook=bool(raw.get("enableOrderBook", False)),
                accepting_orders=bool(raw.get("acceptingOrders", False)),
            )
        except (ValueError, TypeError, json.JSONDecodeError):
            return None


@dataclass
class OrderBook:
    """CLOB /book 返回的订单簿快照"""
    asset_id: str
    market: str
    timestamp_ms: int
    bids: list[tuple[float, float]]   # [(price, size), ...] 按价格升序
    asks: list[tuple[float, float]]   # [(price, size), ...] 按价格升序
    tick_size: float
    min_order_size: float

    @property
    def best_bid(self) -> float | None:
        return self.bids[-1][0] if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return self.asks[0][0] if self.asks else None

    @property
    def spread(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return self.best_ask - self.best_bid

    @property
    def midpoint(self) -> float | None:
        if self.best_bid is None or self.best_ask is None:
            return None
        return (self.best_bid + self.best_ask) / 2

    @classmethod
    def from_clob(cls, raw: dict) -> "OrderBook":
        return cls(
            asset_id=raw.get("asset_id", ""),
            market=raw.get("market", ""),
            timestamp_ms=int(raw.get("timestamp", 0)),
            bids=[(float(b["price"]), float(b["size"])) for b in raw.get("bids", [])],
            asks=[(float(a["price"]), float(a["size"])) for a in raw.get("asks", [])],
            tick_size=float(raw.get("tick_size") or 0),
            min_order_size=float(raw.get("min_order_size") or 0),
        )


@dataclass
class Trade:
    """Data API /trades 返回的成交记录"""
    timestamp: int
    side: str             # "BUY" / "SELL"
    size: float
    price: float
    outcome: str          # "Yes" / "No"
    asset_id: str
    condition_id: str
    market_title: str
    trader_wallet: str
    trader_name: str = ""
    tx_hash: str = ""

    @classmethod
    def from_data_api(cls, raw: dict) -> "Trade":
        return cls(
            timestamp=int(raw.get("timestamp", 0)),
            side=raw.get("side", ""),
            size=float(raw.get("size") or 0),
            price=float(raw.get("price") or 0),
            outcome=raw.get("outcome", ""),
            asset_id=str(raw.get("asset", "")),
            condition_id=raw.get("conditionId", ""),
            market_title=raw.get("title", ""),
            trader_wallet=raw.get("proxyWallet", ""),
            trader_name=raw.get("name", "") or raw.get("pseudonym", ""),
            tx_hash=raw.get("transactionHash", ""),
        )


# ============================================================
# 客户端
# ============================================================

class PolymarketClient:
    """统一封装三个 API 的异步客户端.

    设计要点:
    - 单一 httpx.AsyncClient 实例, 复用连接 (HTTP/2 + keep-alive)
    - 必须带 User-Agent (实测踩坑)
    - 简单的重试逻辑 (生产应该用 tenacity)
    """

    GAMMA_BASE = "https://gamma-api.polymarket.com"
    CLOB_BASE = "https://clob.polymarket.com"
    DATA_BASE = "https://data-api.polymarket.com"

    def __init__(self, timeout: float = 15.0):
        self._client = httpx.AsyncClient(
            timeout=timeout,
            http2=True,
            headers={"User-Agent": "polymarket-rag-demo/0.1"},
            limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        )

    async def close(self):
        await self._client.aclose()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        await self.close()

    async def _get(self, url: str, params: dict | None = None) -> Any:
        """带最多 3 次重试的 GET. 处理 Polymarket 偶发的 5xx."""
        last_err = None
        for attempt in range(3):
            try:
                resp = await self._client.get(url, params=params)
                resp.raise_for_status()
                return resp.json()
            except (httpx.HTTPError, json.JSONDecodeError) as e:
                last_err = e
                if attempt < 2:
                    await asyncio.sleep(0.5 * (2 ** attempt))
        raise RuntimeError(f"GET {url} failed after 3 retries: {last_err}")

    # -------- Gamma API --------

    async def list_markets(
        self,
        limit: int = 20,
        active: bool = True,
        closed: bool = False,
        order: str = "volume24hr",      # 按 24h 成交量排序
        ascending: bool = False,
    ) -> list[Market]:
        """获取活跃市场列表"""
        raw = await self._get(
            f"{self.GAMMA_BASE}/markets",
            params={
                "limit": limit,
                "active": str(active).lower(),
                "closed": str(closed).lower(),
                "order": order,
                "ascending": str(ascending).lower(),
            },
        )
        if not isinstance(raw, list):
            return []
        markets = [Market.from_gamma(m) for m in raw]
        return [m for m in markets if m is not None]

    async def get_market(self, market_id: str) -> Market | None:
        """通过 id 获取单个市场"""
        raw = await self._get(f"{self.GAMMA_BASE}/markets/{market_id}")
        return Market.from_gamma(raw) if isinstance(raw, dict) else None

    # -------- CLOB API --------

    async def get_book(self, token_id: str) -> OrderBook:
        """获取订单簿快照"""
        raw = await self._get(
            f"{self.CLOB_BASE}/book",
            params={"token_id": token_id},
        )
        return OrderBook.from_clob(raw)

    async def get_price(self, token_id: str, side: str = "BUY") -> float:
        """获取最优价 (BUY 返回 best bid, SELL 返回 best ask)"""
        raw = await self._get(
            f"{self.CLOB_BASE}/price",
            params={"token_id": token_id, "side": side},
        )
        return float(raw.get("price", 0))

    async def get_midpoint(self, token_id: str) -> float:
        """获取中点价 (= 市场预测的概率)"""
        raw = await self._get(
            f"{self.CLOB_BASE}/midpoint",
            params={"token_id": token_id},
        )
        return float(raw.get("mid", 0))

    async def get_spread(self, token_id: str) -> float:
        """获取价差 (= ask - bid)"""
        raw = await self._get(
            f"{self.CLOB_BASE}/spread",
            params={"token_id": token_id},
        )
        return float(raw.get("spread", 0))

    # -------- Data API --------

    async def get_trades(
        self,
        market: str | None = None,    # condition_id (0x...)
        user: str | None = None,      # proxyWallet 地址
        limit: int = 50,
    ) -> list[Trade]:
        """获取成交记录, 按市场或用户筛选"""
        params: dict[str, Any] = {"limit": limit}
        if market:
            params["market"] = market
        if user:
            params["user"] = user
        raw = await self._get(f"{self.DATA_BASE}/trades", params=params)
        if not isinstance(raw, list):
            return []
        return [Trade.from_data_api(t) for t in raw]

    async def get_holders(self, market: str, limit: int = 20) -> list[dict]:
        """获取市场的 top holders (谁持有最多 YES/NO token)"""
        raw = await self._get(
            f"{self.DATA_BASE}/holders",
            params={"market": market, "limit": limit},
        )
        return raw if isinstance(raw, list) else []


# ============================================================
# 演示用法
# ============================================================

async def demo():
    async with PolymarketClient() as client:
        # 1. 找 5 个活跃市场, 按 24h 成交量排序
        print("=" * 70)
        print("1. Top markets by 24h volume")
        print("=" * 70)
        markets = await client.list_markets(limit=5, order="volume24hr")
        for m in markets:
            print(f"\n  [{m.id}] {m.question[:65]}")
            print(f"    YES={m.yes_price:.2%}  NO={m.no_price:.2%}")
            print(f"    24h vol=${m.volume_24h:,.0f}  liquidity=${m.liquidity:,.0f}")
            print(f"    condition_id: {m.condition_id[:30]}...")

        if not markets:
            print("没有市场数据, exit")
            return

        # 2. 选第一个市场, 拉它的订单簿
        market = markets[0]
        print(f"\n{'=' * 70}")
        print(f"2. Order book for: {market.question[:60]}")
        print("=" * 70)
        book = await client.get_book(market.yes_token_id)

        print(f"  asset_id: {book.asset_id[:30]}...")
        print(f"  best bid: {book.best_bid}  best ask: {book.best_ask}")
        print(f"  spread:   {book.spread}    midpoint: {book.midpoint}")
        print(f"  tick_size: {book.tick_size}, min_order_size: {book.min_order_size}")
        print(f"\n  Top 5 bids (price, size):")
        for p, s in book.bids[-5:][::-1]:  # 最高的 5 个
            print(f"    {p:.4f} × {s:>10.2f}")
        print(f"\n  Top 5 asks (price, size):")
        for p, s in book.asks[:5]:
            print(f"    {p:.4f} × {s:>10.2f}")

        # 3. 拉这个市场最近的 5 笔成交
        print(f"\n{'=' * 70}")
        print(f"3. Recent trades")
        print("=" * 70)
        trades = await client.get_trades(market=market.condition_id, limit=5)
        for t in trades:
            ago = int(time.time()) - t.timestamp
            ago_str = f"{ago}s" if ago < 60 else f"{ago // 60}m"
            print(f"  {ago_str:>6} ago  {t.side:>4} {t.outcome:>3} "
                  f"size={t.size:>8.2f} @ {t.price:.4f}  "
                  f"by {t.trader_name or t.trader_wallet[:10]}")

        # 4. 并发对比: 同时查询多个市场的中点价 (展示真实生产用法)
        print(f"\n{'=' * 70}")
        print(f"4. Concurrent midpoint fetch for {len(markets)} markets")
        print("=" * 70)
        t0 = time.time()
        mids = await asyncio.gather(*[
            client.get_midpoint(m.yes_token_id) for m in markets
        ])
        elapsed = (time.time() - t0) * 1000
        print(f"  Fetched {len(markets)} midpoints in {elapsed:.0f}ms (并发)")
        for m, mid in zip(markets, mids):
            print(f"    {mid:.4f}  {m.question[:55]}")


if __name__ == "__main__":
    asyncio.run(demo())
