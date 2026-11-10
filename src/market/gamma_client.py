from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import msgspec

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Cursor-paginated endpoint. The old offset-based `/markets` is deprecated
# (response headers: `Deprecation: true`, `Sunset: 2026-05-01`,
# `Warning: use /markets/keyset`): it now returns at most 100 rows per page
# whatever `limit` says, and `?id=` lookups return incomplete results.
# Docs: https://docs.polymarket.com/api-reference/markets/list-markets-keyset-pagination
MARKETS_PATH = "/markets/keyset"

# Server-side cap on `limit` for /markets/keyset (documented: max 100).
MAX_LIMIT = 100

# Each identifier filter (`id`, `clob_token_ids`, ...) accepts at most 100 values.
ID_BATCH_SIZE = 100
# Conservative timeouts. Gamma is usually fast (<200ms) but can spike.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

# Transient failures worth retrying: server hiccups and rate limiting. Other
# 4xx (400, 404, 422...) mean our request is wrong, so retrying is pointless.
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})

logger = logging.getLogger(__name__)


class GammaError(Exception):
    """Raised for unrecoverable Gamma API errors."""


# =============================================================================
# 模块级纯函数：不依赖 client，单独就能测试
# =============================================================================

def parse_json_list(value: Any) -> list:
    """
    把 Gamma 的"JSON 字符串字段"解析成 list。

    Gamma 返回的 `outcomes`、`clobTokenIds` 等字段是 JSON 编码的字符串，
    例如字面上是 `'["Yes", "No"]'` 而不是 `["Yes", "No"]`。偶尔已经是 list。
    两种都处理；其他类型直接抛错，方便发现 schema 变化。

    返回: list，元素类型保持 JSON 里的原样。

    例子:
        None              -> []
        ""                -> []
        '["Yes", "No"]'   -> ["Yes", "No"]
        ["Yes", "No"]     -> ["Yes", "No"]      # 已经是 list，原样返回
        123               -> 抛 TypeError
        "not json"        -> 抛 json.JSONDecodeError
    """
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        if not value:
            return []
        return json.loads(value)
    raise TypeError(f"Unexpected type for JSON-string field: {type(value).__name__}")


def token_ids_of(raw: dict[str, Any]) -> list[str]:
    """
    取出一个 raw market 的 CLOB token id（= WebSocket 订阅用的 asset id）。

    返回: list[str]。二元市场一般是 2 个 [Yes token, No token]，顺序和
    `outcomes` 对应。字段缺失或格式不对时返回 []，并记一条 warning，
    不抛异常（只跳过这一个 market，不影响整轮）。

    例子:
        {"id": "3215007", "clobTokenIds": '["428657...8601", "807735...7796"]'}
                                                  -> ["428657...8601", "807735...7796"]
        {"id": "1"}                               -> []      # 没有 clobTokenIds
        {"id": "1", "clobTokenIds": "not json"}   -> []      # 解析失败
        {"clobTokenIds": '["a", "", 5]'}          -> ["a"]   # 空串和非 str 被过滤
    """
    try:
        token_ids = parse_json_list(raw.get("clobTokenIds"))
    except (json.JSONDecodeError, TypeError):
        logger.warning("bad clobTokenIds for market %s: %r", raw.get("id"), raw.get("clobTokenIds"))
        return []
    return [t for t in token_ids if isinstance(t, str) and t]


# =============================================================================
# MarketInfo：一个 market 里我们会用到的字段 + 原始 dict
# =============================================================================

def _float_or_zero(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


@dataclass(frozen=True)
class MarketInfo:
    """
    一个 Gamma market：只取订阅和判断状态要用的几个字段，原始 dict 放在 `raw`。

    `raw` 原样保留，写 WAL 时用它；其他字段只是从 raw 里读出来的方便访问。
    用 `MarketInfo.from_raw(raw)` 构造。
    """

    id: str
    tokens: tuple[str, ...]   # CLOB token id = WebSocket 订阅用的 asset id，顺序和 outcomes 对应
    volume24hr: float         # 缺失或格式不对时为 0
    is_trading: bool          # 没关闭（closed 不为真）且在接单（acceptingOrders 为真）
    raw: dict[str, Any]

    @classmethod
    def from_raw(cls, raw: dict[str, Any]) -> "MarketInfo | None":
        """
        缺 `id`、缺 `conditionId`、或取不到任何 token 时返回 None（调用方跳过这个 market）。

        例子:
            MarketInfo.from_raw({"id": 3215007, "conditionId": "0x..", "clobTokenIds": '["a", "b"]',
                                 "volume24hr": 1200.5, "closed": False, "acceptingOrders": True})
            -> MarketInfo(id="3215007", tokens=("a", "b"), volume24hr=1200.5, is_trading=True, raw={...})
            MarketInfo.from_raw({"id": "1"})  -> None
        """
        mid = raw.get("id")
        if mid is None or raw.get("conditionId") is None:
            return None
        tokens = tuple(token_ids_of(raw))
        if not tokens:
            return None
        return cls(
            id=str(mid),
            tokens=tokens,
            volume24hr=_float_or_zero(raw.get("volume24hr")),
            is_trading=not raw.get("closed") and bool(raw.get("acceptingOrders")),
            raw=raw,
        )


# =============================================================================
# GammaClient
# =============================================================================

class GammaClient:
    """
    Gamma `/markets/keyset` 的轻量异步 client。

    分三层，每层只做一件事:
        _request          发一次 HTTP 请求（带重试）
        _iter_markets     跟着 cursor 翻页，逐条给出 raw market
        公开查询方法       决定"查什么、怎么筛"，返回 MarketInfo

    MarketInfo.raw 是 Gamma 原样返回的 dict，会被原样写进 WAL，下游再解析。

    用法:
        async with GammaClient() as client:
            markets = await client.top_markets_by_volume24h(n=50)
        tokens = [t for m in markets for t in m.tokens]
    """

    # --- 生命周期 ----------------------------------------------------------

    def __init__(
        self,
        *,
        base_url: str = GAMMA_BASE_URL,
        page_size: int = MAX_LIMIT,
        max_retries: int = 4,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        """
        transport: 测试时传 `httpx.MockTransport(handler)`，不连真实 API。
                   正常使用不用传。
        """
        self._base_url = base_url
        self._page_size = page_size
        self._max_retries = max_retries
        self._transport = transport
        self._timeout = DEFAULT_TIMEOUT
        self._client: httpx.AsyncClient | None = None
        # msgspec is dramatically faster than stdlib json for our payload size.
        self._json_decoder = msgspec.json.Decoder()

    async def __aenter__(self) -> "GammaClient":
        """返回 self，同时创建内部的 httpx.AsyncClient。"""
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            transport=self._transport,
            headers={"Accept": "application/json", "User-Agent": "polymarket-wal/0.1"},
        )
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        """关闭 HTTP 连接。返回 None，with 块里的异常会继续往外抛。"""
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # --- 传输层：只负责"发一次请求 + 重试" ----------------------------------

    async def _request(self, params: dict[str, Any]) -> dict[str, Any]:
        """
        GET /markets/keyset 一次，返回解码后的响应 body（只有一页）。

        返回: dict，例如
            {"$schema": "https://gamma-api.polymarket.com/schemas/MarketsKeysetListResponse.json",
             "markets": [{...raw market...}, ...],   # 最多 limit 条
             "next_cursor": "9CFyipZ_XXmei6zz..."}   # 最后一页没有这个 key

        出错时:
            - 网络错误或 429/5xx：重试最多 max_retries 次（等 0.5s、1s、2s、4s），
              还失败就抛 GammaError
            - 其他非 200（例如 422）：立刻抛 GammaError
            - 不在 async with 里调用：抛 RuntimeError
        """
        if self._client is None:
            raise RuntimeError("GammaClient must be used as an async context manager")

        attempt = 0
        while True:
            try:
                resp = await self._client.get(MARKETS_PATH, params=params)
            except httpx.RequestError as e:
                failure = f"network error: {e}"
                cause: Exception | None = e
            else:
                if resp.status_code == 200:
                    # Decode bytes directly with msgspec — saves a string roundtrip.
                    return self._json_decoder.decode(resp.content)
                if resp.status_code not in RETRYABLE_STATUSES:
                    raise GammaError(f"Gamma returned {resp.status_code}: {resp.text[:200]}")
                failure = f"Gamma returned {resp.status_code}: {resp.text[:200]}"
                cause = None

            if attempt >= self._max_retries:
                raise GammaError(f"{failure} (after {attempt} retries)") from cause
            # Gamma doesn't usually send Retry-After, so we don't read it; the
            # 0.5s base doubling is fast enough.
            await asyncio.sleep(0.5 * (2 ** attempt))
            attempt += 1

    # --- 分页层：只负责"跟着 cursor 走" ------------------------------------

    async def _iter_markets(
        self,
        params: dict[str, Any],
        max_count: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        逐页请求 /markets/keyset，一条一条给出 raw market dict。

        - `params` 每页都会带上；这里再加 `limit`，从第二页起加 `after_cursor`
          （上一页的 `next_cursor`）。
        - 停止条件：某页没有 `next_cursor`（最后一页）、某页为空、
          或已经给出 `max_count` 条。

        返回: 异步生成器（不是 list），要用 `async for` 迭代。调用时不会发请求，
        开始迭代才发；调用方 break 后不会再请求后面的页。

        所有查询方法都走这一条分页路径，不要再加第二条。

        例子:
            [r["id"] async for r in client._iter_markets(
                {"order": "volume24hr", "ascending": "false"}, max_count=3)]
            -> ["3215007", "3215008", "3215009"]
        """
        cursor: str | None = None
        yielded = 0

        while True:
            page_params = {**params, "limit": self._page_size}
            if cursor:
                page_params["after_cursor"] = cursor

            body = await self._request(page_params)
            page = body.get("markets") or []
            for raw in page:
                yield raw
                yielded += 1
                if max_count is not None and yielded >= max_count:
                    return

            cursor = body.get("next_cursor")
            if not page or not cursor:
                return

    # --- 公开查询：返回 MarketInfo ------------------------------------------

    async def top_markets_by_volume24h(
        self,
        n: int | None = None,
        accepting_orders_only: bool = True,
    ) -> list[MarketInfo]:
        """
        按 24h 成交量从高到低取开放中的 market。

        - `n=None` → 取全部开放 market（翻到最后一页）
        - `n=N`    → 保留够 N 个就停
        - n <= 0   → 返回 []，不发请求

        返回: list[MarketInfo]，按 volume24h 降序。

        筛选规则（都在客户端做）:
        - 跳过 `MarketInfo.from_raw` 返回 None 的记录（缺 `id` / `conditionId`，或没有 token）
        - `accepting_orders_only=True` 时跳过 `acceptingOrders` 不为真的。
          /markets/keyset 没有这个过滤参数（未知参数会被静默忽略）。
          已关闭的 market 由接口默认的 `closed=false` 排除。
        - 按 id 去重：翻页期间某个 market 的 24h 成交量变了，理论上可能出现两次。

        注意：volume24hr 排名是某一刻的快照，30 分钟后再调可能是另一批 market。
        要定期刷新同一批 market，先把 id 固定下来，之后用 `markets_by_ids`。

        例子:
            await client.top_markets_by_volume24h(n=2)
            -> [MarketInfo(id="3215007", tokens=("4286...", "8077..."), volume24hr=..., ...),
                MarketInfo(id="3215008", ...)]
        """
        if n is not None and n <= 0:
            return []

        params: dict[str, Any] = {"order": "volume24hr", "ascending": "false"}

        out: list[MarketInfo] = []
        seen_ids: set[str] = set()
        async for raw in self._iter_markets(params):
            m = MarketInfo.from_raw(raw)
            if m is None:
                continue
            if accepting_orders_only and not raw.get("acceptingOrders"):
                continue
            if m.id in seen_ids:
                continue
            seen_ids.add(m.id)

            out.append(m)
            if n is not None and len(out) >= n:
                break
        return out

    async def markets_by_ids(self, ids: list[str]) -> dict[str, MarketInfo]:
        """
        按 market id 查 market，开放和已关闭的都返回（`closed=any`）。

        返回: dict[str, MarketInfo]，market id（str）-> MarketInfo。
        只包含 Gamma 实际返回、且 `MarketInfo.from_raw` 能解析的 id；
        不存在 / 已删除的 id 不会出现，和输入对比就能找出来。

        - 输入里重复的 id 只查一次
        - 非纯数字的 id 跳过并记 warning：`id` 过滤会把每个值当 int 校验，
          有一个不合法就整个请求 422
        - 每批最多 ID_BATCH_SIZE（100）个 id
        - 不传 `closed=any` 的话，接口默认 `closed=false`，
          已结束的 market 会和不存在的 id 一样查不到

        例子:
            await client.markets_by_ids(["3215007", "3215007", "abc", "12", "999999999999"])
            -> {"3215007": MarketInfo(id="3215007", is_trading=True, ...),
                "12":      MarketInfo(id="12", is_trading=False, ...)}
        """
        seen: set[str] = set()
        valid_ids: list[str] = []
        for x in ids:
            s = str(x)
            if s in seen:
                continue
            seen.add(s)
            if not s.isdigit():
                logger.warning("markets_by_ids: skipping non-numeric id %r", s)
                continue
            valid_ids.append(s)

        out: dict[str, MarketInfo] = {}
        for start in range(0, len(valid_ids), ID_BATCH_SIZE):
            batch = valid_ids[start:start + ID_BATCH_SIZE]
            async for raw in self._iter_markets({"id": batch, "closed": "any"}):
                m = MarketInfo.from_raw(raw)
                if m is not None:
                    out[m.id] = m
        return out


# --- Quick smoke test --------------------------------------------------------

async def _main() -> None:
    """拉取全部开放 market，每个 raw dict 一行写到 <项目根>/data/active_markets.jsonl。"""
    async with GammaClient() as client:
        markets = await client.top_markets_by_volume24h()
    print(f"all open markets by volume24hr: {len(markets)} markets, "
          f"{sum(len(m.tokens) for m in markets)} token ids")

    out_path = Path(__file__).resolve().parents[2] / "data" / "active_markets.jsonl"
    with out_path.open("w", encoding="utf-8") as f:
        for m in markets:
            f.write(json.dumps(m.raw, ensure_ascii=False) + "\n")
    print(f"wrote {out_path}")


if __name__ == "__main__":
    asyncio.run(_main())
