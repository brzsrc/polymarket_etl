from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncIterator
from typing import Any

import httpx
import msgspec

from src.market.market_model import Market, parse_binary_market

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Empirically determined: API enforces 1000 even if you ask for more.
MAX_LIMIT = 1000

# When querying by `?id=...&id=...`, the bottleneck isn't the server-side page
# cap (1000) — it's nginx's URI length limit (~8KB; we hit 414 around 800 ids
# of 7 digits each). 500 keeps the URL under ~5.5KB with comfortable headroom
# for longer ids in the future.
ID_BATCH_SIZE = 500
# Conservative timeouts. Gamma is usually fast (<200ms) but can spike.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

# Filter shorthand we end up using all over.
ACTIVE_OB_FILTER: dict[str, Any] = {
    "active": "true",
    "closed": "false",
    "archived": "false",
    "enable_order_book": "true",
    "accepting_orders": "true",
}

logger = logging.getLogger(__name__)


class GammaError(Exception):
    """Raised for unrecoverable Gamma API errors."""


class GammaClient:
    """
    Thin async client for the Gamma `/markets` endpoint.

    The client only deals in *raw records* internally; parsing into `Market`
    happens at the boundary of public methods. That keeps one parsing path
    (`parse_binary_market`) instead of letting parse logic creep into the
    HTTP layer.
    """

    def __init__(self) -> None:
        self._base_url = GAMMA_BASE_URL
        self._page_size = MAX_LIMIT
        self._timeout = DEFAULT_TIMEOUT
        self._client: httpx.AsyncClient | None = None
        self._max_retries = 4
        # msgspec is dramatically faster than stdlib json for our payload size.
        self._json_decoder = msgspec.json.Decoder()

    async def __aenter__(self) -> "GammaClient":
        self._client = httpx.AsyncClient(
            base_url=self._base_url,
            timeout=self._timeout,
            headers={"Accept": "application/json", "User-Agent": "polymarket-wal/0.1"},
        )
        return self

    async def __aexit__(self, *_exc: Any) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    # --- HTTP layer --------------------------------------------------------

    async def _get_page(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """
        One GET /markets call with retry on network errors.
        Returns the decoded JSON list (possibly empty); raises GammaError on
        non-200 responses or after retry budget is exhausted.
        """
        if self._client is None:
            raise RuntimeError("GammaClient must be used as an async context manager")

        # HTTP statuses we retry. Server-side hiccups (500/502/503/504) and
        # rate limiting (429) are transient — retrying with backoff usually
        # works. Other 4xx (400 bad request, 404 not found, 401 unauthorized)
        # indicate our request is wrong, no point retrying.
        retryable_statuses = {429, 500, 502, 503, 504}
        attempt = 0
        while True:
            try:
                resp = await self._client.get("/markets", params=params)
            except httpx.RequestError as e:
                if attempt >= self._max_retries:
                    raise GammaError(f"Network error after {attempt} retries: {e}") from e
                await asyncio.sleep(0.5 * (2 ** attempt))
                attempt += 1
                continue

            if resp.status_code == 200:
                # Decode bytes directly with msgspec — saves a string roundtrip.
                return self._json_decoder.decode(resp.content)

            if resp.status_code in retryable_statuses:
                if attempt >= self._max_retries:
                    raise GammaError(
                        f"Gamma returned {resp.status_code} after {attempt} retries: "
                        f"{resp.text[:200]}"
                    )
                # Same backoff schedule as for network errors. For 429, the
                # server may send a Retry-After header — we don't honor it
                # explicitly because Gamma typically doesn't, and our 0.5s
                # base doubles fast enough.
                await asyncio.sleep(0.5 * (2 ** attempt))
                attempt += 1
                continue

            raise GammaError(f"Gamma returned {resp.status_code}: {resp.text[:200]}")



    def _parse_json_string_field(self, value: Any) -> list:
        """
        Gamma returns `outcomes`, `clobTokenIds` etc. as a JSON-encoded string,
        e.g. literally `'["Yes", "No"]'` not `["Yes", "No"]`. Sometimes (rarely)
        it's already a list. Handle both; raise on anything else so we notice
        schema drift.
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

    def _parse_raw(self, raw: dict[str, Any]) -> list[str] | None:
        """
        Parse one raw Gamma market dict into its list of tokenids.
        """
        try:
            token_ids_list = self._parse_json_string_field(raw.get("clobTokenIds"))
        except (json.JSONDecodeError, TypeError):
            return None

        return token_ids_list

    async def _iter_raw_pages(
        self,
        base_params: dict[str, Any],
        max_count: int | None = None,
    ) -> AsyncIterator[dict[str, Any]]:
        """
        Walk `/markets` with offset-based pagination, yielding raw dicts.

        - `base_params` is sent on every page; we add `limit` and `offset`.
        - Stops on empty page, short page (< page_size), or after `max_count`
          records have been yielded.

        Single pagination path used by every fetch method. Don't add
        another one.
        """
        offset = int(base_params.get("offset", 0))
        yielded = 0

        while True:
            remaining = None if max_count is None else max_count - yielded
            if remaining is not None and remaining <= 0:
                return

            page_size = self._page_size if remaining is None else min(self._page_size, remaining)

            params = {**base_params, "limit": page_size, "offset": offset}

            page = await self._get_page(params)
            if not page:
                return

            for raw in page:
                yield raw
                yielded += 1
                if max_count is not None and yielded >= max_count:
                    return

            # Short page → no more results. Saves one round-trip vs. waiting
            # for an empty response.
            if len(page) < page_size:
                return

            offset += page_size

    # --- Public: iterators -------------------------------------------------

    async def iter_markets(
        self,
        filters: dict[str, Any] | None = None,
        max_count: int | None = None,
    ) -> AsyncIterator[tuple[Market, dict[str, Any]]]:
        """
        Iterate over markets matching `filters` (default ACTIVE_OB_FILTER),
        if max_count = None, then iter all markets,
        yielding only those that parse as binary markets.

        Yields `(Market, raw_dict)`.
        """
        params = dict(filters) if filters is not None else dict(ACTIVE_OB_FILTER)
        async for raw in self._iter_raw_pages(params, max_count=max_count):
            market = parse_binary_market(raw)
            if market is None:
                continue
            yield market, raw

    # --- Public: top-N -----------------------------------------------------

    async def fetch_top_n_by_volume24h(
        self,
        n: int | None = None,
        accepting_orders_only: bool = True,
    ) -> tuple[list[str], list[str], list[dict[str, Any]]]:
        """
        Fetch markets sorted by 24h volume (descending).

        - `n=None` → fetch ALL matching markets (paginates until Gamma
          returns an empty/short page). For `accepting_orders_only=True`
          this is currently ~47k records and takes ~30-40s.
        - `n=N`   → fetch only the first N (still paginated internally
          for N > 1000).

        Pagination under `order=volume24hr` is *mostly* stable but not
        perfectly — a market whose 24h volume changes between page fetches
        can appear on two pages. We dedup by id; when `n` is set, we fetch
        a small overshoot so dedup losses don't underfill the result.

        Returns market_ids, token_ids, raws

        Note: order=volume24hr ranking is a *snapshot*. Calling this 30
        minutes later may return a different population. For periodic
        refreshes of the SAME markets, freeze `market_ids` once and call
        `fetch_markets_by_ids` thereafter.
        """
        if n is not None and n <= 0:
            return [], [], []

        base_params: dict[str, Any] = {
            "order": "volume24hr",
            "ascending": "false",
        }
        if accepting_orders_only:
            base_params["acceptingOrders"] = "true"

        # Overshoot when a hard cap is set, to absorb dedup loss. When
        # n is None, we want everything — no cap.
        max_fetch = None if n is None else n + 50

        market_ids: list[str] = []
        token_ids: list[str] = []
        raws: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        async for raw in self._iter_raw_pages(base_params, max_count=max_fetch):
            mid = raw.get("id")
            condition_id = raw.get("conditionId")
            if mid is None or condition_id is None:
                continue
            mid_s = str(mid)

            if mid_s in seen_ids:
                continue
            seen_ids.add(mid_s)

            market_ids.append(mid_s)
            token_ids.extend(self._parse_raw(raw))
            raws.append(raw)

            if n is not None and len(market_ids) >= n:
                break
        return market_ids, token_ids, raws

    # --- Public: by-id snapshot -------------------------------------------

    async def fetch_markets_by_ids(
        self,
        market_ids: list[str],
    ) -> dict[str, dict[str, Any]]:
        """
        Fetch raw Gamma records for a list of market ids.

        Returns a dict mapping id -> raw record.

        Remove  duplicates in market_ids.

        Ids that Gamma doesn't return (deleted / unknown) are simply absent
        — diff against input to find them.

        Implementation notes (all learned the hard way):
        - Gamma's `/markets` accepts repeated `?id=...` params and returns
          matching records, but the default `limit` (20) silently truncates.
          We always set `limit=len(batch)`.
        - The `id` filter validates each value as int. A single non-numeric
          id makes the WHOLE request fail with HTTP 422. We pre-filter
          non-numeric ids and log a warning rather than tank the batch.
        - Chunked at ID_BATCH_SIZE (500) per request — bounded by nginx's
          URI length limit, NOT by the server-side page cap.
        """
        if self._client is None:
            raise RuntimeError("GammaClient must be used as an async context manager")

        # Dedup while preserving order; drop non-numeric ids (see note above).
        seen: set[str] = set()
        unique_ids: list[str] = []
        for x in market_ids:
            s = str(x)
            if s in seen:
                continue
            seen.add(s)
            if not s.isdigit():
                logger.warning("fetch_markets_by_ids: skipping non-numeric id %r", s)
                continue
            unique_ids.append(s)

        out: dict[str, dict[str, Any]] = {}
        for start in range(0, len(unique_ids), ID_BATCH_SIZE):
            batch = unique_ids[start:start + ID_BATCH_SIZE]
            params: dict[str, Any] = {"id": batch, "limit": len(batch)}
            page = await self._get_page(params)
            for raw in page:
                mid = raw.get("id")
                if mid is not None:
                    out[str(mid)] = raw
        return out

    # --- Public: full discovery cycle -------------------------------------

    async def fetch_all_active_binary_markets(
        self,
        filters: dict[str, Any] | None = None,
    ) -> tuple[list[Market], list[dict[str, Any]]]:
        """
        Pull every page of `filters` (default: ACTIVE_OB_FILTER) from Gamma,
        keeping only tradeable binary markets.

        Returns `(parsed_markets, raw_records)` — parallel lists.

        Persistence (writing raw records to markets.jsonl) is the caller's
        responsibility — wrap with `storage.MarketsJsonlWriter` if you want
        a history log. Keeping IO out of the client makes it usable from
        contexts (tests, ad-hoc scripts) that don't want a file on disk.

        Raises `GammaError` on mid-pagination failure. Half-baked results
        are worse than no results because they'd wrongly trigger to_remove
        for markets that just happened to be on later pages.
        """
        parsed_markets: list[Market] = []
        raw_records: list[dict[str, Any]] = []

        async for market, raw in self.iter_markets(filters=filters):
            parsed_markets.append(market)
            raw_records.append(raw)

        logger.info("fetch_all_active_binary_markets: %d markets", len(parsed_markets))
        return parsed_markets, raw_records


# --- Quick smoke test --------------------------------------------------------

async def _main() -> None:
    async with GammaClient() as client:
        market_ids, token_ids, raws = await client.fetch_top_n_by_volume24h()
        print(f"top 2500 by volume24hr: got {len(market_ids)} ids")


    with open("../../data/active_markets.jsonl", "w", encoding="utf-8") as f:
        for item in raws:
            f.write(json.dumps(item, ensure_ascii=False) + "\n")




if __name__ == "__main__":
    asyncio.run(_main())
