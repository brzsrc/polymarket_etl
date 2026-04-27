from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator
from typing import Any

import httpx
import msgspec
from .models import Market, parse_binary_market

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Empirically determined: API enforces 1000 even if you ask for more.
MAX_LIMIT = 1000
# Conservative timeouts. Gamma is usually fast (<200ms) but can spike.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)

class GammaError(Exception):
    """Raised for unrecoverable Gamma API errors."""

class GammaClient:
    def __init__(self):
        self._base_url = GAMMA_BASE_URL
        self._page_size = MAX_LIMIT
        self._timeout = DEFAULT_TIMEOUT
        self._client: httpx.AsyncClient | None = None
        self._max_retries = 3
        # msgspec is dramatically faster than stdlib json for our payload size.
        self._json_decoder = msgspec.json.Decoder()

    async def __aenter__(self) -> GammaClient:
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

    async def _get_page(self, params: dict[str, Any]) -> list[dict[str, Any]] | None:
        if self._client is None:
            raise RuntimeError("GammaClient must be used as an async context manager")

        attempt = 0
        while True:
            try:
                resp = await self._client.get("/markets", params=params)
            except httpx.RequestError as e:
                # Network-level error (DNS, connection reset, timeout)
                if attempt >= self._max_retries:
                    raise GammaError(f"Network error after {attempt} retries: {e}") from e
                await asyncio.sleep(0.5 * (2 ** attempt))
                attempt += 1
                continue

            if resp.status_code == 200:
                # Decode bytes directly with msgspec — saves a string roundtrip.
                return self._json_decoder.decode(resp.content)

            raise GammaError(
                f"Gamma returned {resp.status_code}: {resp.text[:200]}"
            )

    async def iter_markets(self) -> AsyncIterator[tuple[Market, dict[str, Any]]]:
        """
        Iterate over all markets matching the given filters, handling
        pagination internally.

        Yields ``(Market, raw_dict)``. Records that don't parse as a binary
        market (e.g. multi-outcome) are silently skipped — they're not
        relevant to us.

        On a fatal error mid-iteration, raises ``GammaError``. The caller can
        either retry the whole iteration on the next tick, or use whatever
        partial results it accumulated. We don't try to be clever about
        partial state — this is a discovery cycle, not a database transaction.
        """
        offset = 0
        while True:
            params: dict[str, Any] = {
                "limit": self._page_size,
                "offset": offset,
                # Booleans are sent as "true"/"false" strings; httpx handles this.
                "active": "true",
                "closed": "false",
                "archived": "false",
                "enable_order_book": "true",
            }

            page = await self._get_page(params)

            if not page:
                # Empty page = we've reached the end. Gamma doesn't return a
                # total count, so this is the only way to know.
                return

            for raw in page:
                market = parse_binary_market(raw)
                if market is None:
                    continue
                yield market, raw

            # If we got a short page, that's also the end (avoids one extra
            # request that returns []).
            if len(page) < self._page_size:
                return

            offset += self._page_size


    async def fetch_all_markets(self) -> list[tuple[Market, dict[str, Any]]]:
        """Convenience: collect all markets into a list. ~45k records, a few MB."""
        return [pair async for pair in self.iter_markets()]


async def main():
    async with GammaClient() as client:           # ← async with 启动
        markets = await client.fetch_all_markets()  # ← await 在 async def 里合法
        print(f"拿到 {len(markets)} 个市场")
        for market, raw in markets[:5]:
            print(market.question)
        with open("../data/markets.jsonl", "wb") as f:
            f.write(b"\n".join(msgspec.json.encode(raw) for _, raw in markets))

if __name__ == '__main__':
    asyncio.run(main())
