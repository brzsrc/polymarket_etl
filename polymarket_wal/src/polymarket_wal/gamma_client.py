"""
Gamma API client.

Wraps the public ``GET /markets`` endpoint at ``gamma-api.polymarket.com``.
Active markets number ~45k+ at any given moment and the API caps ``limit`` at
1000, so pagination is mandatory.

Design notes:
- Async (httpx.AsyncClient) — the rest of the service is asyncio.
- Returns BOTH the parsed ``Market`` and the original raw dict on each yield,
  because the discovery layer wants to write the raw record to disk for
  forensic value (Gamma adds/changes fields, raw is the truth).
- 429 handling: respect ``Retry-After`` if present, otherwise exponential
  backoff with jitter. We don't retry forever — give up after a few attempts
  and let the caller decide. (A discovery cycle missing once is fine; the
  next cycle will catch up.)
- We never use ``site:`` or other special operators, just plain query params.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import AsyncIterator
from typing import Any

import httpx
import msgspec

from .models import Market, parse_market

logger = logging.getLogger(__name__)

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"

# Empirically determined: API enforces 1000 even if you ask for more.
MAX_LIMIT = 1000

# Conservative timeouts. Gamma is usually fast (<200ms) but can spike.
DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=30.0, write=5.0, pool=5.0)


class GammaError(Exception):
    """Raised for unrecoverable Gamma API errors."""


class GammaClient:
    """
    Async client for the Gamma markets endpoint.

    Use as an async context manager:

        async with GammaClient() as gamma:
            async for market, raw in gamma.iter_markets(active=True, closed=False):
                ...

    The iterator handles pagination internally. It yields one
    ``(Market, raw_dict)`` per market. Records that fail to parse as a binary
    market are skipped (logged at DEBUG).
    """

    def __init__(
        self,
        base_url: str = GAMMA_BASE_URL,
        timeout: httpx.Timeout = DEFAULT_TIMEOUT,
        max_retries: int = 4,
        page_size: int = MAX_LIMIT,
    ) -> None:
        if page_size > MAX_LIMIT:
            raise ValueError(f"page_size must be <= {MAX_LIMIT} (Gamma's hard limit)")
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._max_retries = max_retries
        self._page_size = page_size
        self._client: httpx.AsyncClient | None = None
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

    async def _get_page(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """
        Fetch one page with retry on 429 / 5xx / network errors.

        Raises ``GammaError`` if all retries are exhausted. The caller (the
        pagination loop) treats this as fatal for that cycle and will try
        again on the next discovery tick.
        """
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
                await self._sleep_backoff(attempt)
                attempt += 1
                continue

            if resp.status_code == 200:
                # Decode bytes directly with msgspec — saves a string roundtrip.
                return self._json_decoder.decode(resp.content)

            if resp.status_code == 429:
                if attempt >= self._max_retries:
                    raise GammaError(f"Rate limited (429) after {attempt} retries")
                # Respect Retry-After if server gave us one. Otherwise back off.
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        delay = self._backoff_seconds(attempt)
                else:
                    delay = self._backoff_seconds(attempt)
                logger.warning(
                    "gamma 429 rate limited, sleeping %.1fs (attempt %d)", delay, attempt
                )
                await asyncio.sleep(delay)
                attempt += 1
                continue

            if 500 <= resp.status_code < 600:
                if attempt >= self._max_retries:
                    raise GammaError(f"Server error {resp.status_code} after {attempt} retries")
                await self._sleep_backoff(attempt)
                attempt += 1
                continue

            # 4xx other than 429 — won't fix itself by retrying.
            raise GammaError(
                f"Gamma returned {resp.status_code}: {resp.text[:200]}"
            )

    @staticmethod
    def _backoff_seconds(attempt: int) -> float:
        # Exponential with full jitter, capped at 30s. attempt is 0-indexed.
        base = min(2 ** attempt, 30)
        return random.uniform(0, base)

    async def _sleep_backoff(self, attempt: int) -> None:
        await asyncio.sleep(self._backoff_seconds(attempt))

    async def iter_markets(
        self,
        active: bool | None = True,
        closed: bool | None = False,
        archived: bool | None = False,
        accepting_orders: bool | None = True,
        enable_order_book: bool | None = True,
        extra_params: dict[str, Any] | None = None,
    ) -> AsyncIterator[tuple[Market, dict[str, Any]]]:
        """
        Iterate over all markets matching the given filters, handling
        pagination internally.

        All filter args are passed to Gamma as query params. Pass ``None`` to
        omit a param entirely (Gamma's default applies).

        IMPORTANT — defense in depth:

        Even with these query filters set, the caller MUST still apply the
        client-side ``is_tradeable_binary_market`` filter to the results.
        Reasons:

        1. Gamma's filter behavior is undocumented and a black box. Our tests
           against the live API in April 2026 showed that
           ``acceptingOrders=true``/``archived=false``/``enableOrderBook=true``
           had no observable effect (because ``active=true&closed=false`` is
           already very clean), but that could change either direction
           silently.
        2. We want to be robust to Gamma silently dropping a filter param —
           if they ignore ``acceptingOrders=true`` one day, we should still
           reject paused markets.
        3. ``parse_market`` does the binary-only filter (token count == 2)
           which Gamma can't express as a query.

        So query filters here are an *optimization hint*, not a contract.

        Yields ``(Market, raw_dict)``. Records that don't parse as a binary
        market (e.g. multi-outcome) are silently skipped.

        On a fatal error mid-iteration, raises ``GammaError``. The caller can
        either retry the whole iteration on the next tick, or use whatever
        partial results it accumulated. We don't try to be clever about
        partial state — this is a discovery cycle, not a database transaction.
        """
        # Build the static portion of the params (everything except offset)
        # once, since it doesn't change per page.
        base_params: dict[str, Any] = {"limit": self._page_size}
        for key, value in (
            ("active", active),
            ("closed", closed),
            ("archived", archived),
            ("acceptingOrders", accepting_orders),
            ("enableOrderBook", enable_order_book),
        ):
            if value is not None:
                # httpx serializes bools as "True"/"False" by default, which
                # Gamma rejects. Force lowercase strings explicitly.
                base_params[key] = str(value).lower()
        if extra_params:
            base_params.update(extra_params)

        offset = 0
        while True:
            params = {**base_params, "offset": offset}

            page = await self._get_page(params)

            if not page:
                # Empty page = we've reached the end. Gamma doesn't return a
                # total count, so this is the only way to know.
                return

            for raw in page:
                market = parse_market(raw)
                if market is None:
                    continue
                yield market, raw

            # If we got a short page, that's also the end (avoids one extra
            # request that returns []).
            if len(page) < self._page_size:
                return

            offset += self._page_size
