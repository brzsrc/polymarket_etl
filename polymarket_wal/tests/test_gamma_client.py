"""
Tests for ``GammaClient`` — pagination, 429 handling, parse-on-iter.

We use ``httpx.MockTransport`` rather than mocking ``httpx.AsyncClient``
directly. MockTransport intercepts at the transport layer, so the full
client request/response pipeline (headers, params, retries, timeouts) runs
for real — much closer to integration than unit-level mocks.
"""

import json
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from polymarket_wal.gamma_client import GammaClient, GammaError


def make_market(market_id: int) -> dict[str, Any]:
    """Minimal valid binary market dict for fixture purposes."""
    return {
        "id": str(market_id),
        "conditionId": f"0x{market_id:064x}",
        "slug": f"market-{market_id}",
        "question": f"Question {market_id}",
        "outcomes": '["Yes","No"]',
        "clobTokenIds": f'["{market_id}001","{market_id}002"]',
        "active": True,
        "closed": False,
        "archived": False,
        "acceptingOrders": True,
        "enableOrderBook": True,
        "endDate": "2026-12-31T23:59:59Z",
        "orderPriceMinTickSize": 0.01,
        "orderMinSize": 5,
        "negRisk": False,
    }


class MockGammaServer:
    """
    Stateful mock that returns paginated fake data.

    Tracks each request so tests can assert on how many calls were made
    and with what params.
    """

    def __init__(
        self,
        total_markets: int,
        page_size: int = 1000,
        rate_limit_first_n_requests: int = 0,
        server_error_first_n_requests: int = 0,
    ) -> None:
        self.total_markets = total_markets
        self.page_size = page_size
        self.requests: list[httpx.Request] = []
        self.rate_limit_remaining = rate_limit_first_n_requests
        self.server_error_remaining = server_error_first_n_requests

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)

        if self.rate_limit_remaining > 0:
            self.rate_limit_remaining -= 1
            return httpx.Response(429, headers={"Retry-After": "0"}, content=b"rate limited")

        if self.server_error_remaining > 0:
            self.server_error_remaining -= 1
            return httpx.Response(503, content=b"service unavailable")

        params = dict(request.url.params)
        offset = int(params.get("offset", 0))
        limit = int(params.get("limit", 1000))

        page = [
            make_market(i)
            for i in range(offset, min(offset + limit, self.total_markets))
        ]
        return httpx.Response(200, json=page)


@pytest.fixture
def patch_backoff():
    """Patch the backoff function to return 0 so retries don't actually sleep."""
    with patch.object(GammaClient, "_backoff_seconds", staticmethod(lambda _attempt: 0.0)):
        yield


async def make_client_with_mock(server: MockGammaServer, page_size: int = 1000) -> GammaClient:
    """Helper: build a GammaClient that talks to MockGammaServer."""
    transport = httpx.MockTransport(server.handler)
    client = GammaClient(page_size=page_size)
    # We bypass __aenter__ to inject our transport. In production code the
    # entered client owns its httpx.AsyncClient; here we replace with one
    # that uses our mock transport.
    client._client = httpx.AsyncClient(
        base_url="https://gamma-api.polymarket.com",
        transport=transport,
        timeout=5.0,
    )
    return client


class TestPagination:
    """Pagination correctness across a few realistic scenarios."""

    @pytest.mark.asyncio
    async def test_single_page_under_limit(self):
        server = MockGammaServer(total_markets=42, page_size=1000)
        client = await make_client_with_mock(server, page_size=1000)
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        assert len(results) == 42
        # One request — short page, no need to fetch again.
        assert len(server.requests) == 1
        assert dict(server.requests[0].url.params).get("offset") == "0"

    @pytest.mark.asyncio
    async def test_exact_multiple_of_page_size(self):
        # 2000 markets, page_size 1000 — should fetch 3 times: [0..1000], [1000..2000], [2000..2000]=empty
        server = MockGammaServer(total_markets=2000)
        client = await make_client_with_mock(server, page_size=1000)
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        assert len(results) == 2000
        # The iterator stops on a short page, but if the boundary is exact,
        # the third request returns empty and stops there.
        assert len(server.requests) == 3
        offsets = [dict(r.url.params)["offset"] for r in server.requests]
        assert offsets == ["0", "1000", "2000"]

    @pytest.mark.asyncio
    async def test_multiple_pages_with_remainder(self):
        # 2500 markets => page1 [0..1000), page2 [1000..2000), page3 [2000..2500)
        # Third page is short (500 < 1000), iterator stops without a 4th call.
        server = MockGammaServer(total_markets=2500)
        client = await make_client_with_mock(server, page_size=1000)
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        assert len(results) == 2500
        assert len(server.requests) == 3

    @pytest.mark.asyncio
    async def test_realistic_size(self):
        # Roughly mimics production: ~45k markets.
        server = MockGammaServer(total_markets=45_500)
        client = await make_client_with_mock(server, page_size=1000)
        try:
            count = 0
            async for _ in client.iter_markets():
                count += 1
        finally:
            await client._client.aclose()

        assert count == 45_500
        # 45 full pages + 1 short page = 46 requests
        assert len(server.requests) == 46


class TestRateLimit:
    @pytest.mark.asyncio
    async def test_429_then_recovers(self, patch_backoff):
        # First two requests get 429, third succeeds.
        server = MockGammaServer(total_markets=10, rate_limit_first_n_requests=2)
        client = await make_client_with_mock(server)
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        assert len(results) == 10
        # 2 rate-limited + 1 successful = 3 requests
        assert len(server.requests) == 3

    @pytest.mark.asyncio
    async def test_429_exhausts_retries_raises(self, patch_backoff):
        # max_retries=4 means 5 attempts total before giving up.
        server = MockGammaServer(total_markets=10, rate_limit_first_n_requests=10)
        client = GammaClient(max_retries=2)
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(server.handler),
        )
        try:
            with pytest.raises(GammaError, match="Rate limited"):
                async for _ in client.iter_markets():
                    pass
        finally:
            await client._client.aclose()

    @pytest.mark.asyncio
    async def test_5xx_then_recovers(self, patch_backoff):
        server = MockGammaServer(total_markets=10, server_error_first_n_requests=1)
        client = await make_client_with_mock(server)
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        assert len(results) == 10
        assert len(server.requests) == 2  # 1 failed + 1 ok


class TestQueryParams:
    @pytest.mark.asyncio
    async def test_active_closed_params_sent(self):
        server = MockGammaServer(total_markets=1)
        client = await make_client_with_mock(server)
        try:
            async for _ in client.iter_markets(active=True, closed=False):
                pass
        finally:
            await client._client.aclose()

        params = dict(server.requests[0].url.params)
        assert params["active"] == "true"
        assert params["closed"] == "false"

    @pytest.mark.asyncio
    async def test_extra_params_merged(self):
        server = MockGammaServer(total_markets=1)
        client = await make_client_with_mock(server)
        try:
            async for _ in client.iter_markets(extra_params={"order": "endDate"}):
                pass
        finally:
            await client._client.aclose()

        params = dict(server.requests[0].url.params)
        assert params["order"] == "endDate"

    @pytest.mark.asyncio
    async def test_default_params_include_all_tradeable_filters(self):
        """By default, all server-side tradeability filters are sent.
        This is defense-in-depth — even though Gamma may already exclude
        these, we explicitly request the filtered set."""
        server = MockGammaServer(total_markets=1)
        client = await make_client_with_mock(server)
        try:
            async for _ in client.iter_markets():
                pass
        finally:
            await client._client.aclose()

        params = dict(server.requests[0].url.params)
        assert params["active"] == "true"
        assert params["closed"] == "false"
        assert params["archived"] == "false"
        assert params["acceptingOrders"] == "true"
        assert params["enableOrderBook"] == "true"

    @pytest.mark.asyncio
    async def test_none_omits_param(self):
        """Passing None for a filter omits it from the request entirely,
        letting Gamma's default apply."""
        server = MockGammaServer(total_markets=1)
        client = await make_client_with_mock(server)
        try:
            async for _ in client.iter_markets(
                active=None,
                accepting_orders=None,
                enable_order_book=None,
            ):
                pass
        finally:
            await client._client.aclose()

        params = dict(server.requests[0].url.params)
        assert "active" not in params
        assert "acceptingOrders" not in params
        assert "enableOrderBook" not in params
        # Non-None ones should still be there
        assert params["closed"] == "false"
        assert params["archived"] == "false"

    @pytest.mark.asyncio
    async def test_query_filter_does_not_remove_client_filter_responsibility(self):
        """Even with all tradeability params sent server-side, if Gamma
        ignores them and returns a non-tradeable market, the client-side
        ``is_tradeable_binary_market`` filter must still catch it.

        This test simulates a buggy/lying Gamma that returns a
        ``acceptingOrders=false`` market despite our query asking for
        ``acceptingOrders=true``."""
        from polymarket_wal.market_filter import is_tradeable_binary_market

        def handler(_request: httpx.Request) -> httpx.Response:
            page = [
                make_market(1),
                {**make_market(2), "acceptingOrders": False},  # Gamma "lies"
            ]
            return httpx.Response(200, json=page)

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        # iter_markets parses both — it doesn't apply the tradeable filter
        assert len(results) == 2
        # The client-side filter is what protects us
        tradeable = [m for m, _ in results if is_tradeable_binary_market(m)]
        assert len(tradeable) == 1
        assert tradeable[0].id == "1"


class TestParseFiltering:
    @pytest.mark.asyncio
    async def test_non_binary_records_silently_skipped(self):
        """Multi-outcome markets in the response stream don't break iteration."""

        def handler(request: httpx.Request) -> httpx.Response:
            page = [
                make_market(1),
                # Multi-outcome — should be filtered out by parse_market
                {
                    "id": "2",
                    "conditionId": "0xfive",
                    "outcomes": '["A","B","C","D","E"]',
                    "clobTokenIds": '["1","2","3","4","5"]',
                    "active": True,
                },
                make_market(3),
            ]
            return httpx.Response(200, json=page)

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        # Only 2 binary markets survive
        assert len(results) == 2
        ids = [m.id for m, _raw in results]
        assert ids == ["1", "3"]

    @pytest.mark.asyncio
    async def test_raw_dict_passed_through_unchanged(self):
        """The raw record yielded alongside the parsed Market is the original
        dict, not a re-serialized version."""
        original = make_market(99)
        original["mystery_future_field"] = "value-we-dont-know-about"

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[original])

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        try:
            results = [pair async for pair in client.iter_markets()]
        finally:
            await client._client.aclose()

        _market, raw = results[0]
        assert raw["mystery_future_field"] == "value-we-dont-know-about"
        assert raw["id"] == "99"
