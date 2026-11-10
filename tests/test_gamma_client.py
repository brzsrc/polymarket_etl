"""
GammaClient tests against a fake Gamma (httpx.MockTransport) — no network.
"""
from __future__ import annotations

import asyncio
import json

import httpx
import pytest

from src.market import gamma_client
from src.market.gamma_client import (
    GammaClient,
    GammaError,
    MarketInfo,
    parse_json_list,
    token_ids_of,
)


def market(mid: str, tokens=("a", "b"), accepting=True, closed=False, **extra) -> dict:
    return {
        "id": mid,
        "conditionId": f"0xcond{mid}",
        "acceptingOrders": accepting,
        "closed": closed,
        "clobTokenIds": json.dumps(list(tokens)),
        **extra,
    }


class FakeGamma:
    """
    Serves `pages` in order, chained by cursor "c1", "c2", ...; the last page
    has no `next_cursor`. Records every request's query params.
    """

    def __init__(self, pages: list[list[dict]]):
        self.pages = pages
        self.requests: list[httpx.QueryParams] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request.url.params)
        cursor = request.url.params.get("after_cursor")
        idx = 0 if cursor is None else int(cursor[1:])
        body: dict = {"markets": self.pages[idx]}
        if idx + 1 < len(self.pages):
            body["next_cursor"] = f"c{idx + 1}"
        return httpx.Response(200, json=body)

    def client(self, **kw) -> GammaClient:
        return GammaClient(transport=httpx.MockTransport(self.handler), **kw)


def run(coro):
    return asyncio.run(coro)


@pytest.fixture(autouse=True)
def no_sleep(monkeypatch):
    """Retries back off with asyncio.sleep; skip the wait in tests."""
    async def _sleep(_s):
        return None
    monkeypatch.setattr(gamma_client.asyncio, "sleep", _sleep)


# --- pure functions ----------------------------------------------------------

def test_parse_json_list():
    assert parse_json_list(None) == []
    assert parse_json_list("") == []
    assert parse_json_list('["Yes", "No"]') == ["Yes", "No"]
    assert parse_json_list(["Yes", "No"]) == ["Yes", "No"]
    with pytest.raises(TypeError):
        parse_json_list(123)
    with pytest.raises(json.JSONDecodeError):
        parse_json_list("not json")


def test_token_ids_of():
    assert token_ids_of({"clobTokenIds": '["t1", "t2"]'}) == ["t1", "t2"]
    assert token_ids_of({"id": "1"}) == []
    assert token_ids_of({"id": "1", "clobTokenIds": "not json"}) == []
    assert token_ids_of({"clobTokenIds": '["a", "", 5]'}) == ["a"]


def test_market_info_from_raw():
    raw = market(3215007, tokens=("a", "b"), volume24hr=1200.5)   # int id -> str
    m = MarketInfo.from_raw(raw)
    assert m == MarketInfo(id="3215007", tokens=("a", "b"), volume24hr=1200.5,
                           is_trading=True, raw=raw)
    assert m.raw is raw


def test_market_info_from_raw_rejects_incomplete():
    assert MarketInfo.from_raw({"id": "1"}) is None                              # no conditionId
    assert MarketInfo.from_raw({"conditionId": "0x", "clobTokenIds": '["a"]'}) is None  # no id
    assert MarketInfo.from_raw(market("1", tokens=())) is None                   # no tokens
    assert MarketInfo.from_raw(market("1", clobTokenIds="not json")) is None


def test_market_info_is_trading_and_volume():
    assert MarketInfo.from_raw(market("1", closed=True)).is_trading is False
    assert MarketInfo.from_raw(market("1", accepting=False)).is_trading is False
    assert MarketInfo.from_raw(market("1", accepting=None)).is_trading is False
    assert MarketInfo.from_raw(market("1")).volume24hr == 0.0                   # missing
    assert MarketInfo.from_raw(market("1", volume24hr=None)).volume24hr == 0.0
    assert MarketInfo.from_raw(market("1", volume24hr="12.5")).volume24hr == 12.5
    assert MarketInfo.from_raw(market("1", volume24hr="bad")).volume24hr == 0.0


# --- lifecycle / transport ---------------------------------------------------

def test_request_outside_context_raises():
    with pytest.raises(RuntimeError):
        run(GammaClient()._request({}))


def test_request_retries_then_succeeds():
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) < 3:
            return httpx.Response(503, text="busy")
        return httpx.Response(200, json={"markets": []})

    async def go():
        async with GammaClient(transport=httpx.MockTransport(handler)) as c:
            return await c._request({})

    assert run(go()) == {"markets": []}
    assert len(calls) == 3


def test_request_gives_up_after_max_retries():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(500, text="boom")

    async def go():
        async with GammaClient(transport=httpx.MockTransport(handler), max_retries=2) as c:
            await c._request({})

    with pytest.raises(GammaError, match="500"):
        run(go())
    assert len(calls) == 3  # first try + 2 retries


def test_request_network_error_is_retried():
    calls = []

    def handler(request):
        calls.append(1)
        if len(calls) == 1:
            raise httpx.ConnectError("down")
        return httpx.Response(200, json={"markets": []})

    async def go():
        async with GammaClient(transport=httpx.MockTransport(handler)) as c:
            return await c._request({})

    assert run(go()) == {"markets": []}
    assert len(calls) == 2


def test_request_non_retryable_fails_fast():
    calls = []

    def handler(request):
        calls.append(1)
        return httpx.Response(422, text="bad id")

    async def go():
        async with GammaClient(transport=httpx.MockTransport(handler)) as c:
            await c._request({})

    with pytest.raises(GammaError, match="422"):
        run(go())
    assert len(calls) == 1


# --- pagination ----------------------------------------------------------------

def test_iter_markets_follows_cursor_until_last_page():
    fake = FakeGamma([[market("1"), market("2")], [market("3")]])

    async def go():
        async with fake.client() as c:
            return [r["id"] async for r in c._iter_markets({"order": "volume24hr"})]

    assert run(go()) == ["1", "2", "3"]
    assert len(fake.requests) == 2
    assert "after_cursor" not in fake.requests[0]
    assert fake.requests[1]["after_cursor"] == "c1"
    assert all(p["order"] == "volume24hr" and p["limit"] == "100" for p in fake.requests)


def test_iter_markets_max_count_stops_without_extra_request():
    fake = FakeGamma([[market("1"), market("2")], [market("3")]])

    async def go():
        async with fake.client() as c:
            return [r["id"] async for r in c._iter_markets({}, max_count=2)]

    assert run(go()) == ["1", "2"]
    assert len(fake.requests) == 1


# --- public queries ------------------------------------------------------------

def test_top_markets_filters_dedups_and_truncates():
    fake = FakeGamma([
        [market("1"), market("2", accepting=False), {"id": "3"},   # 2 not accepting, 3 no conditionId
         market("6", tokens=())],                                   # 6 has no tokens
        [market("1"), market("4"), market("5")],                    # duplicate 1
    ])

    async def go():
        async with fake.client() as c:
            return await c.top_markets_by_volume24h(n=2)

    markets = run(go())
    assert all(isinstance(m, MarketInfo) for m in markets)
    assert [m.id for m in markets] == ["1", "4"]
    assert fake.requests[0]["order"] == "volume24hr"
    assert fake.requests[0]["ascending"] == "false"


def test_top_markets_can_keep_non_accepting():
    fake = FakeGamma([[market("1"), market("2", accepting=False)]])

    async def go():
        async with fake.client() as c:
            return await c.top_markets_by_volume24h(accepting_orders_only=False)

    assert [m.id for m in run(go())] == ["1", "2"]


def test_top_markets_n_zero_makes_no_request():
    fake = FakeGamma([[market("1")]])

    async def go():
        async with fake.client() as c:
            return await c.top_markets_by_volume24h(n=0)

    assert run(go()) == []
    assert fake.requests == []


def test_markets_by_ids_dedups_skips_bad_ids_and_batches():
    ids = [str(i) for i in range(1, 151)]  # 150 ids -> 2 batches

    def handler(request):
        asked = request.url.params.get_list("id")
        assert request.url.params["closed"] == "any"
        # pretend id 7 doesn't exist
        return httpx.Response(200, json={"markets": [market(i, closed=(i == "2")) for i in asked if i != "7"]})

    seen_batches = []

    def recording(request):
        seen_batches.append(request.url.params.get_list("id"))
        return handler(request)

    async def go():
        async with GammaClient(transport=httpx.MockTransport(recording)) as c:
            return await c.markets_by_ids(ids + ["1", "abc"])

    out = run(go())
    assert [len(b) for b in seen_batches] == [100, 50]
    assert "abc" not in out and "7" not in out
    assert len(out) == 149
    assert out["1"].id == "1" and out["1"].is_trading
    assert out["2"].is_trading is False          # closed markets are returned too
