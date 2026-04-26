"""Tests for ``market_discovery``."""

import json
from datetime import datetime, timezone
from pathlib import Path

import httpx
import pytest

from polymarket_wal.gamma_client import GammaClient
from polymarket_wal.market_discovery import (
    MarketsJsonlWriter,
    fetch_all_active_binary_markets,
)
from tests.test_gamma_client import make_market


class TestMarketsJsonlWriter:
    def test_writes_one_line_per_record(self, tmp_path: Path):
        path = tmp_path / "markets.jsonl"
        ts = datetime(2026, 4, 26, 12, 0, 0, tzinfo=timezone.utc)

        with MarketsJsonlWriter(path) as w:
            w.write(ts, {"id": "1", "x": 1})
            w.write(ts, {"id": "2", "x": 2})

        lines = path.read_text().splitlines()
        assert len(lines) == 2
        rec0 = json.loads(lines[0])
        rec1 = json.loads(lines[1])
        assert rec0["raw"]["id"] == "1"
        assert rec1["raw"]["id"] == "2"

    def test_appends_across_sessions(self, tmp_path: Path):
        """Each open is in append mode — reopening doesn't truncate."""
        path = tmp_path / "markets.jsonl"
        ts = datetime.now(timezone.utc)

        with MarketsJsonlWriter(path) as w:
            w.write(ts, {"id": "1"})
        with MarketsJsonlWriter(path) as w:
            w.write(ts, {"id": "2"})

        lines = path.read_text().splitlines()
        assert len(lines) == 2
        assert json.loads(lines[0])["raw"]["id"] == "1"
        assert json.loads(lines[1])["raw"]["id"] == "2"

    def test_ts_recv_iso_utc_z(self, tmp_path: Path):
        """Timestamp format: ISO-8601 with trailing 'Z' (no '+00:00')."""
        path = tmp_path / "markets.jsonl"
        ts = datetime(2026, 4, 26, 12, 0, 0, tzinfo=timezone.utc)

        with MarketsJsonlWriter(path) as w:
            w.write(ts, {"id": "1"})

        rec = json.loads(path.read_text().strip())
        assert rec["ts_recv"].endswith("Z")
        assert "+00:00" not in rec["ts_recv"]

    def test_create_parent_dirs(self, tmp_path: Path):
        path = tmp_path / "deep" / "nested" / "markets.jsonl"
        with MarketsJsonlWriter(path) as w:
            w.write(datetime.now(timezone.utc), {"id": "1"})
        assert path.exists()

    def test_raw_record_preserved_verbatim(self, tmp_path: Path):
        """The raw dict written to disk must round-trip without losing fields."""
        path = tmp_path / "markets.jsonl"
        original = {
            "id": "1",
            "conditionId": "0xabc",
            "outcomes": '["Yes","No"]',
            "clobTokenIds": '["1","2"]',
            "nested": {"a": [1, 2, {"b": "c"}]},
            "unicode_question": "Будет ли — ?",
            "future_field_we_dont_know": ["anything", 42, None, True],
        }
        with MarketsJsonlWriter(path) as w:
            w.write(datetime.now(timezone.utc), original)

        roundtripped = json.loads(path.read_text().strip())["raw"]
        assert roundtripped == original


class TestFetchAllActiveBinaryMarkets:
    @pytest.mark.asyncio
    async def test_full_cycle_with_persistence(self, tmp_path: Path):
        """End-to-end: pull 3 pages of mock data, persist to JSONL,
        return correct token_id set."""

        def handler(request: httpx.Request) -> httpx.Response:
            params = dict(request.url.params)
            offset = int(params.get("offset", 0))
            limit = int(params.get("limit", 1000))
            # Total 2500 markets across 3 pages
            page = [make_market(i) for i in range(offset, min(offset + limit, 2500))]
            return httpx.Response(200, json=page)

        client = GammaClient(page_size=1000)
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )

        path = tmp_path / "markets.jsonl"
        try:
            result = await fetch_all_active_binary_markets(client, markets_jsonl_path=path)
        finally:
            await client._client.aclose()

        assert result.raw_records_seen == 2500
        assert len(result.markets) == 2500
        # 2 tokens per market
        assert len(result.token_ids) == 5000
        assert result.duration_seconds >= 0

        # JSONL has one line per market kept
        lines = path.read_text().splitlines()
        assert len(lines) == 2500

        # Each line is wrapper {ts_recv, raw}
        first = json.loads(lines[0])
        assert "ts_recv" in first
        assert "raw" in first
        assert first["raw"]["id"] == "0"

    @pytest.mark.asyncio
    async def test_filters_non_tradeable(self, tmp_path: Path):
        """Closed and not-accepting markets in the stream are filtered out
        before being kept in result.markets, but they ARE still written to
        JSONL? No — actually we only persist what we keep. Confirm behavior."""

        def handler(_request: httpx.Request) -> httpx.Response:
            page = [
                make_market(1),  # tradeable
                {**make_market(2), "closed": True},  # not tradeable
                {**make_market(3), "acceptingOrders": False},  # not tradeable
                make_market(4),  # tradeable
            ]
            return httpx.Response(200, json=page)

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )

        path = tmp_path / "markets.jsonl"
        try:
            result = await fetch_all_active_binary_markets(client, markets_jsonl_path=path)
        finally:
            await client._client.aclose()

        # Only 2 are tradeable
        assert len(result.markets) == 2
        kept_ids = {m.id for m in result.markets}
        assert kept_ids == {"1", "4"}

        # JSONL persists ONLY the records we kept (matching result.markets).
        # Rationale: markets.jsonl is a record of "what we subscribed to over
        # time", not "everything Gamma returned". Closed markets are noise.
        lines = path.read_text().splitlines()
        assert len(lines) == 2
        persisted_ids = {json.loads(line)["raw"]["id"] for line in lines}
        assert persisted_ids == {"1", "4"}

    @pytest.mark.asyncio
    async def test_no_persistence_when_path_none(self):
        """Passing None as path is a valid 'dry run'."""

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[make_market(1)])

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )
        try:
            result = await fetch_all_active_binary_markets(client, markets_jsonl_path=None)
        finally:
            await client._client.aclose()

        assert len(result.markets) == 1
        # No file created — that's the whole point.

    @pytest.mark.asyncio
    async def test_uniform_ts_recv_within_cycle(self, tmp_path: Path):
        """All records in one cycle share the same ts_recv (cycle-start time).
        This makes 'most recent record per market' queries trivial."""

        def handler(_request: httpx.Request) -> httpx.Response:
            return httpx.Response(200, json=[make_market(i) for i in range(10)])

        client = GammaClient()
        client._client = httpx.AsyncClient(
            base_url="https://gamma-api.polymarket.com",
            transport=httpx.MockTransport(handler),
        )

        path = tmp_path / "markets.jsonl"
        try:
            await fetch_all_active_binary_markets(client, markets_jsonl_path=path)
        finally:
            await client._client.aclose()

        lines = path.read_text().splitlines()
        timestamps = {json.loads(line)["ts_recv"] for line in lines}
        # All 10 records should share the single cycle-start timestamp
        assert len(timestamps) == 1
