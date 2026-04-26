"""Tests for ``models.parse_market``."""

import json
from pathlib import Path

import pytest

from polymarket_wal.models import Market, parse_market

FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


class TestParseMarketRealFixtures:
    """Tests using actual responses captured from gamma-api.polymarket.com."""

    def test_active_binary_market_parses(self):
        raw = load_fixture("binary_market_active.json")
        market = parse_market(raw)

        assert market is not None
        assert isinstance(market, Market)
        assert market.id == raw["id"]
        assert market.condition_id == raw["conditionId"]
        # Binary => exactly 2 token_ids
        assert len(market.token_ids) == 2
        assert all(t.isdigit() for t in market.token_ids)
        assert len(market.outcomes) == 2
        assert market.outcomes == ("Yes", "No")
        assert market.active is True
        assert market.closed is False

    def test_closed_binary_market_parses_but_will_be_filtered(self):
        raw = load_fixture("binary_market_closed.json")
        market = parse_market(raw)
        # parse_market doesn't filter on closed status — that's the
        # filter layer's job. We just want to confirm we don't crash.
        assert market is not None
        assert market.closed is True


class TestParseMarketEdgeCases:
    """Synthetic fixtures for boundary conditions."""

    def test_multi_outcome_returns_none(self):
        raw = load_fixture("multi_outcome_market.json")
        market = parse_market(raw)
        # 5 outcomes => not a binary market => skip.
        assert market is None

    def test_missing_clob_tokens_returns_none(self):
        raw = load_fixture("missing_tokens_market.json")
        market = parse_market(raw)
        assert market is None

    def test_array_format_outcomes_works(self):
        """Defensive: if Gamma ever returns real arrays instead of JSON-strings,
        we should still handle it."""
        raw = load_fixture("array_format_market.json")
        market = parse_market(raw)
        assert market is not None
        assert market.token_ids == ("12345", "67890")
        assert market.outcomes == ("Yes", "No")

    def test_missing_id_returns_none(self):
        raw = {"conditionId": "0xabc", "outcomes": '["Yes","No"]', "clobTokenIds": '["1","2"]'}
        assert parse_market(raw) is None

    def test_missing_condition_id_returns_none(self):
        raw = {"id": "1", "outcomes": '["Yes","No"]', "clobTokenIds": '["1","2"]'}
        assert parse_market(raw) is None

    def test_malformed_outcomes_json_returns_none(self):
        raw = {
            "id": "1",
            "conditionId": "0xabc",
            "outcomes": "[not valid json",
            "clobTokenIds": '["1","2"]',
        }
        assert parse_market(raw) is None

    def test_one_token_one_outcome_returns_none(self):
        raw = {
            "id": "1",
            "conditionId": "0xabc",
            "outcomes": '["Only"]',
            "clobTokenIds": '["1"]',
        }
        assert parse_market(raw) is None

    def test_binary_market_with_minimal_fields(self):
        """All required fields present, optional fields missing."""
        raw = {
            "id": "1",
            "conditionId": "0xabc",
            "outcomes": '["Yes","No"]',
            "clobTokenIds": '["1","2"]',
        }
        market = parse_market(raw)
        assert market is not None
        # Defaults for missing flags
        assert market.active is False
        assert market.closed is False
        assert market.accepting_orders is False
        assert market.tick_size is None

    def test_token_ids_are_str_even_if_int(self):
        """clobTokenIds in JSON could in principle be ints; ensure we stringify."""
        raw = {
            "id": "1",
            "conditionId": "0xabc",
            "outcomes": '["Yes","No"]',
            "clobTokenIds": "[12345, 67890]",  # ints inside the JSON-string
        }
        market = parse_market(raw)
        assert market is not None
        assert market.token_ids == ("12345", "67890")
