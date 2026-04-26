"""Tests for the business filter."""

import json
from pathlib import Path

from polymarket_wal.market_filter import (
    extract_token_ids,
    is_tradeable_binary_market,
)
from polymarket_wal.models import parse_market

FIXTURES = Path(__file__).parent / "fixtures"


def load_market(name: str):
    raw = json.loads((FIXTURES / name).read_text())
    return parse_market(raw)


class TestIsTradeableBinaryMarket:
    def test_active_binary_market_is_tradeable(self):
        m = load_market("binary_market_active.json")
        assert m is not None
        assert is_tradeable_binary_market(m) is True

    def test_closed_market_not_tradeable(self):
        m = load_market("binary_market_closed.json")
        assert m is not None
        assert is_tradeable_binary_market(m) is False

    def test_not_accepting_orders_not_tradeable(self):
        m = load_market("not_accepting_market.json")
        assert m is not None
        assert is_tradeable_binary_market(m) is False


class TestExtractTokenIds:
    def test_extracts_two_per_market(self):
        m1 = load_market("binary_market_active.json")
        m2 = load_market("array_format_market.json")
        assert m1 is not None and m2 is not None

        token_ids = extract_token_ids([m1, m2])
        assert len(token_ids) == 4
        # All four must be present
        assert m1.token_ids[0] in token_ids
        assert m1.token_ids[1] in token_ids
        assert m2.token_ids[0] in token_ids
        assert m2.token_ids[1] in token_ids

    def test_dedupes(self):
        """In the unlikely case two markets share a token (shouldn't happen in
        practice), set semantics dedupe correctly."""
        m1 = load_market("binary_market_active.json")
        assert m1 is not None
        # Same market twice
        token_ids = extract_token_ids([m1, m1])
        assert len(token_ids) == 2

    def test_empty(self):
        assert extract_token_ids([]) == set()
