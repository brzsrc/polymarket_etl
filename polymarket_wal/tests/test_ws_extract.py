"""Tests for ``extract_asset_ids`` — the routing function used by WSConnection
to figure out which assets a message pertains to."""

import json

import pytest

from polymarket_wal.ws.connection import extract_asset_ids


def to_bytes(obj) -> bytes:
    return json.dumps(obj).encode()


class TestExtractAssetIds:
    def test_book_message_top_level_asset_id(self):
        msg = to_bytes({
            "event_type": "book",
            "market": "0xabc",
            "asset_id": "12345",
            "bids": [{"price": "0.5", "size": "10"}],
            "asks": [],
        })
        assert extract_asset_ids(msg) == ("12345",)

    def test_price_change_array_asset_ids(self):
        """price_change can have multiple asset_ids in price_changes[]."""
        msg = to_bytes({
            "event_type": "price_change",
            "market": "0xabc",
            "price_changes": [
                {"asset_id": "111", "price": "0.5", "size": "10", "side": "BUY"},
                {"asset_id": "222", "price": "0.6", "size": "5", "side": "SELL"},
            ],
            "timestamp": "1234",
        })
        assert extract_asset_ids(msg) == ("111", "222")

    def test_price_change_dedupes_repeated_asset_id(self):
        msg = to_bytes({
            "event_type": "price_change",
            "price_changes": [
                {"asset_id": "111", "price": "0.5", "size": "10"},
                {"asset_id": "111", "price": "0.51", "size": "20"},
            ],
        })
        assert extract_asset_ids(msg) == ("111",)

    def test_initial_response_is_a_list_of_books(self):
        """Initial subscription response is a JSON array of book snapshots."""
        msg = to_bytes([
            {"event_type": "book", "asset_id": "111", "bids": [], "asks": []},
            {"event_type": "book", "asset_id": "222", "bids": [], "asks": []},
        ])
        assert extract_asset_ids(msg) == ("111", "222")

    def test_best_bid_ask_top_level(self):
        msg = to_bytes({
            "event_type": "best_bid_ask",
            "market": "0xabc",
            "asset_id": "777",
            "best_bid": "0.5",
            "best_ask": "0.51",
        })
        assert extract_asset_ids(msg) == ("777",)

    def test_new_market_top_level(self):
        msg = to_bytes({
            "event_type": "new_market",
            "asset_id": "888",
            "market": "0xnewmkt",
        })
        assert extract_asset_ids(msg) == ("888",)

    def test_message_without_event_type_still_works(self):
        """Some observed messages have no event_type — we work off field
        presence so should still extract."""
        msg = to_bytes({
            "market": "0xabc",
            "price_changes": [{"asset_id": "999", "price": "0.5"}],
        })
        assert extract_asset_ids(msg) == ("999",)

    def test_malformed_json_returns_empty(self):
        assert extract_asset_ids(b"not valid json{{{") == ()

    def test_empty_dict(self):
        assert extract_asset_ids(b"{}") == ()

    def test_empty_array(self):
        assert extract_asset_ids(b"[]") == ()

    def test_non_string_asset_id_ignored(self):
        """Defensive: if Polymarket ever returns numeric asset_ids,
        we drop them (we expect strings) rather than mis-routing."""
        msg = to_bytes({"event_type": "book", "asset_id": 12345, "bids": []})
        assert extract_asset_ids(msg) == ()

    def test_price_change_with_empty_array(self):
        msg = to_bytes({
            "event_type": "price_change",
            "price_changes": [],
        })
        assert extract_asset_ids(msg) == ()

    def test_preserves_first_seen_order(self):
        """Order matters for dispatch logging / debugging."""
        msg = to_bytes({
            "price_changes": [
                {"asset_id": "C"},
                {"asset_id": "A"},
                {"asset_id": "B"},
            ],
        })
        assert extract_asset_ids(msg) == ("C", "A", "B")
