"""
Data models for Polymarket markets.

We deal with two representations:
1. ``GammaRawMarket`` — what the Gamma API actually returns. It's loose:
   ``outcomes``, ``outcomePrices``, ``clobTokenIds`` are JSON-encoded *strings*
   (not arrays). We don't try to fully validate the schema; we accept anything
   and only require the fields we use downstream. Unknown fields are kept in
   ``extras`` so we can still write the full record to ``markets.jsonl``.

2. ``Market`` — our cleaned-up, post-parse representation. Stable schema we
   own, used by the rest of the codebase.

Why two layers: Gamma changes its response shape from time to time and adds
fields. Anything we don't know about should pass through to disk, so a future
researcher (or future-us) can grep the raw record. But the in-memory code
should work with a clean, typed object.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import msgspec


class Market(msgspec.Struct, frozen=True, kw_only=True):
    """
    Parsed, validated Polymarket market — what the rest of the code uses.

    Only fields we actually need are typed here. The full original record is
    available via ``GammaRawMarket.raw`` if you need anything else.
    """

    # Identity
    id: str
    condition_id: str
    slug: str
    question: str

    # The two CLOB token ids (YES, NO) — these are the asset_ids we'll
    # subscribe to over WebSocket. Stored as decimal strings (uint256).
    token_ids: tuple[str, str]
    outcomes: tuple[str, str]  # e.g. ("Yes", "No")

    # Status flags we filter on
    active: bool
    closed: bool
    archived: bool
    accepting_orders: bool
    enable_order_book: bool

    # Timestamps (ISO-8601 strings as Gamma returns them; we don't reparse)
    end_date: str | None
    start_date: str | None

    # Microstructure params, useful later
    tick_size: float | None  # orderPriceMinTickSize, e.g. 0.01
    min_order_size: float | None  # orderMinSize

    # neg_risk markets behave slightly differently downstream (different
    # exchange contract). We don't filter on it here, just preserve.
    neg_risk: bool


def _parse_json_string_field(value: Any) -> list:
    """
    Gamma returns ``outcomes`` etc. as a JSON-encoded string of an array,
    e.g. the field is literally '["Yes", "No"]' not ["Yes", "No"].
    Sometimes (rarely) it might already be an array. Handle both, fail loudly
    on anything else so we notice schema drift.
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


def parse_binary_market(raw: dict[str, Any]) -> Market | None:
    """
    Parse one raw Gamma market dict into a ``Market``.

    Returns ``None`` if the record is missing fields we need (e.g. an event
    container that snuck in, or a market without CLOB tokens). We don't raise
    on missing optional fields because Gamma's responses include a lot of
    half-populated records and we'd rather skip than crash the whole run.

    raw:
    [{'id': '2036399', 'question': 'US x Iran ceasefire extended by April 22, 2026?', 'conditionId': '0x1d2787cb8aed975d092b2799ed6f4083e9445f7420cdc09e9d47e7d54356c6cd', 
    'clobTokenIds': '["50049642142024617231697970377792489304039200104142714216386619263735691638204", "110959653450933276250915064669875552310439627880508793089816880777942697720191"]'
    'outcomes': '["Yes", "No"]',
    """
    try:
        token_ids_list = _parse_json_string_field(raw.get("clobTokenIds"))
        outcomes_list = _parse_json_string_field(raw.get("outcomes"))
    except (json.JSONDecodeError, TypeError):
        return None

    if len(token_ids_list) != 2 or len(outcomes_list) != 2:
        # We only handle binary markets. Multi-outcome markets (e.g. an
        # election with 5 candidates) come through here too and we drop them.
        return None

    market_id = raw.get("id")
    condition_id = raw.get("conditionId")
    if not market_id or not condition_id:
        return None

    return Market(
        id=str(market_id),
        condition_id=str(condition_id),
        slug=str(raw.get("slug", "")),
        question=str(raw.get("question", "")),
        token_ids=(str(token_ids_list[0]), str(token_ids_list[1])),
        outcomes=(str(outcomes_list[0]), str(outcomes_list[1])),
        active=bool(raw.get("active", False)),
        closed=bool(raw.get("closed", False)),
        archived=bool(raw.get("archived", False)),
        accepting_orders=bool(raw.get("acceptingOrders", False)),
        enable_order_book=bool(raw.get("enableOrderBook", False)),
        end_date=raw.get("endDate"),
        start_date=raw.get("startDate"),
        tick_size=raw.get("orderPriceMinTickSize"),
        min_order_size=raw.get("orderMinSize"),
        neg_risk=bool(raw.get("negRisk", False)),
    )


class MarketsJsonlWriter:
    """
    Append-only JSONL writer for market metadata.

    Not thread-safe; intended to be called from a single asyncio task. We
    keep a single file handle open for the lifetime of the writer (one cycle
    typically writes ~30k lines, no point reopening).

    Use as a context manager so the file gets closed and fsynced on exit.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._fh = None
        # msgspec encoder is reusable and faster than json.dumps.
        self._encoder = msgspec.json.Encoder()

    def __enter__(self) -> "MarketsJsonlWriter":
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Append mode in binary so msgspec.encode (returns bytes) writes
        # without an extra encoding step.
        self._fh = self._path.open("ab")
        return self

    def __exit__(self, *_exc) -> None:
        if self._fh is not None:
            self._fh.flush()
            # fsync ensures the kernel actually flushes to disk. For a
            # discovery cycle that runs every 5-10 min, the cost (~ms) is
            # noise. For a 60s WAL writer in Phase 3 we'll be more careful.
            import os
            os.fsync(self._fh.fileno())
            self._fh.close()
            self._fh = None

    def write(self, ts_recv_ns: int, raw_record: dict) -> None:
        if self._fh is None:
            raise RuntimeError("Writer not opened (use as context manager)")
        wrapper = {
            "ts_recv_ns": ts_recv_ns,
            "raw": raw_record,
        }
        self._fh.write(self._encoder.encode(wrapper))
        self._fh.write(b"\n")
