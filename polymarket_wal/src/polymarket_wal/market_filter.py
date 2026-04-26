"""
Filtering logic for markets we actually want to subscribe to.

Note ``parse_market`` already drops non-binary markets (those whose
``clobTokenIds`` array length isn't 2). This module is for the *business*
filter: even among binary markets, we only care about ones that are
currently tradeable on the CLOB.

A few subtle points:

- ``active=true&closed=false`` is what we ask Gamma for, but Gamma also
  returns plenty of "active" records that aren't actually accepting orders
  (resolved-but-not-yet-archived, paused, etc.). We re-check
  ``acceptingOrders`` and ``enableOrderBook`` here.

- We deliberately do NOT filter on ``endDate`` being in the past. The reason:
  some markets stay technically active and tradeable past their stated
  endDate while resolution is being computed. Better to subscribe and let WS
  data tell us when activity actually stops than to second-guess Gamma.

- ``archived=true`` markets are not tradeable; skip them.
"""

from __future__ import annotations

from .models import Market


def is_tradeable_binary_market(market: Market) -> bool:
    """
    Return True if this binary market should be in our active subscription set.

    Required: active, not closed, not archived, accepting orders, has an
    orderbook enabled. Token count is already guaranteed by ``parse_market``.
    """
    if not market.active:
        return False
    if market.closed:
        return False
    if market.archived:
        return False
    if not market.accepting_orders:
        return False
    if not market.enable_order_book:
        return False
    return True


def extract_token_ids(markets: list[Market]) -> set[str]:
    """
    Flatten a list of binary markets into the set of token_ids we'll
    subscribe to. Each market contributes 2 tokens (YES and NO).
    """
    token_ids: set[str] = set()
    for m in markets:
        token_ids.add(m.token_ids[0])
        token_ids.add(m.token_ids[1])
    return token_ids
