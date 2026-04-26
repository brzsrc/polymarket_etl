"""Polymarket WAL — market discovery + WS capture."""

from .discovery_loop import DiscoveryDiff, DiscoveryLoop
from .gamma_client import GammaClient, GammaError
from .market_discovery import (
    DiscoveryResult,
    MarketsJsonlWriter,
    fetch_all_active_binary_markets,
)
from .market_filter import extract_token_ids, is_tradeable_binary_market
from .models import Market, parse_market
from .ws import (
    ConnectionEvent,
    MessageHandler,
    WSConnection,
    WSPool,
    extract_asset_ids,
)

__all__ = [
    "ConnectionEvent",
    "DiscoveryDiff",
    "DiscoveryLoop",
    "DiscoveryResult",
    "GammaClient",
    "GammaError",
    "Market",
    "MarketsJsonlWriter",
    "MessageHandler",
    "WSConnection",
    "WSPool",
    "extract_asset_ids",
    "extract_token_ids",
    "fetch_all_active_binary_markets",
    "is_tradeable_binary_market",
    "parse_market",
]
