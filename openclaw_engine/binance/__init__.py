"""
openclaw_engine/binance/__init__.py

Binance REST API client and data fetching.
Extracted from public_market_data.py.

Preserves:
- All endpoint paths and query params
- HTTP client behavior (requests.get, timeout, headers)
- Concurrent fetch via asyncio.gather where used
- Error handling behavior (exception → empty fallback)

Import: from openclaw_engine.binance import BinanceClient, binance_symbol_snapshot, binance_universe
"""
from .client import BinanceClient
from .snapshots import binance_symbol_snapshot, binance_universe, binance_klines, bybit_symbol_snapshot, coingecko_markets
from .endpoints import BINANCE_REST, BINANCE_WS, ENDPOINT_MAP

__all__ = [
    "BinanceClient",
    "binance_symbol_snapshot",
    "binance_universe",
    "binance_klines",
    "bybit_symbol_snapshot",
    "coingecko_markets",
    "BINANCE_REST",
    "BINANCE_WS",
    "ENDPOINT_MAP",
]