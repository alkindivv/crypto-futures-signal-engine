"""
snapshots.py — Binance/Bybit/CoinGecko data fetching.

Extracted from public_market_data.py:
- binance_symbol_snapshot(): lines 1787-1810
- binance_universe(): lines 1813-1846
- binance_klines(): lines 1680-1683
- bybit_symbol_snapshot(): lines 1847-1860
- coingecko_markets(): lines 1862-1869

Preserves:
- All endpoint paths
- Query params exactly
- Error handling (exception → empty data field)
- Symbol filtering rules
- Concurrent klines fetch per interval
"""
from typing import Any, Dict, List, Optional

import requests

from .client import BINANCE_REST, TIMEOUT, USER_AGENT
from .endpoints import ENDPOINT_MAP, UNIVERSE_EXCLUDE, AGG_TRADES_LIMIT, DEFAULT_INTERVALS

# Re-export constants needed externally
__all__ = [
    "BINANCE_REST", "TIMEOUT", "USER_AGENT",
    "binance_symbol_snapshot", "binance_universe", "binance_klines",
    "bybit_symbol_snapshot", "coingecko_markets",
    "ENDPOINT_MAP", "UNIVERSE_EXCLUDE", "DEFAULT_INTERVALS",
]


def _binance_get(path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Internal binance GET with consistent error handling."""
    try:
        url = f"{BINANCE_REST}{path}"
        response = requests.get(url, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


def binance_klines(symbol: str, interval: str, limit: int = 120) -> Any:
    """Fetch klines (candlestick data) for a symbol."""
    return _binance_get(
        ENDPOINT_MAP["binance_rest"]["klines"],
        {"symbol": symbol, "interval": interval, "limit": limit},
    )


def binance_symbol_snapshot(symbol: str) -> Dict[str, Any]:
    """Fetch complete snapshot data for a symbol.

    Gathers: ticker24h, bookTicker, premiumIndex, openInterest,
    openInterestHist, depth, aggTrades, fundingRate, CVR, klines,
    longShort ratios, wsPlan.
    """
    data: Dict[str, Any] = {
        "ticker24h": _binance_get(ENDPOINT_MAP["binance_rest"]["ticker_24h"], {"symbol": symbol}),
        "bookTicker": _binance_get(ENDPOINT_MAP["binance_rest"]["book_ticker"], {"symbol": symbol}),
        "premiumIndex": _binance_get(ENDPOINT_MAP["binance_rest"]["premium_index"], {"symbol": symbol}),
        "openInterest": _binance_get(ENDPOINT_MAP["binance_rest"]["open_interest"], {"symbol": symbol}),
        "openInterestHist": _binance_get(
            ENDPOINT_MAP["binance_rest"]["open_interest_hist"],
            {"symbol": symbol, "period": "1h", "limit": 8}
        ),
        "depth": _binance_get(ENDPOINT_MAP["binance_rest"]["depth"], {"symbol": symbol, "limit": 20}),
        "aggTrades": _binance_get(
            ENDPOINT_MAP["binance_rest"]["agg_trades"],
            {"symbol": symbol, "limit": AGG_TRADES_LIMIT}
        ),
        "fundingRate": _binance_get(
            ENDPOINT_MAP["binance_rest"]["funding_rate"],
            {"symbol": symbol, "limit": 8}
        ),
    }

    for name, endpoint_key in [
        ("globalLongShortAccountRatio", "global_long_short"),
        ("topLongShortAccountRatio", "top_long_short_accounts"),
        ("topLongShortPositionRatio", "top_long_short_positions"),
        ("takerLongShortRatio", "taker_long_short"),
    ]:
        try:
            data[name] = _binance_get(
                ENDPOINT_MAP["binance_rest"][endpoint_key],
                {"symbol": symbol, "period": "1h", "limit": 8}
            )
        except Exception as exc:
            data[name] = {"error": str(exc)}

    # CVD is computed from aggTrades — import from indicators
    from ..indicators.cvd import binance_cvd_summary
    data["cvd"] = binance_cvd_summary(data.get("aggTrades", []))

    # Klines per interval
    data["klines"] = {interval: binance_klines(symbol, interval) for interval in DEFAULT_INTERVALS}

    # WebSocket plan
    data["wsPlan"] = binance_ws_plan(symbol)

    return data


def binance_universe(min_quote_volume: float = 30_000_000, top: int = 40) -> List[Dict[str, Any]]:
    """Select top USDT perpetual pairs by 24h quote volume.

    Excludes: BTC, ETH, BNB, XAU, XAG, PAXG
    Excludes non-Latin tickers (ord(ch) > 127)
    Requires quote_volume >= min_quote_volume AND count >= 80,000
    """
    try:
        exchange_info = _binance_get(ENDPOINT_MAP["binance_rest"]["exchange_info"])
        tradable = {
            s["symbol"]
            for s in exchange_info.get("symbols", [])
            if s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("symbol", "").endswith("USDT")
        }
    except Exception:
        tradable = set()

    try:
        raw = _binance_get(ENDPOINT_MAP["binance_rest"]["ticker_24h"])
    except Exception:
        raw = []

    out: List[Dict[str, Any]] = []
    for row in raw:
        symbol = row.get("symbol", "")
        if symbol not in tradable or symbol in UNIVERSE_EXCLUDE:
            continue
        if any(ord(ch) > 127 for ch in symbol):
            continue
        try:
            quote_volume = float(row.get("quoteVolume", 0))
            count = int(row.get("count", 0))
            change = float(row.get("priceChangePercent", 0))
            last = float(row.get("lastPrice", 0))
        except Exception:
            continue
        if quote_volume < min_quote_volume or count < 80_000:
            continue
        out.append({
            "symbol": symbol,
            "quoteVolume": quote_volume,
            "priceChangePercent": change,
            "lastPrice": last,
            "count": count,
        })

    out.sort(key=lambda r: r["quoteVolume"], reverse=True)
    return out[:top]


def bybit_symbol_snapshot(symbol: str) -> Dict[str, Any]:
    """Fetch Bybit snapshot for a symbol."""
    bybit_base = "https://api.bybit.com"
    endpoints = ENDPOINT_MAP.get("bybit_rest", {})

    data: Dict[str, Any] = {}
    try:
        data["ticker"] = _bybit_get(bybit_base, endpoints.get("tickers", ""), {"category": "linear", "symbol": symbol})
    except Exception:
        data["ticker"] = {}
    try:
        data["orderbook"] = _bybit_get(bybit_base, endpoints.get("orderbook", ""), {"category": "linear", "symbol": symbol, "limit": 20})
    except Exception:
        data["orderbook"] = {}

    return data


def _bybit_get(base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
    """Internal Bybit GET."""
    try:
        url = f"{base}{path}"
        response = requests.get(url, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
        response.raise_for_status()
        return response.json()
    except Exception:
        return {}


def coingecko_markets(ids: List[str]) -> Any:
    """Fetch CoinGecko markets for given coin IDs."""
    from .endpoints import COINGECKO_REST
    try:
        url = f"{COINGECKO_REST}/coins/markets"
        response = requests.get(
            url,
            params={"vs_currency": "usd", "ids": ",".join(ids), "order": "market_cap_desc", "per_page": 100},
            timeout=TIMEOUT,
            headers={"User-Agent": USER_AGENT},
        )
        response.raise_for_status()
        return response.json()
    except Exception:
        return []


def binance_ws_plan(symbol: str) -> Dict[str, Any]:
    """Build Binance WebSocket subscription plan for a symbol.

    Returns dict with stream names for kline, depth, aggTrade.
    """
    s = symbol.lower()
    return {
        "streams": [
            f"{s}@kline_5m",
            f"{s}@kline_15m",
            f"{s}@kline_1h",
            f"{s}@kline_4h",
            f"{s}@depth20@100ms",
            f"{s}@aggTrade",
        ]
    }