"""
endpoints.py — Binance/Bybit/CoinGecko endpoint definitions.

Extracted from public_market_data.py:
- ENDPOINT_MAP: lines 546-610
- BINANCE_REST: line 38
- BINANCE_WS: line 39
"""
from typing import Dict, Any

BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/ws"
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
COINGECKO_REST = "https://api.coingecko.com/api/v3"

ENDPOINT_MAP: Dict[str, Dict[str, Any]] = {
    "binance_rest": {
        "exchange_info": "/fapi/v1/exchangeInfo",
        "klines": "/fapi/v1/klines",
        "ticker_24h": "/fapi/v1/ticker/24hr",
        "book_ticker": "/fapi/v1/ticker/bookTicker",
        "depth": "/fapi/v1/depth",
        "agg_trades": "/fapi/v1/aggTrades",
        "premium_index": "/fapi/v1/premiumIndex",
        "funding_rate": "/fapi/v1/fundingRate",
        "open_interest": "/fapi/v1/openInterest",
        "open_interest_hist": "/futures/data/openInterestHist",
        "global_long_short": "/futures/data/globalLongShortAccountRatio",
        "top_long_short_accounts": "/futures/data/topLongShortAccountRatio",
        "top_long_short_positions": "/futures/data/topLongShortPositionRatio",
        "taker_long_short": "/futures/data/takerlongshortRatio",
    },
    "binance_ws": {
        "kline_5m": "<symbol>@kline_5m",
        "kline_15m": "<symbol>@kline_15m",
        "kline_1h": "<symbol>@kline_1h",
        "kline_4h": "<symbol>@kline_4h",
        "mark_price": "<symbol>@markPrice",
        "depth": "<symbol>@depth20@100ms",
        "agg_trade": "<symbol>@aggTrade",
        "force_order": "<symbol>@forceOrder",
    },
    "bybit_rest": {
        "instruments": "/v5/market/instruments-info",
        "kline": "/v5/market/kline",
        "tickers": "/v5/market/tickers",
        "orderbook": "/v5/market/orderbook",
        "recent_trade": "/v5/market/recent-trade",
        "funding_history": "/v5/market/funding/history",
        "open_interest": "/v5/market/open-interest",
    },
    "bybit_ws": {
        "ticker": "tickers.SYMBOL",
        "kline_5": "kline.5.SYMBOL",
        "kline_15": "kline.15.SYMBOL",
        "kline_60": "kline.60.SYMBOL",
        "kline_240": "kline.240.SYMBOL",
        "orderbook": "orderbook.50.SYMBOL",
        "public_trade": "publicTrade.SYMBOL",
    },
    "coingecko_rest": {
        "markets": "/coins/markets",
        "trending": "/search/trending",
        "coin": "/coins/{id}",
        "categories": "/coins/categories",
    },
}

# Filtered-out symbols in universe selection
UNIVERSE_EXCLUDE = {"BTCUSDT", "ETHUSDT", "BNBUSDT", "XAUUSDT", "XAGUSDT", "PAXGUSDT"}

AGG_TRADES_LIMIT = 100
DEFAULT_INTERVALS = ["1m", "5m", "15m", "1h", "4h"]