"""
openclaw_engine/indicators/__init__.py

Indicator computations extracted from public_market_data.py.
All indicators are DISPLAY-ONLY — do not influence grade, cleanScore, or readiness
unless explicitly called out in the execution filter layer.

Extracted from:
- features() function: public_market_data.py lines 8295-8335 (EMA, RSI, position, volume)
- MACD/ATR/VWAP block: lines 7920-8020 (MACD, ATR, VWAP computation + warnings)
- binance_cvd_summary(): lines 1683-1780 (CVD from agg_trades)
- _atr_stop: lines 8144-8148 (ATR-based stop buffer)

Import: from openclaw_engine.indicators import ema, rsi, atr, cvd, features
"""
from .moving_average import ema, sma, moving_average_converge
from .rsi import rsi_from_closes
from .atr import atr14_from_klines, atr_stop_buffer
from .cvd import cvd_from_agg_trades, binance_cvd_summary
from .features import features_from_klines

__all__ = [
    "ema",
    "sma",
    "moving_average_converge",
    "rsi_from_closes",
    "atr14_from_klines",
    "atr_stop_buffer",
    "cvd_from_agg_trades",
    "binance_cvd_summary",
    "features_from_klines",
]