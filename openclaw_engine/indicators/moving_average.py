"""
moving_average.py — Pure moving average computations.

Extracted from public_market_data.py:
- _ema (line 8303): true exponential smoothing (mult = 2.0/(period+1))
- _ema (line 7933): true exponential smoothing for MACD (same formula)
- EMA fallback (line 8312-8313): if NEW_INDICATOR_ENABLED['ema']=False, use SMA

IMPORTANT: These are DISPLAY-ONLY. They do NOT influence grade/cleanScore/readiness
unless explicitly used in the execution filter layer.
"""
from typing import List, Optional


def ema(vals: List[float], period: int) -> Optional[float]:
    """True exponential moving average (Wilder smoothing).

    Formula: mult = 2.0 / (period + 1)
    Seeded with SMA for first period values.

    This is the production EMA — do NOT replace with simple moving average.
    The SMA fallback exists only as a rollback when NEW_INDICATOR_ENABLED['ema']=False.
    """
    if len(vals) < period:
        return None
    mult = 2.0 / (period + 1)
    ema_val = sum(vals[:period]) / period  # seed with SMA
    for v in vals[period:]:
        ema_val = (v - ema_val) * mult + ema_val
    return ema_val


def sma(vals: List[float], period: int) -> Optional[float]:
    """Simple moving average (arithmetic mean).

    Used as fallback when NEW_INDICATOR_ENABLED['ema']=False.
    """
    if len(vals) < period:
        return None
    return sum(vals[-period:]) / period


def moving_average_converge(closes: List[float], fast_period: int = 12, slow_period: int = 26) -> Optional[float]:
    """MACD line = EMA(fast) - EMA(slow). Returns MACD value or None."""
    if len(closes) < slow_period:
        return None
    ema_fast = ema(closes, fast_period)
    ema_slow = ema(closes, slow_period)
    if ema_fast is None or ema_slow is None:
        return None
    return ema_fast - ema_slow


def macd_signal_line(macd_series: List[float], signal_period: int = 9) -> Optional[float]:
    """Compute MACD signal line = EMA of MACD series."""
    if len(macd_series) < signal_period:
        return None
    mult = 2.0 / (signal_period + 1)
    sig = sum(macd_series[:signal_period]) / signal_period
    for val in macd_series[signal_period:]:
        sig = (val - sig) * mult + sig
    return sig