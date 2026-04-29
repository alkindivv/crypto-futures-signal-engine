"""
atr.py — Average True Range computation.

Extracted from public_market_data.py:
- ATR computation: lines 7961-7970 (TR from 1h klines)
- _atr_stop buffer: lines 8144-8148 (max of range-based or ATR-based buffer)
- atr_stop_multiplier = 2.0 (Supertrend standard)

ATR is behavior-critical — used in:
- Warning detection (atr_warn if atr_14/price > threshold)
- Stop loss buffer computation in actionable setup plan
"""
from typing import List, Optional


def atr14_from_klines(c: List[float], h: List[float], l: List[float]) -> Optional[float]:
    """Compute ATR(14) from 1h klines close/high/low.

    Returns None if insufficient data (< 15 bars).
    """
    if len(c) < 15:
        return None

    trs: List[float] = []
    for i in range(-14, 0):
        hl = h[i] - l[i]
        hc = abs(h[i] - c[i - 1])
        lc = abs(l[i] - c[i - 1])
        trs.append(max(hl, hc, lc))

    if not trs:
        return None
    return sum(trs) / len(trs)


def atr_stop_buffer(buffer_abs: float, atr: Optional[float], multiplier: float = 2.0) -> float:
    """Return max of range-based buffer or ATR-based buffer.

    Used in actionable setup plan for stop loss computation.
    multiplier=2.0 is the standard Supertrend multiplier.
    """
    if atr is not None and atr > 0:
        return max(buffer_abs, multiplier * atr)
    return buffer_abs