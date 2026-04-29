"""
rsi.py — RSI computation.

Extracted from public_market_data.py:
- features() function lines 8319-8323: RSI(14) from closes

RSI is DISPLAY-ONLY in features() output — does not affect grade/cleanScore/readiness.
The execution filter uses RSI only for signal strength scoring (bull_score),
which IS behavior-critical and preserved.
"""
from typing import List, Optional


def rsi_from_closes(closes: List[float], period: int = 14) -> float:
    """Compute RSI(period) from close prices.

    Returns 0.0 if avg_loss is 0 (no losses = overbought extreme).
    Returns 100.0 if avg_gain is 0 (no gains = oversold extreme).
    """
    if len(closes) < period + 1:
        # Not enough data — return neutral
        return 50.0

    gains: List[float] = []
    losses: List[float] = []
    for idx in range(-period, 0):
        change = closes[idx] - closes[idx - 1]
        if change > 0:
            gains.append(change)
            losses.append(0.0)
        else:
            gains.append(0.0)
            losses.append(-change)

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    if avg_loss == 0:
        return 100.0  # No losses = extremely overbought ( RSI = 100)
    return 100.0 - (100.0 / (1.0 + (avg_gain / avg_loss)))


def rsi_to_signal(rsi: float) -> int:
    """Convert RSI value to directional signal (-1, 0, +1)."""
    if rsi > 52:
        return 1   # Bullish bias
    if rsi < 48:
        return -1  # Bearish bias
    return 0       # Neutral