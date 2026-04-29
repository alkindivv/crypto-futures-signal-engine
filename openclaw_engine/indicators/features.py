"""
features.py — Multi-interval feature extraction.

Extracted from public_market_data.py:
- features() function: lines 8295-8335
- Called from binance_symbol_snapshot() to build features per interval

Intervals used: 1m, 5m, 15m, 1h, 4h
Features: last, recentHigh, recentLow, above20EMA, above50EMA, rsi, volume ratio

BEHAVIOR-CRITICAL: These features are used in:
1. bull_score computation (above20/above50 across timeframes + RSI bias)
2. clean_score computation (volume ratio)
3. Display notes for notifications

This is NOT display-only — the output feeds scoring and grading.
"""
from typing import Any, Dict, List

from .moving_average import ema
from .rsi import rsi_from_closes

DEFAULT_INTERVALS = ["1m", "5m", "15m", "1h", "4h"]


def features_from_klines(klines: Dict[str, List[List[Any]]]) -> Dict[str, Dict[str, float]]:
    """Compute features across multiple intervals.

    For each interval, computes: last, recentHigh, recentLow,
    above20 (1 if price > EMA20 else -1), above50, rsi, vol_ratio.

    This is called per symbol per snapshot — behavior-critical for scoring.
    """
    f: Dict[str, Dict[str, float]] = {}

    for interval in DEFAULT_INTERVALS:
        rows = klines.get(interval, [])
        if not rows:
            f[interval] = {
                "last": 0.0, "recentHigh": 0.0, "recentLow": 0.0,
                "above20": 0.0, "above50": 0.0, "rsi": 50.0, "pos": 0.5, "vol": 1.0,
            }
            continue

        closes = [float(r[4]) for r in rows]
        highs = [float(r[2]) for r in rows]
        lows = [float(r[3]) for r in rows]
        vols = [float(r[5]) for r in rows]
        last = closes[-1]

        # EMA
        ema20_val = ema(closes, 20)
        ema50_val = ema(closes, 50)
        above20 = 1.0 if (ema20_val is not None and last > ema20_val) else -1.0
        above50 = 1.0 if (ema50_val is not None and last > ema50_val) else -1.0

        # RSI
        rsi_val = rsi_from_closes(closes, 14)

        # Recent high/low (24 bars)
        recent_high = max(highs[-24:]) if len(highs) >= 24 else max(highs)
        recent_low = min(lows[-24:]) if len(lows) >= 24 else min(lows)

        # Position in recent range (0-1, 0.5 = midpoint)
        pos = (last - recent_low) / (recent_high - recent_low + 1e-12)

        # Volume ratio vs 20-bar average
        vol_avg = sum(vols[-20:]) / 20 if len(vols) >= 20 else sum(vols) / max(1, len(vols))
        vol_ratio = vols[-1] / (vol_avg + 1e-12)

        f[interval] = {
            "last": last,
            "recentHigh": recent_high,
            "recentLow": recent_low,
            "above20": above20,
            "above50": above50,
            "rsi": rsi_val,
            "pos": pos,
            "vol": vol_ratio,
        }

    return f


def features_to_bull_score(f: Dict[str, Dict[str, float]]) -> int:
    """Convert multi-interval features to bull_score (directional strength).

    Sums: above20 across 15m/1h/4h + above50 across 15m/1h/4h + RSI biases.
    This is the DIRECT scoring path — behavior-critical.
    """
    score = 0
    for interval in ["15m", "1h", "4h"]:
        if interval in f:
            score += int(f[interval].get("above20", 0))
            score += int(f[interval].get("above50", 0))
            rsi = f[interval].get("rsi", 50)
            if rsi > 52:
                score += 1
            elif rsi < 48:
                score -= 1
    return score


def features_to_clean_score(f: Dict[str, Dict[str, float]], spread_bps: float, funding: float, oi_change_pct: float) -> float:
    """Compute clean_score from features + market conditions.

    Starts at 100.0, subtracts penalties for spread/funding/OI,
    adds volume bonus, clamps to [0, 100].
    """
    clean = 100.0
    clean -= min(spread_bps * 2.5, 40)
    clean -= 20 if abs(funding) > 0.0008 else 0
    clean -= 15 if abs(oi_change_pct) > 20 else 0
    vol_sum = (f.get("15m", {}).get("vol", 1.0) + f.get("1h", {}).get("vol", 1.0))
    clean += min(vol_sum * 5, 10)
    return max(0.0, min(100.0, clean))