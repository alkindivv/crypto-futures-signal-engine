"""
clean.py — Bull score and clean score computation.

Extracted from public_market_data.py:
- _score_symbol() bull_score computation: lines 8345-8353
- _score_symbol() base clean_score computation: lines 8354-8359
- news/social penalties: lines 8370-8373

BEHAVIOR-CRITICAL:
- bull_score is the primary directional signal for grading and readiness
- clean_score feeds directly into grade thresholds (88, 93, 97)
- News/social penalties only apply when prod_mode=False (skipped in production)
"""
from typing import Any, Dict, List

# RSI thresholds for bull_score bias
RSI_ABOVE = 52
RSI_BELOW = 48


def bull_score_from_features(f: Dict[str, Dict[str, float]]) -> int:
    """Compute bull_score from multi-interval features.

    bull_score = sum of (above20 + above50) for 15m/1h/4h
               + RSI bias for 15m/1h/4h (+1 if >52, -1 if <48, else 0)

    Range: approximately -15 to +15
    Direction: >= 2 = long, <= -2 = short, else watch
    """
    score = 0
    for interval in ["15m", "1h", "4h"]:
        if interval in f:
            score += int(f[interval].get("above20", 0))
            score += int(f[interval].get("above50", 0))
            rsi = f[interval].get("rsi", 50)
            if rsi > RSI_ABOVE:
                score += 1
            elif rsi < RSI_BELOW:
                score -= 1
    return score


def clean_score_from_market(
    spread_bps: float,
    funding: float,
    oi_change_pct: float,
    vol_15m: float,
    vol_1h: float,
) -> float:
    """Compute base clean_score from market metrics.

    Starts at 100.0.
    Penalties: spread, funding, OI change
    Bonuses: volume ratio
    Clamped to [0.0, 100.0]
    """
    clean = 100.0
    clean -= min(spread_bps * 2.5, 40)
    clean -= 20 if abs(funding) > 0.0008 else 0
    clean -= 15 if abs(oi_change_pct) > 20 else 0
    clean += min((vol_15m + vol_1h) * 5, 10)
    return max(0.0, min(100.0, clean))


def clean_score_with_news(
    base_clean: float,
    headline_flags: Dict[str, Any],
    prod_mode: bool = False,
) -> float:
    """Apply news/social keyword penalties to clean_score.

    In prod_mode=True (production scan): this step is SKIPPED.
    In prod_mode=False (research/snapshot): negative keywords penalize,
    positive keywords add small bonus.
    """
    if prod_mode:
        return base_clean

    if not isinstance(headline_flags, dict):
        return base_clean

    negative_count = int(headline_flags.get("negativeCount", 0))
    positive_count = int(headline_flags.get("positiveCount", 0))

    clean = base_clean
    clean -= min(negative_count * 4, 12)
    clean += min(positive_count * 2, 6)
    return max(0.0, min(100.0, clean))