"""
cvd.py — Cumulative Volume Delta computation.

Extracted from public_market_data.py:
- binance_cvd_summary(): lines 1683-1785
- cvd is used in execution_filter_overlay() for CVD divergence risk flag
- CVD is behavior-critical for risk flag computation

CVD is NOT display-only — it affects riskFlags in execution filter.
"""
from typing import Any, Dict, List, Optional


def cvd_from_agg_trades(agg_trades: Any) -> Dict[str, float]:
    """Compute CVD metrics from agg_trades list.

    Returns a dict with:
    - cvd: cumulative CVD series (last value = cvd_last)
    - delta_ratio: (buy_notional - sell_notional) / total_notional
    - recent_delta_ratio: delta ratio over last 40 trades
    - cvd_slope: cvd_last / trade_count
    """
    rows = agg_trades if isinstance(agg_trades, list) else []
    if not rows:
        return {
            "cvd_last": 0.0,
            "delta_ratio": 0.0,
            "recent_delta_ratio": 0.0,
            "cvd_slope": 0.0,
        }

    buy_notional = 0.0
    sell_notional = 0.0
    signed_notionals: List[float] = []

    for row in rows:
        try:
            price = float(row.get("p"))
            qty = float(row.get("q"))
        except Exception:
            continue
        if price <= 0 or qty <= 0:
            continue
        notional = price * qty
        is_sell_aggressor = bool(row.get("m", False))
        signed_notional = -notional if is_sell_aggressor else notional
        if is_sell_aggressor:
            sell_notional += notional
        else:
            buy_notional += notional
        signed_notionals.append(signed_notional)

    total_notional = buy_notional + sell_notional
    delta_notional = buy_notional - sell_notional
    delta_ratio = 0.0 if total_notional == 0 else delta_notional / total_notional

    recent_n = min(40, len(signed_notionals))
    recent_signed = signed_notionals[-recent_n:] if recent_n else []
    recent_total = sum(abs(value) for value in recent_signed)
    recent_delta = sum(recent_signed)
    recent_delta_ratio = 0.0 if recent_total == 0 else recent_delta / recent_total

    cvd_last = sum(signed_notionals)
    cvd_slope = cvd_last / len(signed_notionals) if signed_notionals else 0.0

    return {
        "cvd_last": cvd_last,
        "delta_ratio": delta_ratio,
        "recent_delta_ratio": recent_delta_ratio,
        "cvd_slope": cvd_slope,
    }


def binance_cvd_summary(agg_trades: Any) -> Dict[str, Any]:
    """Full CVD summary from Binance agg_trades.

    Returns dict matching the original binance_cvd_summary() output shape.
    """
    rows = agg_trades if isinstance(agg_trades, list) else []
    if not rows:
        return {
            "bars": 0, "buyQty": 0.0, "sellQty": 0.0,
            "buyNotional": 0.0, "sellNotional": 0.0,
            "deltaQty": 0.0, "deltaNotional": 0.0,
            "deltaRatio": 0.0, "recentDeltaRatio": 0.0,
            "cvdLast": 0.0, "cvdSlopePerTrade": 0.0,
            "priceChangePct": 0.0, "divergence": None,
            "bias": "neutral", "efficiency": 0.0,
        }

    buy_qty = 0.0
    sell_qty = 0.0
    buy_notional = 0.0
    sell_notional = 0.0
    signed_notionals: List[float] = []
    signed_qtys: List[float] = []
    prices: List[float] = []
    cumulative = 0.0

    for row in rows:
        try:
            price = float(row.get("p"))
            qty = float(row.get("q"))
        except Exception:
            continue
        if price <= 0 or qty <= 0:
            continue
        notional = price * qty
        is_sell_aggressor = bool(row.get("m", False))
        signed_qty = -qty if is_sell_aggressor else qty
        signed_notional = -notional if is_sell_aggressor else notional
        if is_sell_aggressor:
            sell_qty += qty
            sell_notional += notional
        else:
            buy_qty += qty
            buy_notional += notional
        cumulative += signed_notional
        signed_qtys.append(signed_qty)
        signed_notionals.append(signed_notional)
        prices.append(price)

    total_notional = buy_notional + sell_notional
    total_qty = buy_qty + sell_qty
    delta_notional = buy_notional - sell_notional
    delta_qty = buy_qty - sell_qty
    delta_ratio = 0.0 if total_notional == 0 else delta_notional / total_notional

    recent_n = min(40, len(signed_notionals))
    recent_signed = signed_notionals[-recent_n:] if recent_n else []
    recent_total = sum(abs(value) for value in recent_signed)
    recent_delta = sum(recent_signed)
    recent_delta_ratio = 0.0 if recent_total == 0 else recent_delta / recent_total

    cvd_last = cumulative
    cvd_slope = cvd_last / len(signed_notionals) if signed_notionals else 0.0

    price_change_pct = 0.0
    if len(prices) >= 2 and prices[0] != 0:
        price_change_pct = ((prices[-1] - prices[0]) / prices[0]) * 100

    path = sum(abs(value) for value in signed_notionals)
    efficiency = 0.0 if path == 0 else abs(delta_notional) / path

    return {
        "bars": len(rows),
        "buyQty": buy_qty,
        "sellQty": sell_qty,
        "buyNotional": buy_notional,
        "sellNotional": sell_notional,
        "deltaQty": delta_qty,
        "deltaNotional": delta_notional,
        "deltaRatio": delta_ratio,
        "recentDeltaRatio": recent_delta_ratio,
        "cvdLast": cvd_last,
        "cvdSlopePerTrade": cvd_slope,
        "priceChangePct": price_change_pct,
        "divergence": None,
        "bias": "neutral",
        "efficiency": efficiency,
    }