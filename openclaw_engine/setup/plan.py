"""
plan.py — Actionable setup plan computation.

Extracted from public_market_data.py:
- price_decimals(): lines 8063-8078
- round_price(): lines 8079-8082
- normalize_take_profit_ladder(): lines 8085-8092
- actionable_setup_plan(): lines 8093-8218
- _build_crowding_signals(): lines 8220-8255

BEHAVIOR-CRITICAL:
- Entry zone determines whether price is "in zone" for READY_IN_ZONE delivery
- doNotChase flag determines ENTRY_NOW vs wait_bounce vs ready_on_retest
- Stop loss and TP are used in notification rendering
- ATR supertrend is display-only (NEW_INDICATOR_ENABLED['supertrend'])
"""
from typing import Any, Dict, Optional, Tuple

# Supertrend ATR multiplier — mirrors NEW_INDICATOR_ENABLED['supertrend']
NEW_INDICATOR_ENABLED = {
    "supertrend": False,
}

# ATR price ratio max (from scoring/execution.py)
ATR_PRICE_RATIO_MAX = 0.05  # 5%


def price_decimals(price: Optional[float]) -> int:
    """Determine decimal precision for a price."""
    value = abs(float(price or 0.0))
    if value >= 1000:
        return 1
    if value >= 100:
        return 2
    if value >= 10:
        return 3
    if value >= 1:
        return 4
    if value >= 0.1:
        return 5
    if value >= 0.01:
        return 6
    return 7


def round_price(price: Optional[float], reference: Optional[float] = None) -> Optional[float]:
    """Round price to appropriate decimal places based on reference price."""
    if price is None:
        return None
    ref = reference if reference not in {None, 0} else price
    return round(float(price), price_decimals(ref))


def normalize_take_profit_ladder(side: str, tp1: float, tp2: float, tp3: float) -> Tuple[float, float, float]:
    """Normalize TP values so they are monotonically ordered by side.

    Short: highest value first (descending)
    Long: lowest value first (ascending)
    """
    values = [float(tp1), float(tp2), float(tp3)]
    if str(side or "").lower().strip() == "short":
        values = sorted(values, reverse=True)
    else:
        values = sorted(values)
    return values[0], values[1], values[2]


def _atr_stop_buffer(ref_range: float, atr_14: Optional[float], multiplier: float = 2.0) -> float:
    """Return max of range-based buffer or ATR-based buffer (Supertrend standard)."""
    range_buffer = ref_range * 0.18
    if atr_14 is not None and atr_14 > 0:
        return max(range_buffer, multiplier * atr_14)
    return range_buffer


def _build_crowding_signals(execution: Dict[str, Any], side: str) -> Dict[str, Any]:
    """Build crowding/detection signals from top trader L/S data."""
    ratios = execution.get("ratios", {}) if isinstance(execution, dict) else {}
    top_positions_ratio = ratios.get("topPositions")
    top_accounts_ratio = ratios.get("topAccounts")
    global_ratio = ratios.get("globalLongShort")

    def safe_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    top_pos = safe_float(top_positions_ratio)
    top_acc = safe_float(top_accounts_ratio)
    glob = safe_float(global_ratio)
    del top_acc, glob  # unused but preserved from source

    # Dominant side of top traders
    if top_pos is not None:
        if top_pos > 1.5:
            top_trader_bias = "long_crowded"
        elif top_pos < 0.67:
            top_trader_bias = "short_crowded"
        else:
            top_trader_bias = "balanced"
    else:
        top_trader_bias = "unknown"

    # Squeeze risk for this setup's side
    squeeze_risk = "none"
    if side == "short":
        if top_pos is not None and top_pos > 2.0:
            squeeze_risk = "high"
        elif top_pos is not None and top_pos > 1.5:
            squeeze_risk = "medium"
    elif side == "long":
        if top_pos is not None and top_pos < 0.5:
            squeeze_risk = "high"
        elif top_pos is not None and top_pos < 0.67:
            squeeze_risk = "medium"

    return {
        "topTraderBias": top_trader_bias,
        "squeezeRisk": squeeze_risk,
    }


def actionable_setup_plan(row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Build complete actionable setup plan from a scored row.

    Returns entry zone, stop loss, TP1/TP2/TP3, trigger/invalidation text,
    doNotChase flag, and execution notes.

    Returns None if:
    - direction is not long or short
    - grade is NO_TRADE
    - disqualified
    - no valid price

    Key behaviors:
    - doNotChase=True when price is too deep in range (loc15/loc1h thresholds)
    - ATR supertrend buffer used when NEW_INDICATOR_ENABLED['supertrend']
    - TP values normalized per side (ascending for long, descending for short)
    """
    side = str(row.get("direction") or "").lower().strip()
    if side not in {"long", "short"}:
        return None
    if row.get("grade") == "NO_TRADE" or row.get("disqualified"):
        return None

    features = row.get("features", {}) if isinstance(row.get("features"), dict) else {}
    execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
    m15 = features.get("15m", {}) if isinstance(features.get("15m"), dict) else {}
    h1 = features.get("1h", {}) if isinstance(features.get("1h"), dict) else {}
    last = float(m15.get("last") or row.get("lastPrice") or 0.0)
    if last <= 0:
        return None

    m15_high = float(m15.get("recentHigh") or last)
    m15_low = float(m15.get("recentLow") or last)
    h1_high = float(h1.get("recentHigh") or m15_high)
    h1_low = float(h1.get("recentLow") or m15_low)
    swing_high = max(m15_high, h1_high, last)
    swing_low = min(m15_low, h1_low, last)
    range_15 = max(m15_high - m15_low, last * 0.0025)
    range_1h = max(h1_high - h1_low, last * 0.004)
    ref_cap = max(range_15 * 2.5, last * 0.02)
    ref_range = min(max(range_15, range_1h * 0.3, last * 0.003), ref_cap)

    recent_location = execution.get("recentLocation", {}) if isinstance(execution.get("recentLocation"), dict) else {}
    loc15 = recent_location.get("m15")
    loc1h = recent_location.get("h1")
    risk_flags = list(execution.get("riskFlags", []) or [])
    management = list(row.get("managementGuidance", []) or [])

    # ─── Supertrend ATR stop zone ──────────────────────────────────────────
    atr_14 = None
    supertrend_note = None
    if NEW_INDICATOR_ENABLED.get("supertrend", False):
        try:
            atr_14 = execution.get("atr14")
            atr_warn = execution.get("_indicatorFlags", {}).get("atrWarn", False)
            if atr_14 is not None and atr_14 > 0 and last > 0:
                atr_ratio = atr_14 / last
                if atr_ratio <= ATR_PRICE_RATIO_MAX:
                    supertrend_note = f"ATR(14)={atr_14:.4f} stop buffer active"
                    if atr_warn:
                        supertrend_note += " [check volatility]"
        except Exception:
            pass

    # ─── doNotChase detection ─────────────────────────────────────────────
    late_short = side == "short" and (
        "late_short_location" in risk_flags
        or (loc15 is not None and float(loc15) <= 0.18)
        or (loc1h is not None and float(loc1h) <= 0.12)
    )
    late_long = side == "long" and (
        "late_long_location" in risk_flags
        or (loc15 is not None and float(loc15) >= 0.82)
        or (loc1h is not None and float(loc1h) >= 0.88)
    )
    do_not_chase = late_short or late_long

    # ─── Entry zone, stop loss, TP ─────────────────────────────────────────
    if side == "short":
        entry_style = "short on bounce" if do_not_chase else "short on rejection"
        entry_low = last + (ref_range * 0.18 if do_not_chase else -ref_range * 0.03)
        entry_high = last + (ref_range * 0.5 if do_not_chase else ref_range * 0.18)
        stop_buffer = _atr_stop_buffer(ref_range, atr_14, 2.0)
        stop_loss = max(m15_high, entry_high) + stop_buffer
        raw_tp1 = min(last - ref_range * 0.35, max(0.0, swing_low + ref_range * 0.04))
        raw_tp2 = max(0.0, last - ref_range * 0.75)
        raw_tp3 = max(0.0, last - ref_range * 1.15)
        trigger = (
            "Tunggu bounce ke entry zone lalu cari rejection candle 5m/15m"
            if do_not_chase
            else "Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah"
        )
        invalidation = (
            "Batalkan short bila 15m close kuat di atas stop atau reclaim high lokal bertahan"
        )
        current_status = "wait_bounce" if do_not_chase else "ready_on_retest"
    else:
        entry_style = "long on dip" if do_not_chase else "long on reclaim"
        entry_low = last - (ref_range * 0.5 if do_not_chase else ref_range * 0.18)
        entry_high = last + (ref_range * 0.03 if do_not_chase else ref_range * 0.12)
        stop_buffer = _atr_stop_buffer(ref_range, atr_14, 2.0)
        stop_loss = min(m15_low, entry_low) - stop_buffer
        raw_tp1 = max(last + ref_range * 0.35, swing_high - ref_range * 0.04)
        raw_tp2 = last + ref_range * 0.75
        raw_tp3 = last + ref_range * 1.15
        trigger = (
            "Tunggu dip ke entry zone lalu cari reclaim candle 5m/15m"
            if do_not_chase
            else "Eksekusi hanya jika hold area entry tetap kuat dan reclaim terjaga"
        )
        invalidation = (
            "Batalkan long bila 15m close kuat di bawah stop atau breakdown low lokal bertahan"
        )
        current_status = "wait_pullback" if do_not_chase else "ready_on_retest"

    tp1, tp2, tp3 = normalize_take_profit_ladder(side, raw_tp1, raw_tp2, raw_tp3)

    if entry_low > entry_high:
        entry_low, entry_high = entry_high, entry_low

    execution_note = (
        management[0]
        if management
        else "Ambil partial di TP1 atau sekitar +1R, lalu geser stop ke break-even bila tape tetap mendukung"
    )
    if do_not_chase:
        execution_note = "Jangan entry market sekarang, tunggu retest ke zona entry yang lebih bersih"

    return {
        "currentPrice": round_price(last, reference=last),
        "entryStyle": entry_style,
        "entryZone": {
            "low": round_price(entry_low, reference=last),
            "high": round_price(entry_high, reference=last),
        },
        "stopLoss": round_price(stop_loss, reference=last),
        "takeProfit": {
            "tp1": round_price(tp1, reference=last),
            "tp2": round_price(tp2, reference=last),
            "tp3": round_price(tp3, reference=last),
        },
        "trigger": trigger,
        "invalidation": invalidation,
        "doNotChase": do_not_chase,
        "currentStatus": current_status,
        "executionNote": execution_note,
        "supertrendNote": supertrend_note,
        "riskFlags": risk_flags,
        "crowdingSignals": _build_crowding_signals(execution, side),
    }