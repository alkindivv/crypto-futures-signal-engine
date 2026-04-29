"""
execution.py — Execution filter overlay.

Extracted from public_market_data.py:
- execution_filter_overlay(): lines 7633-8056
- liquidation_overlay(): lines 7389-7471
- latest_ratio_value(): lines 7473-7486
- ATR_PRICE_RATIO_MAX: line 260
- MACD_PRICE_RATIO_MAX: line 262
- VWAP_PRICE_DIVERGENCE_MAX: line 264

BEHAVIOR-CRITICAL:
- riskFlags directly affect grading (is_hard_disqualified uses them)
- cleanScoreDelta directly modifies clean_score before grading
- Side-specific (long vs short vs watch) bias in CVD and crowding checks

This is NOT display-only — it changes grades, not just notes.
"""
from typing import Any, Dict, List, Optional

# Indicator warning thresholds
MACD_PRICE_RATIO_MAX = 0.01      # 1% of price
ATR_PRICE_RATIO_MAX = 0.05       # 5% of price
VWAP_PRICE_DIVERGENCE_MAX = 0.05  # 5%

# New indicators feature flag — mirrors NEW_INDICATOR_ENABLED in source
NEW_INDICATOR_ENABLED = {
    "macd": False,
    "atr": False,
    "vwap": False,
    "ema": True,
}


def latest_ratio_value(payload: Any, keys: List[str]) -> Optional[float]:
    """Extract a float ratio value from a ratio payload (e.g. longShortRatio).

    Returns the last available value for the first matching key.
    """
    rows = payload if isinstance(payload, list) else []
    if not rows:
        return None
    row = rows[-1]
    if not isinstance(row, dict):
        return None
    for key in keys:
        try:
            if row.get(key) is not None:
                return float(row.get(key))
        except Exception:
            continue
    return None


# Placeholder for liquidation state path — set at module level by caller
_liquidation_state_path: Optional[str] = None


def set_liquidation_state_path(path: str) -> None:
    global _liquidation_state_path
    _liquidation_state_path = path


def execution_filter_overlay(
    symbol: str,
    bull_score: int,
    snapshot: Dict[str, Any],
    execution_feedback: Optional[Dict[str, Any]] = None,
    liquidation_state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Compute execution filter overlay for a symbol.

    Adds cleanScoreDelta (added to clean_score before grading) and riskFlags.
    Side-specific logic for long vs short vs watch.
    New indicators (MACD, ATR, VWAP) stored in output but NEVER affect grading.
    """
    clean_delta = 0.0
    notes: List[str] = []
    management: List[str] = []
    risk_flags: List[str] = []
    side = "long" if bull_score >= 2 else "short" if bull_score <= -2 else "watch"

    binance = snapshot.get("binance", {}) if isinstance(snapshot, dict) else {}
    ticker24h = binance.get("ticker24h", {}) if isinstance(binance, dict) else {}
    depth = binance.get("depth", {}) if isinstance(binance, dict) else {}
    agg_trades = binance.get("aggTrades", []) if isinstance(binance, dict) else []
    book = binance.get("bookTicker", {}) if isinstance(binance, dict) else {}
    premium = binance.get("premiumIndex", {}) if isinstance(binance, dict) else {}
    open_interest = binance.get("openInterest", {}) if isinstance(binance, dict) else {}
    oi_hist = binance.get("openInterestHist", []) if isinstance(binance, dict) else []
    cvd = binance.get("cvd", {}) if isinstance(binance, dict) else {}
    klines = binance.get("klines", {}) if isinstance(binance, dict) else {}

    # ─── Base market metrics ────────────────────────────────────────────────
    try:
        bid = float(book.get("bidPrice", 0))
        ask = float(book.get("askPrice", 0))
        mid = (bid + ask) / 2 if bid and ask else 0.0
        spread_bps = 0.0 if mid == 0 else ((ask - bid) / mid) * 10000
    except Exception:
        spread_bps = 0.0

    try:
        quote_volume = float(ticker24h.get("quoteVolume", 0))
    except Exception:
        quote_volume = 0.0

    try:
        trade_count = int(ticker24h.get("count", 0))
    except Exception:
        trade_count = 0

    try:
        funding = float(premium.get("lastFundingRate", 0))
    except Exception:
        funding = 0.0

    try:
        oi_now = float(open_interest.get("openInterest", 0))
        oi_old = float(oi_hist[0].get("sumOpenInterest", 0)) if isinstance(oi_hist, list) and oi_hist else 0.0
        oi_change_pct = 0.0 if oi_old == 0 else ((oi_now - oi_old) / oi_old) * 100
    except Exception:
        oi_change_pct = 0.0

    # ─── Depth ────────────────────────────────────────────────────────────
    bid_notional = 0.0
    ask_notional = 0.0
    try:
        bids = depth.get("bids", []) if isinstance(depth, dict) else []
        asks = depth.get("asks", []) if isinstance(depth, dict) else []
        bid_notional = sum(float(price) * float(size) for price, size in bids[:10])
        ask_notional = sum(float(price) * float(size) for price, size in asks[:10])
    except Exception:
        pass
    depth_total = bid_notional + ask_notional
    depth_imbalance = 0.0 if depth_total == 0 else (bid_notional - ask_notional) / depth_total

    # ─── Aggression ────────────────────────────────────────────────────────
    buy_aggression = 0.0
    try:
        if isinstance(agg_trades, list) and agg_trades:
            buy_count = sum(1 for row in agg_trades if not row.get("m", False))
            buy_aggression = buy_count / len(agg_trades)
    except Exception:
        buy_aggression = 0.0

    # ─── CVD ──────────────────────────────────────────────────────────────
    try:
        cvd_delta_ratio = float(cvd.get("deltaRatio", 0.0) or 0.0)
    except Exception:
        cvd_delta_ratio = 0.0

    try:
        recent_cvd_delta_ratio = float(cvd.get("recentDeltaRatio", 0.0) or 0.0)
    except Exception:
        recent_cvd_delta_ratio = 0.0

    try:
        cvd_efficiency = float(cvd.get("efficiency", 0.0) or 0.0)
    except Exception:
        cvd_efficiency = 0.0

    cvd_divergence = cvd.get("divergence") if isinstance(cvd, dict) else None
    cvd_bias = cvd.get("bias") if isinstance(cvd, dict) else None

    # ─── Liquidation overlay (placeholder — uses caller-supplied state) ──
    liquidation = _liquidation_overlay_placeholder(symbol, side, snapshot, liquidation_state_path)
    clean_delta += float(liquidation.get("cleanScoreDelta", 0.0) or 0.0)
    risk_flags.extend([flag for flag in (liquidation.get("riskFlags") or []) if flag not in risk_flags])
    notes.extend([note for note in (liquidation.get("notes") or []) if note not in notes])

    # ─── Recent location (15m and 1h) ─────────────────────────────────────
    recent_location_15m = None
    recent_location_1h = None
    try:
        rows_15m = klines.get("15m", []) if isinstance(klines, dict) else []
        if rows_15m:
            highs = [float(row[2]) for row in rows_15m[-8:]]
            lows = [float(row[3]) for row in rows_15m[-8:]]
            closes = [float(row[4]) for row in rows_15m[-8:]]
            denom = (max(highs) - min(lows)) + 1e-12
            recent_location_15m = (closes[-1] - min(lows)) / denom
    except Exception:
        recent_location_15m = None

    try:
        rows_1h = klines.get("1h", []) if isinstance(klines, dict) else []
        if rows_1h:
            highs = [float(row[2]) for row in rows_1h[-8:]]
            lows = [float(row[3]) for row in rows_1h[-8:]]
            closes = [float(row[4]) for row in rows_1h[-8:]]
            denom = (max(highs) - min(lows)) + 1e-12
            recent_location_1h = (closes[-1] - min(lows)) / denom
    except Exception:
        recent_location_1h = None

    # ─── Funding ratios ────────────────────────────────────────────────────
    global_ratio = latest_ratio_value(binance.get("globalLongShortAccountRatio"), ["longShortRatio"])
    top_account_ratio = latest_ratio_value(binance.get("topLongShortAccountRatio"), ["longShortRatio"])
    top_position_ratio = latest_ratio_value(binance.get("topLongShortPositionRatio"), ["longShortRatio"])
    taker_ratio = latest_ratio_value(binance.get("takerLongShortRatio"), ["buySellRatio", "buyVolSellVolRatio"])

    # ─── Quote volume scoring ──────────────────────────────────────────────
    if quote_volume >= 1_000_000_000:
        clean_delta += 1.25
        notes.append("Execution quality gets a boost from very high quote volume")
    elif quote_volume >= 300_000_000:
        clean_delta += 0.55
        notes.append("Execution quality is supported by healthy quote volume")
    elif quote_volume < 80_000_000:
        clean_delta -= 1.5
        risk_flags.append("thin_quote_volume")
        notes.append("Execution quality is penalized because quote volume is too thin")

    # ─── Spread scoring ────────────────────────────────────────────────────
    if spread_bps > 12:
        clean_delta -= 3.0
        risk_flags.append("wide_spread")
        notes.append("Spread is too wide for clean execution")
    elif spread_bps > 6:
        clean_delta -= 1.4
        risk_flags.append("medium_spread")
        notes.append("Spread is wide enough to reduce execution quality")
    elif spread_bps < 2:
        clean_delta += 0.4
        notes.append("Tight spread supports cleaner execution")

    # ─── Depth scoring ─────────────────────────────────────────────────────
    if depth_total >= 8_000_000:
        clean_delta += 1.0
        notes.append("Top-of-book depth is strong")
    elif depth_total >= 3_000_000:
        clean_delta += 0.4
        notes.append("Top-of-book depth is acceptable")
    elif depth_total < 1_000_000:
        clean_delta -= 1.8
        risk_flags.append("thin_depth")
        notes.append("Top-of-book depth is too thin")

    # ─── Trade count scoring ───────────────────────────────────────────────
    if trade_count < 120_000:
        clean_delta -= 0.85
        risk_flags.append("low_trade_count")
        notes.append("Low trade count suggests weaker tape quality")
    elif trade_count > 300_000:
        clean_delta += 0.35

    # ─── OI change scoring ─────────────────────────────────────────────────
    if abs(oi_change_pct) > 22:
        clean_delta -= 0.9
        risk_flags.append("hot_oi")
        notes.append("Open interest expansion is hot enough to raise execution risk")

    # ─── Side-specific CVD checks ──────────────────────────────────────────
    if side == "long":
        if cvd_bias == "bullish" and recent_cvd_delta_ratio > 0.08:
            clean_delta += 0.75
            notes.append("CVD dari aggTrades mendukung long dengan delta beli yang konsisten")
        elif cvd_divergence == "bearish":
            clean_delta -= 1.1
            risk_flags.append("bearish_cvd_divergence")
            notes.append("CVD menunjukkan bearish divergence terhadap ide long")
        elif cvd_delta_ratio < -0.06 and cvd_efficiency > 0.2:
            clean_delta -= 0.6
            notes.append("CVD bersih condong jual, jadi long kehilangan dukungan tape")
        if recent_cvd_delta_ratio < -0.25 and cvd_efficiency > 0.18:
            clean_delta -= 1.0
            risk_flags.append("recent_sell_pressure")
            notes.append("Recent tape masih sell-heavy, jadi long pullback seperti AAVE-type setup harus ditahan lebih ketat")
        if recent_location_15m is not None and recent_location_1h is not None:
            if recent_location_15m > 0.78 and recent_location_1h > 0.62 and recent_cvd_delta_ratio < 0.12:
                clean_delta -= 1.1
                risk_flags.append("late_long_location")
                notes.append("Long terlihat terlalu tinggi di range intraday tanpa tape yang cukup kuat, jadi risiko telat masuk meningkat")

    if side == "short":
        if cvd_bias == "bearish" and recent_cvd_delta_ratio < -0.08:
            clean_delta += 0.75
            notes.append("CVD dari aggTrades mendukung short dengan delta jual yang konsisten")
        elif cvd_divergence == "bullish":
            clean_delta -= 1.1
            risk_flags.append("bullish_cvd_divergence")
            notes.append("CVD menunjukkan bullish divergence terhadap ide short")
        elif cvd_delta_ratio > 0.06 and cvd_efficiency > 0.2:
            clean_delta -= 0.6
            notes.append("CVD bersih condong beli, jadi short kehilangan dukungan tape")
        if recent_cvd_delta_ratio > 0.25 and cvd_efficiency > 0.18:
            clean_delta -= 1.0
            risk_flags.append("recent_buy_pressure")
            notes.append("Recent tape masih buy-heavy, jadi short seperti ADA-type setup harus lebih selektif")
        if recent_location_15m is not None and recent_location_1h is not None:
            if recent_location_15m < 0.22 and recent_location_1h < 0.38 and recent_cvd_delta_ratio > -0.15:
                clean_delta -= 1.1
                risk_flags.append("late_short_location")
                notes.append("Short terlihat terlalu dekat low range tanpa continuation tape yang cukup, jadi risiko telat masuk meningkat")

    # ─── Short squeeze risk ────────────────────────────────────────────────
    if side == "short":
        short_squeeze_risk = 0
        if funding < -0.0005:
            short_squeeze_risk += 1
            notes.append("Negative funding increases short crowding risk")
        if any(r is not None and r < 0.9 for r in [global_ratio, top_account_ratio, top_position_ratio]):
            short_squeeze_risk += 1
            notes.append("Positioning ratios suggest the short side is already crowded")
        if taker_ratio is not None and taker_ratio > 1.08:
            short_squeeze_risk += 1
            notes.append("Recent taker flow leans buy-heavy against the short idea")
        if buy_aggression > 0.6 and depth_imbalance > 0.08:
            short_squeeze_risk += 1
            notes.append("Aggressive buy flow and bid-side depth raise squeeze risk")
        if quote_volume < 150_000_000 or depth_total < 2_000_000:
            short_squeeze_risk += 1
            notes.append("Lower-liquidity short candidate gets extra squeeze penalty")
        if short_squeeze_risk >= 3:
            clean_delta -= 2.8
            risk_flags.append("short_squeeze_risk")
        elif short_squeeze_risk == 2:
            clean_delta -= 1.6
            risk_flags.append("short_squeeze_risk")
        if short_squeeze_risk > 0:
            management.append("Kalau tetap short, wajib ambil partial lebih cepat dan pindah stop ke break-even setelah TP1 atau +1R")

    # ─── Long crowding risk ────────────────────────────────────────────────
    if side == "long":
        long_crowding_risk = 0
        if funding > 0.0008:
            long_crowding_risk += 1
            notes.append("Positive funding raises long crowding risk")
        if any(r is not None and r > 1.25 for r in [global_ratio, top_account_ratio, top_position_ratio]):
            long_crowding_risk += 1
            notes.append("Positioning ratios show long crowding")
        if taker_ratio is not None and taker_ratio < 0.92:
            long_crowding_risk += 1
            notes.append("Recent taker flow leans sell-heavy against the long idea")
        if buy_aggression < 0.4 and depth_imbalance < -0.08:
            long_crowding_risk += 1
            notes.append("Offer-side pressure hurts long execution quality")
        if long_crowding_risk >= 3:
            clean_delta -= 2.2
            risk_flags.append("long_crowding_risk")
        elif long_crowding_risk == 2:
            clean_delta -= 1.2
            risk_flags.append("long_crowding_risk")

    # ─── Execution feedback from history ──────────────────────────────────
    feedback = execution_feedback or {}
    if isinstance(feedback, dict):
        symbols_data = feedback.get("symbols", {}) or {}
        side_data = (symbols_data.get(symbol.upper()) or {}).get("sides", {}) or {}
        side_feedback = side_data.get(side) if side in {"long", "short"} else {}
        side_total = int(side_feedback.get("total", 0) or 0) if isinstance(side_feedback, dict) else 0
        side_outcomes = side_feedback.get("outcomes", {}) if isinstance(side_feedback, dict) else {}
        overall_total = int(feedback.get("total", 0) or 0)
    else:
        side_total = 0
        side_outcomes = {}
        overall_total = 0

    if side_total >= 2:
        stop_count = int(side_outcomes.get("stop_loss", 0) or 0)
        no_fill_count = int(side_outcomes.get("no_fill", 0) or 0)
        runner_count = int(side_outcomes.get("runner", 0) or 0)
        tp_count = int(side_outcomes.get("take_profit", 0) or 0)
        stop_rate = stop_count / side_total
        no_fill_rate = no_fill_count / side_total
        if stop_rate >= 0.6:
            clean_delta -= 2.0
            risk_flags.append("bad_symbol_history")
            notes.append("Historical feedback for this symbol and side is poor")
        elif stop_rate >= 0.4:
            clean_delta -= 1.0
            notes.append("Historical feedback shows this symbol is not consistently executable")
        if no_fill_rate >= 0.4:
            clean_delta -= 0.75
            risk_flags.append("no_fill_history")
            management.append("Nama ini sering no-fill, jadi hindari limit terlalu pasif dan tunggu reclaim atau rejection yang benar-benar jelas")
        if runner_count > tp_count and runner_count >= 1:
            management.append("Runner history suggests ambil partial lebih cepat, lalu geser stop ke break-even")
    elif overall_total >= 2:
        notes.append("Historical feedback exists, but not enough on this exact side yet")

    if side in {"long", "short"} and not management:
        management.append("Default management: ambil partial di TP1 atau sekitar +1R, lalu geser stop ke break-even bila tape tetap mendukung")

    # ─── New indicators (MACD, ATR, VWAP) — display only, never affect grading ──
    macd_histogram = None
    macd_signal = None
    macd_line = None
    atr_14 = None
    vwap_value = None
    macd_note = None
    atr_note = None
    vwap_note = None
    macd_warn = False
    atr_warn = False
    vwap_warn = False

    if any(NEW_INDICATOR_ENABLED.get(k) for k in ("macd", "atr", "vwap")):
        try:
            rows_1h = klines.get("1h", []) if isinstance(klines, dict) else []
            if rows_1h and len(rows_1h) >= 26:
                c_1h = [float(r[4]) for r in rows_1h]
                h_1h = [float(r[2]) for r in rows_1h]
                l_1h = [float(r[3]) for r in rows_1h]
                v_1h = [float(r[5]) for r in rows_1h]

                # MACD: EMA(12) - EMA(26)
                def _ema_inline(vals, period):
                    if len(vals) < period:
                        return None
                    mult = 2.0 / (period + 1)
                    e = sum(vals[:period]) / period
                    for val in vals[period:]:
                        e = (val - e) * mult + e
                    return e

                ema_12 = _ema_inline(c_1h, 12)
                ema_26 = _ema_inline(c_1h, 26)
                if ema_12 is not None and ema_26 is not None:
                    macd_line = round(ema_12 - ema_26, 8)
                    macd_series = []
                    for i in range(26, len(c_1h)):
                        e12 = _ema_inline(c_1h[:i+1], 12) if i >= 11 else None
                        e26 = _ema_inline(c_1h[:i+1], 26) if i >= 25 else None
                        if e12 is not None and e26 is not None:
                            macd_series.append(e12 - e26)
                    if len(macd_series) >= 9:
                        sig = sum(macd_series[:9]) / 9
                        mult = 2.0 / 10.0
                        for h_val in macd_series[9:]:
                            sig = (h_val - sig) * mult + sig
                        macd_signal = round(sig, 8)
                    if macd_line is not None and macd_signal is not None:
                        macd_histogram = round(macd_line - macd_signal, 8)
                    if macd_histogram is not None and c_1h[-1] > 0:
                        if abs(macd_histogram) / c_1h[-1] > MACD_PRICE_RATIO_MAX:
                            macd_warn = True

                # ATR(14) from 1h klines
                if len(c_1h) >= 15:
                    trs = []
                    for i in range(-14, 0):
                        hl = h_1h[i] - l_1h[i]
                        hc = abs(h_1h[i] - c_1h[i-1])
                        lc = abs(l_1h[i] - c_1h[i-1])
                        trs.append(max(hl, hc, lc))
                    if trs:
                        atr_14 = round(sum(trs) / len(trs), 8)
                        if c_1h[-1] > 0 and atr_14 / c_1h[-1] > ATR_PRICE_RATIO_MAX:
                            atr_warn = True

                # VWAP from 1h klines (rolling 24-bar)
                if v_1h and sum(v_1h[-24:]) > 0:
                    typ = [(h_1h[i] + l_1h[i] + c_1h[i]) / 3 for i in range(len(c_1h))]
                    pv = [typ[i] * v_1h[i] for i in range(len(typ))]
                    total_vol = sum(v_1h[-24:])
                    if total_vol > 0:
                        vwap_value = round(sum(pv[-24:]) / total_vol, 8)
                        if c_1h[-1] > 0 and vwap_value > 0:
                            if abs(c_1h[-1] - vwap_value) / c_1h[-1] > VWAP_PRICE_DIVERGENCE_MAX:
                                vwap_warn = True

                # Notes (display only)
                if NEW_INDICATOR_ENABLED.get("macd") and macd_histogram is not None:
                    direction = "bullish" if macd_histogram > 0 else "bearish"
                    macd_note = f"MACD histogram {direction} ({macd_histogram:.6f})"
                    if macd_warn:
                        macd_note += " [unusual magnitude]"
                if NEW_INDICATOR_ENABLED.get("atr") and atr_14 is not None:
                    atr_note = f"ATR(14)={atr_14:.4f}"
                    if atr_warn:
                        atr_note += " [unusual volatility]"
                if NEW_INDICATOR_ENABLED.get("vwap") and vwap_value is not None:
                    above = c_1h[-1] > vwap_value
                    vwap_dir = "above" if above else "below"
                    vwap_note = f"Price {vwap_dir} VWAP ({vwap_value:.4f})"
                    if vwap_warn:
                        vwap_note += " [unusual divergence]"
                    notes.append(vwap_note)
                if macd_note:
                    notes.append(macd_note)
                if atr_note:
                    notes.append(atr_note)
        except Exception:
            # Never let new indicators crash execution filter
            pass

    clean_delta = max(-6.0, min(2.5, round(clean_delta, 2)))

    return {
        "symbol": symbol.upper(),
        "side": side,
        "cleanScoreDelta": clean_delta,
        "quoteVolume": round(quote_volume, 2),
        "tradeCount": trade_count,
        "depthNotionalTop10": round(depth_total, 2),
        "depthImbalance": round(depth_imbalance, 4),
        "buyAggression": round(buy_aggression, 4),
        "cvd": cvd,
        "liquidation": liquidation,
        "funding": round(funding, 6),
        "oiChangePct": round(oi_change_pct, 2),
        "ratios": {
            "globalLongShort": global_ratio,
            "topAccounts": top_account_ratio,
            "topPositions": top_position_ratio,
            "taker": taker_ratio,
        },
        "riskFlags": risk_flags,
        "notes": notes,
        "managementGuidance": management,
        "feedbackSummary": feedback,
        "recentLocation": {
            "m15": round(recent_location_15m, 4) if recent_location_15m is not None else None,
            "h1": round(recent_location_1h, 4) if recent_location_1h is not None else None,
        },
        # New indicators stored but NEVER used for grade/cleanScore/readiness
        "macdHistogram": macd_histogram,
        "macdSignal": macd_signal,
        "macdLine": macd_line,
        "atr14": atr_14,
        "vwap": vwap_value,
        "_indicatorFlags": {
            "macdWarn": macd_warn,
            "atrWarn": atr_warn,
            "vwapWarn": vwap_warn,
        },
    }


def _liquidation_overlay_placeholder(
    symbol: str,
    side: str,
    snapshot: Dict[str, Any],
    liquidation_state_path: Optional[str] = None,
) -> Dict[str, Any]:
    """Placeholder liquidation overlay.

    In the full system, this reads from liquidation_events.json state file.
    The extracted version returns a no-op overlay unless a real state path
    is provided by the caller.
    """
    # Minimal default — real implementation reads from liquidation_events.json
    return {
        "symbol": symbol.upper(),
        "side": side,
        "available": False,
        "stale": True,
        "cleanScoreDelta": 0.0,
        "riskFlags": [],
        "notes": [],
        "supportBias": None,
    }