"""
grades.py — Grading system and helpers.

Extracted from public_market_data.py:
- GRADE_PRIORITY: lines 161
- ALERT_GRADES: line 162
- WATCHLIST_GRADES: line 163
- SEVERE_RISK_FLAGS: line 165
- SEVERE_HEADLINE_KEYWORDS: line 164
- direction_label(): lines 7488-7493
- grade_priority(): line 7495-7496
- scan_sort_key(): lines 7498-7503
- is_hard_disqualified(): lines 7505-7528
- grade_row(): lines 7549-7625
- apply_grade_to_row(): lines 7627-7631

Key behaviors preserved:
- Grade tiers: A+, A, B, C, NO_TRADE
- A+ requires clean >= 97 AND abs(bull) >= 7 AND execution_delta > -1.5 AND severe_risk == 0
- A requires clean >= 93 AND abs(bull) >= 5 AND execution_delta > -2.5 AND severe_risk <= 1
- alertEligible = grade in {"A+", "A"} and not disqualified
- watchlistEligible = grade in {"A+", "A", "B"} and not disqualified
- direction_label: bull >= 2 → long, bull <= -2 → short, else watch
"""
from typing import Any, Dict, List, Optional

# Grade priority for sorting (higher = more priority)
GRADE_PRIORITY = {"A+": 4, "A": 3, "B": 2, "C": 1, "NO_TRADE": 0}

# Grades eligible for Telegram alerts (LIVE_STRICT / READY_IN_ZONE)
ALERT_GRADES = {"A+", "A"}

# Grades eligible for watchlist delivery
WATCHLIST_GRADES = {"A+", "A", "B"}

# Severe risk flags that block A+ grade
SEVERE_RISK_FLAGS = {"wide_spread", "short_squeeze_risk", "long_crowding_risk", "hot_oi"}

# Headline keywords that are immediately disqualifying
SEVERE_HEADLINE_KEYWORDS = {"hack", "exploit", "breach", "delist"}


def direction_label(bull_score: int) -> str:
    """Convert bull_score to direction label.

    bull >= 2  → 'long'
    bull <= -2 → 'short'
    otherwise  → 'watch'
    """
    if bull_score >= 2:
        return "long"
    if bull_score <= -2:
        return "short"
    return "watch"


def grade_priority(grade: Optional[str]) -> int:
    """Sort priority for a grade."""
    return GRADE_PRIORITY.get(str(grade or "NO_TRADE"), 0)


def scan_sort_key(row: Dict[str, Any]) -> Any:
    """Sort key for scan results: grade priority → cleanScore → |bullScore| → quoteVolume."""
    return (
        grade_priority(row.get("grade")),
        float(row.get("cleanScore", 0.0)),
        abs(int(row.get("bullScore", 0) or 0)),
        float(row.get("quoteVolume", 0.0) or 0.0),
    )


def is_hard_disqualified(row: Dict[str, Any]) -> Dict[str, Any]:
    """Check hard disqualification rules for a row.

    Returns {"disqualified": bool, "disqualifiers": List[str]}.
    These rules produce an immediate NO_TRADE regardless of score.
    """
    execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
    risk_flags = set(execution.get("riskFlags", []) or [])
    disqualifiers: List[str] = []

    try:
        execution_delta = float(execution.get("cleanScoreDelta", 0.0) or 0.0)
    except Exception:
        execution_delta = 0.0

    try:
        bull_score = int(row.get("bullScore", 0) or 0)
    except Exception:
        bull_score = 0

    try:
        pos_15m = float((((row.get("features") or {}).get("15m") or {}).get("pos", 0.5)) or 0.5)
    except Exception:
        pos_15m = 0.5

    negative_keywords = set(((row.get("headlineFlags") or {}).get("negativeKeywords") or []))
    severe_headlines = sorted(negative_keywords & SEVERE_HEADLINE_KEYWORDS)

    # Rule 1: wide spread
    if "wide_spread" in risk_flags:
        disqualifiers.append("Spread terlalu lebar untuk setup high-grade")

    # Rule 2: both thin quote volume AND thin depth
    if {"thin_quote_volume", "thin_depth"}.issubset(risk_flags):
        disqualifiers.append("Likuiditas terlalu tipis, volume dan depth sama-sama lemah")

    # Rule 3: thin depth AND low trade count
    if {"thin_depth", "low_trade_count"}.issubset(risk_flags):
        disqualifiers.append("Depth tipis dan trade count rendah membuat tape terlalu rapuh")

    # Rule 4: execution penalty too severe
    if execution_delta <= -4.0:
        disqualifiers.append("Execution penalty terlalu berat")

    # Rule 5: hot OI + thin depth + crowding
    if "hot_oi" in risk_flags and "thin_depth" in risk_flags and (
        "short_squeeze_risk" in risk_flags or "long_crowding_risk" in risk_flags
    ):
        disqualifiers.append("OI panas plus crowding dan depth tipis terlalu berbahaya")

    # Rule 6: long too crowded and price at top of range
    if bull_score >= 2 and "long_crowding_risk" in risk_flags and pos_15m >= 0.9:
        disqualifiers.append("Long terlalu crowded dan harga sudah terlalu dekat area atas")

    # Rule 7: short too squeeze-prone and price at bottom of range
    if bull_score <= -2 and "short_squeeze_risk" in risk_flags and pos_15m <= 0.1:
        disqualifiers.append("Short terlalu squeeze-prone dan harga sudah terlalu dekat area bawah")

    # Rule 8: severe headlines
    if severe_headlines:
        disqualifiers.append(f"Headline risk berat terdeteksi: {', '.join(severe_headlines)}")

    return {
        "disqualified": bool(disqualifiers),
        "disqualifiers": disqualifiers,
    }


def grade_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Compute grade for a scored row.

    Grade thresholds:
    - A+: clean >= 97 AND abs(bull) >= 7 AND execution_delta > -1.5 AND severe_risk == 0
    - A:  clean >= 93 AND abs(bull) >= 5 AND execution_delta > -2.5 AND severe_risk <= 1
    - B:  clean >= 88 AND abs(bull) >= 4
    - C:  clean >= 80 AND abs(bull) >= 2

    alertEligible = grade in ALERT_GRADES and not disqualified
    watchlistEligible = grade in WATCHLIST_GRADES and not disqualified
    """
    clean_score = round(float(row.get("cleanScore", 0.0) or 0.0), 2)
    bull_score = int(row.get("bullScore", 0) or 0)
    direction = direction_label(bull_score)
    execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
    risk_flags = list(execution.get("riskFlags", []) or [])
    risk_flag_set = set(risk_flags)
    execution_delta = round(float(execution.get("cleanScoreDelta", 0.0) or 0.0), 2)
    severe_risk_count = len(risk_flag_set & SEVERE_RISK_FLAGS)
    grade_reasons: List[str] = []

    disqualification = is_hard_disqualified(row)
    disqualified = bool(disqualification.get("disqualified"))
    disqualifiers = list(disqualification.get("disqualifiers") or [])

    # Directional alignment reasons
    if abs(bull_score) >= 7:
        grade_reasons.append("Directional alignment multi-timeframe sangat kuat")
    elif abs(bull_score) >= 5:
        grade_reasons.append("Directional alignment multi-timeframe cukup kuat")
    elif abs(bull_score) >= 4:
        grade_reasons.append("Bias arah ada, tapi conviction belum elite")
    elif abs(bull_score) >= 2:
        grade_reasons.append("Bias arah masih ada, tapi belum cukup kuat untuk actionable setup")
    else:
        grade_reasons.append("Bias arah terlalu campuran untuk setup aktif")

    # Clean score reasons
    if clean_score >= 97:
        grade_reasons.append("Clean score masuk tier tertinggi")
    elif clean_score >= 93:
        grade_reasons.append("Clean score cukup tinggi untuk setup actionable")
    elif clean_score >= 88:
        grade_reasons.append("Clean score cukup untuk watchlist")
    elif clean_score >= 80:
        grade_reasons.append("Clean score borderline dan perlu disiplin")
    else:
        grade_reasons.append("Clean score terlalu rendah")

    # Execution quality reasons
    if execution_delta >= 0:
        grade_reasons.append("Execution filter mendukung, bukan menghambat")
    elif execution_delta > -1.5:
        grade_reasons.append("Execution quality masih layak walau ada sedikit friksi")
    else:
        grade_reasons.append("Execution quality menurunkan conviction")

    # Risk flags
    if risk_flags:
        grade_reasons.append(f"Risk flags aktif: {', '.join(risk_flags[:3])}")
    else:
        grade_reasons.append("Tidak ada risk flag utama yang aktif")

    # Grade determination
    grade = "NO_TRADE"
    if not disqualified:
        if clean_score >= 97 and abs(bull_score) >= 7 and execution_delta > -1.5 and severe_risk_count == 0:
            grade = "A+"
        elif clean_score >= 93 and abs(bull_score) >= 5 and execution_delta > -2.5 and severe_risk_count <= 1:
            grade = "A"
        elif clean_score >= 88 and abs(bull_score) >= 4:
            grade = "B"
        elif clean_score >= 80 and abs(bull_score) >= 2:
            grade = "C"

    if grade == "NO_TRADE":
        if disqualified:
            grade_reasons.extend(disqualifiers)
        else:
            grade_reasons.append("Belum lolos threshold grade formal")

    return {
        **row,
        "direction": direction,
        "grade": grade,
        "gradeScore": clean_score,
        "alertEligible": grade in ALERT_GRADES and not disqualified,
        "watchlistEligible": grade in WATCHLIST_GRADES and not disqualified,
        "disqualified": disqualified,
        "disqualifiers": disqualifiers,
        "gradeReasons": grade_reasons[:6],
    }