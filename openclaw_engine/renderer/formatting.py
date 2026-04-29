"""
renderer/formatting.py — Stateless notification formatting functions.

Extracted from public_market_data.py lines 8551-8877:
- notification_format_value(): formatAny value to display string
- notification_join_flags(): join list of flags
- _entry_badges(): build badges string for entry
- notification_entry_signature(): hash of material fields
- notification_update_reasons(): diff reasons between entry versions
- notification_sort_key(): sort key for entries (lane rank + grade + cleanScore)
- notification_render_entry_compact(): single-line entry render
- notification_render_entry_detailed(): multi-line entry render
- notification_render_delta(): render a single change/delta

These are pure text-transformation functions. No file I/O, no state mutations.
"""
import hashlib
import json
from typing import Any, Dict, List, Optional

from openclaw_engine.scoring import grade_priority


def notification_format_value(value: Any) -> str:
    """Format any value for human-readable display."""
    if value is None:
        return "none"
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, (int, float)):
        text = format(value, ".10f").rstrip("0").rstrip(".")
        return text or "0"
    return str(value)


def notification_join_flags(values: Any) -> str:
    """Join a list of flag values into a comma-separated string."""
    if not isinstance(values, list):
        return "none"
    cleaned = [str(item) for item in values if item not in (None, "")]
    return ", ".join(cleaned) if cleaned else "none"


def _entry_badges(entry: Dict[str, Any]) -> str:
    """Build badges string for entry: ENTRY_CONFIRMED + delivery status."""
    badges: List[str] = []
    if entry.get("lane") == "LIVE" and entry.get("isFresh"):
        badges.append("ENTRY_CONFIRMED")
    ds = entry.get("deliveryStatus")
    if ds == "sent":
        badges.append("✅ Delivered")
    elif ds == "failed":
        badges.append("⚠️ Failed")
    elif ds == "pending":
        badges.append("⏳ Pending")
    return " | " + ", ".join(badges) if badges else ""


def notification_entry_signature(entry: Dict[str, Any]) -> str:
    """Compute a short hash over all material notification fields.

    Used for change detection — any drift in material fields produces
    a different hash.
    """
    payload = {
        "lane": entry.get("lane"),
        "symbol": entry.get("symbol"),
        "side": entry.get("side"),
        "grade": entry.get("grade"),
        "lifecycleState": entry.get("lifecycleState"),
        "readinessState": entry.get("readinessState"),
        "deliveryState": entry.get("deliveryState"),
        "entryZoneLow": entry.get("entryZoneLow"),
        "entryZoneHigh": entry.get("entryZoneHigh"),
        "stopLoss": entry.get("stopLoss"),
        "tp1": entry.get("tp1"),
        "tp2": entry.get("tp2"),
        "tp3": entry.get("tp3"),
        "trigger": entry.get("trigger"),
        "invalidation": entry.get("invalidation"),
        "doNotChase": entry.get("doNotChase"),
        "riskFlags": entry.get("riskFlags"),
        "crowdingNote": entry.get("crowdingNote"),
        "executionNote": entry.get("executionNote"),
        "setupVersion": entry.get("setupVersion"),
        "readinessVersion": entry.get("readinessVersion"),
    }
    raw = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def notification_update_reasons(entry: Dict[str, Any], previous: Dict[str, Any]) -> List[str]:
    """Return list of reasons why entry differs from previous version."""
    reasons: List[str] = []
    if previous.get("lane") != entry.get("lane"):
        reasons.append("LANE_CHANGED")
    if previous.get("lifecycleState") != entry.get("lifecycleState"):
        reasons.append("LIFECYCLE_CHANGED")
    if previous.get("setupVersion") != entry.get("setupVersion"):
        reasons.append("SETUP_VERSION_CHANGED")
    if previous.get("readinessVersion") != entry.get("readinessVersion"):
        reasons.append("READINESS_VERSION_CHANGED")
    return reasons


def notification_sort_key(entry: Dict[str, Any]) -> Any:
    """Sort key for notification entries: lane rank + grade + cleanScore + symbol + side."""
    lane_rank_map = {"LIVE": 4, "READY": 3, "CONFIRMED": 2, "STANDBY": 1}
    lane_rank = lane_rank_map.get(entry.get("lane") or "", 0)
    return (
        lane_rank,
        grade_priority(entry.get("grade")),
        float(entry.get("cleanScore", 0.0) or 0.0),
        str(entry.get("symbol") or ""),
        str(entry.get("side") or ""),
    )


def notification_render_entry_compact(entry: Dict[str, Any]) -> str:
    """Render a single entry as one compact line."""
    badges = _entry_badges(entry)
    confirmed_str = ""
    if entry.get("confirmedAt"):
        confirmed_str = f" | confirmed {entry.get('confirmedAt')[:19]}"
    return (
        f"{entry.get('symbol')} {entry.get('side')} | {entry.get('lane')}{badges}{confirmed_str} | "
        f"grade {notification_format_value(entry.get('grade'))} | lifecycle {notification_format_value(entry.get('lifecycleState'))} | "
        f"readiness {notification_format_value(entry.get('readinessState'))} | delivery {notification_format_value(entry.get('deliveryState'))} | "
        f"currentPrice {notification_format_value(entry.get('currentPrice'))} | "
        f"entryZone {notification_format_value(entry.get('entryZoneLow'))} to {notification_format_value(entry.get('entryZoneHigh'))} | "
        f"stopLoss {notification_format_value(entry.get('stopLoss'))} | "
        f"TP1/TP2/TP3 {notification_format_value(entry.get('tp1'))} / {notification_format_value(entry.get('tp2'))} / {notification_format_value(entry.get('tp3'))} | "
        f"trigger {notification_format_value(entry.get('trigger'))} | invalidation {notification_format_value(entry.get('invalidation'))} | "
        f"doNotChase {notification_format_value(entry.get('doNotChase'))} | risk flags {notification_join_flags(entry.get('riskFlags'))} | "
        f"crowding note {notification_format_value(entry.get('crowdingNote'))} | executionNote {notification_format_value(entry.get('executionNote'))} | "
        f"setupVersion {notification_format_value(entry.get('setupVersion'))} | readinessVersion {notification_format_value(entry.get('readinessVersion'))}"
    )


def notification_render_entry_detailed(entry: Dict[str, Any]) -> str:
    """Render a single entry as multi-line block."""
    badges = _entry_badges(entry)
    confirmed_str = f"\n- confirmedAt: {entry.get('confirmedAt')}" if entry.get("confirmedAt") else ""
    lines = [
        f"{entry.get('symbol')} {entry.get('side')} | {entry.get('lane')}{badges}",
        f"- grade: {notification_format_value(entry.get('grade'))}",
        f"- lifecycleState: {notification_format_value(entry.get('lifecycleState'))}",
        f"- readinessState: {notification_format_value(entry.get('readinessState'))}",
        f"- deliveryState: {notification_format_value(entry.get('deliveryState'))}",
        f"- currentPrice: {notification_format_value(entry.get('currentPrice'))}",
        f"- entryZone: {notification_format_value(entry.get('entryZoneLow'))} to {notification_format_value(entry.get('entryZoneHigh'))}",
        f"- stopLoss: {notification_format_value(entry.get('stopLoss'))}",
        f"- TP1/TP2/TP3: {notification_format_value(entry.get('tp1'))} / {notification_format_value(entry.get('tp2'))} / {notification_format_value(entry.get('tp3'))}",
        f"- trigger: {notification_format_value(entry.get('trigger'))}",
        f"- invalidation: {notification_format_value(entry.get('invalidation'))}",
        f"- doNotChase: {notification_format_value(entry.get('doNotChase'))}",
        f"- currentStatus: {notification_format_value(entry.get('currentStatus'))}",
        f"- risk flags: {notification_join_flags(entry.get('riskFlags'))}",
        f"- crowding note: {notification_format_value(entry.get('crowdingNote'))}",
        f"- executionNote: {notification_format_value(entry.get('executionNote'))}",
        f"- setupVersion: {notification_format_value(entry.get('setupVersion'))}",
        f"- readinessVersion: {notification_format_value(entry.get('readinessVersion'))}{confirmed_str}",
    ]
    return "\n".join(lines)


def notification_render_delta(change: Dict[str, Any]) -> str:
    """Render a single change/delta as one line."""
    kind = change.get("kind")
    if kind in {"PAIR_NEW", "PAIR_UPDATED"}:
        entry = change.get("entry", {}) if isinstance(change.get("entry"), dict) else {}
        prefix = "NEW" if kind == "PAIR_NEW" else "UPDATED"
        reasons = change.get("reasons", []) if isinstance(change.get("reasons"), list) else []
        reason_text = f" | reason {', '.join(str(r) for r in reasons if r)}" if reasons else ""
        return f"{prefix} {entry.get('lane')}{reason_text} | {notification_render_entry_compact(entry)}"
    if kind == "PAIR_EXITED":
        return (
            f"EXIT {notification_format_value(change.get('lane'))} | {notification_format_value(change.get('symbol'))} {notification_format_value(change.get('side'))} | "
            f"lifecycleState {notification_format_value(change.get('lifecycleState'))} | reason {notification_format_value(change.get('reasonCode'))} | "
            f"setupVersion {notification_format_value(change.get('setupVersion'))} | readinessVersion {notification_format_value(change.get('readinessVersion'))}"
        )
    delta = change.get("delta", {}) if isinstance(change.get("delta"), dict) else {}
    return (
        f"TRANSITION | {notification_format_value(delta.get('symbol'))} {notification_format_value(delta.get('side'))} | "
        f"{notification_format_value(delta.get('layer'))}: {notification_format_value(delta.get('fromState'))} -> {notification_format_value(delta.get('toState'))} | "
        f"reason {notification_format_value(delta.get('reasonCode'))} | setupVersion {notification_format_value(delta.get('setupVersion'))} | "
        f"readinessVersion {notification_format_value(delta.get('readinessVersion'))}"
    )
