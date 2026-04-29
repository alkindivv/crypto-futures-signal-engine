"""
projections/notification_entry.py — Pure notification projection functions.

Extracted from public_market_data.py lines 8570-8651:
- notification_lane_for_setup(): pure lane classifier
- notification_entry_from_setup(): pure DTO builder

These are pure projection functions — no file I/O, no state mutations, no network calls.
"""
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from openclaw_engine.state import (
    CANONICAL_SETUP_ACTIVE_STATES,
    LIVE_TRIGGER_STANDBY_READINESS_STATES,
    delivery_state,
    final_confirmation_identity,
    setup_lifecycle_state,
    setup_readiness_state,
)
from openclaw_engine.helpers.time_helpers import parse_iso_timestamp


def notification_crowding_note(actionable: Dict[str, Any]) -> str:
    """Extract crowding note string from an actionable dict.

    Reads: actionable["crowdingSignals"]["crowdingNote"]
    Returns "none" if not present or not a dict.
    """
    crowding = actionable.get("crowdingSignals") if isinstance(actionable, dict) else {}
    if not isinstance(crowding, dict):
        return "none"
    return str(crowding.get("crowdingNote") or "none")


def notification_lane_for_setup(
    setup: Dict[str, Any],
    monitor: Optional[Dict[str, Any]] = None,
    live_trust: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Determine the notification lane for a setup/monitor pair.

    Reads readiness/lifecycle state from setup via canonical accessors.
    Returns: "LIVE" | "CONFIRMED" | "READY" | "STANDBY" | None
    """
    fc = monitor.get("finalConfirmation") if isinstance(monitor, dict) else None
    fc_active = isinstance(fc, dict) and fc.get("state") == "active"
    readiness_state = setup_readiness_state(setup)
    lifecycle_state = setup_lifecycle_state(setup)
    actionable_now = (
        readiness_state == "retest_ready"
        and lifecycle_state in ("actionable", "notified")
    )
    live_trust_allowed = (
        True if not isinstance(live_trust, dict) else bool(live_trust.get("trustLive", True))
    )

    # LIVE = confirmed + currently actionable
    if fc_active and actionable_now:
        return "LIVE" if live_trust_allowed else "CONFIRMED"
    # CONFIRMED = confirmed + NOT currently actionable (past event, not current opportunity)
    if fc_active and not actionable_now:
        return "CONFIRMED"
    # READY = no active confirmation + currently actionable (setup ready, no trigger confirm yet)
    if not fc_active and actionable_now:
        return "READY"
    # STANDBY = tracked, not actionable
    if (
        readiness_state in LIVE_TRIGGER_STANDBY_READINESS_STATES
        and lifecycle_state in CANONICAL_SETUP_ACTIVE_STATES
    ):
        return "STANDBY"
    return None  # NO_TRADE


def notification_entry_from_setup(
    setup_key: str,
    setup: Dict[str, Any],
    alert: Optional[Dict[str, Any]] = None,
    monitor: Optional[Dict[str, Any]] = None,
    live_trust: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a notification entry DTO from a setup record.

    This is a pure projection — reads already-loaded dicts, returns flat DTO.
    No file I/O, no state mutations.
    """
    if not isinstance(setup, dict):
        return None
    actionable = setup.get("actionableAlert") if isinstance(setup.get("actionableAlert"), dict) else {}
    fc = monitor.get("finalConfirmation") if isinstance(monitor, dict) else None
    fc_active = isinstance(fc, dict) and fc.get("state") == "active"
    readiness_state = setup_readiness_state(setup)
    lifecycle_state = setup_lifecycle_state(setup)
    actionable_now = (
        readiness_state == "retest_ready"
        and lifecycle_state in ("actionable", "notified")
    )
    lane = notification_lane_for_setup(setup, monitor=monitor, live_trust=live_trust)
    if not lane:
        return None
    fc_identity = final_confirmation_identity(monitor, active_only=True)
    entry_zone = actionable.get("entryZone") if isinstance(actionable.get("entryZone"), dict) else {}
    take_profit = actionable.get("takeProfit") if isinstance(actionable.get("takeProfit"), dict) else {}
    live_trust_allowed = (
        True if not isinstance(live_trust, dict) else bool(live_trust.get("trustLive", True))
    )
    live_trust_blocked = bool(fc_active and actionable_now and not live_trust_allowed)
    entry = {
        "setupKey": setup_key,
        "lane": lane,
        "symbol": str(setup.get("symbol") or ""),
        "side": str(setup.get("side") or ""),
        "grade": setup.get("grade"),
        "lifecycleState": lifecycle_state,
        "readinessState": readiness_state,
        "deliveryState": delivery_state(alert) if alert else None,
        "currentPrice": actionable.get("currentPrice"),
        "entryStyle": actionable.get("entryStyle"),
        "entryZoneLow": entry_zone.get("low"),
        "entryZoneHigh": entry_zone.get("high"),
        "stopLoss": actionable.get("stopLoss"),
        "tp1": take_profit.get("tp1"),
        "tp2": take_profit.get("tp2"),
        "tp3": take_profit.get("tp3"),
        "trigger": actionable.get("trigger"),
        "invalidation": actionable.get("invalidation"),
        "doNotChase": actionable.get("doNotChase"),
        "currentStatus": actionable.get("currentStatus"),
        "riskFlags": (
            actionable.get("riskFlags")
            if isinstance(actionable.get("riskFlags"), list)
            else (setup.get("riskFlags") or [])
        ),
        "crowdingNote": _crowding_note(actionable),
        "executionNote": actionable.get("executionNote"),
        "setupVersion": setup.get("setupVersion"),
        "readinessVersion": setup.get("readinessVersion"),
        "cleanScore": setup.get("cleanScore"),
        "gradeScore": setup.get("gradeScore"),
        "changedAt": (
            (setup.get("lifecycle") or {}).get("changedAt")
            if isinstance(setup.get("lifecycle"), dict)
            else setup.get("updatedAt")
        ),
        "liveTrustAllowed": live_trust_allowed,
        "liveTrustBlocked": live_trust_blocked,
        "liveTrustReason": (live_trust.get("reason") if isinstance(live_trust, dict) else None),
        "liveTrustStatus": (live_trust.get("derivedStatus") if isinstance(live_trust, dict) else None),
    }
    if lane in {"LIVE", "CONFIRMED"} and fc_identity:
        entry["observedSetupVersion"] = setup.get("setupVersion")
        entry["observedReadinessVersion"] = setup.get("readinessVersion")
        entry["setupVersion"] = fc_identity.get("setupVersion") or entry.get("setupVersion")
        entry["readinessVersion"] = fc_identity.get("readinessVersion") or entry.get("readinessVersion")
    if isinstance(fc, dict):
        entry["finalConfirmation"] = fc
        entry["confirmedAt"] = fc.get("confirmedAt")
        entry["deliveryStatus"] = fc.get("deliveryStatus")
        entry["deliveryError"] = fc.get("deliveryError")
        confirmed_at = fc.get("confirmedAt")
        is_fresh = False
        if confirmed_at and isinstance(confirmed_at, str):
            confirmed_dt = parse_iso_timestamp(confirmed_at)
            if confirmed_dt:
                ago = datetime.now(timezone.utc) - confirmed_dt
                is_fresh = ago.total_seconds() < 600
        entry["isFresh"] = is_fresh
    else:
        entry["confirmedAt"] = None
        entry["deliveryStatus"] = None
        entry["deliveryError"] = None
        entry["isFresh"] = False
    return entry


def _crowding_note(actionable: Dict[str, Any]) -> str:
    """Inline version of notification_crowding_note to avoid circular deps."""
    crowding = actionable.get("crowdingSignals") if isinstance(actionable, dict) else {}
    if not isinstance(crowding, dict):
        return "none"
    return str(crowding.get("crowdingNote") or "none")
