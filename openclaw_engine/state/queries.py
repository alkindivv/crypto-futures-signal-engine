"""
state/queries.py — Pure state query functions.

Extracted from public_market_data.py lines 2657–2742.
All functions are pure dict-accessors — no file I/O, no mutations.

Constants (source lines):
- CANONICAL_SETUP_ACTIVE_STATES: line 167
- LIVE_TRIGGER_STANDBY_READINESS_STATES: line 226
- DELIVERY_STATE_VALUES: line 210
- LEGACY_SETUP_STATE_MAP: line 168
- CANONICAL_TO_LEGACY_SETUP_STATE_MAP: line 176
- LEGACY_READINESS_STATE_MAP: line 184
- CANONICAL_TO_LEGACY_READINESS_STATE_MAP: line 189
- LEGACY_RUNTIME_STATE_MAP: line 194
- CANONICAL_TO_LEGACY_RUNTIME_STATE_MAP: line 202

Functions (source lines):
- canonical_setup_state: 2657
- canonical_readiness_state: 2669
- canonical_runtime_state: 2681
- canonical_delivery_state: 2687
- setup_lifecycle_state: 2699
- setup_readiness_state: 2708
- monitor_lifecycle_state: 2717
- monitor_readiness_state: 2725
- delivery_state: 2733
- final_confirmation_identity: 5029
"""
from typing import Any, Dict, Optional

# ─── Constants ─────────────────────────────────────────────────────────────────

CANONICAL_SETUP_ACTIVE_STATES = {"candidate", "watchlist", "actionable", "notified"}

LIVE_TRIGGER_STANDBY_READINESS_STATES = {"pullback_standby", "bounce_standby"}

DELIVERY_STATE_VALUES = {"idle", "pending", "sent", "cleared"}

LEGACY_SETUP_STATE_MAP = {
    "candidate": "candidate",
    "watchlist": "watchlist",
    "alert_eligible": "actionable",
    "alerted": "notified",
    "invalidated": "invalidated",
    "stale": "stale",
}

CANONICAL_TO_LEGACY_SETUP_STATE_MAP = {
    "candidate": "candidate",
    "watchlist": "watchlist",
    "actionable": "alert_eligible",
    "notified": "alerted",
    "invalidated": "invalidated",
    "stale": "stale",
}

LEGACY_READINESS_STATE_MAP = {
    "wait_pullback": "pullback_standby",
    "wait_bounce": "bounce_standby",
    "ready_on_retest": "retest_ready",
}

CANONICAL_TO_LEGACY_READINESS_STATE_MAP = {
    "pullback_standby": "wait_pullback",
    "bounce_standby": "wait_bounce",
    "retest_ready": "ready_on_retest",
}

LEGACY_RUNTIME_STATE_MAP = {
    "inactive": "inactive",
    "armed": "armed",
    "zone_touched": "zone_touched",
    "trigger_confirmed": "confirmed",
    "alert_sent": "notified",
    "cooldown": "cooldown",
}

CANONICAL_TO_LEGACY_RUNTIME_STATE_MAP = {
    "inactive": "inactive",
    "armed": "armed",
    "zone_touched": "zone_touched",
    "confirmed": "trigger_confirmed",
    "notified": "alert_sent",
    "cooldown": "cooldown",
}

# ─── Canonical state maps ───────────────────────────────────────────────────────

CANONICAL_SETUP_STATE_MAP: Dict[str, str] = LEGACY_SETUP_STATE_MAP
CANONICAL_READINESS_STATE_MAP: Dict[str, str] = LEGACY_READINESS_STATE_MAP
CANONICAL_RUNTIME_STATE_MAP: Dict[str, str] = LEGACY_RUNTIME_STATE_MAP


# ─── Pure state query functions ────────────────────────────────────────────────

def canonical_setup_state(value: Any) -> Optional[str]:
    """Map a state value to its canonical form.

    Canonical forms: candidate, watchlist, actionable, notified, invalidated, stale
    """
    state = str(value or "").strip().lower()
    if not state:
        return None
    return CANONICAL_SETUP_STATE_MAP.get(state, state)


def canonical_readiness_state(value: Any) -> Optional[str]:
    """Map a readiness value to its canonical form.

    Canonical forms: pullback_standby, bounce_standby, retest_ready, standby
    """
    state = str(value or "").strip().lower()
    if not state:
        return None
    return LEGACY_READINESS_STATE_MAP.get(state, state)


def canonical_runtime_state(value: Any) -> Optional[str]:
    """Map a runtime value to its canonical form.

    Canonical forms: inactive, armed, zone_touched, confirmed, notified, cooldown
    """
    state = str(value or "").strip().lower()
    if not state:
        return None
    return LEGACY_RUNTIME_STATE_MAP.get(state, state)


def canonical_delivery_state(value: Any) -> Optional[str]:
    """Validate a delivery state value.

    Valid canonical forms: idle, pending, sent, cleared
    """
    state = str(value or "").strip().lower()
    if not state:
        return None
    return state if state in DELIVERY_STATE_VALUES else None


def setup_lifecycle_state(record: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extract canonical lifecycle state from a setup record.

    Reads: record["lifecycle"]["state"]
    """
    if not isinstance(record, dict):
        return None
    lifecycle = record.get("lifecycle") if isinstance(record.get("lifecycle"), dict) else {}
    state = lifecycle.get("state")
    if state:
        return canonical_setup_state(state)
    return None


def setup_readiness_state(record: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extract canonical readiness state from a setup record.

    Reads: record["readiness"]["state"]
    """
    if not isinstance(record, dict):
        return None
    readiness = record.get("readiness") if isinstance(record.get("readiness"), dict) else {}
    state = readiness.get("state")
    if state:
        return canonical_readiness_state(state)
    return None


def monitor_lifecycle_state(record: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extract canonical lifecycle state from a monitor record.

    Checks nested lifecycle.state first, then falls back to top-level lifecycleState.
    """
    nested = setup_lifecycle_state(record)
    if nested:
        return nested
    if not isinstance(record, dict):
        return None
    return canonical_setup_state(record.get("lifecycleState"))


def monitor_readiness_state(record: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extract canonical readiness state from a monitor record.

    Checks nested readiness.state first, then falls back to top-level readinessState.
    """
    nested = setup_readiness_state(record)
    if nested:
        return nested
    if not isinstance(record, dict):
        return None
    return canonical_readiness_state(record.get("readinessState"))


def delivery_state(record: Optional[Dict[str, Any]]) -> Optional[str]:
    """Extract canonical delivery state from a delivery record.

    Reads: record["delivery"]["state"]
    """
    if not isinstance(record, dict):
        return None
    delivery = record.get("delivery") if isinstance(record.get("delivery"), dict) else {}
    state = delivery.get("state")
    if state:
        return str(state).strip().lower()
    return None


def final_confirmation_identity(
    monitor: Optional[Dict[str, Any]],
    active_only: bool = False,
) -> Dict[str, Any]:
    """Extract finalConfirmation identity payload from a monitor record.

    Returns empty dict if no valid finalConfirmation found.
    Set active_only=True to require state == 'active'.
    """
    if not isinstance(monitor, dict):
        return {}
    fc = monitor.get("finalConfirmation")
    if not isinstance(fc, dict):
        return {}
    if active_only and fc.get("state") != "active":
        return {}
    return {
        "setupVersion": fc.get("setupVersion"),
        "readinessVersion": fc.get("readinessVersion"),
        "confirmedAt": fc.get("confirmedAt"),
        "resetAt": fc.get("resetAt"),
        "state": fc.get("state"),
    }