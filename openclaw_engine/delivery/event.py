"""
delivery/event.py — Entry-ready event delivery pure functions.

Extracted from public_market_data.py lines 8854-8867:
- entry_ready_event_priority(): map event_type to priority integer
- entry_ready_sort_key(): sort key for EntryReadyEvent objects

These are pure functions — no file I/O, no state mutations.
"""
from typing import Any

try:
    from telegram_execution_cards import EntryReadyEvent
except Exception:
    EntryReadyEvent = None  # type: ignore

from openclaw_engine.scoring import grade_priority


EVENT_PRIORITY_MAP = {"LIVE_STRICT": 2, "READY_IN_ZONE": 1, "CANCEL": 0}


def entry_ready_event_priority(event_type: str) -> int:
    """Map an event type string to a sort priority.

    Higher = more urgent.
    """
    return EVENT_PRIORITY_MAP.get(str(event_type or ""), -1)


def entry_ready_sort_key(event: Any) -> Any:
    """Sort key for EntryReadyEvent: priority + grade + cleanScore + symbol + side."""
    if EntryReadyEvent is not None and not isinstance(event, EntryReadyEvent):
        return (-1, 0, 0.0, "", "")
    meta = event.metadata if isinstance(event.metadata, dict) else {}
    return (
        entry_ready_event_priority(event.event_type),
        grade_priority(event.grade),
        float(meta.get("cleanScore", 0.0) or 0.0),
        str(event.symbol or ""),
        str(event.side or ""),
    )
