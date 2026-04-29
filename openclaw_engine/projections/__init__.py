"""
openclaw_engine.projections — Notification projection layer.

Pure functions for building notification DTOs and lane assignment.
These are standalone projections — no file I/O, no state mutations.
"""
from openclaw_engine.projections.notification_entry import (
    notification_crowding_note,
    notification_entry_from_setup,
    notification_lane_for_setup,
)

__all__ = [
    "notification_crowding_note",
    "notification_entry_from_setup",
    "notification_lane_for_setup",
]
