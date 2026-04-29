#!/usr/bin/env python3
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

WIB = timezone(timedelta(hours=7))


@dataclass
class EntryReadyEvent:
    event_type: str
    symbol: str
    side: str
    grade: Optional[str] = None
    entry_low: Optional[float] = None
    entry_high: Optional[float] = None
    stop_loss: Optional[float] = None
    tp1: Optional[float] = None
    tp2: Optional[float] = None
    tp3: Optional[float] = None
    current_price: Optional[float] = None
    confirmed_at: Optional[str] = None
    trigger: Optional[str] = None
    invalidation: Optional[str] = None
    execution_note: Optional[str] = None
    risk_flags: List[str] = field(default_factory=list)
    crowding_note: Optional[str] = None
    current_status: Optional[str] = None
    cancel_reason: Optional[str] = None
    setup_key: Optional[str] = None
    simulated: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)


class TelegramExecutionCardFormatter:
    HEADER_LABELS = {
        "READY_IN_ZONE": "ENTRY READY — LIMIT",
        "LIVE_STRICT": "ENTRY READY — CONFIRMED",
        "CANCEL": "SETUP INVALID",
    }

    CONVICTION_MAP = {
        "A+": "Very high conviction",
        "A": "High conviction",
        "B": "Moderate conviction",
        "C": "Low conviction",
    }

    RISK_FLAG_MAP = {
        "thin_depth": "Thin depth.",
        "short_squeeze_risk": "Short squeeze risk is elevated.",
        "long_squeeze_risk": "Long squeeze risk is elevated.",
        "recent_buy_pressure": "Buy pressure is elevated.",
        "recent_sell_pressure": "Sell pressure is elevated.",
        "long_crowded": "Long crowding risk is elevated.",
        "short_crowded": "Short crowding risk is elevated.",
    }

    def render(self, event: EntryReadyEvent) -> Tuple[str, List[str]]:
        if event.event_type == "READY_IN_ZONE":
            return self.render_ready_in_zone(event)
        if event.event_type == "LIVE_STRICT":
            return self.render_live_strict(event)
        if event.event_type == "CANCEL":
            return self.render_cancel(event)
        raise ValueError(f"unsupported event_type: {event.event_type}")

    def render_ready_in_zone(self, event: EntryReadyEvent) -> Tuple[str, List[str]]:
        fallbacks: List[str] = []
        header = self._render_header(event)
        status_line = self._ready_status_line(event, fallbacks)
        action_line = self._ready_action_line(event, fallbacks)
        thesis_line = self._thesis_line(event, fallbacks)
        risk_line = self._risk_line(event, fallbacks)
        card = self._join_sections([
            header,
            self._render_setup_section(event, title="Setup", fallbacks=fallbacks),
            "Status\n" + status_line + "\n" + action_line,
            "Thesis\n" + thesis_line,
            "Risk\n" + risk_line,
        ])
        return card, fallbacks

    def render_live_strict(self, event: EntryReadyEvent) -> Tuple[str, List[str]]:
        fallbacks: List[str] = []
        header = self._render_header(event)
        status_line = self._confirmed_status_line(event, fallbacks)
        action_line = self._confirmed_action_line(event, fallbacks)
        thesis_line = self._confirmed_thesis_line(event, fallbacks)
        risk_line = self._risk_line(event, fallbacks)
        card = self._join_sections([
            header,
            self._render_setup_section(event, title="Setup", fallbacks=fallbacks, execution_area=True),
            "Status\n" + status_line + "\n" + action_line,
            "Thesis\n" + thesis_line,
            "Risk\n" + risk_line,
        ])
        return card, fallbacks

    def render_cancel(self, event: EntryReadyEvent) -> Tuple[str, List[str]]:
        fallbacks: List[str] = []
        header = self._render_header(event, include_conviction=False)
        status_line = self._cancel_status_line(event, fallbacks)
        action_line = self._cancel_action_line(event, fallbacks)
        thesis_line = self._cancel_thesis_line(event, fallbacks)
        risk_line = self._cancel_risk_line(event, fallbacks)
        card = self._join_sections([
            header,
            self._render_setup_section(event, title="Setup", fallbacks=fallbacks, previous=True),
            "Status\n" + status_line + "\n" + action_line,
            "Thesis\n" + thesis_line,
            "Risk\n" + risk_line,
        ])
        return card, fallbacks

    def _render_header(self, event: EntryReadyEvent, include_conviction: bool = True) -> str:
        lines = [
            f"{self._clean(event.symbol)} {self._clean(str(event.side or '').upper())}".strip(),
        ]
        if include_conviction:
            conviction = self.CONVICTION_MAP.get(str(event.grade or "").upper(), None)
            if conviction and event.grade:
                lines.append(f"Grade {self._clean(event.grade)} | {conviction}")
            elif event.grade:
                lines.append(f"Grade {self._clean(event.grade)}")
            else:
                lines.append("Grade n/a")
        else:
            lines.append(f"Grade {self._clean(event.grade or 'n/a')}")
        lines.append(self.HEADER_LABELS.get(event.event_type, event.event_type))
        return "\n".join(lines)

    def _render_setup_section(self, event: EntryReadyEvent, title: str, fallbacks: List[str], execution_area: bool = False, previous: bool = False) -> str:
        zone_label = "Execution area" if execution_area else ("Previous entry zone" if previous else "Entry zone")
        stop_label = "Previous stop" if previous else "Stop loss"
        low = self._format_price(event.entry_low, fallbacks, "entry_low")
        high = self._format_price(event.entry_high, fallbacks, "entry_high")
        stop = self._format_price(event.stop_loss, fallbacks, "stop_loss")
        ordered_tp1, ordered_tp2, ordered_tp3 = self._display_take_profits(event, fallbacks)
        return "\n".join([
            title,
            f"{zone_label}: {low} - {high}",
            f"{stop_label}: {stop}",
            f"TP1: {ordered_tp1} | TP2: {ordered_tp2} | TP3: {ordered_tp3}",
        ])

    def _ready_status_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        current = self._format_price(event.current_price, fallbacks, "current_price", default="inside entry zone")
        if current == "inside entry zone":
            return "Price now: inside entry zone"
        return f"Price now: {current}, inside entry zone"

    def _confirmed_status_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        confirmed = self._format_wib_time(event.confirmed_at)
        if not confirmed:
            fallbacks.append("confirmed_time_missing")
            confirmed = "recent confirmation"
        return f"Confirmed: {confirmed}"

    def _cancel_status_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        reason = self._clean(event.cancel_reason)
        if reason:
            return f"Original setup no longer valid because {reason}."
        fallbacks.append("cancel_reason_generic")
        return "Original setup no longer valid."

    def _ready_action_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        zone = self._zone_text(event, fallbacks)
        side = self._clean(str(event.side or '').upper())
        boundary = self._format_price(event.entry_high, fallbacks, "entry_high_action")
        return f"Action: Place {side} limit inside {zone}. Do not chase above {boundary}."

    def _confirmed_action_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        zone = self._zone_text(event, fallbacks)
        side = self._clean(str(event.side or '').upper())
        return f"Action: {side} setup confirmed. Execute only near {zone}. Skip if price is already extended."

    def _cancel_action_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        side = self._clean(str(event.side or '').upper())
        return f"Action: Cancel any pending {side} order. Wait for a new setup."

    def _thesis_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        if event.side == "short":
            if event.trigger:
                return "Price is still failing to reclaim the entry area, keeping downside continuation valid."
            fallbacks.append("thesis_short_generic")
            return "Price remains weak around the entry area, keeping downside continuation valid."
        if event.side == "long":
            if event.trigger:
                return "The entry area is holding and the setup remains valid for upside continuation."
            fallbacks.append("thesis_long_generic")
            return "Price is holding the entry area, keeping upside continuation valid."
        fallbacks.append("thesis_unknown_side")
        return self._one_line(event.trigger or "Setup conditions remain valid.")

    def _confirmed_thesis_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        note = self._clean((event.metadata or {}).get("contextVerdict") or (event.metadata or {}).get("triggerReason"))
        if note:
            return "The 15m context remains aligned, keeping the confirmed setup valid."
        fallbacks.append("confirmed_thesis_from_trigger")
        return "The confirmation structure remains aligned, keeping the setup valid near the trigger area."

    def _cancel_thesis_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        if event.invalidation:
            return "The original execution context is broken and the setup no longer carries valid entry conditions."
        if event.cancel_reason:
            return "The original execution context is broken and the setup no longer carries valid entry conditions."
        fallbacks.append("cancel_thesis_generic")
        return "The original execution context is broken and the setup no longer carries valid entry conditions."

    def _cancel_risk_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        return "Treat any fresh move as a new setup, not continuation of the old one."

    def _risk_line(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        crowding = self._clean(event.crowding_note).lower()
        if "lean long" in crowding or "lean short" in crowding or "crowding risk" in crowding or "caution" in crowding:
            return "Crowding risk is building. Take partials early and protect after TP1."
        if "thin_depth" in [str(flag).strip().lower() for flag in (event.risk_flags or [])]:
            return f"Thin depth. {self._generic_invalidation_line(event.side)}"
        return self._generic_invalidation_line(event.side)

    def _generic_invalidation_line(self, side: str) -> str:
        if side == "short":
            return "Setup invalid if price reclaims and holds above the stop zone."
        if side == "long":
            return "Setup invalid if price loses and holds below the stop zone."
        return "Setup invalid if the stop zone is lost on a strong close."

    def _zone_text(self, event: EntryReadyEvent, fallbacks: List[str]) -> str:
        low = self._format_price(event.entry_low, fallbacks, "entry_low_action")
        high = self._format_price(event.entry_high, fallbacks, "entry_high_action")
        return f"{low} - {high}"

    def _display_take_profits(self, event: EntryReadyEvent, fallbacks: List[str]) -> Tuple[str, str, str]:
        raw_values = [("tp1", event.tp1), ("tp2", event.tp2), ("tp3", event.tp3)]
        valid = []
        missing = []
        for label, value in raw_values:
            if value is None:
                fallbacks.append(f"{label}_missing")
                missing.append(label)
            else:
                try:
                    valid.append((label, float(value)))
                except Exception:
                    fallbacks.append(f"{label}_invalid")
                    missing.append(label)
        if event.side == "short":
            valid_sorted = sorted(valid, key=lambda item: item[1], reverse=True)
        else:
            valid_sorted = sorted(valid, key=lambda item: item[1])
        if [label for label, _ in valid_sorted] != [label for label, _ in valid]:
            fallbacks.append("tp_reordered_for_display")
        ordered = [self._format_price(value, fallbacks, label) for label, value in valid_sorted]
        while len(ordered) < 3:
            ordered.append("open")
        return ordered[0], ordered[1], ordered[2]

    def _format_price(self, value: Optional[float], fallbacks: List[str], label: str, default: str = "n/a") -> str:
        if value is None:
            fallbacks.append(f"{label}_missing")
            return default
        if isinstance(value, int):
            return str(value)
        try:
            text = f"{float(value):.10f}".rstrip("0").rstrip(".")
            return text or "0"
        except Exception:
            fallbacks.append(f"{label}_invalid")
            return default

    def _format_wib_time(self, iso_text: Optional[str]) -> Optional[str]:
        if not iso_text:
            return None
        try:
            dt = datetime.fromisoformat(str(iso_text).replace("Z", "+00:00"))
        except Exception:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(WIB).strftime("%H:%M WIB")

    def _one_line(self, text: str, max_len: int = 120) -> str:
        cleaned = self._clean(text)
        if len(cleaned) <= max_len:
            return cleaned
        trimmed = cleaned[: max_len - 1].rstrip()
        last_space = trimmed.rfind(" ")
        if last_space > 40:
            trimmed = trimmed[:last_space]
        return trimmed.rstrip(" ,.;:") + "…"

    def _clean(self, text: Any) -> str:
        return " ".join(str(text or "").replace("\n", " ").split()).strip()

    def _join_sections(self, sections: List[str]) -> str:
        return "\n\n".join(section.strip() for section in sections if section and section.strip()).strip()
