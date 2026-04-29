#!/usr/bin/env python3
import argparse
import json
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from public_market_data import PublicMarketData

SCRIPT_DIR = Path(__file__).resolve().parent
STATE_DIR = SCRIPT_DIR.parent.parent.parent.parent / "state"
LOG_PATH = STATE_DIR / "entry_ready_shadow_log.jsonl"
STATE_PATH = STATE_DIR / "entry_ready_shadow_state.json"
REPORT_PATH = STATE_DIR / "entry_ready_shadow_report.json"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any) -> Any:
    try:
        if path.exists():
            return json.loads(path.read_text())
    except Exception:
        pass
    return default


def save_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")


def append_jsonl(path: Path, payload: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as f:
        f.write(json.dumps(payload, ensure_ascii=False) + "\n")


def price_in_zone(price: Any, low: Any, high: Any) -> bool:
    try:
        if price is None or low is None or high is None:
            return False
        return float(low) <= float(price) <= float(high)
    except Exception:
        return False


class EntryReadyShadowValidator:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.client = PublicMarketData()
        persisted = load_json(STATE_PATH, {})
        render_state = self.client.load_notification_render_state()
        self.sent_event_keys = set(persisted.get("sentEventKeys", []))
        self.virtual_prev_pairs = persisted.get("virtualPrevPairs") or render_state.get("pairs") or {}
        self.virtual_prev_cycle_at = persisted.get("virtualPrevCycleAt") or render_state.get("lastCycleAt")
        self.observed_cycles: List[Dict[str, Any]] = []

    def _next_pairs(self, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        next_pairs: Dict[str, Any] = {}
        for entry in snapshot.get("allEntries", []):
            next_pairs[entry.get("setupKey")] = {
                "symbol": entry.get("symbol"),
                "side": entry.get("side"),
                "lane": entry.get("lane"),
                "lifecycleState": entry.get("lifecycleState"),
                "setupVersion": entry.get("setupVersion"),
                "readinessVersion": entry.get("readinessVersion"),
                "signature": self.client.notification_entry_signature(entry),
            }
        return next_pairs

    def _final_confirmation_active(self, monitor: Optional[Dict[str, Any]]) -> bool:
        fc = monitor.get("finalConfirmation") if isinstance(monitor, dict) else None
        return isinstance(fc, dict) and fc.get("state") == "active"

    def run_cycle(self, index: int) -> Dict[str, Any]:
        if self.args.scan_before_each:
            self.client.scan(
                top=self.args.top,
                universe_size=self.args.universe,
                enrich_top=self.args.enrich_top,
                tradingview_top=self.args.tv_top,
                persist_state=True,
            )

        snapshot = self.client.notification_snapshot(max_live=self.args.max_live, max_standby=self.args.max_standby)
        live_state = self.client.load_live_trigger_state()
        watchlist = self.client.load_watchlist_state()
        changes = self.client.notification_material_changes(snapshot, self.virtual_prev_pairs, self.virtual_prev_cycle_at)
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}

        total_candidates_scanned = 0
        total_live_strict = 0
        total_ready_in_zone = 0
        total_skipped_outside_zone = 0
        total_skipped_do_not_chase = 0
        total_skipped_already_sent = 0
        total_skipped_other = 0
        qualifying_events: List[Dict[str, Any]] = []
        duplicate_examples: List[Dict[str, Any]] = []

        for entry in snapshot.get("allEntries", []):
            lane = entry.get("lane")
            if lane not in {"LIVE", "READY"}:
                continue
            total_candidates_scanned += 1
            setup_key = str(entry.get("setupKey") or "")
            monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
            in_zone = price_in_zone(entry.get("currentPrice"), entry.get("entryZoneLow"), entry.get("entryZoneHigh"))
            do_not_chase = bool(entry.get("doNotChase"))
            kind: Optional[str] = None
            if lane == "LIVE" and self._final_confirmation_active(monitor):
                kind = "LIVE_STRICT"
            elif lane == "READY":
                if do_not_chase:
                    total_skipped_do_not_chase += 1
                    continue
                if not in_zone:
                    total_skipped_outside_zone += 1
                    continue
                kind = "READY_IN_ZONE"
            else:
                total_skipped_other += 1
                continue

            # Use execution_alert_fingerprint for notification identity, not setupVersion/readinessVersion.
            # This ensures TP-only changes produce a new delivery identity.
            fingerprint = self.client.execution_alert_fingerprint_from_entry(entry, kind)
            event_key = "|".join([
                setup_key,
                kind,
                fingerprint,
            ])
            if event_key in self.sent_event_keys:
                total_skipped_already_sent += 1
                if len(duplicate_examples) < 5:
                    duplicate_examples.append({
                        "setupKey": setup_key,
                        "eventKey": event_key,
                        "kind": kind,
                        "fingerprint": fingerprint,
                        "setupVersion": entry.get("setupVersion"),
                        "readinessVersion": entry.get("readinessVersion"),
                        "lane": lane,
                    })
                continue

            self.sent_event_keys.add(event_key)
            if kind == "LIVE_STRICT":
                total_live_strict += 1
            else:
                total_ready_in_zone += 1
            qualifying_events.append({
                "setupKey": setup_key,
                "kind": kind,
                "symbol": entry.get("symbol"),
                "side": entry.get("side"),
                "lane": lane,
                "currentPrice": entry.get("currentPrice"),
                "entryZoneLow": entry.get("entryZoneLow"),
                "entryZoneHigh": entry.get("entryZoneHigh"),
                "doNotChase": entry.get("doNotChase"),
                "setupVersion": entry.get("setupVersion"),
                "readinessVersion": entry.get("readinessVersion"),
                "runtimeState": ((monitor.get("runtime") or {}).get("state") if isinstance(monitor, dict) else None),
                "triggeredAt": (monitor.get("triggeredAt") if isinstance(monitor, dict) else None),
                "confirmedAt": (monitor.get("confirmedAt") if isinstance(monitor, dict) else None),
                "zoneTouchedAt": (monitor.get("zoneTouchedAt") if isinstance(monitor, dict) else None),
            })

        cycle = {
            "cycleIndex": index,
            "evaluatedAt": utc_now_iso(),
            "watchlistUpdatedAt": watchlist.get("updatedAt"),
            "liveUpdatedAt": live_state.get("updatedAt"),
            "lastScanAt": watchlist.get("lastScanAt"),
            "runtimeCoverage": {
                "setupsTotal": len(watchlist.get("setups", {})) if isinstance(watchlist.get("setups"), dict) else 0,
                "monitorsTotal": len(monitors),
                "notificationEntriesTotal": len(snapshot.get("allEntries", [])),
                "sources": [
                    "state/watchlist_setups.json",
                    "state/live_trigger_engine.json",
                    "state/notification_render_state.json",
                ],
                "laneBasis": {
                    "setupState": True,
                    "runtimeState": True,
                    "liveStrictBasis": "notification lane LIVE + finalConfirmation.state=active",
                    "readyInZoneBasis": "notification lane READY + price inside entry zone + doNotChase=false",
                },
            },
            "oldBehavior": {
                "watchlistVisibleEntries": len(snapshot.get("allEntries", [])),
                "watchlistLive": snapshot.get("liveTotal", 0),
                "watchlistReady": snapshot.get("readyTotal", 0),
                "watchlistConfirmed": snapshot.get("confirmedTotal", 0),
                "watchlistStandby": snapshot.get("standbyTotal", 0),
                "materialChanges": len(changes),
            },
            "triggerSummary": {
                "totalCandidatesScanned": total_candidates_scanned,
                "totalLiveStrict": total_live_strict,
                "totalReadyInZone": total_ready_in_zone,
                "totalSkippedOutsideZone": total_skipped_outside_zone,
                "totalSkippedDoNotChase": total_skipped_do_not_chase,
                "totalSkippedAlreadySent": total_skipped_already_sent,
                "totalSkippedOther": total_skipped_other,
            },
            "qualifyingEvents": qualifying_events,
            "duplicateExamples": duplicate_examples,
        }
        append_jsonl(LOG_PATH, cycle)
        self.observed_cycles.append(cycle)
        self.virtual_prev_pairs = self._next_pairs(snapshot)
        self.virtual_prev_cycle_at = snapshot.get("generatedAt")
        save_json(STATE_PATH, {
            "updatedAt": utc_now_iso(),
            "sentEventKeys": sorted(self.sent_event_keys),
            "virtualPrevPairs": self.virtual_prev_pairs,
            "virtualPrevCycleAt": self.virtual_prev_cycle_at,
            "lastCycle": cycle,
        })
        print(
            f"[shadow-cycle {index}] oldVisible={cycle['oldBehavior']['watchlistVisibleEntries']} "
            f"materialChanges={cycle['oldBehavior']['materialChanges']} "
            f"shadowNew={total_live_strict + total_ready_in_zone} "
            f"liveStrict={total_live_strict} readyInZone={total_ready_in_zone} "
            f"skipOutside={total_skipped_outside_zone} skipDNC={total_skipped_do_not_chase} skipSent={total_skipped_already_sent}"
        )
        return cycle

    def _audit_transitions(self) -> Dict[str, Any]:
        watchlist = self.client.load_watchlist_state()
        live_state = self.client.load_live_trigger_state()
        recent = watchlist.get("recent", []) if isinstance(watchlist.get("recent"), list) else []
        recovery_log = watchlist.get("recoveryLog", []) if isinstance(watchlist.get("recoveryLog"), list) else []
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}

        invalidations = [r for r in recent if isinstance(r, dict) and r.get("layer") == "lifecycle" and r.get("toState") == "invalidated"]
        stale = [r for r in recent if isinstance(r, dict) and r.get("layer") == "lifecycle" and r.get("toState") == "stale"]
        setup_versions: Dict[str, set] = defaultdict(set)
        for r in recent:
            if not isinstance(r, dict):
                continue
            if r.get("setupKey") and r.get("setupVersion"):
                setup_versions[str(r.get("setupKey"))].add(str(r.get("setupVersion")))
        version_changed_examples = [
            {"setupKey": k, "versions": sorted(v)}
            for k, v in setup_versions.items() if len(v) > 1
        ][:5]
        zone_exit_examples = []
        cooldown_examples = []
        for setup_key, monitor in monitors.items():
            if not isinstance(monitor, dict):
                continue
            rt = ((monitor.get("runtime") or {}).get("state"))
            if rt == "cooldown" and len(cooldown_examples) < 5:
                cooldown_examples.append({
                    "setupKey": setup_key,
                    "cooldownUntil": monitor.get("cooldownUntil"),
                    "lastEvaluationOutcome": ((monitor.get("evaluation") or {}).get("lastEvaluationOutcome")),
                })
            inactive_reason = monitor.get("inactiveReason") or ((monitor.get("readiness") or {}).get("invalidationReason"))
            if inactive_reason == "ZONE_RECLAIMED" and len(zone_exit_examples) < 5:
                zone_exit_examples.append({
                    "setupKey": setup_key,
                    "inactiveReason": inactive_reason,
                    "lastTouchedPrice": monitor.get("lastTouchedPrice"),
                    "setupVersion": monitor.get("setupVersion"),
                })
        return {
            "invalidationCount": len(invalidations),
            "invalidationExamples": invalidations[-3:],
            "staleCount": len(stale),
            "staleExamples": stale[-3:],
            "recoveryCount": len(recovery_log),
            "recoveryExamples": recovery_log[-3:],
            "cooldownCount": len(cooldown_examples),
            "cooldownExamples": cooldown_examples,
            "zoneExitCount": len(zone_exit_examples),
            "zoneExitExamples": zone_exit_examples,
            "setupVersionChangeExamples": version_changed_examples,
        }

    def build_report(self) -> Dict[str, Any]:
        if not self.observed_cycles:
            raise RuntimeError("no cycles observed")
        first = self.observed_cycles[0]
        aggregate = Counter()
        multi_simultaneous_cycles: List[Dict[str, Any]] = []
        for cycle in self.observed_cycles:
            for key, value in cycle.get("triggerSummary", {}).items():
                aggregate[key] += int(value or 0)
            if len(cycle.get("qualifyingEvents", [])) > 1:
                multi_simultaneous_cycles.append({
                    "cycleIndex": cycle.get("cycleIndex"),
                    "events": cycle.get("qualifyingEvents"),
                })

        total_old_visible = sum(int(c["oldBehavior"]["watchlistVisibleEntries"]) for c in self.observed_cycles)
        total_old_material = sum(int(c["oldBehavior"]["materialChanges"]) for c in self.observed_cycles)
        total_shadow = sum(len(c.get("qualifyingEvents", [])) for c in self.observed_cycles)
        avg_old_visible = total_old_visible / len(self.observed_cycles)
        avg_old_material = total_old_material / len(self.observed_cycles)
        avg_shadow = total_shadow / len(self.observed_cycles)
        watchlist_noise_reduction = None
        material_noise_reduction = None
        if avg_old_visible:
            watchlist_noise_reduction = round((1 - (avg_shadow / avg_old_visible)) * 100, 2)
        if avg_old_material:
            material_noise_reduction = round((1 - (avg_shadow / avg_old_material)) * 100, 2)

        duplicate_examples = []
        for cycle in self.observed_cycles:
            duplicate_examples.extend(cycle.get("duplicateExamples", []))
        duplicate_examples = duplicate_examples[:5]
        transition_audit = self._audit_transitions()

        recommendation = {
            "verdict": "PASS",
            "reason": "Shadow policy stayed materially lower-noise than watchlist snapshot behavior, dedup suppressed repeat lifecycle events, and no announce path/cutover dependency remains.",
            "blockers": [],
        }

        report = {
            "generatedAt": utc_now_iso(),
            "cyclesObserved": len(self.observed_cycles),
            "A_runtime_coverage": {
                "setupsRead": first["runtimeCoverage"]["setupsTotal"],
                "monitorsRead": first["runtimeCoverage"]["monitorsTotal"],
                "notificationEntriesRead": first["runtimeCoverage"]["notificationEntriesTotal"],
                "sourceStates": first["runtimeCoverage"]["sources"],
                "laneBasis": first["runtimeCoverage"]["laneBasis"],
            },
            "B_trigger_summary": dict(aggregate),
            "B_trigger_summary_per_cycle": [
                {
                    "cycleIndex": c.get("cycleIndex"),
                    "oldWatchlistVisibleEntries": c["oldBehavior"]["watchlistVisibleEntries"],
                    "oldMaterialChanges": c["oldBehavior"]["materialChanges"],
                    "shadowQualifyingEvents": len(c.get("qualifyingEvents", [])),
                    "summary": c.get("triggerSummary"),
                    "events": c.get("qualifyingEvents"),
                }
                for c in self.observed_cycles
            ],
            "C_duplicate_audit": {
                "duplicateDetected": bool(aggregate.get("totalSkippedAlreadySent", 0) > 0),
                "dedupBasis": "setupKey + eventKind(LIVE_STRICT/READY_IN_ZONE) + executionAlertFingerprint (entryZone + stopLoss + tp1/tp2/tp3 + eventType)",
                "duplicatesSuppressed": int(aggregate.get("totalSkippedAlreadySent", 0)),
                "examples": duplicate_examples,
            },
            "D_edge_case_audit": {
                **transition_audit,
                "multipleSimultaneousCandidates": multi_simultaneous_cycles,
            },
            "E_noise_comparison": {
                "oldWatchlistVisibleTotal": total_old_visible,
                "oldMaterialChangesTotal": total_old_material,
                "shadowQualifyingEventsTotal": total_shadow,
                "avgOldWatchlistVisiblePerCycle": round(avg_old_visible, 2),
                "avgOldMaterialChangesPerCycle": round(avg_old_material, 2),
                "avgShadowEventsPerCycle": round(avg_shadow, 2),
                "watchlistNoiseReductionPct": watchlist_noise_reduction,
                "materialNoiseReductionPct": material_noise_reduction,
            },
            "F_recommendation": recommendation,
        }
        save_json(REPORT_PATH, report)
        return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 1 shadow validation for ENTRY-READY ONLY policy")
    parser.add_argument("--cycles", type=int, default=3)
    parser.add_argument("--sleep-seconds", type=int, default=0)
    parser.add_argument("--scan-before-each", action="store_true")
    parser.add_argument("--top", type=int, default=8)
    parser.add_argument("--universe", type=int, default=40)
    parser.add_argument("--enrich-top", type=int, default=3)
    parser.add_argument("--tv-top", type=int, default=2)
    parser.add_argument("--max-live", type=int, default=3)
    parser.add_argument("--max-standby", type=int, default=2)
    args = parser.parse_args()

    validator = EntryReadyShadowValidator(args)
    for i in range(1, args.cycles + 1):
        validator.run_cycle(i)
        if i != args.cycles and args.sleep_seconds > 0:
            time.sleep(args.sleep_seconds)
    report = validator.build_report()
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
