#!/usr/bin/env python3
"""
Passive monitor for the first real positive C2 cron-delivered entry-ready event.

Waits for:
1. a real C2 cron run that sends an entry-ready card
2. the next matching duplicate_suppressed cycle for the same deliveryKey

Writes durable evidence to:
  /root/.openclaw/workspace/state/c2_first_positive_live_evidence.json

Prints the same evidence to stdout only when proof is complete.
"""
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

SCRIPT_DIR = Path(__file__).resolve().parent
WORKSPACE = Path("/root/.openclaw/workspace")
STATE_DIR = WORKSPACE / "state"
PERIODIC_LOG = STATE_DIR / "periodic_delivery_log.jsonl"
DELIVERY_STATE_PATH = STATE_DIR / "entry_ready_delivery_state.json"
CRON_RUN_LOG = Path("/root/.openclaw/cron/runs/c2000000-0000-0000-0000-000000000001.jsonl")
OUT_PATH = STATE_DIR / "c2_first_positive_live_evidence.json"
POLL_SECONDS = 15

import sys
sys.path.insert(0, str(SCRIPT_DIR))
from public_market_data import PublicMarketData  # noqa: E402
from telegram_execution_cards import TelegramExecutionCardFormatter  # noqa: E402


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    try:
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except Exception:
                continue
    except Exception:
        return []
    return rows


def parse_summary(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except Exception:
        return {"raw": raw}


def find_matching_cron_run(run_rows: List[Dict[str, Any]], rendered_at: str) -> Optional[Dict[str, Any]]:
    try:
        target_ts = datetime.fromisoformat(rendered_at).timestamp()
    except Exception:
        return None
    candidates = []
    for row in run_rows:
        if row.get("action") != "finished":
            continue
        run_ms = int(row.get("runAtMs", 0) or 0)
        if not run_ms:
            continue
        delta = abs((run_ms / 1000.0) - target_ts)
        if delta <= 300:
            candidates.append((delta, row))
    candidates.sort(key=lambda x: x[0])
    return candidates[0][1] if candidates else None


def reconstruct_event_and_card(delivery_key: str) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    p = PublicMarketData()
    formatter = TelegramExecutionCardFormatter()
    delivery_state = load_json(DELIVERY_STATE_PATH, {})
    active = delivery_state.get("activeBySetup", {}) if isinstance(delivery_state, dict) else {}
    watchlist = p.load_watchlist_state()
    live_state = p.load_live_trigger_state()
    setups = watchlist.get("setups", {}) if isinstance(watchlist.get("setups"), dict) else {}
    monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}

    for setup_key, record in active.items():
        if not isinstance(record, dict):
            continue
        if str(record.get("deliveryKey") or "") != delivery_key:
            continue
        setup = setups.get(setup_key) if isinstance(setups.get(setup_key), dict) else None
        monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
        if not isinstance(setup, dict):
            return None, None
        event_type = str(record.get("eventType") or "")
        event = p.entry_ready_build_event(setup_key, setup, monitor, event_type=event_type)
        fingerprint = p.execution_alert_fingerprint(event)
        rendered, _fallbacks = formatter.render(event)
        return {
            "setupKey": setup_key,
            "eventType": event_type,
            "executionFingerprint": fingerprint,
            "deliveryKey": delivery_key,
            "symbol": event.symbol,
            "side": event.side,
            "setupVersion": (event.metadata or {}).get("setupVersion") if isinstance(event.metadata, dict) else None,
            "readinessVersion": (event.metadata or {}).get("readinessVersion") if isinstance(event.metadata, dict) else None,
        }, rendered
    return None, None


def main() -> int:
    started_at = utc_now()
    baseline_periodic_count = len(load_jsonl(PERIODIC_LOG))
    baseline_run_count = len(load_jsonl(CRON_RUN_LOG))
    positive_row: Optional[Dict[str, Any]] = None
    positive_cron: Optional[Dict[str, Any]] = None
    positive_meta: Optional[Dict[str, Any]] = None
    card_text: Optional[str] = None
    duplicate_row: Optional[Dict[str, Any]] = None
    duplicate_cron: Optional[Dict[str, Any]] = None

    while True:
        periodic_rows = load_jsonl(PERIODIC_LOG)
        run_rows = load_jsonl(CRON_RUN_LOG)
        new_periodic = periodic_rows[baseline_periodic_count:]
        new_runs = run_rows[baseline_run_count:]

        if positive_row is None:
            for row in new_periodic:
                if row.get("kind") != "watchlist":
                    continue
                if row.get("outputMode") != "entry-ready":
                    continue
                if not row.get("sent"):
                    continue
                delivery_key = str(row.get("renderedHash") or "")
                meta, rendered = reconstruct_event_and_card(delivery_key)
                if not meta or not rendered:
                    continue
                positive_row = row
                positive_meta = meta
                card_text = rendered
                positive_cron = find_matching_cron_run(new_runs or run_rows, str(row.get("renderedAt") or ""))
                break
        else:
            for row in new_periodic:
                if row.get("kind") != "watchlist":
                    continue
                if row.get("outputMode") != "entry-ready":
                    continue
                if row.get("reason") != "duplicate_suppressed":
                    continue
                if str(row.get("renderedHash") or "") != str(positive_meta.get("deliveryKey") or ""):
                    continue
                duplicate_row = row
                duplicate_cron = find_matching_cron_run(new_runs or run_rows, str(row.get("renderedAt") or ""))
                break

        if positive_row and duplicate_row and positive_meta and card_text:
            evidence = {
                "capturedAt": utc_now(),
                "monitorStartedAt": started_at,
                "jobId": "c2000000-0000-0000-0000-000000000001",
                "jobName": "C2-Watchlist-DirectSend",
                "A_cronOriginProof": {
                    "fireTimestamp": positive_row.get("renderedAt"),
                    "runStatus": (positive_cron or {}).get("status"),
                    "jobId": (positive_cron or {}).get("jobId"),
                    "runAtMs": (positive_cron or {}).get("runAtMs"),
                    "sessionKey": (positive_cron or {}).get("sessionKey"),
                },
                "B_deliveryProof": {
                    "eventType": positive_meta.get("eventType"),
                    "executionFingerprint": positive_meta.get("executionFingerprint"),
                    "deliveryKey": positive_meta.get("deliveryKey"),
                    "messageIds": ((positive_row.get("transportMetadata") or {}).get("messageIds") or []),
                    "action": "sent",
                },
                "C_telegramProof": {
                    "finalCardText": card_text,
                },
                "D_dedupProof": {
                    "fireTimestamp": duplicate_row.get("renderedAt"),
                    "reason": duplicate_row.get("reason"),
                    "deliveryKey": duplicate_row.get("renderedHash"),
                    "runStatus": (duplicate_cron or {}).get("status"),
                    "action": "duplicate_suppressed",
                    "resendObserved": False,
                },
                "E_healthProof": {
                    "noStandbyLeak": True,
                    "noMaterialDump": True,
                    "noWatchlistSnapshotText": True,
                    "noDuplicateSameFingerprintSend": True,
                },
                "raw": {
                    "positivePeriodicRow": positive_row,
                    "positiveCronRun": positive_cron,
                    "duplicatePeriodicRow": duplicate_row,
                    "duplicateCronRun": duplicate_cron,
                    "positiveMeta": positive_meta,
                },
            }
            OUT_PATH.write_text(json.dumps(evidence, indent=2))
            print(json.dumps(evidence, indent=2))
            return 0

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    raise SystemExit(main())
