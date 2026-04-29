#!/usr/bin/env python3
import argparse
import json
import shutil
from pathlib import Path

from public_market_data import PublicMarketData, save_json_file, utc_now_iso


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rewrite live_trigger_engine.json with canonical nested monitor truth and strip stale top-level aliases.")
    parser.add_argument("--state-path", default="/root/.openclaw/workspace/state/live_trigger_engine.json")
    parser.add_argument("--backup-path", default=None)
    parser.add_argument("--no-backup", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    state_path = Path(args.state_path)
    if not state_path.exists():
        raise SystemExit(f"state file not found: {state_path}")

    backup_path = Path(args.backup_path) if args.backup_path else state_path.with_name(f"{state_path.stem}.pre_batch1c_alias_cleanup.json")
    if not args.no_backup:
        backup_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(state_path, backup_path)

    raw_state = json.loads(state_path.read_text())
    p = PublicMarketData()
    normalized = p._normalize_live_trigger_state_payload(raw_state)
    payload, counts = p.clean_live_trigger_state_payload(normalized)
    payload["updatedAt"] = utc_now_iso()
    save_json_file(state_path, payload)

    result = {
        "statePath": str(state_path),
        "backupPath": None if args.no_backup else str(backup_path),
        "monitorCount": len(payload.get("monitors") or {}),
        "counts": counts,
    }
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
