#!/usr/bin/env python3
"""
Live Trigger Engine Watchdog
Auto-starts and restarts the live trigger engine if it dies.
Run via systemd timer every 2 minutes, or as a standalone daemon.
"""
import sys, os, signal, subprocess, time, pathlib, json
from datetime import datetime, timezone
from pathlib import Path

SKILL_DIR = Path(__file__).resolve().parent.parent
SCRIPT_PATH = SKILL_DIR / "scripts" / "public_market_data.py"
PID_PATH = Path("/root/.openclaw/workspace/state/live_trigger_engine.pid")
STATE_PATH = Path("/root/.openclaw/workspace/state/live_trigger_engine.json")
LOG_PATH = Path("/root/.openclaw/workspace/state/live_trigger_watchdog.log")
WATCHDOG_PID_PATH = Path("/root/.openclaw/workspace/state/live_trigger_watchdog.pid")
WATCHDOG_STATE_PATH = Path("/root/.openclaw/workspace/state/live_trigger_watchdog_state.json")

# Default engine config (matches Phase 8 defaults)
DEFAULT_CHANNEL = "telegram"
DEFAULT_TARGET = "851120836"
DEFAULT_TRIGGER_TF = "5m"
DEFAULT_CONTEXT_TF = "15m"
DEFAULT_POLL_SECONDS = 15
DEFAULT_COOLDOWN_SECONDS = 1800

HEARTBEAT_INTERVAL_SECONDS = 60
STALE_THRESHOLD_SECONDS = 180  # 3 minutes without heartbeat = restart
WATCHDOG_WS_PERSISTENCE_SECONDS = 120
LIVE_TRIGGER_LIFECYCLE_OPEN_STATES = {"entered", "tp1", "tp2"}


def log(msg: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a") as f:
        f.write(line + "\n")


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def find_engine_pids() -> list[int]:
    try:
        result = subprocess.run(["pgrep", "-f", "public_market_data.py live-trigger-run"], capture_output=True, text=True, check=False)
        if result.returncode not in {0, 1}:
            return []
        current_pid = os.getpid()
        pids: list[int] = []
        for raw in (result.stdout or "").splitlines():
            raw = raw.strip()
            if not raw.isdigit():
                continue
            pid = int(raw)
            if pid == current_pid:
                continue
            if is_process_alive(pid):
                pids.append(pid)
        return sorted(set(pids))
    except Exception:
        return []


def load_state() -> dict:
    try:
        if STATE_PATH.exists():
            with STATE_PATH.open() as f:
                return json.load(f)
    except Exception:
        pass
    return {}


def load_watchdog_state() -> dict:
    try:
        if WATCHDOG_STATE_PATH.exists():
            with WATCHDOG_STATE_PATH.open() as f:
                payload = json.load(f)
                if isinstance(payload, dict):
                    return payload
    except Exception:
        pass
    return {}


def save_watchdog_state(state: dict) -> None:
    WATCHDOG_STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with WATCHDOG_STATE_PATH.open("w") as f:
        json.dump(state if isinstance(state, dict) else {}, f, indent=2)


def clear_watchdog_ws_marker(reason: str | None = None) -> None:
    state = load_watchdog_state()
    state["wsStaleObservedAt"] = None
    if reason is not None:
        state["lastReason"] = reason
    save_watchdog_state(state)


def mark_watchdog_restart(reason: str) -> None:
    state = load_watchdog_state()
    state["wsStaleObservedAt"] = None
    state["lastReason"] = reason
    state["lastRestartAt"] = datetime.now(timezone.utc).isoformat()
    save_watchdog_state(state)


def read_engine_pid() -> int | None:
    try:
        if PID_PATH.exists():
            txt = PID_PATH.read_text().strip()
            if txt.isdigit():
                return int(txt)
    except Exception:
        pass
    return None


def read_watchdog_pid() -> int | None:
    try:
        if WATCHDOG_PID_PATH.exists():
            txt = WATCHDOG_PID_PATH.read_text().strip()
            if txt.isdigit():
                return int(txt)
    except Exception:
        pass
    return None


def write_watchdog_pid(pid: int) -> None:
    WATCHDOG_PID_PATH.write_text(str(pid))


def start_engine(reason: str = "manual") -> bool:
    """Start the live trigger engine."""
    existing_pids = find_engine_pids()
    if existing_pids:
        PID_PATH.write_text(str(existing_pids[0]))
        clear_watchdog_ws_marker(reason=f"existing_process:{reason}")
        log(f"Engine already running with PID(s) {existing_pids}. Skip new start.")
        return True
    cmd = [
        sys.executable,
        str(SCRIPT_PATH),
        "live-trigger-run",
        "--channel", DEFAULT_CHANNEL,
        "--target", DEFAULT_TARGET,
        "--trigger-timeframe", DEFAULT_TRIGGER_TF,
        "--context-timeframe", DEFAULT_CONTEXT_TF,
        "--poll-seconds", str(DEFAULT_POLL_SECONDS),
        "--cooldown-seconds", str(DEFAULT_COOLDOWN_SECONDS),
    ]
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    # Ensure openclaw_engine package is importable
    env = os.environ.copy()
    pythonpath = env.get("PYTHONPATH", "")
    skill_pythonpath = str(SKILL_DIR)
    env["PYTHONPATH"] = f"{skill_pythonpath}{os.pathsep}{pythonpath}" if pythonpath else skill_pythonpath
    try:
        with LOG_PATH.open("a") as log_handle:
            proc = subprocess.Popen(cmd, stdout=log_handle, stderr=log_handle, start_new_session=True, env=env)
        PID_PATH.write_text(str(proc.pid))
        mark_watchdog_restart(reason)
        log(f"Engine started with PID {proc.pid}")
        return True
    except Exception as e:
        log(f"Failed to start engine: {e}")
        return False


def stop_engine(pid: int) -> None:
    target_pids = sorted(set(([pid] if pid else []) + find_engine_pids()))
    for target_pid in target_pids:
        try:
            os.kill(target_pid, signal.SIGTERM)
        except OSError:
            pass
    deadline = time.time() + 4.0
    while time.time() < deadline and any(is_process_alive(target_pid) for target_pid in target_pids):
        time.sleep(0.2)
    for target_pid in target_pids:
        if is_process_alive(target_pid):
            try:
                os.kill(target_pid, signal.SIGKILL)
            except OSError:
                pass
    try:
        PID_PATH.unlink(missing_ok=True)
    except Exception:
        pass


def monitor_lifecycle_open(monitor: dict) -> bool:
    lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
    state = lifecycle.get("state")
    return str(state) in LIVE_TRIGGER_LIFECYCLE_OPEN_STATES if state else False


def expected_symbol_count(state: dict) -> int:
    monitors = state.get("monitors", {}) if isinstance(state.get("monitors"), dict) else {}
    symbols = set()
    for monitor in monitors.values():
        if not isinstance(monitor, dict):
            continue
        if not monitor.get("active") and not monitor_lifecycle_open(monitor):
            continue
        symbol = str(monitor.get("symbol") or "").upper()
        if symbol:
            symbols.add(symbol)
    return len(symbols)


def ws_stale_threshold_seconds(state: dict) -> int:
    config = state.get("config", {}) if isinstance(state.get("config"), dict) else {}
    poll_seconds = config.get("pollSeconds") or DEFAULT_POLL_SECONDS
    try:
        poll_seconds = int(poll_seconds)
    except Exception:
        poll_seconds = DEFAULT_POLL_SECONDS
    return max(poll_seconds * 4, 60)


def parse_ts(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def websocket_health(state: dict) -> dict:
    now = datetime.now(timezone.utc)
    started_at = parse_ts(state.get("startedAt"))
    last_ws = parse_ts(state.get("lastWsMessageAt"))
    threshold = ws_stale_threshold_seconds(state)
    expected_symbols = expected_symbol_count(state)
    engine_age_sec = (now - started_at).total_seconds() if started_at else None
    if expected_symbols == 0:
        return {
            "expectedSymbols": 0,
            "wsFresh": True,
            "wsAgeSec": None if not last_ws else (now - last_ws).total_seconds(),
            "thresholdSec": threshold,
            "reason": None,
        }
    if last_ws is None:
        stale = engine_age_sec is None or engine_age_sec > threshold
        return {
            "expectedSymbols": expected_symbols,
            "wsFresh": not stale,
            "wsAgeSec": None,
            "thresholdSec": threshold,
            "reason": "ws_missing" if stale else None,
        }
    age = (now - last_ws).total_seconds()
    stale = age > threshold
    return {
        "expectedSymbols": expected_symbols,
        "wsFresh": not stale,
        "wsAgeSec": age,
        "thresholdSec": threshold,
        "reason": "ws_stale" if stale else None,
    }


def check_and_restart() -> bool:
    """Check engine health and restart if needed. Returns True if engine is running."""
    pid = read_engine_pid()
    running_pids = find_engine_pids()
    state = load_state()
    watchdog_state = load_watchdog_state()
    status = state.get("status", "unknown")
    last_heartbeat = state.get("lastHeartbeatAt")
    ws_health = websocket_health(state)

    log(
        f"Check: pid={pid}, running_pids={running_pids}, status={status}, heartbeat={last_heartbeat}, "
        f"expectedSymbols={ws_health['expectedSymbols']}, wsFresh={ws_health['wsFresh']}, reason={ws_health['reason']}"
    )

    if len(running_pids) > 1:
        log(f"Duplicate engine processes detected {running_pids}. Restarting clean.")
        stop_engine(running_pids[0])
        return start_engine(reason="duplicate_processes")

    if running_pids and (pid is None or pid not in running_pids):
        PID_PATH.write_text(str(running_pids[0]))
        pid = running_pids[0]

    if pid is not None and is_process_alive(pid):
        if last_heartbeat:
            try:
                hb = datetime.fromisoformat(last_heartbeat.replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - hb).total_seconds()
                if age > STALE_THRESHOLD_SECONDS:
                    log(f"Heartbeat stale ({age:.0f}s). Restarting engine.")
                    stop_engine(pid)
                    return start_engine(reason="heartbeat_stale")
                if not ws_health["wsFresh"] and ws_health["expectedSymbols"] > 0:
                    observed_at = parse_ts(watchdog_state.get("wsStaleObservedAt"))
                    now = datetime.now(timezone.utc)
                    ws_age_label = "missing" if ws_health.get("wsAgeSec") is None else f"{ws_health['wsAgeSec']:.0f}s"
                    if observed_at is None:
                        watchdog_state["wsStaleObservedAt"] = now.isoformat()
                        watchdog_state["lastReason"] = ws_health["reason"]
                        save_watchdog_state(watchdog_state)
                        log(
                            f"Engine alive but degraded ({ws_health['reason']} age={ws_age_label}, symbols={ws_health['expectedSymbols']}). "
                            f"Starting persistence window {WATCHDOG_WS_PERSISTENCE_SECONDS}s."
                        )
                        return True
                    stale_age = (now - observed_at).total_seconds()
                    last_restart_at = parse_ts(watchdog_state.get("lastRestartAt"))
                    if last_restart_at and (now - last_restart_at).total_seconds() < WATCHDOG_WS_PERSISTENCE_SECONDS:
                        log(
                            f"Engine alive but degraded ({ws_health['reason']}); restart suppressed because last restart was "
                            f"{(now - last_restart_at).total_seconds():.0f}s ago."
                        )
                        return True
                    if stale_age >= WATCHDOG_WS_PERSISTENCE_SECONDS:
                        log(
                            f"Engine alive but degraded ({ws_health['reason']} age={ws_age_label}, "
                            f"symbols={ws_health['expectedSymbols']}). Restarting after persistence window."
                        )
                        stop_engine(pid)
                        return start_engine(reason=ws_health["reason"] or "ws_degraded")
                    log(
                        f"Engine alive but degraded ({ws_health['reason']} age={ws_age_label}, "
                        f"symbols={ws_health['expectedSymbols']}). Waiting {WATCHDOG_WS_PERSISTENCE_SECONDS - stale_age:.0f}s more before restart."
                    )
                    return True
                clear_watchdog_ws_marker(reason="healthy")
                log(f"Engine alive and healthy (heartbeat {age:.0f}s old).")
                return True
            except Exception as e:
                log(f"Error parsing heartbeat: {e}")
        else:
            clear_watchdog_ws_marker(reason="no_heartbeat_yet")
            log("No heartbeat yet but process alive. Accept as-is.")
            return True
    else:
        if pid is not None:
            log(f"Engine PID {pid} not alive. Restarting.")
        else:
            log("No engine PID found. Starting engine.")
        return start_engine(reason="process_missing")


def daemon_loop() -> None:
    """Run as a foreground daemon (for systemd service)."""
    pid = os.getpid()
    write_watchdog_pid(pid)
    log(f"Watchdog daemon started with PID {pid}")

    # Install signal handlers
    def sigterm_handler(sig, frame):
        log("Watchdog received SIGTERM. Exiting.")
        sys.exit(0)
    signal.signal(signal.SIGTERM, sigterm_handler)

    while True:
        check_and_restart()
        time.sleep(HEARTBEAT_INTERVAL_SECONDS)


def run_once() -> None:
    """Run a single check (for cron-based watchdog)."""
    log("=== Watchdog check ===")
    ok = check_and_restart()
    if ok:
        log("Engine is running.")
    else:
        log("Engine start failed.")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--daemon":
        daemon_loop()
    else:
        run_once()
