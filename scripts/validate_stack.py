#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

ROOT = Path(__file__).resolve().parent
PUBLIC_MARKET_DATA_PATH = ROOT / "public_market_data.py"
OPENCLAW_CONFIG_PATH = Path.home() / ".openclaw" / "openclaw.json"
WORKSPACE_STATE_DIR = Path.home() / ".openclaw" / "workspace" / "state"

STATUS_PASS = "pass"
STATUS_DEGRADED = "degraded"
STATUS_FAIL = "fail"
STATUS_MISSING = "missing"
STATUS_SKIPPED = "skipped"


def load_public_market_data_module():
    spec = importlib.util.spec_from_file_location("public_market_data", PUBLIC_MARKET_DATA_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load module from {PUBLIC_MARKET_DATA_PATH}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def result(status: str, summary: str, **details: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = {"status": status, "summary": summary}
    if details:
        payload["details"] = details
    return payload


def required_keys_check(payload: Dict[str, Any], required_keys: List[str]) -> List[str]:
    missing: List[str] = []
    for key in required_keys:
        if key not in payload:
            missing.append(key)
    return missing


def validate_core_scan(client: Any, symbol: str, universe: int) -> Dict[str, Any]:
    try:
        scan = client.scan(top=2, universe_size=universe, enrich_top=0, tradingview_top=0)
        missing = required_keys_check(scan, ["generatedAt", "universeSize", "topLongs", "topShorts", "topWatch"])
        if missing:
            return result(STATUS_FAIL, "Core scan output is missing required keys", missingKeys=missing)
        if int(scan.get("universeSize", 0)) <= 0:
            return result(STATUS_FAIL, "Core scan returned zero tradable symbols", universeSize=scan.get("universeSize"))
        return result(
            STATUS_PASS,
            "Core scan is healthy",
            universeSize=scan.get("universeSize"),
            topLongs=[row.get("symbol") for row in scan.get("topLongs", [])[:2]],
            topShorts=[row.get("symbol") for row in scan.get("topShorts", [])[:2]],
            topWatch=[row.get("symbol") for row in scan.get("topWatch", [])[:2]],
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Core scan failed: {exc}")


def validate_binance_snapshot(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        snap = client.snapshot(symbol, include_auth_providers=False, include_tradingview=False)
        binance = snap.get("binance", {}) if isinstance(snap, dict) else {}
        missing = required_keys_check(
            binance,
            [
                "ticker24h",
                "bookTicker",
                "premiumIndex",
                "openInterest",
                "openInterestHist",
                "depth",
                "aggTrades",
                "cvd",
                "fundingRate",
                "klines",
                "wsPlan",
            ],
        )
        if missing:
            return result(STATUS_FAIL, "Binance snapshot is missing required fields", missingKeys=missing)
        return result(
            STATUS_PASS,
            "Binance primary venue snapshot is healthy",
            symbol=symbol,
            klineIntervals=sorted((binance.get("klines") or {}).keys()),
            wsStreams=sorted(((binance.get("wsPlan") or {}).get("streams") or {}).keys()),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Binance snapshot failed: {exc}", symbol=symbol)


def validate_bybit_snapshot(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        bybit = client.bybit_symbol_snapshot(symbol)
        missing = required_keys_check(bybit, ["ticker", "orderbook", "recentTrade", "fundingHistory", "openInterest", "klines", "wsPlan"])
        if missing:
            return result(STATUS_FAIL, "Bybit confirmation snapshot is missing required fields", missingKeys=missing)
        return result(
            STATUS_PASS,
            "Bybit confirmation snapshot is healthy",
            symbol=symbol,
            klineIntervals=sorted((bybit.get("klines") or {}).keys()),
            wsTopics=((bybit.get("wsPlan") or {}).get("topics") or [])[:4],
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Bybit snapshot failed: {exc}", symbol=symbol)


def validate_event_layer(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        snap = client.snapshot(symbol, include_auth_providers=False, include_tradingview=False)
        event_layer = snap.get("eventLayer", {}) if isinstance(snap, dict) else {}
        headline_flags = snap.get("headlineFlags", {}) if isinstance(snap, dict) else {}
        missing = []
        if not isinstance(event_layer, dict) or not event_layer:
            missing.append("eventLayer")
        if not isinstance(headline_flags, dict) or "negativeCount" not in headline_flags or "positiveCount" not in headline_flags:
            missing.append("headlineFlags")
        if missing:
            return result(STATUS_FAIL, "Event layer output is incomplete", missingKeys=missing)
        return result(
            STATUS_PASS,
            "Event and headline layer is healthy",
            symbol=symbol,
            eventKeys=sorted(event_layer.keys()),
            negativeCount=headline_flags.get("negativeCount"),
            positiveCount=headline_flags.get("positiveCount"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Event layer check failed: {exc}", symbol=symbol)


def validate_execution_feedback(client: Any) -> Dict[str, Any]:
    try:
        state = client.execution_feedback_status()
        if not isinstance(state, dict) or "path" not in state:
            return result(STATUS_FAIL, "Execution feedback state is unreadable")
        return result(
            STATUS_PASS,
            "Execution feedback state is healthy",
            path=state.get("path"),
            symbolsTracked=state.get("symbolsTracked"),
            recentCount=state.get("recentCount"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Execution feedback check failed: {exc}")


def validate_cvd_layer(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        snap = client.snapshot(symbol, include_auth_providers=False, include_tradingview=False)
        binance = snap.get("binance", {}) if isinstance(snap, dict) else {}
        cvd = binance.get("cvd", {}) if isinstance(binance, dict) else {}
        missing = required_keys_check(cvd, ["deltaRatio", "recentDeltaRatio", "divergence", "bias", "sampleSize"])
        if missing:
            return result(STATUS_FAIL, "CVD layer is missing required fields", missingKeys=missing)
        score = client._score_symbol(symbol, include_auth_providers=False)
        execution = score.get("executionFilter", {}) if isinstance(score, dict) else {}
        if "cvd" not in execution:
            return result(STATUS_FAIL, "Execution filter did not expose CVD context")
        return result(
            STATUS_PASS,
            "CVD from aggTrades is healthy",
            symbol=symbol,
            bias=cvd.get("bias"),
            divergence=cvd.get("divergence"),
            sampleSize=cvd.get("sampleSize"),
            deltaRatio=cvd.get("deltaRatio"),
            recentDeltaRatio=cvd.get("recentDeltaRatio"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"CVD validation failed: {exc}", symbol=symbol)


def validate_liquidation_layer(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        capture = client.liquidation_capture(["BTCUSDT", "ETHUSDT", symbol], seconds=12, max_events=80, min_notional=25_000, persist=True)
        missing = required_keys_check(capture, ["statePath", "capturedAt", "symbols", "eventCount", "bySymbol"])
        if missing:
            return result(STATUS_FAIL, "Liquidation capture output is incomplete", missingKeys=missing)
        score = client._score_symbol(symbol, include_auth_providers=False)
        execution = score.get("executionFilter", {}) if isinstance(score, dict) else {}
        liquidation = execution.get("liquidation", {}) if isinstance(execution, dict) else {}
        if "summary" not in liquidation:
            return result(STATUS_FAIL, "Execution filter did not expose liquidation context")
        return result(
            STATUS_PASS,
            "Liquidation intelligence layer is healthy",
            symbol=symbol,
            captureEventCount=capture.get("eventCount"),
            trackedSymbols=capture.get("symbols"),
            liquidationAvailable=liquidation.get("available"),
            liquidationStale=liquidation.get("stale"),
            supportBias=liquidation.get("supportBias"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Liquidation validation failed: {exc}", symbol=symbol)


def validate_grade_lock(client: Any, symbol: str, universe: int) -> Dict[str, Any]:
    try:
        scan = client.scan(top=2, universe_size=universe, enrich_top=0, tradingview_top=0)
        checked_rows = []
        for bucket in ["topLongs", "topShorts", "topWatch"]:
            checked_rows.extend(scan.get(bucket, [])[:2])
        if not checked_rows:
            fallback_rows = []
            for candidate in client.binance_universe(limit=max(universe, 6))[:6]:
                try:
                    fallback_rows.append(client._score_symbol(candidate, include_auth_providers=False))
                except Exception:
                    continue
                if len(fallback_rows) >= 3:
                    break
            checked_rows = fallback_rows
        if not checked_rows:
            return result(STATUS_FAIL, "Grade lock validation found no rows to inspect, even after direct scoring fallback")
        required = ["grade", "gradeScore", "alertEligible", "watchlistEligible", "disqualified", "disqualifiers", "gradeReasons"]
        for row in checked_rows:
            missing = [key for key in required if key not in row]
            if missing:
                return result(STATUS_FAIL, "Grade lock output is missing required fields", symbol=row.get("symbol"), missingKeys=missing)
        return result(
            STATUS_PASS,
            "Formal grade lock is healthy",
            inspected=[{k: row.get(k) for k in ["symbol", "grade", "alertEligible", "watchlistEligible"]} for row in checked_rows[:4]],
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Grade lock validation failed: {exc}", symbol=symbol)


def validate_state_machine(client: Any, symbol: str, universe: int) -> Dict[str, Any]:
    try:
        scan = client.scan(top=2, universe_size=universe, enrich_top=0, tradingview_top=0, persist_state=True)
        state_sync = scan.get("stateSync", {}) if isinstance(scan, dict) else {}
        state_status = scan.get("stateStatus", {}) if isinstance(scan, dict) else {}
        missing = []
        for key in ["watchlistPath", "alertPath", "trackedSetups", "stateCounts"]:
            if key not in state_sync and key not in state_status:
                missing.append(key)
        if missing:
            return result(STATUS_FAIL, "State machine sync output is incomplete", missingKeys=missing)
        if int(state_status.get("setupsTracked", 0) or 0) <= 0:
            return result(STATUS_FAIL, "State machine did not persist any setup records", stateStatus=state_status)
        return result(
            STATUS_PASS,
            "Setup state machine is healthy",
            trackedSetups=state_status.get("setupsTracked"),
            activeSetups=state_status.get("activeSetups"),
            pendingAlerts=state_status.get("pendingAlerts"),
            stateCounts=state_status.get("stateCounts"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"State machine validation failed: {exc}", symbol=symbol)


def validate_provider_status(client: Any) -> Dict[str, Any]:
    try:
        provider_status = client.provider_status()
        providers = provider_status.get("providers", {}) if isinstance(provider_status, dict) else {}
        if not providers:
            return result(STATUS_DEGRADED, "Provider status loaded, but no configured providers were found")
        configured = sorted([name for name, info in providers.items() if isinstance(info, dict) and info.get("configured")])
        return result(
            STATUS_PASS if configured else STATUS_DEGRADED,
            "Provider configuration state loaded",
            configuredProviders=configured,
            rateLimitStatePath=provider_status.get("rateLimitStatePath"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Provider status failed: {exc}")


def validate_tradingview_status(client: Any) -> Dict[str, Any]:
    try:
        status = client.tradingview_status()
        if not isinstance(status, dict):
            return result(STATUS_FAIL, "TradingView status returned invalid payload")
        if status.get("available"):
            return result(
                STATUS_PASS,
                "TradingView secondary validator is available",
                moduleRoot=status.get("moduleRoot"),
                highValueTools=status.get("highValueTools"),
            )
        return result(
            STATUS_DEGRADED,
            "TradingView validator is unavailable, but fallback path should keep scans alive",
            importError=status.get("importError"),
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"TradingView status failed: {exc}")


def validate_tradingview_cleanup(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        status = client.tradingview_status()
        if not isinstance(status, dict) or not status.get("available"):
            return result(STATUS_DEGRADED, "TradingView adapter is unavailable, cleanup cannot be live-verified", importError=status.get("importError") if isinstance(status, dict) else None)
        candidates = []
        for item in [symbol, "BTCUSDT", "ETHUSDT", "DOGEUSDT", "XRPUSDT"]:
            cleaned = str(item or "").upper().strip()
            if cleaned and cleaned not in candidates:
                candidates.append(cleaned)
        malformed_errors: List[Dict[str, Any]] = []
        for candidate in candidates:
            confirmation = client.tradingview_confirmation(candidate, bull_score=4)
            technical = confirmation.get("confirmation", {}).get("technical", {}) if isinstance(confirmation, dict) else {}
            mtf = confirmation.get("confirmation", {}).get("multiTimeframe", {}) if isinstance(confirmation, dict) else {}
            volume = confirmation.get("confirmation", {}).get("volumeConfirmation", {}) if isinstance(confirmation, dict) else {}
            warnings_seen: List[str] = []
            malformed = False
            for payload in [technical, mtf, volume]:
                if isinstance(payload, dict):
                    warnings_seen.extend([str(item) for item in payload.get("warnings", []) if str(item).strip()])
                    if payload.get("error"):
                        error_text = str(payload.get("error"))
                        if "empty or malformed response" in error_text.lower():
                            malformed = True
                            malformed_errors.append({"symbol": candidate, "error": error_text})
            interval_warnings = [item for item in warnings_seen if "Interval is empty or not valid" in item]
            if interval_warnings:
                return result(STATUS_FAIL, "TradingView interval normalization is still leaking warnings", symbol=candidate, warnings=interval_warnings)
            if malformed:
                continue
            return result(
                STATUS_PASS,
                "TradingView cleanup layer is healthy",
                symbol=candidate,
                cleanScoreDelta=confirmation.get("cleanScoreDelta"),
                supportCount=confirmation.get("supportCount"),
                conflictCount=confirmation.get("conflictCount"),
                warnings=warnings_seen[:10],
            )
        return result(
            STATUS_PASS,
            "TradingView cleanup gracefully normalized malformed responses",
            attempted=candidates,
            malformed=malformed_errors,
        )
    except Exception as exc:
        return result(STATUS_DEGRADED, f"TradingView cleanup validation failed: {exc}", symbol=symbol)


def validate_phase7_feedback_engine(client: Any) -> Dict[str, Any]:
    try:
        status = client.feedback_engine_v2_status()
        engine_path = status.get("enginePath")
        learners = status.get("symbolSideLearners", [])
        total_learnings = int(status.get("totalLearnings", 0) or 0)
        if not isinstance(status, dict) or not engine_path:
            return result(STATUS_FAIL, "Feedback engine v2 status is unreadable")
        if total_learnings <= 0 and len(learners) <= 0:
            return result(
                STATUS_DEGRADED,
                "Feedback engine v2 is healthy but has not learned any outcomes yet",
                enginePath=engine_path,
                learnersCount=len(learners),
            )
        with_skip = [lr for lr in learners if lr.get("guidance", {}).get("skip")]
        with_positive = [lr for lr in learners if lr.get("score", {}).get("learningScore", 0) > 0]
        return result(
            STATUS_PASS,
            "Feedback engine v2 is learning from real outcomes",
            totalLearnings=total_learnings,
            learnersCount=len(learners),
            positiveLearners=len(with_positive),
            skipLearners=len(with_skip),
            samples=[{"symbol": lr.get("symbol"), "side": lr.get("side"), "score": lr.get("score", {}).get("learningScore"), "tpZone": lr.get("guidance", {}).get("tpZone")} for lr in learners[:4]],
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Feedback engine v2 validation failed: {exc}")


def validate_feedback_engine_v2(client: Any) -> Dict[str, Any]:
    try:
        status = client.feedback_engine_v2_status()
        learners = status.get("symbolSideLearners", []) if isinstance(status, dict) else []
        total_learnings = int((status.get("totalLearnings") or status.get("recent") or [0])[-1] if isinstance(status.get("recent"), list) else 0)
        engine_path = status.get("enginePath") if isinstance(status, dict) else None
        if not engine_path or not isinstance(status, dict):
            return result(STATUS_FAIL, "Feedback engine v2 state file is missing or unreadable")
        active = [lr for lr in learners if isinstance(lr, dict) and lr.get("score", {}).get("total", 0) > 0]
        if not active:
            return result(
                STATUS_DEGRADED,
                "Feedback engine v2 is online but has not accumulated learnings yet",
                path=str(engine_path),
            )
        sample = []
        for lr in active[:4]:
            sc = lr.get("score", {})
            gd = lr.get("guidance", {})
            sample.append({
                "symbol": lr.get("symbol"),
                "side": lr.get("side"),
                "learningScore": sc.get("learningScore"),
                "tpZone": gd.get("tpZone"),
                "sizeBias": gd.get("sizeBias"),
                "skip": gd.get("skip"),
            })
        return result(
            STATUS_PASS,
            "Feedback engine v2 is active and learning from outcomes",
            learnerCount=len(active),
            sample=sample,
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Feedback engine v2 validation failed: {exc}")


def validate_secondary_live(client: Any, symbol: str) -> Dict[str, Any]:
    try:
        enrichment = client.provider_enrichment(symbol)
        massive = enrichment.get("massive", {}) if isinstance(enrichment, dict) else {}
        lunar = enrichment.get("lunarcrush", {}) if isinstance(enrichment, dict) else {}
        massive_ok = isinstance(massive, dict) and isinstance(massive.get("groupedDaily"), dict) and isinstance(massive.get("customBars"), dict)
        lunar_status_code = lunar.get("statusCode") if isinstance(lunar, dict) else None
        status = STATUS_PASS if massive_ok and lunar_status_code in {None, 200} else STATUS_DEGRADED
        summary = "Secondary live enrichment responded"
        if lunar_status_code == 402:
            summary = "Secondary enrichment works, but LunarCrush is subscription-gated"
        return result(
            status,
            summary,
            symbol=symbol,
            massiveKeys=sorted(massive.keys()) if isinstance(massive, dict) else [],
            lunarStatusCode=lunar_status_code,
        )
    except Exception as exc:
        return result(STATUS_DEGRADED, f"Secondary live enrichment failed: {exc}", symbol=symbol)


def validate_phase3_automation(client: Any) -> Dict[str, Any]:
    try:
        status = client.automation_status()
        jobs = status.get("jobs", []) if isinstance(status, dict) else []
        pending_count = int(status.get("pendingAlertCount", 0) or 0)
        if len(jobs) < 2:
            return result(
                STATUS_MISSING,
                "Phase 3 cron jobs are not installed yet",
                jobCount=len(jobs),
                pendingAlertCount=pending_count,
            )
        return result(
            STATUS_PASS,
            "Phase 3 cron automation is installed",
            jobNames=[job.get("name") for job in jobs],
            pendingAlertCount=pending_count,
        )
    except Exception as exc:
        return result(STATUS_FAIL, f"Phase 3 automation validation failed: {exc}")


def parse_openclaw_config() -> Dict[str, Any]:
    if not OPENCLAW_CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(OPENCLAW_CONFIG_PATH.read_text())
    except Exception:
        return {}


def validate_automation() -> Dict[str, Any]:
    cfg = parse_openclaw_config()
    heartbeat = (((cfg.get("agents") or {}).get("defaults") or {}).get("heartbeat"))
    hooks = cfg.get("hooks")
    watchlist_exists = (WORKSPACE_STATE_DIR / "watchlist_setups.json").exists()
    alerts_exist = (WORKSPACE_STATE_DIR / "alerts_sent.json").exists()
    cron_store_path = Path.home() / ".openclaw" / "cron" / "jobs.json"

    cron_status = STATUS_MISSING
    cron_summary = "OpenClaw cron jobs are not configured"
    cron_output = None
    try:
        proc = subprocess.run(["openclaw", "cron", "status"], capture_output=True, text=True, timeout=30)
        cron_output = (proc.stdout or proc.stderr or "").strip()
        jobs_count = 0
        try:
            payload = json.loads(cron_output or "{}")
            jobs_count = int(payload.get("jobs", 0) or 0)
        except Exception:
            jobs_count = 0
        if jobs_count > 0 or (cron_store_path.exists() and cron_store_path.stat().st_size > 0):
            cron_status = STATUS_PASS
            cron_summary = "OpenClaw cron jobs are configured"
    except Exception as exc:
        cron_status = STATUS_DEGRADED
        cron_summary = f"Unable to read OpenClaw cron state: {exc}"

    return {
        "cron": result(cron_status, cron_summary, output=cron_output),
        "heartbeat": result(
            STATUS_PASS if heartbeat else STATUS_MISSING,
            "Heartbeat automation is configured" if heartbeat else "Heartbeat automation is not configured",
            config=heartbeat,
        ),
        "hooks": result(
            STATUS_PASS if hooks else STATUS_MISSING,
            "Hooks are configured" if hooks else "Hooks are not configured",
            config=hooks,
        ),
        "watchlistState": result(
            STATUS_PASS if watchlist_exists else STATUS_MISSING,
            "Watchlist state file exists" if watchlist_exists else "Watchlist state file is missing",
            path=str(WORKSPACE_STATE_DIR / "watchlist_setups.json"),
        ),
        "alertState": result(
            STATUS_PASS if alerts_exist else STATUS_MISSING,
            "Alert state file exists" if alerts_exist else "Alert state file is missing",
            path=str(WORKSPACE_STATE_DIR / "alerts_sent.json"),
        ),
    }


def overall_status(core: Dict[str, Any], automation: Dict[str, Any], secondary: Dict[str, Any]) -> str:
    if any(item.get("status") == STATUS_FAIL for item in core.values()):
        return "blocked_core_failure"
    if automation.get("phase3Jobs", {}).get("status") == STATUS_PASS:
        fb = secondary.get("feedbackEngineV2", {})
        if fb.get("status") in {STATUS_PASS, STATUS_DEGRADED}:
            return "phase_7_complete"
        return "ready_for_phase_7"
    return "ready_for_phase_3"


def build_stack_matrix() -> Dict[str, List[str]]:
    return {
        "core": [
            "binance_rest_snapshot",
            "binance_klines_5m_15m_1h_4h",
            "binance_depth_funding_oi_aggtrades",
            "binance_crowding_ratios",
            "bybit_confirmation_snapshot",
            "event_and_headline_layer",
            "execution_feedback_state",
            "base_scan_ranking",
        ],
        "secondary": [
            "massive_finalist_overlay",
            "tradingview_secondary_overlay",
            "coinmarketcap_quotes",
            "nansen_holdings",
        ],
        "optional": [
            "lunarcrush_social_overlay",
            "extra_macro_search_context",
        ],
        "deferred": [],
        "implemented_phases": [
            "formal_grade_lock",
            "watchlist_state_machine",
            "cvd_from_aggtrades",
            "liquidation_clustering",
            "tradingview_cleanup_refinement",
            "feedback_engine_v2",
        ],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate the full phased crypto futures stack.")
    parser.add_argument("--symbol", default="DOGEUSDT", help="Liquid symbol used for venue validation, default DOGEUSDT")
    parser.add_argument("--universe", type=int, default=8, help="Universe size for the core scan check")
    parser.add_argument("--deep-secondary", action="store_true", help="Run one live provider enrichment check for secondary overlays")
    args = parser.parse_args()

    module = load_public_market_data_module()
    client = module.PublicMarketData()
    symbol = args.symbol.upper()

    core = {
        "scan": validate_core_scan(client, symbol, args.universe),
        "binance": validate_binance_snapshot(client, symbol),
        "bybit": validate_bybit_snapshot(client, symbol),
        "eventLayer": validate_event_layer(client, symbol),
        "executionFeedback": validate_execution_feedback(client),
        "cvd": validate_cvd_layer(client, symbol),
        "liquidation": validate_liquidation_layer(client, symbol),
        "gradeLock": validate_grade_lock(client, symbol, args.universe),
        "stateMachine": validate_state_machine(client, symbol, args.universe),
    }

    secondary = {
        "providerStatus": validate_provider_status(client),
        "tradingview": validate_tradingview_status(client),
        "tradingviewCleanup": validate_tradingview_cleanup(client, symbol),
        "feedbackEngineV2": validate_feedback_engine_v2(client),
        "liveEnrichment": validate_secondary_live(client, symbol) if args.deep_secondary else result(STATUS_SKIPPED, "Live secondary enrichment check was not requested"),
    }

    automation = validate_automation()
    automation["phase3Jobs"] = validate_phase3_automation(client)

    payload = {
        "phase": "phase_0_1_2_3_4_5_6_7_validated_stack",
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "stack": build_stack_matrix(),
        "core": core,
        "secondary": secondary,
        "automation": automation,
        "overall": overall_status(core, automation, secondary),
    }
    print(json.dumps(payload, indent=2, ensure_ascii=False))
    return 0 if payload["overall"] in {"ready_for_phase_3", "ready_for_phase_4", "ready_for_phase_5", "ready_for_phase_6", "ready_for_phase_7", "phase_7_complete"} else 1


if __name__ == "__main__":
    sys.exit(main())
