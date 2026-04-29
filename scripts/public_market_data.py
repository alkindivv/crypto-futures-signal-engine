#!/usr/bin/env python3
import asyncio
import argparse
import calendar
import hashlib
import json
import os
import re
import signal
import shutil
import subprocess
import sys
import time
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Ensure openclaw_engine package resolves when running as a script
_SCRIPT_DIR = Path(__file__).resolve().parent
_PACKAGE_DIR = _SCRIPT_DIR.parent
if str(_PACKAGE_DIR) not in sys.path:
    sys.path.insert(0, str(_PACKAGE_DIR))

import requests

try:
    import websockets
except Exception:
    websockets = None

try:
    from tradingview_adapter import TradingViewAdapter
except Exception:
    TradingViewAdapter = None

try:
    from telegram_execution_cards import EntryReadyEvent, TelegramExecutionCardFormatter
except Exception:
    EntryReadyEvent = None
    TelegramExecutionCardFormatter = None

# Phase 2 Step 6: extracted state query and projection functions
try:
    from openclaw_engine.state import (
        canonical_setup_state as _canonical_setup_state,
        canonical_readiness_state as _canonical_readiness_state,
        canonical_runtime_state as _canonical_runtime_state,
        canonical_delivery_state as _canonical_delivery_state,
        setup_lifecycle_state as _setup_lifecycle_state,
        setup_readiness_state as _setup_readiness_state,
        monitor_lifecycle_state as _monitor_lifecycle_state,
        monitor_readiness_state as _monitor_readiness_state,
        delivery_state as _delivery_state,
        final_confirmation_identity as _final_confirmation_identity,
    )
    from openclaw_engine.projections import (
        notification_crowding_note as _notification_crowding_note,
        notification_lane_for_setup as _notification_lane_for_setup,
        notification_entry_from_setup as _notification_entry_from_setup,
    )
    from openclaw_engine.renderer import (
        notification_format_value as _notification_format_value,
        notification_join_flags as _notification_join_flags,
        notification_entry_signature as _notification_entry_signature,
        notification_update_reasons as _notification_update_reasons,
        notification_sort_key as _notification_sort_key,
        notification_render_entry_compact as _notification_render_entry_compact,
        notification_render_entry_detailed as _notification_render_entry_detailed,
        notification_render_delta as _notification_render_delta,
    )
    from openclaw_engine.delivery import (
        entry_ready_event_priority as _entry_ready_event_priority,
        entry_ready_sort_key as _entry_ready_sort_key,
    )
except Exception:
    _canonical_setup_state = None
    _canonical_readiness_state = None
    _canonical_runtime_state = None
    _canonical_delivery_state = None
    _setup_lifecycle_state = None
    _setup_readiness_state = None
    _monitor_lifecycle_state = None
    _monitor_readiness_state = None
    _delivery_state = None
    _final_confirmation_identity = None
    _notification_crowding_note = None
    _notification_lane_for_setup = None
    _notification_entry_from_setup = None
    _notification_format_value = None
    _notification_join_flags = None
    _notification_entry_signature = None
    _notification_update_reasons = None
    _notification_sort_key = None
    _notification_render_entry_compact = None
    _notification_render_entry_detailed = None
    _notification_render_delta = None
    _entry_ready_event_priority = None
    _entry_ready_sort_key = None

BINANCE_REST = "https://fapi.binance.com"
BINANCE_WS = "wss://fstream.binance.com/ws"
BYBIT_REST = "https://api.bybit.com"
BYBIT_WS = "wss://stream.bybit.com/v5/public/linear"
COINGECKO_REST = "https://api.coingecko.com/api/v3"
USER_AGENT = "openclaw-crypto-futures-depth-analysis/1.0"
TIMEOUT = 20
WORKSPACE_DIR = Path(__file__).resolve().parents[4]
STATE_DIR = WORKSPACE_DIR / "state"
ENV_PATH = WORKSPACE_DIR / ".env"
RATE_LIMIT_STATE_PATH = STATE_DIR / "provider_rate_limits.json"
EXECUTION_FEEDBACK_STATE_PATH = STATE_DIR / "execution_feedback.json"
WATCHLIST_STATE_PATH = STATE_DIR / "watchlist_setups.json"
ALERT_STATE_PATH = STATE_DIR / "alerts_sent.json"
AUTOMATION_STATE_PATH = STATE_DIR / "automation_jobs.json"
LIQUIDATION_STATE_PATH = STATE_DIR / "liquidation_events.json"
FEEDBACK_ENGINE_STATE_PATH = STATE_DIR / "feedback_engine_v2.json"
LIVE_TRIGGER_STATE_PATH = STATE_DIR / "live_trigger_engine.json"
NOTIFICATION_RENDER_STATE_PATH = STATE_DIR / "notification_render_state.json"
LIVE_TRIGGER_PID_PATH = STATE_DIR / "live_trigger_engine.pid"
LIVE_TRIGGER_LOG_PATH = STATE_DIR / "live_trigger_engine.log"
PERIODIC_DELIVERY_LOG_PATH = STATE_DIR / "periodic_delivery_log.jsonl"
ENTRY_READY_DRY_RUN_STATE_PATH = STATE_DIR / "entry_ready_dry_run_state.json"
ENTRY_READY_DRY_RUN_LOG_PATH = STATE_DIR / "entry_ready_dry_run_log.jsonl"
ENTRY_READY_DELIVERY_STATE_PATH = STATE_DIR / "entry_ready_delivery_state.json"
DISCOVERY_RADAR_STATE_PATH = STATE_DIR / "discovery_radar_latest.json"
DISCOVERY_RADAR_LOG_PATH = STATE_DIR / "discovery_radar_log.jsonl"
DISCOVERY_RADAR_PREVIEW_STATE_PATH = STATE_DIR / "discovery_radar_preview_latest.json"
DISCOVERY_RADAR_PREVIEW_LOG_PATH = STATE_DIR / "discovery_radar_preview_log.jsonl"
PRODUCTION_SIGNAL_EVAL_LOG_PATH = STATE_DIR / "production_signal_eval.jsonl"
OPENCLAW_CRON_STORE_PATH = Path.home() / ".openclaw" / "cron" / "jobs.json"

DEFAULT_INTERVALS = ["5m", "15m", "1h", "4h"]
AGG_TRADES_LIMIT = 200
LIQUIDATION_STATE_MAX_EVENTS = 120
LIQUIDATION_STALE_MINUTES = 45
BYBIT_INTERVAL_MAP = {"5m": "5", "15m": "15", "1h": "60", "4h": "240"}
SYMBOL_TO_COINGECKO = {
    "BTCUSDT": "bitcoin",
    "ETHUSDT": "ethereum",
    "SOLUSDT": "solana",
    "XRPUSDT": "ripple",
    "LINKUSDT": "chainlink",
    "DOGEUSDT": "dogecoin",
    "AVAXUSDT": "avalanche-2",
    "ADAUSDT": "cardano",
    "SUIUSDT": "sui",
    "TAOUSDT": "bittensor",
    "HYPEUSDT": "hyperliquid",
}
SYMBOL_TO_QUERY = {
    "BTCUSDT": "Bitcoin BTC",
    "ETHUSDT": "Ethereum ETH",
    "SOLUSDT": "Solana SOL",
    "XRPUSDT": "XRP Ripple",
    "LINKUSDT": "Chainlink LINK",
    "DOGEUSDT": "Dogecoin DOGE",
    "AVAXUSDT": "Avalanche AVAX",
    "ADAUSDT": "Cardano ADA",
    "SUIUSDT": "Sui SUI",
    "TAOUSDT": "Bittensor TAO",
    "HYPEUSDT": "Hyperliquid HYPE",
}
SYMBOL_TO_MASSIVE = {
    symbol: f"X:{symbol.replace('USDT', 'USD')}"
    for symbol in [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
        "XRPUSDT",
        "LINKUSDT",
        "DOGEUSDT",
        "AVAXUSDT",
        "ADAUSDT",
        "SUIUSDT",
        "TAOUSDT",
        "HYPEUSDT",
    ]
}
NEGATIVE_KEYWORDS = [
    "exploit",
    "hack",
    "unlock",
    "delist",
    "lawsuit",
    "investigation",
    "outage",
    "liquidation",
    "creditor",
    "distribution",
    "breach",
    "drain",
]
POSITIVE_KEYWORDS = [
    "etf",
    "approval",
    "launch",
    "listing",
    "partnership",
    "integration",
    "upgrade",
    "expands",
    "inflows",
    "recovery",
]
PROVIDER_ENV_KEYS = {
    "massive": "MASSIVE_API_KEY",
    "coinmarketcap": "COINMARKETCAP_API_KEY",
    "nansen": "NANSEN_API_KEY",
    "lunarcrush": "LUNARCRUSH_API_KEY",
}
PROVIDER_LIMITS = {
    "massive": [{"window": 60, "limit": 5, "strategy": "sleep"}],
    "coinmarketcap": [{"window": "month", "limit": 10_000, "strategy": "error"}],
    "nansen": [
        {"window": 1, "limit": 20, "strategy": "sleep"},
        {"window": 60, "limit": 300, "strategy": "sleep"},
    ],
    "lunarcrush": [
        {"window": 60, "limit": 4, "strategy": "sleep"},
        {"window": "day", "limit": 100, "strategy": "error"},
    ],
}
GRADE_PRIORITY = {"A+": 4, "A": 3, "B": 2, "C": 1, "NO_TRADE": 0}
ALERT_GRADES = {"A+", "A"}
WATCHLIST_GRADES = {"A+", "A", "B"}
SEVERE_HEADLINE_KEYWORDS = {"hack", "exploit", "breach", "delist"}
SEVERE_RISK_FLAGS = {"wide_spread", "short_squeeze_risk", "long_crowding_risk", "hot_oi"}
SETUP_ACTIVE_STATES = {"candidate", "watchlist", "alert_eligible", "alerted"}
CANONICAL_SETUP_ACTIVE_STATES = {"candidate", "watchlist", "actionable", "notified"}
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
DELIVERY_STATE_VALUES = {"idle", "pending", "sent", "cleared"}
MIGRATION_DIAGNOSTIC_BUCKETS = ("old-only", "mixed", "canonical", "malformed")
FEEDBACK_OUTCOME_MAP = {
    "stop_loss": -1.0,
    "no_fill": -0.5,
    "break_even": 0.0,
    "runner": 0.7,
    "take_profit": 1.0,
    "invalidated": -0.3,
}
ENTROPY_THRESHOLD = 0.6
REPEATABILITY_MIN_TOTAL = 3
FEEDBACK_V2_MAX_RECENT = 200
FEEDBACK_V2_PROD_MIN_SAMPLES = 3  # minimum executions before feedback may alter production cleanScore
LIVE_TRIGGER_MONITOR_STATES = {"alert_eligible", "alerted"}
LIVE_TRIGGER_PRIMARY_READINESS_STATES = {"retest_ready"}
LIVE_TRIGGER_STANDBY_READINESS_STATES = {"pullback_standby", "bounce_standby"}

# B1: Readiness Hysteresis — demotion requires consecutive confirmations
CONFIRM_DEMOTIONS = 2  # consecutive same-observed scans before demotion commits
HYSTERESIS_CONFIRMATIONS_FIELD = "hysteresisConfirmations"  # persisted in readiness record
HYSTERESIS_TARGET_FIELD = "hysteresisTarget"  # what demotion we're confirming toward
OBSERVED_STATE_FIELD = "observedState"  # raw scanner state for this scan
LIVE_TRIGGER_EVALUATION_STATES = {"armed", "zone_touched"}

# ─── NEW INDICATOR ROLLBACK SYSTEM ─────────────────────────────────────────────
# Each new indicator is feature-flagged independently.
# If indicator causes signal degradation (more flips, worse quality), set to False.
# Divergence thresholds: how much the new indicator value can differ before flagging.
NEW_INDICATOR_ENABLED = {
    "ema": True,       # Replace SMA(20/50) with true EMA(20/50)
    "macd": True,      # MACD histogram as macdNote in execution filter
    "supertrend": True, # ATR-based stop zone in actionable_setup_plan
    "vwap": True,      # VWAP as vwapNote in execution filter
}

# ─── CONFIRMATION RELAXATION FLAGS (Fix Option A) ──────────────────────────────
# Relaxes confirmation threshold for zone_touched → confirmed transition.
# Rollback: set any flag to False → original strict threshold restored.
CONFIRMATION_RELAX_ENABLED = {
    "soft_failed_reclaim": True,  # close ≤ zone_low + 20% zone range (was: close ≤ zone_low)
    "soft_tested_deep": True,      # high ≥ zone_low + 70% zone range (was: high ≥ zone_mid)
    "minimal_wick": True,          # upper_wick > 0 (was: upper_wick ≥ zone_range * 0.12)
    "momentum_check": True,        # body_ratio ≥ MOMENTUM_BODY_RATIO_MIN when enabled, bypassed when disabled
}
# Momentum check: body_ratio threshold (relaxed from 30% to 20%)
MOMENTUM_BODY_RATIO_MIN = 0.20
# Divergence threshold: EMA vs SMA delta above this % triggers warning
EMA_SMA_DIVERGENCE_PCT = 2.0   # percent
# ATR reasonableness gate: ATR/price above this = suspicious
ATR_PRICE_RATIO_MAX = 0.05     # 5% of price
# MACD sanity: |histogram| / price above this = suspicious
MACD_PRICE_RATIO_MAX = 0.01    # 1% of price
# VWAP sanity: |price - vwap| / price above this = suspicious
VWAP_PRICE_DIVERGENCE_MAX = 0.05  # 5%

# B2: Runtime allowed-transitions matrix
# None = self-loop (already in this state); "ALLOWED" = legal with listed guard
# "COERCE_inactive" = illegal direct jump; coerce to inactive
# "REJECT" = illegal; reject the transition, log, don't apply
RUNTIME_ALLOWED_TRANSITIONS: Dict[str, Dict[str, str]] = {
    # from_state -> {to_state: guard/policy}
    "inactive": {
        "inactive": "SELF-LOOP",
        "armed": "TARGET_REARMED",
        # zone_touched/confirmed/notified/cooldown = REJECT
    },
    "armed": {
        "inactive": "HARD_INVALIDATION",
        "armed": "SELF-LOOP",
        "zone_touched": "ZONE_TOUCH_CONFIRMED",
        # confirmed/notified/cooldown = REJECT
    },
    "zone_touched": {
        "inactive": "HARD_INVALIDATION",
        "armed": "REJECT",  # TARGET_REARMED = illegal; ZONE_ESCAPE handled specially in code
        "zone_touched": "SELF-LOOP",
        "confirmed": "TRIGGER_CONFIRMED",
        # notified/cooldown = REJECT
    },
    "confirmed": {
        "inactive": "HARD_INVALIDATION",
        "armed": "REJECT",  # committed trigger, re-arm via TARGET_REARMED is unsafe
        # zone_touched = REJECT
        "confirmed": "SELF-LOOP",
        "notified": "DELIVERY_SENT",
        # cooldown = REJECT
    },
    "notified": {
        "inactive": "SETUP_CHANGED",  # lifecycle/readiness changed after alert sent
        "armed": "REJECT",             # TARGET_REARMED = illegal (committed alert sent); ALERT_EXPIRED handled in code
        # zone_touched/confirmed = REJECT
        "notified": "SELF-LOOP",
        "cooldown": "COOLDOWN_STARTED",
    },
    "cooldown": {
        "inactive": "TARGET_REMOVED",
        "armed": "COOLDOWN_EXPIRED",
        # zone_touched/confirmed/notified = REJECT
        "cooldown": "SELF-LOOP",
    },
}
# Convenience set: mid-state monitors that should NOT be forcibly re-armed
RUNTIME_MID_STATES: set = {"zone_touched", "confirmed", "notified"}
LIVE_TRIGGER_LIFECYCLE_OPEN_STATES = {"entered", "tp1", "tp2"}

# B3: Cooldown exit states
COOLDOWN_EXIT_STATES = {"inactive", "armed"}
# Readiness states that permit cooldown → armed exit
COOLDOWN_EXIT_READINESS_ALLOWED = {"retest_ready"}
# Readiness states that force cooldown → inactive exit
COOLDOWN_EXIT_READINESS_BLOCKED = {"pullback_standby", "bounce_standby"}

# =============================================================================
# B4: INVALIDATION FRAMEWORK
# =============================================================================
# Invalidation taxonomy — single source of truth for all invalidation types.
# Each entry: invalidation_type -> {
#   "lifecycle": lifecycle effect  (None=unchanged, "invalidated", "stale")
#   "readiness": readiness effect  (None=unchanged, None=cleared)
#   "delivery":  delivery effect   (None=unchanged, "cleared")
#   "sticky":    whether this persists until new setupVersion
#   "description": human-readable description
# }
# Recovery: HARD_INVALIDATION/STALE/TARGET_REMOVED = sticky (needs new version)
#            SOFT_DEMOTION/CONTEXT_BROKEN/ZONE_BROKEN = non-sticky (next scan may clear)
INVALIDATION_TAXONOMY: Dict[str, Dict[str, Any]] = {
    # --- HARD INVALIDATIONS (lifecycle → invalidated, sticky) ---
    "HARD_INVALIDATION": {
        "lifecycle": "invalidated",
        "readiness": None,       # cleared
        "delivery": "cleared",   # pending alerts wiped
        "runtime": "inactive",  # must go inactive
        "sticky": True,
        "description": "Hard block: setup disqualified, NO_TRADE grade, or structural failure. Requires new setupVersion to recover.",
    },
    # Pre-defined HARD_INVALIDATION triggers:
    "SCAN_INVALIDATE_DISQUALIFIED": {
        "lifecycle": "invalidated", "readiness": None, "delivery": "cleared", "runtime": "inactive",
        "sticky": True,
        "description": "Setup grade=NO_TRADE or disqualified flag set by scanner.",
    },
    "SCAN_INVALIDATE_THRESHOLD_LOSS": {
        "lifecycle": "invalidated", "readiness": None, "delivery": "cleared", "runtime": "inactive",
        "sticky": True,
        "description": "Setup no longer passes scanner quality thresholds.",
    },
    "ZONE_BROKEN": {
        "lifecycle": "invalidated", "readiness": None, "delivery": "cleared", "runtime": "inactive",
        "sticky": True,
        "description": "Entry zone structurally broken: price closed back inside zone (zone reclaimed), making setup invalid.",
    },
    "CONTEXT_BROKEN": {
        "lifecycle": "invalidated", "readiness": None, "delivery": "cleared", "runtime": "inactive",
        "sticky": True,
        "description": "Context candle closes outside entry zone — market regime shifted. Hard block.",
    },
    "SETUP_CHANGED": {
        "lifecycle": "invalidated", "readiness": None, "delivery": "cleared", "runtime": "inactive",
        "sticky": True,
        "description": "Setup version changed after alert was sent — old alert no longer valid.",
    },
    # --- STALE (lifecycle → stale, sticky) ---
    "STALE": {
        "lifecycle": "stale", "readiness": None, "delivery": None, "runtime": "inactive",
        "sticky": True,
        "description": "Setup not observed in current scan. Stale until re-observed.",
    },
    "SCAN_STALE_NOT_REAFFIRMED": {
        "lifecycle": "stale", "readiness": None, "delivery": None, "runtime": "inactive",
        "sticky": True,
        "description": "Setup missing from scan — not reaffirmed.",
    },
    "TARGET_REMOVED": {
        "lifecycle": "stale", "readiness": None, "delivery": None, "runtime": "inactive",
        "sticky": True,
        "description": "Setup removed from watchlist/target universe.",
    },
    # --- SOFT DEMOTION (lifecycle unchanged, readiness → standby, non-sticky) ---
    "SOFT_DEMOTION": {
        "lifecycle": None, "readiness": "pullback_standby", "delivery": None, "runtime": "inactive",
        "sticky": False,
        "description": "Setup still valid but market conditions unfavorable. Reversible on next scan.",
    },
    "SCAN_DEMOTE_STANDBY": {
        "lifecycle": None, "readiness": "pullback_standby", "delivery": None, "runtime": "inactive",
        "sticky": False,
        "description": "Readiness demoted by scan observation (hysteresis confirmed).",
    },
    "ZONE_RECLAIMED": {
        "lifecycle": None, "readiness": "pullback_standby", "delivery": None, "runtime": "inactive",
        "sticky": False,
        "description": "Price reclaimed zone during retest — soft demotion, not hard invalidation.",
    },
}

# All invalidation types that cascade to ALL layers (lifecycle + readiness + delivery + runtime)
INVALIDATION_FULL_CASCADE: set = {
    "HARD_INVALIDATION", "SCAN_INVALIDATE_DISQUALIFIED", "SCAN_INVALIDATE_THRESHOLD_LOSS",
    "ZONE_BROKEN", "CONTEXT_BROKEN", "SETUP_CHANGED",
    "STALE", "SCAN_STALE_NOT_REAFFIRMED", "TARGET_REMOVED",
}
# All invalidation types that clear delivery state
INVALIDATION_CLEAR_DELIVERY: set = {
    "HARD_INVALIDATION", "SCAN_INVALIDATE_DISQUALIFIED", "SCAN_INVALIDATE_THRESHOLD_LOSS",
    "ZONE_BROKEN", "CONTEXT_BROKEN", "SETUP_CHANGED",
}
# All sticky invalidation types (require new setupVersion to recover)
INVALIDATION_STICKY: set = {
    "HARD_INVALIDATION", "SCAN_INVALIDATE_DISQUALIFIED", "SCAN_INVALIDATE_THRESHOLD_LOSS",
    "ZONE_BROKEN", "CONTEXT_BROKEN", "SETUP_CHANGED",
    "STALE", "SCAN_STALE_NOT_REAFFIRMED", "TARGET_REMOVED",
}
LIVE_TRIGGER_DEFAULT_COOLDOWN_SECONDS = 1800
LIVE_TRIGGER_DEFAULT_POLL_SECONDS = 15

# =============================================================================
# W1: SCANNER INVALIDATION SOURCE MAP
# =============================================================================
# Maps scanner/feed events → INVALIDATION_TAXONOMY types.
# Single canonical source for all scan-triggered invalidations.
# Usage: call apply_invalidation(state, key, SCAN_INVALIDATION_MAP[source_event], ...)
#
# Precedence: when multiple events fire on same setup in one scan cycle:
#   1. HARD_INVALIDATION triggers > STALE triggers > SOFT_DEMOTION triggers
#   2. grade=NO_TRADE / disqualified overrides threshold_fail
#   3. missing from scan (stale) overrides zone/context detection
#   4. All cascade to runtime via apply_invalidation (not inline transitions)
#
# Integration points:
#   - sync_setup_state() row loop: SCAN_INVALIDATE_DISQUALIFIED, SCAN_INVALIDATE_THRESHOLD_LOSS
#   - sync_setup_state() unseen loop: SCAN_STALE_NOT_REAFFIRMED, SCAN_INVALIDATE_THRESHOLD_LOSS
#   - reconcile_live_trigger_targets(): TARGET_REMOVED (monitor no longer in target_map)
#   - reconcile_live_trigger_targets(): SCAN_DEMOTE_STANDBY via readiness_changed
#   - live_trigger_run quality gate: stale candle → CONTEXT_BROKEN (already implemented)
#   - live_trigger_run zone/context eval None: ZONE_BROKEN, CONTEXT_BROKEN (already impl.)
SCAN_INVALIDATION_SOURCE_MAP: Dict[str, str] = {
    # Grade/disqualification events
    "grade_no_trade": "SCAN_INVALIDATE_DISQUALIFIED",
    "disqualified": "SCAN_INVALIDATE_DISQUALIFIED",
    "hard_disqualified": "SCAN_INVALIDATE_DISQUALIFIED",
    # Quality threshold events
    "threshold_fail": "SCAN_INVALIDATE_THRESHOLD_LOSS",
    "quality_threshold_breach": "SCAN_INVALIDATE_THRESHOLD_LOSS",
    # Stale/not-reaffirmed events
    "setup_missing_from_scan": "SCAN_STALE_NOT_REAFFIRMED",
    "not_reaffirmed": "SCAN_STALE_NOT_REAFFIRMED",
    "scan_stale": "SCAN_STALE_NOT_REAFFIRMED",
    # Target universe removal
    "target_removed": "TARGET_REMOVED",
    "symbol_removed": "TARGET_REMOVED",
    # Readiness demotion (hysteresis-confirmed)
    "readiness_demotion_confirmed": "SCAN_DEMOTE_STANDBY",
    "hysteresis_demotion": "SCAN_DEMOTE_STANDBY",
    # Soft demotion (no hysteresis required)
    "soft_demotion": "SOFT_DEMOTION",
    "market_conditions_unfavorable": "SOFT_DEMOTION",
    # Zone reclaim (price back in zone, not confirmed)
    "zone_reclaimed": "ZONE_RECLAIMED",
    "zone_touch_failed": "ZONE_RECLAIMED",
}

# Scanner events that trigger hard invalidation (lifecycle → invalidated)
SCAN_INVALIDATION_HARD_EVENTS: set = {
    "grade_no_trade", "disqualified", "hard_disqualified",
    "threshold_fail", "quality_threshold_breach",
    "setup_missing_from_scan", "not_reaffirmed", "scan_stale",
    "target_removed", "symbol_removed",
    "zone_reclaimed",  # ZONE_BROKEN in taxonomy
}

# Scanner events that clear delivery on invalidation
SCAN_INVALIDATION_CLEAR_DELIVERY_EVENTS: set = {
    "grade_no_trade", "disqualified", "hard_disqualified",
    "threshold_fail", "quality_threshold_breach",
    "target_removed", "symbol_removed",
}

# =============================================================================
# W2: RECOVERY LIFECYCLE CONTRACT
# =============================================================================
# Formalizes deterministic recovery paths from invalid/stale/soft_demotion states.
#
# Recovery is the ONLY legal path for lifecycle to exit {invalidated, stale}:
#   - Manual user action CANNOT directly restore lifecycle
#   - Scanner must reaffirm with NEW setupVersion + retest_ready readiness
#   - Runtime re-arms via reconcile_live_trigger_targets() normal path
#
# RECOVERY_TRANSITIONS key: (prev_lifecycle, prev_readiness, curr_lifecycle, curr_readiness)
#   → {reason_code, runtime_target, delivery_reset}
#
# delivery.sent sticky reset: only on SCAN_COMMIT where prev_delivery in {cleared, idle}
#   and curr_delivery transitions to {pending, sent}. This handles the case where a newly
#   valid setup creates an alert delivery record for the first time (None → pending).

RECOVERY_TRANSITIONS: Dict[tuple, Dict[str, str]] = {
    # invalidated → actionable (setupVersion changed, readiness retest_ready)
    ("invalidated", "retest_ready", "actionable", "retest_ready"): {
        "reason_code": "SETUPVERSION_REAFFIRMED",
        "runtime_target": "armed",
        "delivery_reset": False,
    },
    ("invalidated", None, "actionable", "retest_ready"): {
        "reason_code": "SETUPVERSION_REAFFIRMED",
        "runtime_target": "armed",
        "delivery_reset": True,
    },
    ("invalidated", "pullback_standby", "actionable", "retest_ready"): {
        "reason_code": "SETUPVERSION_REAFFIRMED",
        "runtime_target": "armed",
        "delivery_reset": True,
    },
    # stale → actionable (setupVersion changed, readiness retest_ready)
    ("stale", "retest_ready", "actionable", "retest_ready"): {
        "reason_code": "SETUPVERSION_REAFFIRMED",
        "runtime_target": "armed",
        "delivery_reset": False,
    },
    ("stale", None, "actionable", "retest_ready"): {
        "reason_code": "SETUPVERSION_REAFFIRMED",
        "runtime_target": "armed",
        "delivery_reset": True,
    },
    # soft_demotion → actionable (readiness promotion, no lifecycle change from scan)
    ("actionable", "pullback_standby", "actionable", "retest_ready"): {
        "reason_code": "SCAN_PROMOTE_RETEST_READY",
        "runtime_target": "armed",
        "delivery_reset": False,
    },
    ("actionable", "bounce_standby", "actionable", "retest_ready"): {
        "reason_code": "SCAN_PROMOTE_RETEST_READY",
        "runtime_target": "armed",
        "delivery_reset": False,
    },
}

ENDPOINT_MAP = {
    "binance_rest": {
        "exchange_info": "/fapi/v1/exchangeInfo",
        "klines": "/fapi/v1/klines",
        "ticker_24h": "/fapi/v1/ticker/24hr",
        "book_ticker": "/fapi/v1/ticker/bookTicker",
        "depth": "/fapi/v1/depth",
        "agg_trades": "/fapi/v1/aggTrades",
        "premium_index": "/fapi/v1/premiumIndex",
        "funding_rate": "/fapi/v1/fundingRate",
        "open_interest": "/fapi/v1/openInterest",
        "open_interest_hist": "/futures/data/openInterestHist",
        "global_long_short": "/futures/data/globalLongShortAccountRatio",
        "top_long_short_accounts": "/futures/data/topLongShortAccountRatio",
        "top_long_short_positions": "/futures/data/topLongShortPositionRatio",
        "taker_long_short": "/futures/data/takerlongshortRatio",
    },
    "binance_ws": {
        "kline_5m": "<symbol>@kline_5m",
        "kline_15m": "<symbol>@kline_15m",
        "kline_1h": "<symbol>@kline_1h",
        "kline_4h": "<symbol>@kline_4h",
        "mark_price": "<symbol>@markPrice",
        "depth": "<symbol>@depth20@100ms",
        "agg_trade": "<symbol>@aggTrade",
        "force_order": "<symbol>@forceOrder",
    },
    "bybit_rest": {
        "instruments": "/v5/market/instruments-info",
        "kline": "/v5/market/kline",
        "tickers": "/v5/market/tickers",
        "orderbook": "/v5/market/orderbook",
        "recent_trade": "/v5/market/recent-trade",
        "funding_history": "/v5/market/funding/history",
        "open_interest": "/v5/market/open-interest",
    },
    "bybit_ws": {
        "ticker": "tickers.SYMBOL",
        "kline_5": "kline.5.SYMBOL",
        "kline_15": "kline.15.SYMBOL",
        "kline_60": "kline.60.SYMBOL",
        "kline_240": "kline.240.SYMBOL",
        "orderbook": "orderbook.50.SYMBOL",
        "public_trade": "publicTrade.SYMBOL",
    },
    "coingecko_rest": {
        "markets": "/coins/markets",
        "trending": "/search/trending",
        "coin": "/coins/{id}",
        "categories": "/coins/categories",
    },
    "public_search": {
        "google_news_rss": "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en",
        "binance_announcements": "https://www.binance.com/en/support/announcement",
        "bybit_announcements": "https://announcements.bybit.com/en-US/",
        "bls_cpi": "https://www.bls.gov/schedule/news_release/cpi.htm",
        "bls_ppi": "https://www.bls.gov/schedule/news_release/ppi.htm",
        "federal_reserve_calendar": "https://www.federalreserve.gov/newsevents/calendar.htm",
        "tradingeconomics_us_calendar": "https://tradingeconomics.com/united-states/calendar",
    },
    "coinmarketcap_rest": {
        "base": "https://pro-api.coinmarketcap.com",
        "quotes_latest": "/v2/cryptocurrency/quotes/latest",
    },
    "nansen_rest": {
        "base": "https://api.nansen.ai",
        "smart_money_holdings": "/api/v1/smart-money/holdings",
    },
    "massive_rest": {
        "base": "https://api.massive.com",
        "prev_bar": "/v2/aggs/ticker/{ticker}/prev",
        "custom_bars": "/v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{date_from}/{date_to}",
        "grouped_daily": "/v2/aggs/grouped/locale/global/market/crypto/{date}",
        "reference_tickers": "/v3/reference/tickers",
    },
    "lunarcrush_rest": {
        "base": "https://lunarcrush.com",
        "coins_list_v2": "/api4/public/coins/list/v2",
        "coins_list_v1": "/api4/public/coins/list/v1",
    },
}


def load_env_file(path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not path.exists():
        return env
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_json_file(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except Exception:
        return default


def save_json_file(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.name}.tmp.{os.getpid()}.{int(time.time() * 1000)}")
    temp_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False))
    os.replace(temp_path, path)


def append_jsonl(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False) + "\n")


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def unix_ms_to_iso(value: Optional[Any]) -> Optional[str]:
    if value in {None, "", 0}:
        return None
    try:
        return datetime.fromtimestamp(float(value) / 1000.0, tz=timezone.utc).isoformat()
    except Exception:
        return None


def parse_iso_timestamp(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value))
    except Exception:
        return None


def interval_to_seconds(interval: str) -> int:
    normalized = str(interval or "").strip().lower()
    mapping = {
        "1m": 60,
        "3m": 180,
        "5m": 300,
        "15m": 900,
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
    }
    return mapping.get(normalized, 300)


def process_is_alive(pid: Optional[int]) -> bool:
    if not pid or int(pid) <= 0:
        return False
    try:
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def read_pid_file(path: Path) -> Optional[int]:
    try:
        if path.exists():
            raw = path.read_text().strip()
            if raw.isdigit():
                return int(raw)
    except Exception:
        return None
    return None


def find_process_pids(pattern: str) -> List[int]:
    try:
        result = subprocess.run(["pgrep", "-f", pattern], capture_output=True, text=True, check=False)
        if result.returncode not in {0, 1}:
            return []
        current_pid = os.getpid()
        pids = []
        for raw in (result.stdout or "").splitlines():
            raw = raw.strip()
            if not raw.isdigit():
                continue
            pid = int(raw)
            if pid == current_pid:
                continue
            if process_is_alive(pid):
                pids.append(pid)
        return sorted(set(pids))
    except Exception:
        return []


def live_trigger_running_pids() -> List[int]:
    return find_process_pids("public_market_data.py live-trigger-run")


class ProviderRateLimiter:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def _load(self) -> Dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text())
        except Exception:
            return {}

    def _save(self, state: Dict[str, Any]) -> None:
        self.path.write_text(json.dumps(state, indent=2, ensure_ascii=False))

    def acquire(self, provider: str) -> None:
        rules = PROVIDER_LIMITS.get(provider, [])
        if not rules:
            return
        while True:
            now = time.time()
            state = self._load()
            provider_state = state.setdefault(provider, {})
            wait_for = 0.0
            for rule in rules:
                window = rule["window"]
                limit = int(rule["limit"])
                strategy = rule["strategy"]
                if isinstance(window, int):
                    key = f"sec_{window}"
                    timestamps = [float(ts) for ts in provider_state.get(key, []) if now - float(ts) < window]
                    provider_state[key] = timestamps
                    if len(timestamps) >= limit:
                        if strategy == "sleep":
                            wait_for = max(wait_for, window - (now - timestamps[0]) + 0.25)
                        else:
                            raise RuntimeError(f"Rate limit hit for {provider}: {limit}/{window}s")
                elif window == "day":
                    key = time.strftime("day_%Y-%m-%d", time.gmtime(now))
                    count = int(provider_state.get(key, 0))
                    if count >= limit:
                        if strategy == "sleep":
                            tomorrow = calendar.timegm(time.strptime(time.strftime("%Y-%m-%d", time.gmtime(now + 86400)), "%Y-%m-%d"))
                            wait_for = max(wait_for, tomorrow - now + 0.25)
                        else:
                            raise RuntimeError(f"Daily quota exhausted for {provider}: {limit}/day")
                elif window == "month":
                    key = time.strftime("month_%Y-%m", time.gmtime(now))
                    count = int(provider_state.get(key, 0))
                    if count >= limit:
                        raise RuntimeError(f"Monthly quota exhausted for {provider}: {limit}/month")
            if wait_for > 0:
                time.sleep(wait_for)
                continue
            for rule in rules:
                window = rule["window"]
                if isinstance(window, int):
                    key = f"sec_{window}"
                    timestamps = [float(ts) for ts in provider_state.get(key, []) if now - float(ts) < window]
                    timestamps.append(now)
                    provider_state[key] = timestamps
                elif window == "day":
                    key = time.strftime("day_%Y-%m-%d", time.gmtime(now))
                    provider_state[key] = int(provider_state.get(key, 0)) + 1
                elif window == "month":
                    key = time.strftime("month_%Y-%m", time.gmtime(now))
                    provider_state[key] = int(provider_state.get(key, 0)) + 1
            state[provider] = provider_state
            self._save(state)
            return


def http_get(url: str, params: Optional[Dict[str, Any]] = None) -> Any:
    response = requests.get(url, params=params, timeout=TIMEOUT, headers={"User-Agent": USER_AGENT})
    response.raise_for_status()
    ctype = response.headers.get("content-type", "")
    if "application/json" in ctype:
        return response.json()
    return response.text


class PublicMarketData:
    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": USER_AGENT})
        self.env = load_env_file(ENV_PATH)
        self.rate_limiter = ProviderRateLimiter(RATE_LIMIT_STATE_PATH)
        self.tradingview = TradingViewAdapter() if TradingViewAdapter else None
        self.openclaw_node_path: Optional[str] = None
        self.openclaw_node_source: Optional[str] = None
        self.openclaw_script_path: Optional[str] = None
        self.openclaw_script_source: Optional[str] = None
        self.openclaw_transport_source: Optional[str] = None
        self.openclaw_transport_checked_at: Optional[str] = None
        self.openclaw_transport_error: Optional[str] = None
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        self.resolve_openclaw_transport(refresh=True)

    def get(self, base: str, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        response = self.session.get(base + path, params=params, timeout=TIMEOUT)
        response.raise_for_status()
        ctype = response.headers.get("content-type", "")
        if "application/json" in ctype:
            return response.json()
        return response.text

    def run_command(self, args: List[str], expect_json: bool = False) -> Any:
        proc = subprocess.run(args, capture_output=True, text=True, timeout=60)
        if proc.returncode != 0:
            raise RuntimeError((proc.stderr or proc.stdout or "command failed").strip())
        output = (proc.stdout or "").strip()
        if expect_json:
            try:
                return json.loads(output or "{}")
            except json.JSONDecodeError as exc:
                stderr = (proc.stderr or "").strip()
                raise RuntimeError(
                    f"command returned non-JSON output; stdout={output!r}; stderr={stderr!r}"
                ) from exc
        return output

    def _resolve_path_candidate(self, candidates: List[Dict[str, str]], missing_label: str, require_executable: bool = True) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        resolution_notes: List[str] = []
        for candidate in candidates:
            raw_path = str(candidate.get("path") or "").strip()
            if not raw_path:
                continue
            expanded = Path(raw_path).expanduser()
            try:
                resolved = expanded.resolve()
            except Exception:
                resolved = expanded
            exists = resolved.exists() and resolved.is_file()
            executable_ok = os.access(resolved, os.X_OK) if require_executable else True
            if exists and executable_ok:
                return str(resolved), str(candidate.get("source") or "unknown"), None
            resolution_notes.append(f"{candidate.get('source')}={resolved}")
        error = (
            f"{missing_label}; PATH="
            f"{os.environ.get('PATH') or ''}; checked="
            + ", ".join(resolution_notes[:10])
        )
        return None, None, error

    def _compose_openclaw_transport_source(self) -> Optional[str]:
        if self.openclaw_script_source and self.openclaw_node_source:
            return f"{self.openclaw_script_source} | {self.openclaw_node_source}"
        return self.openclaw_script_source or self.openclaw_node_source

    def _npm_global_openclaw_script_candidates(self) -> List[Dict[str, str]]:
        npm_path = shutil.which("npm")
        if not npm_path:
            return []

        candidates: List[Dict[str, str]] = []

        def add(source: str, value: Optional[str]) -> None:
            if not value:
                return
            candidates.append({"source": source, "path": value})

        try:
            proc = subprocess.run([npm_path, "root", "-g"], capture_output=True, text=True, timeout=5)
            npm_root = (proc.stdout or "").strip() if proc.returncode == 0 else ""
            if npm_root:
                add("npm:root -g", str(Path(npm_root) / "openclaw" / "openclaw.mjs"))
        except Exception:
            pass

        try:
            npm_resolved = Path(npm_path).expanduser().resolve()
            derived_root = npm_resolved.parent.parent / "lib" / "node_modules" / "openclaw" / "openclaw.mjs"
            add("npm:derived", str(derived_root))
        except Exception:
            pass

        seen: set = set()
        deduped: List[Dict[str, str]] = []
        for item in candidates:
            key = str(item.get("path") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def openclaw_script_candidates(self) -> List[Dict[str, str]]:
        home = Path.home()
        candidates: List[Dict[str, str]] = []

        def add(source: str, value: Optional[str]) -> None:
            if not value:
                return
            candidates.append({"source": source, "path": value})

        add("env:OPENCLAW_SCRIPT", self.env.get("OPENCLAW_SCRIPT") or os.environ.get("OPENCLAW_SCRIPT"))
        add("env:OPENCLAW_BIN", self.env.get("OPENCLAW_BIN") or os.environ.get("OPENCLAW_BIN"))

        add("known:usr_local", "/usr/local/lib/node_modules/openclaw/openclaw.mjs")
        add("known:usr", "/usr/lib/node_modules/openclaw/openclaw.mjs")
        add("known:nvm:default", str(home / ".nvm/versions/node/v22.22.2/lib/node_modules/openclaw/openclaw.mjs"))
        add("known:nvm:current", str(home / ".nvm/current/lib/node_modules/openclaw/openclaw.mjs"))

        for item in self._npm_global_openclaw_script_candidates():
            add(str(item.get("source") or "npm:unknown"), str(item.get("path") or ""))

        versions_dir = home / ".nvm/versions/node"
        if versions_dir.exists():
            for path in sorted(versions_dir.glob("*/lib/node_modules/openclaw/openclaw.mjs"), reverse=True):
                add("nvm:glob:script", str(path))

        openclaw_path = shutil.which("openclaw")
        if openclaw_path:
            add("which:openclaw", openclaw_path)
        add("nvm:wrapper", str(home / ".nvm/versions/node/v22.22.2/bin/openclaw"))
        add("nvm:wrapper:current", str(home / ".nvm/current/bin/openclaw"))
        if versions_dir.exists():
            for path in sorted(versions_dir.glob("*/bin/openclaw"), reverse=True):
                add("nvm:glob", str(path))

        seen: set = set()
        deduped: List[Dict[str, str]] = []
        for item in candidates:
            key = str(item.get("path") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def openclaw_node_candidates(self, script_path: Optional[str] = None) -> List[Dict[str, str]]:
        home = Path.home()
        candidates: List[Dict[str, str]] = []

        def add(source: str, value: Optional[str]) -> None:
            if not value:
                return
            candidates.append({"source": source, "path": value})

        add("env:OPENCLAW_NODE", self.env.get("OPENCLAW_NODE") or os.environ.get("OPENCLAW_NODE"))
        add("env:NODE_BINARY", self.env.get("NODE_BINARY") or os.environ.get("NODE_BINARY"))

        resolved_script = Path(script_path).expanduser().resolve() if script_path else None
        if resolved_script:
            script_parts = resolved_script.parts
            if "lib" in script_parts and "node_modules" in script_parts:
                lib_index = script_parts.index("lib")
                version_root = Path(*script_parts[:lib_index])
                add("script:sibling-node", str(version_root / "bin" / "node"))
            add("script:parent-node", str(resolved_script.parent / "node"))

        add("known:nvm:default", str(home / ".nvm/versions/node/v22.22.2/bin/node"))
        add("known:nvm:current", str(home / ".nvm/current/bin/node"))
        add("known:usr_local", "/usr/local/bin/node")
        add("known:usr", "/usr/bin/node")
        add("known:local_bin", str(home / ".local/bin/node"))

        versions_dir = home / ".nvm/versions/node"
        if versions_dir.exists():
            for path in sorted(versions_dir.glob("*/bin/node"), reverse=True):
                add("nvm:glob", str(path))

        add("which:PATH", shutil.which("node"))

        seen: set = set()
        deduped: List[Dict[str, str]] = []
        for item in candidates:
            key = str(item.get("path") or "")
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def resolve_openclaw_script_path(self, refresh: bool = False) -> Optional[str]:
        if self.openclaw_script_path and not refresh:
            return self.openclaw_script_path

        checked_at = utc_now_iso()
        script_path, script_source, script_error = self._resolve_path_candidate(
            self.openclaw_script_candidates(),
            "openclaw script not found",
            require_executable=False,
        )
        self.openclaw_script_path = script_path
        self.openclaw_script_source = script_source
        self.openclaw_transport_source = self._compose_openclaw_transport_source()
        self.openclaw_transport_checked_at = checked_at
        if script_error:
            self.openclaw_transport_error = script_error
        elif self.openclaw_node_path:
            self.openclaw_transport_error = None
        return self.openclaw_script_path

    def resolve_openclaw_node_binary(self, refresh: bool = False) -> Optional[str]:
        if self.openclaw_node_path and not refresh:
            return self.openclaw_node_path

        checked_at = utc_now_iso()
        script_path = self.resolve_openclaw_script_path(refresh=False)
        node_path, node_source, node_error = self._resolve_path_candidate(
            self.openclaw_node_candidates(script_path=script_path),
            "node binary not found for openclaw transport",
            require_executable=True,
        )
        self.openclaw_node_path = node_path
        self.openclaw_node_source = node_source
        self.openclaw_transport_source = self._compose_openclaw_transport_source()
        self.openclaw_transport_checked_at = checked_at
        if node_error:
            self.openclaw_transport_error = node_error
        elif self.openclaw_script_path:
            self.openclaw_transport_error = None
        return self.openclaw_node_path

    def resolve_openclaw_transport(self, refresh: bool = False) -> Optional[List[str]]:
        if self.openclaw_node_path and self.openclaw_script_path and not refresh:
            return [self.openclaw_node_path, self.openclaw_script_path]

        checked_at = utc_now_iso()
        self.openclaw_transport_checked_at = checked_at
        self.openclaw_transport_error = None
        self.openclaw_transport_source = None
        self.openclaw_node_source = None
        self.openclaw_script_source = None
        if refresh:
            self.openclaw_node_path = None
            self.openclaw_script_path = None

        script_path = self.resolve_openclaw_script_path(refresh=refresh)
        node_path = self.resolve_openclaw_node_binary(refresh=refresh)

        if script_path and node_path:
            self.openclaw_transport_error = None
            return [node_path, script_path]

        if not self.openclaw_transport_error:
            if not script_path:
                self.openclaw_transport_error = "openclaw script not found"
            elif not node_path:
                self.openclaw_transport_error = "node binary not found for openclaw transport"
        return None

    def resolve_openclaw_binary(self, refresh: bool = False) -> Optional[str]:
        return self.resolve_openclaw_script_path(refresh=refresh)

    def openclaw_transport_info(self, error: Optional[str] = None) -> Dict[str, Any]:
        return {
            "kind": "openclaw_cli",
            "launchMode": "explicit_node_script",
            "nodeBinaryPath": self.openclaw_node_path,
            "scriptPath": self.openclaw_script_path,
            "resolverSource": self.openclaw_transport_source,
            "checkedAt": self.openclaw_transport_checked_at,
            "error": error if error is not None else self.openclaw_transport_error,
            "binaryPath": self.openclaw_script_path,
            "binarySource": self.openclaw_transport_source,
        }

    def openclaw_message_send_command(self, channel: str, target: str, message: str, refresh: bool = False) -> Optional[List[str]]:
        transport = self.resolve_openclaw_transport(refresh=refresh)
        if not transport:
            return None
        node_path, script_path = transport
        return [
            node_path,
            script_path,
            "message",
            "send",
            "--channel",
            channel,
            "--target",
            target,
            "--message",
            message,
        ]

    def provider_key(self, provider: str) -> str:
        env_key = PROVIDER_ENV_KEYS[provider]
        value = self.env.get(env_key) or os.environ.get(env_key)
        if not value:
            raise RuntimeError(f"Missing {env_key} in {ENV_PATH}")
        return value

    def provider_status(self) -> Dict[str, Any]:
        state = self.rate_limiter._load()
        providers = {}
        for provider, env_key in PROVIDER_ENV_KEYS.items():
            providers[provider] = {
                "envKey": env_key,
                "configured": bool(self.env.get(env_key) or os.environ.get(env_key)),
                "limits": PROVIDER_LIMITS.get(provider, []),
                "usageState": state.get(provider, {}),
            }
        return {
            "envPath": str(ENV_PATH),
            "rateLimitStatePath": str(RATE_LIMIT_STATE_PATH),
            "providers": providers,
        }

    def provider_get(self, provider: str, base: str, path: str, params: Optional[Dict[str, Any]] = None, headers: Optional[Dict[str, str]] = None) -> Any:
        self.rate_limiter.acquire(provider)
        final_headers = {"User-Agent": USER_AGENT}
        if headers:
            final_headers.update(headers)
        final_params = dict(params or {})
        if provider == "massive":
            final_headers["Authorization"] = f"Bearer {self.provider_key('massive')}"
        if provider == "coinmarketcap":
            final_headers["X-CMC_PRO_API_KEY"] = self.provider_key("coinmarketcap")
        if provider == "lunarcrush":
            final_headers["Authorization"] = f"Bearer {self.provider_key('lunarcrush')}"
        response = self.session.get(base + path, params=final_params, headers=final_headers, timeout=TIMEOUT)
        response.raise_for_status()
        ctype = response.headers.get("content-type", "")
        if "application/json" in ctype:
            return response.json()
        return response.text

    def provider_post(self, provider: str, base: str, path: str, payload: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> Any:
        self.rate_limiter.acquire(provider)
        final_headers = {"User-Agent": USER_AGENT, "Content-Type": "application/json"}
        if headers:
            final_headers.update(headers)
        if provider == "nansen":
            final_headers["apikey"] = self.provider_key("nansen")
        response = self.session.post(base + path, json=payload, headers=final_headers, timeout=TIMEOUT)
        response.raise_for_status()
        ctype = response.headers.get("content-type", "")
        if "application/json" in ctype:
            return response.json()
        return response.text

    def provider_error_payload(self, provider: str, exc: Exception) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "provider": provider,
            "error": str(exc),
        }
        response = getattr(exc, "response", None)
        if response is not None:
            payload["statusCode"] = response.status_code
            try:
                payload["body"] = response.json()
            except Exception:
                payload["body"] = response.text[:800]
        return payload

    def find_nested_value(self, data: Any, candidates: List[str]) -> Any:
        normalized_candidates = {candidate.lower().replace("_", "").replace("-", "") for candidate in candidates}
        if isinstance(data, dict):
            normalized_keys = {key.lower().replace("_", "").replace("-", ""): key for key in data.keys()}
            for candidate in normalized_candidates:
                if candidate in normalized_keys:
                    return data[normalized_keys[candidate]]
            for value in data.values():
                result = self.find_nested_value(value, candidates)
                if result is not None:
                    return result
        if isinstance(data, list):
            for value in data:
                result = self.find_nested_value(value, candidates)
                if result is not None:
                    return result
        return None

    def symbol_root(self, symbol: str) -> str:
        return symbol.replace("USDT", "")

    def massive_ticker(self, symbol: str) -> str:
        return SYMBOL_TO_MASSIVE.get(symbol, f"X:{self.symbol_root(symbol)}USD")

    def utc_date(self, days_offset: int = 0) -> str:
        return (datetime.now(timezone.utc) + timedelta(days=days_offset)).strftime("%Y-%m-%d")

    def cmc_quotes_latest(self, symbols: List[str]) -> Any:
        return self.provider_get(
            "coinmarketcap",
            ENDPOINT_MAP["coinmarketcap_rest"]["base"],
            ENDPOINT_MAP["coinmarketcap_rest"]["quotes_latest"],
            {"symbol": ",".join(symbols), "convert": "USD"},
        )

    def nansen_smart_money_holdings(self, chains: List[str], token_symbols: Optional[List[str]] = None, page: int = 1, per_page: int = 10) -> Any:
        payload: Dict[str, Any] = {
            "chains": chains,
            "pagination": {"page": page, "per_page": per_page},
        }
        if token_symbols:
            payload["token_symbols"] = token_symbols
        return self.provider_post(
            "nansen",
            ENDPOINT_MAP["nansen_rest"]["base"],
            ENDPOINT_MAP["nansen_rest"]["smart_money_holdings"],
            payload,
        )

    def massive_reference_tickers(self, limit: int = 10) -> Dict[str, Any]:
        try:
            raw = self.provider_get(
                "massive",
                ENDPOINT_MAP["massive_rest"]["base"],
                ENDPOINT_MAP["massive_rest"]["reference_tickers"],
                {"market": "crypto", "active": "true", "limit": limit, "sort": "ticker", "order": "asc"},
            )
            return {
                "provider": "massive",
                "summary": {
                    "count": len(raw.get("results", [])) if isinstance(raw, dict) and isinstance(raw.get("results"), list) else 0,
                    "nextUrl": raw.get("next_url") if isinstance(raw, dict) else None,
                },
                "raw": raw,
            }
        except Exception as exc:
            return self.provider_error_payload("massive", exc)

    def massive_prev_bar(self, symbol: str) -> Dict[str, Any]:
        ticker = self.massive_ticker(symbol)
        try:
            raw = self.provider_get(
                "massive",
                ENDPOINT_MAP["massive_rest"]["base"],
                ENDPOINT_MAP["massive_rest"]["prev_bar"].format(ticker=ticker),
            )
            row = raw.get("results", [{}])[0] if isinstance(raw, dict) and isinstance(raw.get("results"), list) and raw.get("results") else {}
            return {
                "provider": "massive",
                "ticker": ticker,
                "summary": {
                    "open": row.get("o"),
                    "close": row.get("c"),
                    "high": row.get("h"),
                    "low": row.get("l"),
                    "volume": row.get("v"),
                    "vwap": row.get("vw"),
                    "trades": row.get("n"),
                    "changePct": (((float(row.get("c")) - float(row.get("o"))) / float(row.get("o"))) * 100) if row.get("o") not in {None, 0} and row.get("c") is not None else None,
                },
                "raw": raw,
            }
        except Exception as exc:
            return self.provider_error_payload("massive", exc)

    def massive_grouped_daily(self, date_str: Optional[str] = None) -> Dict[str, Any]:
        query_date = date_str or self.utc_date(-1)
        try:
            raw = self.provider_get(
                "massive",
                ENDPOINT_MAP["massive_rest"]["base"],
                ENDPOINT_MAP["massive_rest"]["grouped_daily"].format(date=query_date),
                {"adjusted": "true"},
            )
            return {
                "provider": "massive",
                "date": query_date,
                "summary": {
                    "resultsCount": raw.get("resultsCount") if isinstance(raw, dict) else None,
                    "queryCount": raw.get("queryCount") if isinstance(raw, dict) else None,
                },
                "raw": raw,
            }
        except Exception as exc:
            return self.provider_error_payload("massive", exc)

    def massive_custom_bars(self, symbol: str, multiplier: int = 1, timespan: str = "hour", date_from: Optional[str] = None, date_to: Optional[str] = None, limit: int = 200) -> Dict[str, Any]:
        ticker = self.massive_ticker(symbol)
        start = date_from or self.utc_date(-2)
        end = date_to or self.utc_date(0)
        try:
            raw = self.provider_get(
                "massive",
                ENDPOINT_MAP["massive_rest"]["base"],
                ENDPOINT_MAP["massive_rest"]["custom_bars"].format(ticker=ticker, multiplier=multiplier, timespan=timespan, date_from=start, date_to=end),
                {"adjusted": "true", "sort": "asc", "limit": limit},
            )
            rows = raw.get("results", []) if isinstance(raw, dict) and isinstance(raw.get("results"), list) else []
            first = rows[0] if rows else {}
            last = rows[-1] if rows else {}
            move_pct = None
            if first.get("c") not in {None, 0} and last.get("c") is not None:
                move_pct = ((float(last.get("c")) - float(first.get("c"))) / float(first.get("c"))) * 100
            up_bars = 0
            down_bars = 0
            closes: List[float] = []
            for row in rows:
                try:
                    if row.get("c") is not None:
                        closes.append(float(row.get("c")))
                    if row.get("c") is not None and row.get("o") is not None:
                        if float(row.get("c")) > float(row.get("o")):
                            up_bars += 1
                        elif float(row.get("c")) < float(row.get("o")):
                            down_bars += 1
                except Exception:
                    continue
            high = max((float(row.get("h")) for row in rows if row.get("h") is not None), default=None)
            low = min((float(row.get("l")) for row in rows if row.get("l") is not None), default=None)
            directional_ratio = ((up_bars - down_bars) / len(rows)) if rows else None
            efficiency = None
            if len(closes) >= 2:
                path = sum(abs(closes[i] - closes[i - 1]) for i in range(1, len(closes)))
                net = abs(closes[-1] - closes[0])
                efficiency = 0.0 if path == 0 else net / path
            close_location = None
            if high is not None and low is not None and last.get("c") is not None and high != low:
                close_location = (float(last.get("c")) - low) / (high - low)
            range_pct = None
            if high is not None and low is not None and first.get("c") not in {None, 0}:
                range_pct = ((high - low) / float(first.get("c"))) * 100
            return {
                "provider": "massive",
                "ticker": ticker,
                "summary": {
                    "bars": len(rows),
                    "firstClose": first.get("c"),
                    "lastClose": last.get("c"),
                    "high": high,
                    "low": low,
                    "movePct": move_pct,
                    "upBars": up_bars,
                    "downBars": down_bars,
                    "directionalRatio": directional_ratio,
                    "efficiency": efficiency,
                    "closeLocation": close_location,
                    "rangePct": range_pct,
                },
                "raw": raw,
            }
        except Exception as exc:
            return self.provider_error_payload("massive", exc)

    def massive_symbol_grouped_row(self, symbol: str, grouped_daily: Dict[str, Any]) -> Dict[str, Any]:
        ticker = self.massive_ticker(symbol)
        results = grouped_daily.get("raw", {}).get("results", []) if isinstance(grouped_daily, dict) else []
        picked = None
        ranked_changes: List[tuple] = []
        ranked_volumes: List[tuple] = []
        ranked_trades: List[tuple] = []
        if isinstance(results, list):
            for row in results:
                try:
                    if row.get("o") not in {None, 0} and row.get("c") is not None:
                        pct = ((float(row.get("c")) - float(row.get("o"))) / float(row.get("o"))) * 100
                        ranked_changes.append((row.get("T"), pct))
                except Exception:
                    pass
                try:
                    if row.get("v") is not None:
                        ranked_volumes.append((row.get("T"), float(row.get("v"))))
                except Exception:
                    pass
                try:
                    if row.get("n") is not None:
                        ranked_trades.append((row.get("T"), float(row.get("n"))))
                except Exception:
                    pass
                if row.get("T") == ticker:
                    picked = row
        if not picked:
            return {
                "provider": "massive",
                "ticker": ticker,
                "warning": "Ticker not found in grouped daily results",
            }
        change_pct = None
        if picked.get("o") not in {None, 0} and picked.get("c") is not None:
            change_pct = ((float(picked.get("c")) - float(picked.get("o"))) / float(picked.get("o"))) * 100

        def percentile_rank(rows: List[tuple], target: str) -> Optional[float]:
            if not rows:
                return None
            sorted_rows = sorted(rows, key=lambda item: item[1])
            idx = next((i for i, item in enumerate(sorted_rows) if item[0] == target), None)
            if idx is None:
                return None
            return (idx / max(1, len(sorted_rows) - 1)) * 100

        close_vs_vwap_pct = None
        try:
            if picked.get("vw") not in {None, 0} and picked.get("c") is not None:
                close_vs_vwap_pct = ((float(picked.get("c")) - float(picked.get("vw"))) / float(picked.get("vw"))) * 100
        except Exception:
            pass
        return {
            "provider": "massive",
            "ticker": ticker,
            "summary": {
                "open": picked.get("o"),
                "close": picked.get("c"),
                "high": picked.get("h"),
                "low": picked.get("l"),
                "volume": picked.get("v"),
                "vwap": picked.get("vw"),
                "trades": picked.get("n"),
                "changePct": change_pct,
                "breadthPercentile": percentile_rank(ranked_changes, ticker),
                "volumePercentile": percentile_rank(ranked_volumes, ticker),
                "tradePercentile": percentile_rank(ranked_trades, ticker),
                "closeVsVwapPct": close_vs_vwap_pct,
            },
            "raw": picked,
        }

    def massive_symbol_confirmation(self, symbol: str, grouped_daily: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        grouped = grouped_daily if grouped_daily is not None else self.massive_grouped_daily()
        grouped_symbol = self.massive_symbol_grouped_row(symbol, grouped) if isinstance(grouped, dict) and grouped.get("provider") == "massive" and "raw" in grouped else grouped
        prev_bar = self.massive_prev_bar(symbol)
        custom_bars = self.massive_custom_bars(symbol)
        return {
            "provider": "massive",
            "ticker": self.massive_ticker(symbol),
            "groupedDaily": grouped_symbol,
            "prevBar": prev_bar,
            "customBars": custom_bars,
        }

    def lunarcrush_symbol_metrics(self, symbol: str) -> Dict[str, Any]:
        root = self.symbol_root(symbol)
        last_error: Optional[Dict[str, Any]] = None
        for endpoint in ["coins_list_v2", "coins_list_v1"]:
            try:
                raw = self.provider_get(
                    "lunarcrush",
                    ENDPOINT_MAP["lunarcrush_rest"]["base"],
                    ENDPOINT_MAP["lunarcrush_rest"][endpoint],
                    {"symbol": root},
                )
                rows = []
                if isinstance(raw, dict):
                    if isinstance(raw.get("data"), list):
                        rows = raw["data"]
                    elif isinstance(raw.get("data"), dict):
                        rows = [raw["data"]]
                    elif isinstance(raw.get("config"), dict) and isinstance(raw.get("items"), list):
                        rows = raw["items"]
                picked = None
                for row in rows:
                    row_symbol = str(self.find_nested_value(row, ["symbol", "s", "code"]) or "").upper()
                    if row_symbol == root:
                        picked = row
                        break
                if picked is None and rows:
                    picked = rows[0]
                summary = {}
                if picked is not None:
                    summary = {
                        "symbol": self.find_nested_value(picked, ["symbol", "code", "s"]),
                        "name": self.find_nested_value(picked, ["name", "n"]),
                        "galaxyScore": self.find_nested_value(picked, ["galaxy_score", "galaxyscore"]),
                        "altRank": self.find_nested_value(picked, ["alt_rank", "altrank"]),
                        "socialDominance": self.find_nested_value(picked, ["social_dominance", "socialdominance"]),
                        "socialVolume24h": self.find_nested_value(picked, ["social_volume_24h", "social_volume", "socialvolume24h"]),
                        "sentiment": self.find_nested_value(picked, ["sentiment", "sentiment_score", "sentimentscore"]),
                    }
                return {
                    "provider": "lunarcrush",
                    "endpoint": endpoint,
                    "summary": summary,
                    "raw": raw,
                }
            except Exception as exc:
                last_error = self.provider_error_payload("lunarcrush", exc)
        return last_error or {"provider": "lunarcrush", "error": "Unknown error"}

    def provider_enrichment(self, symbol: str, massive_grouped_daily: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        return {
            "symbol": symbol,
            "massive": self.massive_symbol_confirmation(symbol, grouped_daily=massive_grouped_daily),
            "lunarcrush": self.lunarcrush_symbol_metrics(symbol),
        }

    def provider_overlay(self, symbol: str, bull_score: int, enrichment: Dict[str, Any]) -> Dict[str, Any]:
        clean_delta = 0.0
        notes: List[str] = []
        massive = enrichment.get("massive", {}) if isinstance(enrichment, dict) else {}
        grouped_summary = ((((massive.get("groupedDaily") or {}).get("summary")) or {}) if isinstance(massive, dict) and isinstance(massive.get("groupedDaily"), dict) else {})
        prev_summary = ((((massive.get("prevBar") or {}).get("summary")) or {}) if isinstance(massive, dict) and isinstance(massive.get("prevBar"), dict) else {})
        custom_summary = ((((massive.get("customBars") or {}).get("summary")) or {}) if isinstance(massive, dict) and isinstance(massive.get("customBars"), dict) else {})
        change_pct = grouped_summary.get("changePct")
        breadth_percentile = grouped_summary.get("breadthPercentile")
        volume_percentile = grouped_summary.get("volumePercentile")
        close_vs_vwap_pct = grouped_summary.get("closeVsVwapPct")
        try:
            if change_pct is not None:
                change_pct = float(change_pct)
                if bull_score >= 2 and change_pct > 0:
                    clean_delta += 1.5
                    notes.append("Massive grouped daily confirms bullish spot direction")
                elif bull_score <= -2 and change_pct < 0:
                    clean_delta += 1.5
                    notes.append("Massive grouped daily confirms bearish spot direction")
                elif bull_score >= 2 and change_pct < 0:
                    clean_delta -= 2.0
                    notes.append("Massive grouped daily conflicts with bullish futures structure")
                elif bull_score <= -2 and change_pct > 0:
                    clean_delta -= 2.0
                    notes.append("Massive grouped daily conflicts with bearish futures structure")
        except Exception:
            pass
        try:
            if breadth_percentile is not None:
                breadth_percentile = float(breadth_percentile)
                if bull_score >= 2 and breadth_percentile >= 80:
                    clean_delta += 1.0
                    notes.append("Massive breadth rank is in the top quintile")
                elif bull_score <= -2 and breadth_percentile <= 20:
                    clean_delta += 1.0
                    notes.append("Massive breadth rank is in the bottom quintile")
                elif bull_score >= 2 and breadth_percentile <= 35:
                    clean_delta -= 1.0
                    notes.append("Massive breadth rank is weak for a bullish setup")
                elif bull_score <= -2 and breadth_percentile >= 65:
                    clean_delta -= 1.0
                    notes.append("Massive breadth rank is too strong for a bearish setup")
        except Exception:
            pass
        try:
            if volume_percentile is not None:
                volume_percentile = float(volume_percentile)
                if volume_percentile >= 75:
                    clean_delta += 0.5
                    notes.append("Massive grouped daily volume percentile is supportive")
        except Exception:
            pass
        try:
            if close_vs_vwap_pct is not None:
                close_vs_vwap_pct = float(close_vs_vwap_pct)
                if bull_score >= 2 and close_vs_vwap_pct > 0:
                    clean_delta += 0.5
                    notes.append("Massive close is above grouped daily VWAP")
                elif bull_score <= -2 and close_vs_vwap_pct < 0:
                    clean_delta += 0.5
                    notes.append("Massive close is below grouped daily VWAP")
        except Exception:
            pass
        try:
            custom_move = custom_summary.get("movePct")
            custom_directional_ratio = custom_summary.get("directionalRatio")
            custom_efficiency = custom_summary.get("efficiency")
            custom_close_location = custom_summary.get("closeLocation")
            if custom_move is not None:
                custom_move = float(custom_move)
                if bull_score >= 2 and custom_move > 0:
                    clean_delta += 2.0
                    notes.append("Massive custom bars confirm bullish finalist momentum")
                elif bull_score <= -2 and custom_move < 0:
                    clean_delta += 2.0
                    notes.append("Massive custom bars confirm bearish finalist momentum")
                elif bull_score >= 2 and custom_move < 0:
                    clean_delta -= 3.0
                    notes.append("Massive custom bars reject bullish finalist momentum")
                elif bull_score <= -2 and custom_move > 0:
                    clean_delta -= 3.0
                    notes.append("Massive custom bars reject bearish finalist momentum")
            if custom_directional_ratio is not None:
                custom_directional_ratio = float(custom_directional_ratio)
                if bull_score >= 2 and custom_directional_ratio >= 0.2:
                    clean_delta += 0.75
                    notes.append("Massive custom bars show positive bar-by-bar persistence")
                elif bull_score <= -2 and custom_directional_ratio <= -0.2:
                    clean_delta += 0.75
                    notes.append("Massive custom bars show negative bar-by-bar persistence")
            if custom_efficiency is not None:
                custom_efficiency = float(custom_efficiency)
                if custom_efficiency >= 0.55:
                    clean_delta += 0.75
                    notes.append("Massive custom bars trend efficiency is strong")
                elif custom_efficiency <= 0.2:
                    clean_delta -= 0.5
                    notes.append("Massive custom bars trend efficiency is noisy")
            if custom_close_location is not None:
                custom_close_location = float(custom_close_location)
                if bull_score >= 2 and custom_close_location >= 0.65:
                    clean_delta += 0.5
                    notes.append("Massive custom bars close near the upper end of range")
                elif bull_score <= -2 and custom_close_location <= 0.35:
                    clean_delta += 0.5
                    notes.append("Massive custom bars close near the lower end of range")
        except Exception:
            pass
        try:
            prev_change = prev_summary.get("changePct")
            if prev_change is not None:
                prev_change = float(prev_change)
                if bull_score >= 2 and prev_change > 0:
                    clean_delta += 0.75
                    notes.append("Massive previous day bar supports bullish backdrop")
                elif bull_score <= -2 and prev_change < 0:
                    clean_delta += 0.75
                    notes.append("Massive previous day bar supports bearish backdrop")
        except Exception:
            pass
        for massive_layer_name, massive_layer in [("grouped daily", massive.get("groupedDaily")), ("prev bar", massive.get("prevBar")), ("custom bars", massive.get("customBars"))]:
            if isinstance(massive_layer, dict) and massive_layer.get("statusCode") in {402, 403}:
                notes.append(f"Massive key is configured but current plan is not entitled for Massive {massive_layer_name}")
            if isinstance(massive_layer, dict) and massive_layer.get("error") and not massive_layer.get("statusCode"):
                notes.append(f"Massive {massive_layer_name} request failed")

        lunar = enrichment.get("lunarcrush", {}) if isinstance(enrichment, dict) else {}
        lunar_summary = lunar.get("summary", {}) if isinstance(lunar, dict) else {}
        try:
            galaxy = lunar_summary.get("galaxyScore")
            if galaxy is not None:
                galaxy = float(galaxy)
                if galaxy >= 60:
                    clean_delta += 1.5
                    notes.append("LunarCrush social quality is supportive")
                elif galaxy <= 40:
                    clean_delta -= 1.5
                    notes.append("LunarCrush social quality is weak")
        except Exception:
            pass
        try:
            alt_rank = lunar_summary.get("altRank")
            if alt_rank is not None:
                alt_rank = float(alt_rank)
                if alt_rank <= 100:
                    clean_delta += 1.0
                    notes.append("LunarCrush AltRank is strong")
                elif alt_rank >= 1000:
                    clean_delta -= 1.0
                    notes.append("LunarCrush AltRank is weak")
        except Exception:
            pass
        if isinstance(lunar, dict) and lunar.get("statusCode") == 402:
            notes.append("LunarCrush key is configured but current subscription does not allow the requested endpoint")

        return {
            "symbol": symbol,
            "cleanScoreDelta": round(clean_delta, 2),
            "notes": notes,
            "providers": enrichment,
        }

    def binance_klines(self, symbol: str, interval: str, limit: int = 120) -> Any:
        return self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["klines"], {"symbol": symbol, "interval": interval, "limit": limit})

    def binance_cvd_summary(self, agg_trades: Any) -> Dict[str, Any]:
        rows = agg_trades if isinstance(agg_trades, list) else []
        if not rows:
            return {
                "bars": 0,
                "buyQty": 0.0,
                "sellQty": 0.0,
                "buyNotional": 0.0,
                "sellNotional": 0.0,
                "deltaQty": 0.0,
                "deltaNotional": 0.0,
                "deltaRatio": 0.0,
                "recentDeltaRatio": 0.0,
                "cvdLast": 0.0,
                "cvdSlopePerTrade": 0.0,
                "priceChangePct": 0.0,
                "divergence": None,
                "bias": "neutral",
                "efficiency": 0.0,
            }

        buy_qty = 0.0
        sell_qty = 0.0
        buy_notional = 0.0
        sell_notional = 0.0
        signed_notionals: List[float] = []
        signed_qtys: List[float] = []
        prices: List[float] = []
        cumulative = 0.0
        cvd_series: List[float] = []

        for row in rows:
            try:
                price = float(row.get("p"))
                qty = float(row.get("q"))
            except Exception:
                continue
            if price <= 0 or qty <= 0:
                continue
            notional = price * qty
            is_sell_aggressor = bool(row.get("m", False))
            signed_qty = -qty if is_sell_aggressor else qty
            signed_notional = -notional if is_sell_aggressor else notional
            if is_sell_aggressor:
                sell_qty += qty
                sell_notional += notional
            else:
                buy_qty += qty
                buy_notional += notional
            cumulative += signed_notional
            signed_qtys.append(signed_qty)
            signed_notionals.append(signed_notional)
            prices.append(price)
            cvd_series.append(cumulative)

        total_notional = buy_notional + sell_notional
        total_qty = buy_qty + sell_qty
        delta_notional = buy_notional - sell_notional
        delta_qty = buy_qty - sell_qty
        delta_ratio = 0.0 if total_notional == 0 else delta_notional / total_notional
        recent_n = min(40, len(signed_notionals))
        recent_signed = signed_notionals[-recent_n:] if recent_n else []
        recent_total = sum(abs(value) for value in recent_signed)
        recent_delta = sum(recent_signed)
        recent_delta_ratio = 0.0 if recent_total == 0 else recent_delta / recent_total
        cvd_last = cvd_series[-1] if cvd_series else 0.0
        cvd_slope = 0.0 if not cvd_series else cvd_last / len(cvd_series)
        price_change_pct = 0.0
        if len(prices) >= 2 and prices[0] != 0:
            price_change_pct = ((prices[-1] - prices[0]) / prices[0]) * 100
        path = sum(abs(value) for value in signed_notionals)
        efficiency = 0.0 if path == 0 else abs(delta_notional) / path
        divergence = None
        if price_change_pct > 0.12 and recent_delta_ratio < -0.08:
            divergence = "bearish"
        elif price_change_pct < -0.12 and recent_delta_ratio > 0.08:
            divergence = "bullish"
        bias = "neutral"
        if delta_ratio >= 0.08 or recent_delta_ratio >= 0.12:
            bias = "bullish"
        elif delta_ratio <= -0.08 or recent_delta_ratio <= -0.12:
            bias = "bearish"

        return {
            "bars": len(cvd_series),
            "buyQty": round(buy_qty, 6),
            "sellQty": round(sell_qty, 6),
            "buyNotional": round(buy_notional, 2),
            "sellNotional": round(sell_notional, 2),
            "deltaQty": round(delta_qty, 6),
            "deltaNotional": round(delta_notional, 2),
            "deltaRatio": round(delta_ratio, 4),
            "recentDeltaNotional": round(recent_delta, 2),
            "recentDeltaRatio": round(recent_delta_ratio, 4),
            "cvdLast": round(cvd_last, 2),
            "cvdSlopePerTrade": round(cvd_slope, 4),
            "priceChangePct": round(price_change_pct, 4),
            "divergence": divergence,
            "bias": bias,
            "efficiency": round(efficiency, 4),
            "sampleSize": len(cvd_series),
            "window": {"recentTrades": recent_n, "totalTrades": len(cvd_series)},
        }

    def binance_symbol_snapshot(self, symbol: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "ticker24h": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["ticker_24h"], {"symbol": symbol}),
            "bookTicker": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["book_ticker"], {"symbol": symbol}),
            "premiumIndex": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["premium_index"], {"symbol": symbol}),
            "openInterest": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["open_interest"], {"symbol": symbol}),
            "openInterestHist": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["open_interest_hist"], {"symbol": symbol, "period": "1h", "limit": 8}),
            "depth": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["depth"], {"symbol": symbol, "limit": 20}),
            "aggTrades": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["agg_trades"], {"symbol": symbol, "limit": AGG_TRADES_LIMIT}),
            "fundingRate": self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["funding_rate"], {"symbol": symbol, "limit": 8}),
        }
        for name, endpoint in [
            ("globalLongShortAccountRatio", "global_long_short"),
            ("topLongShortAccountRatio", "top_long_short_accounts"),
            ("topLongShortPositionRatio", "top_long_short_positions"),
            ("takerLongShortRatio", "taker_long_short"),
        ]:
            try:
                data[name] = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"][endpoint], {"symbol": symbol, "period": "1h", "limit": 8})
            except Exception as exc:
                data[name] = {"error": str(exc)}
        data["cvd"] = self.binance_cvd_summary(data.get("aggTrades", []))
        data["klines"] = {interval: self.binance_klines(symbol, interval) for interval in DEFAULT_INTERVALS}
        data["wsPlan"] = self.binance_ws_plan(symbol)
        return data

    def binance_universe(self, min_quote_volume: float = 30_000_000, top: int = 40) -> List[Dict[str, Any]]:
        exchange_info = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["exchange_info"])
        tradable = {
            s["symbol"]
            for s in exchange_info.get("symbols", [])
            if s.get("status") == "TRADING" and s.get("contractType") == "PERPETUAL" and s.get("symbol", "").endswith("USDT")
        }
        raw = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["ticker_24h"])
        out = []
        for row in raw:
            symbol = row.get("symbol", "")
            if symbol not in tradable or symbol in {"BTCUSDT", "ETHUSDT", "BNBUSDT", "XAUUSDT", "XAGUSDT", "PAXGUSDT"}:
                continue
            if any(ord(ch) > 127 for ch in symbol):
                continue
            try:
                quote_volume = float(row.get("quoteVolume", 0))
                count = int(row.get("count", 0))
                change = float(row.get("priceChangePercent", 0))
                last = float(row.get("lastPrice", 0))
            except Exception:
                continue
            if quote_volume < min_quote_volume or count < 80_000:
                continue
            out.append({
                "symbol": symbol,
                "quoteVolume": quote_volume,
                "priceChangePercent": change,
                "lastPrice": last,
                "count": count,
            })
        out.sort(key=lambda row: row["quoteVolume"], reverse=True)
        return out[:top]

    def bybit_symbol_snapshot(self, symbol: str) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "ticker": self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["tickers"], {"category": "linear", "symbol": symbol}),
            "orderbook": self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["orderbook"], {"category": "linear", "symbol": symbol, "limit": 50}),
            "recentTrade": self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["recent_trade"], {"category": "linear", "symbol": symbol, "limit": 20}),
            "fundingHistory": self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["funding_history"], {"category": "linear", "symbol": symbol, "limit": 8}),
            "openInterest": self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["open_interest"], {"category": "linear", "symbol": symbol, "intervalTime": "1h", "limit": 8}),
        }
        data["klines"] = {
            interval: self.get(BYBIT_REST, ENDPOINT_MAP["bybit_rest"]["kline"], {"category": "linear", "symbol": symbol, "interval": BYBIT_INTERVAL_MAP[interval], "limit": 120})
            for interval in DEFAULT_INTERVALS
        }
        data["wsPlan"] = self.bybit_ws_plan(symbol)
        return data

    def coingecko_markets(self, ids: List[str]) -> Any:
        return self.get(COINGECKO_REST, ENDPOINT_MAP["coingecko_rest"]["markets"], {
            "vs_currency": "usd",
            "ids": ",".join(ids),
            "price_change_percentage": "24h,7d",
            "sparkline": "false",
        })

    def news_search(self, query: str, limit: int = 5) -> List[Dict[str, Any]]:
        url = ENDPOINT_MAP["public_search"]["google_news_rss"].format(query=urllib.parse.quote(query))
        xml_text = self.session.get(url, timeout=TIMEOUT).text
        root = ET.fromstring(xml_text)
        out = []
        for item in root.findall(".//item")[:limit]:
            out.append({
                "title": item.findtext("title"),
                "pubDate": item.findtext("pubDate"),
                "link": item.findtext("link"),
            })
        return out

    def symbol_query(self, symbol: str) -> str:
        return SYMBOL_TO_QUERY.get(symbol, symbol.replace("USDT", ""))

    def exchange_announcements(self, symbol: Optional[str] = None, limit: int = 5) -> Dict[str, Any]:
        coin_query = self.symbol_query(symbol) if symbol else "crypto"
        return {
            "binance": self.news_search(f"site:binance.com/en/support/announcement Binance Futures {coin_query}", limit=limit),
            "bybit": self.news_search(f"site:announcements.bybit.com futures {coin_query}", limit=limit),
        }

    def token_unlock_context(self, symbol: str, limit: int = 5) -> Dict[str, Any]:
        query = self.symbol_query(symbol)
        return {
            "query": query,
            "results": self.news_search(f"{query} token unlock vesting supply unlock", limit=limit),
        }

    def macro_context(self, limit: int = 8) -> Dict[str, Any]:
        headlines = self.news_search("CPI PPI FOMC Fed jobless claims crypto market", limit=limit)
        official = {
            "bls_cpi": ENDPOINT_MAP["public_search"]["bls_cpi"],
            "bls_ppi": ENDPOINT_MAP["public_search"]["bls_ppi"],
            "federal_reserve_calendar": ENDPOINT_MAP["public_search"]["federal_reserve_calendar"],
            "tradingeconomics_us_calendar": ENDPOINT_MAP["public_search"]["tradingeconomics_us_calendar"],
        }
        return {"official": official, "headlines": headlines}

    def headline_flags(self, items: List[Dict[str, Any]]) -> Dict[str, Any]:
        titles = [str(item.get("title", "")) for item in items]
        joined = " \n".join(titles).lower()
        negative = sorted({kw for kw in NEGATIVE_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", joined)})
        positive = sorted({kw for kw in POSITIVE_KEYWORDS if re.search(rf"\b{re.escape(kw)}\b", joined)})
        return {
            "negativeKeywords": negative,
            "positiveKeywords": positive,
            "negativeCount": len(negative),
            "positiveCount": len(positive),
        }

    def macro_schedule(self) -> Dict[str, str]:
        return {
            "bls_cpi": ENDPOINT_MAP["public_search"]["bls_cpi"],
            "bls_ppi": ENDPOINT_MAP["public_search"]["bls_ppi"],
            "federal_reserve_calendar": ENDPOINT_MAP["public_search"]["federal_reserve_calendar"],
            "tradingeconomics_us_calendar": ENDPOINT_MAP["public_search"]["tradingeconomics_us_calendar"],
        }

    def binance_ws_plan(self, symbol: str) -> Dict[str, Any]:
        lower = symbol.lower()
        return {
            "base": BINANCE_WS,
            "streams": {
                "kline_5m": f"{lower}@kline_5m",
                "kline_15m": f"{lower}@kline_15m",
                "kline_1h": f"{lower}@kline_1h",
                "kline_4h": f"{lower}@kline_4h",
                "markPrice": f"{lower}@markPrice",
                "depth": f"{lower}@depth20@100ms",
                "aggTrade": f"{lower}@aggTrade",
                "forceOrder": f"{lower}@forceOrder",
            },
        }

    def bybit_ws_plan(self, symbol: str) -> Dict[str, Any]:
        return {
            "base": BYBIT_WS,
            "topics": [
                f"tickers.{symbol}",
                f"kline.5.{symbol}",
                f"kline.15.{symbol}",
                f"kline.60.{symbol}",
                f"kline.240.{symbol}",
                f"orderbook.50.{symbol}",
                f"publicTrade.{symbol}",
            ],
        }

    def snapshot(self, symbol: str, include_auth_providers: bool = True, include_tradingview: bool = False) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "symbol": symbol,
            "timestamp": int(time.time()),
            "binance": self.binance_symbol_snapshot(symbol),
            "bybit": self.bybit_symbol_snapshot(symbol),
            "macroSchedule": self.macro_schedule(),
        }
        cg_id = SYMBOL_TO_COINGECKO.get(symbol)
        if cg_id:
            try:
                result["coingecko"] = self.coingecko_markets([cg_id])
            except Exception as exc:
                result["coingecko"] = {"error": str(exc)}
        else:
            result["coingecko"] = {"warning": "No CoinGecko mapping configured for symbol"}
        try:
            coin_query = self.symbol_query(symbol) + " crypto"
            result["news"] = self.news_search(coin_query)
        except Exception as exc:
            result["news"] = {"error": str(exc)}
        try:
            result["eventLayer"] = {
                "tokenUnlock": self.token_unlock_context(symbol),
                "exchangeAnnouncements": self.exchange_announcements(symbol),
                "macro": self.macro_context(),
            }
            event_items = []
            if isinstance(result.get("news"), list):
                event_items.extend(result["news"])
            event_items.extend(result["eventLayer"]["tokenUnlock"].get("results", []))
            event_items.extend(result["eventLayer"]["exchangeAnnouncements"].get("binance", []))
            event_items.extend(result["eventLayer"]["exchangeAnnouncements"].get("bybit", []))
            event_items.extend(result["eventLayer"]["macro"].get("headlines", []))
            result["headlineFlags"] = self.headline_flags(event_items)
        except Exception as exc:
            result["eventLayer"] = {"error": str(exc)}
        try:
            result["providerStatus"] = self.provider_status()
        except Exception as exc:
            result["providerStatus"] = {"error": str(exc)}
        try:
            result["tradingviewStatus"] = self.tradingview_status()
        except Exception as exc:
            result["tradingviewStatus"] = {"error": str(exc)}
        try:
            result["executionFeedback"] = self.execution_feedback_status(symbol)
        except Exception as exc:
            result["executionFeedback"] = {"error": str(exc)}
        try:
            result["liquidationStatus"] = self.liquidation_state_status(symbol, reference_price=self.symbol_reference_price(result))
        except Exception as exc:
            result["liquidationStatus"] = {"error": str(exc)}
        if include_auth_providers:
            try:
                enrichment = self.provider_enrichment(symbol)
                result["authProviderLayer"] = {
                    "enrichment": enrichment,
                    "overlay": self.provider_overlay(symbol, 0, enrichment),
                }
            except Exception as exc:
                result["authProviderLayer"] = {"error": str(exc)}
        if include_tradingview:
            try:
                preview = self._score_symbol(symbol, include_auth_providers=False)
                result["tradingviewLayer"] = self.tradingview_confirmation(symbol, bull_score=int(preview.get("bullScore", 0)))
            except Exception as exc:
                result["tradingviewLayer"] = {"error": str(exc)}
        return result

    def tradingview_status(self) -> Dict[str, Any]:
        if self.tradingview is None:
            return {
                "available": False,
                "importError": "tradingview adapter module is not loaded",
            }
        return self.tradingview.status()

    def tradingview_confirmation(self, symbol: str, bull_score: int = 0, exchange: str = "binance", timeframe: str = "15m") -> Dict[str, Any]:
        if self.tradingview is None:
            return {
                "symbol": symbol,
                "cleanScoreDelta": 0.0,
                "notes": ["TradingView adapter module is not available"],
                "confirmation": None,
            }
        status = self.tradingview.status()
        if not status.get("available"):
            return {
                "symbol": symbol,
                "cleanScoreDelta": 0.0,
                "notes": [f"TradingView adapter unavailable: {status.get('importError') or 'unknown error'}"],
                "confirmation": None,
            }
        try:
            return self.tradingview.overlay(symbol, bull_score=bull_score, exchange=exchange, timeframe=timeframe)
        except Exception as exc:
            return {
                "symbol": symbol,
                "cleanScoreDelta": 0.0,
                "notes": [f"TradingView confirmation failed: {exc}"],
                "confirmation": None,
            }

    def apply_tradingview_enrichment_to_row(self, row: Dict[str, Any], confirmation: Dict[str, Any]) -> Dict[str, Any]:
        row["tradingviewOverlay"] = confirmation
        row["cleanScore"] = round(max(0.0, min(100.0, float(row.get("cleanScore", 0.0)) + float(confirmation.get("cleanScoreDelta", 0.0)))), 2)
        self.apply_grade_to_row(row)
        return row

    def load_execution_feedback_state(self) -> Dict[str, Any]:
        return load_json_file(EXECUTION_FEEDBACK_STATE_PATH, {"updatedAt": None, "symbols": {}, "recent": []})

    def save_execution_feedback_state(self, state: Dict[str, Any]) -> None:
        state["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save_json_file(EXECUTION_FEEDBACK_STATE_PATH, state)

    def execution_feedback_status(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        state = self.load_execution_feedback_state()
        if symbol:
            return {
                "path": str(EXECUTION_FEEDBACK_STATE_PATH),
                "updatedAt": state.get("updatedAt"),
                "symbol": symbol.upper(),
                "feedback": (state.get("symbols") or {}).get(symbol.upper(), {}),
            }
        return {
            "path": str(EXECUTION_FEEDBACK_STATE_PATH),
            "updatedAt": state.get("updatedAt"),
            "symbolsTracked": len((state.get("symbols") or {}).keys()),
            "recentCount": len(state.get("recent") or []),
            "symbols": state.get("symbols") or {},
        }

    def record_execution_feedback(self, symbol: str, side: str, outcome: str, entry_quality: str = "ok", notes: Optional[str] = None) -> Dict[str, Any]:
        symbol = symbol.upper()
        side = side.lower().strip()
        outcome = outcome.lower().strip()
        entry_quality = entry_quality.lower().strip()
        if side not in {"long", "short"}:
            raise RuntimeError("side must be long or short")
        if outcome not in {"stop_loss", "take_profit", "runner", "no_fill", "break_even", "invalidated"}:
            raise RuntimeError("outcome must be one of: stop_loss, take_profit, runner, no_fill, break_even, invalidated")
        if entry_quality not in {"clean", "ok", "poor"}:
            raise RuntimeError("entry-quality must be one of: clean, ok, poor")

        state = self.load_execution_feedback_state()
        symbols = state.setdefault("symbols", {})
        recent = state.setdefault("recent", [])
        symbol_state = symbols.setdefault(symbol, {
            "total": 0,
            "outcomes": {},
            "sides": {},
            "entryQuality": {},
            "recent": [],
        })
        side_state = symbol_state.setdefault("sides", {}).setdefault(side, {"total": 0, "outcomes": {}})
        event = {
            "symbol": symbol,
            "side": side,
            "outcome": outcome,
            "entryQuality": entry_quality,
            "notes": notes or "",
            "loggedAt": datetime.now(timezone.utc).isoformat(),
        }
        symbol_state["total"] = int(symbol_state.get("total", 0)) + 1
        symbol_state.setdefault("outcomes", {})[outcome] = int(symbol_state.get("outcomes", {}).get(outcome, 0)) + 1
        symbol_state.setdefault("entryQuality", {})[entry_quality] = int(symbol_state.get("entryQuality", {}).get(entry_quality, 0)) + 1
        side_state["total"] = int(side_state.get("total", 0)) + 1
        side_state.setdefault("outcomes", {})[outcome] = int(side_state.get("outcomes", {}).get(outcome, 0)) + 1
        symbol_state.setdefault("recent", []).append(event)
        symbol_state["recent"] = symbol_state["recent"][-12:]
        recent.append(event)
        state["recent"] = recent[-50:]
        self.save_execution_feedback_state(state)
        return {
            "saved": True,
            "path": str(EXECUTION_FEEDBACK_STATE_PATH),
            "event": event,
            "summary": self.execution_feedback_status(symbol),
        }

    # ─── Phase 7: Feedback Engine v2 ───────────────────────────────────────

    def load_feedback_engine_state(self) -> Dict[str, Any]:
        return load_json_file(FEEDBACK_ENGINE_STATE_PATH, {
            "updatedAt": None,
            "symbolSides": {},
            "learnings": [],
            "recent": [],
        })

    def save_feedback_engine_state(self, state: Dict[str, Any]) -> None:
        state["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save_json_file(FEEDBACK_ENGINE_STATE_PATH, state)

    def _feedback_engine_key(self, symbol: str, side: str) -> str:
        return f"{symbol.upper()}:{side.lower()}"

    def _compute_outcome_entropy(self, outcomes: Dict[str, int]) -> float:
        total = sum(outcomes.values())
        if total == 0:
            return 0.0
        entropy = 0.0
        import math
        for count in outcomes.values():
            if count <= 0:
                continue
            p = count / total
            entropy -= p * math.log2(max(p, 1e-9))
        max_entropy = math.log2(max(len(outcomes), 1))
        return entropy / max_entropy if max_entropy > 0 else 0.0

    def _compute_learning_score(self, side_state: Dict[str, Any]) -> Dict[str, Any]:
        total = int(side_state.get("total", 0) or 0)
        outcomes = side_state.get("outcomes", {}) if isinstance(side_state.get("outcomes"), dict) else {}
        tp = int(outcomes.get("take_profit", 0) or 0)
        sl = int(outcomes.get("stop_loss", 0) or 0)
        runner = int(outcomes.get("runner", 0) or 0)
        no_fill = int(outcomes.get("no_fill", 0) or 0)
        be = int(outcomes.get("break_even", 0) or 0)
        inv = int(outcomes.get("invalidated", 0) or 0)
        weighted = (
            tp * FEEDBACK_OUTCOME_MAP["take_profit"] +
            sl * FEEDBACK_OUTCOME_MAP["stop_loss"] +
            runner * FEEDBACK_OUTCOME_MAP["runner"] +
            no_fill * FEEDBACK_OUTCOME_MAP["no_fill"] +
            be * FEEDBACK_OUTCOME_MAP["break_even"] +
            inv * FEEDBACK_OUTCOME_MAP["invalidated"]
        )
        learning_score = weighted / max(total, 1)
        tp_likelihood = tp / max(tp + sl + runner + be + inv, 1)
        sl_likelihood = sl / max(tp + sl + runner + be + inv, 1)
        entropy = self._compute_outcome_entropy(outcomes)
        high_entropy = entropy >= ENTROPY_THRESHOLD
        return {
            "total": total,
            "weightedScore": round(weighted, 3),
            "learningScore": round(learning_score, 3),
            "tpLikelihood": round(tp_likelihood, 3),
            "slLikelihood": round(sl_likelihood, 3),
            "outcomeCounts": {
                "take_profit": tp, "stop_loss": sl, "runner": runner,
                "no_fill": no_fill, "break_even": be, "invalidated": inv,
            },
            "entropy": round(entropy, 3),
            "highEntropy": high_entropy,
        }

    def _feedback_confidence_tier(self, score_info: Dict[str, Any]) -> str:
        total = int(score_info.get("total", 0) or 0)
        entropy = float(score_info.get("entropy", 0.0) or 0.0)
        learning_score = abs(float(score_info.get("learningScore", 0.0) or 0.0))
        if total <= 1:
            return "weak"
        if total == 2:
            return "provisional"
        if total >= 5 and entropy <= 0.45 and learning_score >= 0.25:
            return "trusted"
        if total >= 3 and entropy <= 0.75 and learning_score >= 0.1:
            return "developing"
        return "provisional"

    def _repeatability_gate(self, score_info: Dict[str, Any], confidence_tier: str) -> Dict[str, Any]:
        total = int(score_info.get("total", 0) or 0)
        learning_score = float(score_info.get("learningScore", 0.0) or 0.0)
        entropy = float(score_info.get("entropy", 0.0) or 0.0)
        high_entropy = bool(score_info.get("highEntropy", False))
        tp_likelihood = float(score_info.get("tpLikelihood", 0.0) or 0.0)
        sl_likelihood = float(score_info.get("slLikelihood", 0.0) or 0.0)
        reasons: List[str] = []

        allow_positive = False
        if learning_score > 0:
            if total < REPEATABILITY_MIN_TOTAL:
                reasons.append("Positive history exists, but sample size is still too small")
            if high_entropy:
                reasons.append("Positive history is still noisy, so repeatability is not proven yet")
            if tp_likelihood < 0.5:
                reasons.append("Positive outcomes are not dominant enough yet")
            if not reasons and confidence_tier in {"developing", "trusted"}:
                allow_positive = True

        allow_negative = learning_score < 0 or sl_likelihood > 0 or (total >= 1 and learning_score <= 0)

        if learning_score > 0 and allow_positive:
            status = "passed"
        elif learning_score > 0:
            status = "blocked"
        elif learning_score < 0:
            status = "negative_active"
        else:
            status = "neutral"

        return {
            "status": status,
            "allowPositivePromotion": allow_positive,
            "allowNegativePenalty": allow_negative,
            "reasons": reasons,
            "minTotal": REPEATABILITY_MIN_TOTAL,
            "confidenceTier": confidence_tier,
            "entropy": round(entropy, 3),
        }

    def _build_symbol_side_learner(self, symbol: str, side: str,
                                   feedback_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        symbol = symbol.upper()
        side = side.lower().strip()
        key = self._feedback_engine_key(symbol, side)
        if feedback_state is None:
            all_fb = self.load_execution_feedback_state()
            symbol_fb = ((all_fb.get("symbols") or {}).get(symbol) or {})
        else:
            all_fb = feedback_state
            symbol_fb = ((all_fb.get("symbols") or {}).get(symbol) or {})
        side_state = ((symbol_fb.get("sides") or {}).get(side) or {"total": 0, "outcomes": {}})
        score_info = self._compute_learning_score(side_state)
        confidence_tier = self._feedback_confidence_tier(score_info)
        repeatability_gate = self._repeatability_gate(score_info, confidence_tier)
        return {
            "key": key,
            "symbol": symbol,
            "side": side,
            "score": score_info,
            "confidenceTier": confidence_tier,
            "repeatabilityGate": repeatability_gate,
        }

    def _infer_tp_sl_guidance(self, learning_score: float, tp_likelihood: float,
                               sl_likelihood: float, high_entropy: bool,
                               total: int, confidence_tier: str = "weak",
                               repeatability_gate: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        tp_zone = "conservative"
        size_bias = "neutral"
        skip = False
        reasons: List[str] = []
        repeatability_gate = repeatability_gate or {}
        if total >= 3 and learning_score <= -0.6:
            skip = True
            reasons.append("Historical learning says this symbol/side loses money too often")
        elif total >= 3 and learning_score <= -0.2:
            size_bias = "reduce_25pct"
            reasons.append("Historical edge is slightly negative, suggest reduce position size")
        elif total >= 2 and learning_score < 0 and sl_likelihood >= 0.5:
            size_bias = "reduce_25pct"
            tp_zone = "tight"
            reasons.append("Mixed profile with meaningful SL history, so reduce size and treat the setup as fragile")
        elif total == 1 and learning_score <= -0.9:
            size_bias = "reduce_25pct"
            tp_zone = "tight"
            reasons.append("Single confirmed stop-loss is enough to demand caution on the next similar setup")
        elif learning_score > 0 and not repeatability_gate.get("allowPositivePromotion", False):
            tp_zone = "conservative"
            reasons.append(f"Positive history exists, but confidence tier is still {confidence_tier} and repeatability is not proven yet")
        elif total >= 2 and sl_likelihood > 0.6:
            tp_zone = "tight"
            reasons.append("High SL likelihood, use tighter stop and take partial faster")
        elif total >= 2 and tp_likelihood > 0.5 and repeatability_gate.get("allowPositivePromotion", False):
            tp_zone = "full_target"
            reasons.append("Good TP likelihood, allow more room for trade to develop")
        elif total >= 2 and tp_likelihood > 0.5:
            tp_zone = "neutral"
            reasons.append("TP history exists, but repeatability gate is not open yet, so avoid over-trusting the pattern")
        elif high_entropy:
            tp_zone = "neutral"
            reasons.append("Outcome entropy is high: mixed history, stay neutral on management")
        else:
            reasons.append("Insufficient history: follow default management rules")
        return {
            "tpZone": tp_zone,
            "sizeBias": size_bias,
            "skip": skip,
            "adaptiveReasons": reasons,
        }

    def _learn_from_state_transitions(self) -> Dict[str, Any]:
        watchlist = self.load_watchlist_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        transitions = watchlist.get("recent", []) if isinstance(watchlist, dict) else []
        fb_state = self.load_execution_feedback_state()
        fb_symbols = fb_state.get("symbols", {}) if isinstance(fb_state, dict) else {}

        engine = self.load_feedback_engine_state()
        ingested = 0
        new_learnings: List[Dict[str, Any]] = []

        for t in reversed(transitions[-100:]):
            if not isinstance(t, dict):
                continue
            sym = str(t.get("symbol") or "").upper()
            side = str(t.get("side") or "").lower()
            to_state = str(t.get("to") or "").lower()
            from_state = str(t.get("from") or "").lower()
            if not sym or side not in {"long", "short"}:
                continue
            if to_state in {"invalidated", "stale"}:
                fb_sym = fb_symbols.get(sym, {})
                side_fb = (fb_sym.get("sides", {}).get(side, {}) if isinstance(fb_sym, dict) else {})
                side_total = int(side_fb.get("total", 0) or 0)
                sym_total = int(fb_sym.get("total", 0) or 0)
                already = any(
                    lr.get("symbol") == sym and lr.get("side") == side
                    and lr.get("transitionState") == to_state
                    and lr.get("source") == "auto_state_transition"
                    for lr in (engine.get("learnings") or [])[-50:]
                )
                if already or (side_total + sym_total) == 0:
                    continue
                outcome = "invalidated" if to_state == "invalidated" else "no_fill"
                new_learnings.append({
                    "symbol": sym, "side": side,
                    "outcome": outcome, "entryQuality": "ok",
                    "notes": f"Auto-learned from state machine transition {from_state}->{to_state}",
                    "transitionState": to_state, "source": "auto_state_transition",
                    "loggedAt": t.get("loggedAt") or datetime.now(timezone.utc).isoformat(),
                })

        for lr in new_learnings:
            sym = lr["symbol"]
            side = lr["side"]
            fb_state = self.load_execution_feedback_state()
            symbols = fb_state.setdefault("symbols", {})
            recent = fb_state.setdefault("recent", [])
            symbol_state = symbols.setdefault(sym, {
                "total": 0, "outcomes": {}, "sides": {}, "entryQuality": {}, "recent": [],
            })
            side_state = symbol_state.setdefault("sides", {}).setdefault(side, {"total": 0, "outcomes": {}})
            symbol_state["total"] = int(symbol_state.get("total", 0)) + 1
            symbol_state.setdefault("outcomes", {})[lr["outcome"]] = int(symbol_state.get("outcomes", {}).get(lr["outcome"], 0)) + 1
            symbol_state.setdefault("entryQuality", {})[lr.get("entryQuality", "ok")] = int(symbol_state.get("entryQuality", {}).get(lr.get("entryQuality", "ok"), 0)) + 1
            side_state["total"] = int(side_state.get("total", 0)) + 1
            side_state.setdefault("outcomes", {})[lr["outcome"]] = int(side_state.get("outcomes", {}).get(lr["outcome"], 0)) + 1
            symbol_state.setdefault("recent", []).append(lr)
            symbol_state["recent"] = symbol_state["recent"][-12:]
            recent.append(lr)
            fb_state["recent"] = recent[-50:]
            self.save_execution_feedback_state(fb_state)
            ingested += 1

        engine["learnings"] = (engine.get("learnings") or []) + new_learnings
        engine["learnings"] = engine["learnings"][-FEEDBACK_V2_MAX_RECENT:]
        engine["recent"] = (engine.get("recent") or []) + [f"ingested {ingested} transitions"][-10:]
        self.save_feedback_engine_state(engine)
        return {
            "ingested": ingested,
            "newLearnings": len(new_learnings),
            "totalLearnings": len(engine.get("learnings", [])),
            "enginePath": str(FEEDBACK_ENGINE_STATE_PATH),
            "learnings": new_learnings,
        }

    def feedback_engine_v2_status(self, symbol: Optional[str] = None,
                                  side: Optional[str] = None) -> Dict[str, Any]:
        engine = self.load_feedback_engine_state()
        fb = self.load_execution_feedback_state()
        fb_symbols = fb.get("symbols", {})

        results: List[Dict[str, Any]] = []
        if symbol and side:
            learner = self._build_symbol_side_learner(symbol, side, feedback_state=fb)
            guidance = self._infer_tp_sl_guidance(
                learner["score"]["learningScore"],
                learner["score"]["tpLikelihood"],
                learner["score"]["slLikelihood"],
                learner["score"]["highEntropy"],
                learner["score"]["total"],
                learner.get("confidenceTier", "weak"),
                learner.get("repeatabilityGate"),
            )
            learner["guidance"] = guidance
            results.append(learner)
        else:
            for sym, sym_fb in fb_symbols.items():
                if isinstance(sym_fb, dict):
                    for s in ["long", "short"]:
                        learner = self._build_symbol_side_learner(sym, s, feedback_state=fb)
                        if learner["score"]["total"] > 0:
                            guidance = self._infer_tp_sl_guidance(
                                learner["score"]["learningScore"],
                                learner["score"]["tpLikelihood"],
                                learner["score"]["slLikelihood"],
                                learner["score"]["highEntropy"],
                                learner["score"]["total"],
                                learner.get("confidenceTier", "weak"),
                                learner.get("repeatabilityGate"),
                            )
                            learner["guidance"] = guidance
                            results.append(learner)

        return {
            "enginePath": str(FEEDBACK_ENGINE_STATE_PATH),
            "feedbackPath": str(EXECUTION_FEEDBACK_STATE_PATH),
            "engineUpdatedAt": engine.get("updatedAt"),
            "totalLearnings": len(engine.get("learnings", [])),
            "symbolSideLearners": results,
        }

    def feedback_engine_v2_ingest(self) -> Dict[str, Any]:
        return self._learn_from_state_transitions()

    def _apply_feedback_v2_to_row(self, row: Dict[str, Any], prod_mode: bool = False) -> Dict[str, Any]:
        symbol = str(row.get("symbol") or "").upper()
        direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
        if direction not in {"long", "short"}:
            return row

        learner = self._build_symbol_side_learner(symbol, direction)
        score_info = learner["score"]
        confidence_tier = learner.get("confidenceTier", "weak")
        repeatability_gate = learner.get("repeatabilityGate") or {}
        total = score_info["total"]
        # In prod_mode: freeze feedback influence until minimum sample threshold is met.
        # This prevents a tiny handful of executions from altering production conviction.
        if prod_mode and total < FEEDBACK_V2_PROD_MIN_SAMPLES:
            row["feedbackV2"] = {
                "frozen": True,
                "reason": "prod_mode_min_samples_not_met",
                "total": total,
                "minimumRequired": FEEDBACK_V2_PROD_MIN_SAMPLES,
                "confidenceTier": confidence_tier,
            }
            return row
        if total < 1:
            return row

        learning_score = score_info["learningScore"]
        guidance = self._infer_tp_sl_guidance(
            learning_score, score_info["tpLikelihood"],
            score_info["slLikelihood"], score_info["highEntropy"], total,
            confidence_tier, repeatability_gate,
        )

        if guidance["skip"]:
            row["cleanScore"] = max(0.0, float(row.get("cleanScore", 0.0)) - 12.0)
            row.setdefault("feedbackV2Notes", []).extend(guidance["adaptiveReasons"])
            self.apply_grade_to_row(row)
            return row

        delta = 0.0
        if total == 1 and learning_score <= -0.9:
            delta -= 3.0
        if guidance["sizeBias"] == "reduce_25pct":
            delta -= 4.0
        elif guidance["tpZone"] == "tight":
            delta -= 2.0
        elif guidance["tpZone"] == "full_target" and learning_score > 0.3 and repeatability_gate.get("allowPositivePromotion", False):
            delta += 2.0

        if learning_score > 0 and not repeatability_gate.get("allowPositivePromotion", False):
            delta -= 0.75 if total <= 1 else 0.5

        if score_info["highEntropy"]:
            delta -= 0.5
        if total >= 2 and learning_score < 0 and score_info["slLikelihood"] >= 0.5:
            delta -= 1.5
        if total >= 2 and learning_score <= -0.1 and score_info["highEntropy"]:
            delta -= 1.0

        row["cleanScore"] = round(max(0.0, min(100.0, float(row.get("cleanScore", 0.0)) + delta)), 2)
        row["feedbackV2"] = {
            "learningScore": learning_score,
            "tpLikelihood": score_info["tpLikelihood"],
            "slLikelihood": score_info["slLikelihood"],
            "entropy": score_info["entropy"],
            "total": total,
            "confidenceTier": confidence_tier,
            "repeatabilityGate": repeatability_gate,
            "tpZone": guidance["tpZone"],
            "sizeBias": guidance["sizeBias"],
            "cleanScoreDelta": round(delta, 2),
            "adaptiveReasons": guidance["adaptiveReasons"],
        }
        self.apply_grade_to_row(row)
        return row

    # ─── End Phase 7: Feedback Engine v2 ───────────────────────────────────

    def load_watchlist_state(self) -> Dict[str, Any]:
        raw_state = load_json_file(WATCHLIST_STATE_PATH, {"updatedAt": None, "lastScanAt": None, "scanCount": 0, "setups": {}, "recent": []})
        return self._normalize_watchlist_state_payload(raw_state)

    def save_watchlist_state(self, state: Dict[str, Any]) -> None:
        payload = dict(state)
        payload.pop("migrationDiagnostics", None)
        payload["updatedAt"] = datetime.now(timezone.utc).isoformat()
        state["updatedAt"] = payload["updatedAt"]
        save_json_file(WATCHLIST_STATE_PATH, payload)

    def load_alert_state(self) -> Dict[str, Any]:
        raw_state = load_json_file(ALERT_STATE_PATH, {"updatedAt": None, "lastSyncAt": None, "setups": {}, "recent": []})
        return self._normalize_alert_state_payload(raw_state)

    def save_alert_state(self, state: Dict[str, Any]) -> None:
        payload = dict(state)
        payload.pop("migrationDiagnostics", None)
        payload["updatedAt"] = datetime.now(timezone.utc).isoformat()
        state["updatedAt"] = payload["updatedAt"]
        save_json_file(ALERT_STATE_PATH, payload)

    def load_automation_state(self) -> Dict[str, Any]:
        return load_json_file(AUTOMATION_STATE_PATH, {"updatedAt": None, "jobs": {}, "lastInstallAt": None})

    def save_automation_state(self, state: Dict[str, Any]) -> None:
        state["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save_json_file(AUTOMATION_STATE_PATH, state)

    def setup_state_key(self, symbol: str, side: str) -> str:
        return f"{symbol.upper()}:{side.lower()}"

    # ── Version-hash sensitivity constants (PATCH-1 & PATCH-2) ────────────────
    # Execution-critical risk flags: these represent actual fill/execution constraints.
    # Contextual flags (sentiment, crowding) are display-only and do NOT bump version.
    EXECUTION_CRITICAL_RISK_FLAGS = frozenset({
        "thin_depth", "no_fill_history", "liquidation_zone",
    })

    @staticmethod
    def _tick_for_price(price: float) -> float:
        """Return Binance min tick increment for a given price level."""
        p = abs(price)
        if p >= 10000:   return 0.01
        elif p >= 100:   return 0.001
        elif p >= 1:     return 0.0001
        elif p >= 0.01:  return 0.000001   # e.g. DOGE
        else:            return 0.00000001  # e.g. PEPE, SHIB (8-decimal)

    @staticmethod
    def _quantum(ref: float) -> float:
        """Noise-absorption quantum = max(0.20%% of |ref|, 1 Binance tick)."""
        return max(abs(ref) * 0.002, PublicMarketData._tick_for_price(abs(ref)))

    def _quantized_for_hash(self, ref: float, target: float) -> float:
        """Round target to nearest quantum grid anchored to ref.
        Absorbs sub-threshold drift; material changes cross grid and change hash."""
        q = self._quantum(ref)
        return round(target / q) * q

    def _drift_pct(self, before: float, after: float) -> float:
        """Return |drift| as fraction. Returns 0.0 if before is zero."""
        if before == 0:
            return 0.0
        return abs(after - before) / abs(before)

    def _field_tolerance(self, ref: float) -> float:
        """Noise-absorption tolerance for a field: max(0.20%%, 1-tick fraction of ref).
        For high-price assets (BTC): tick fraction << 0.20%% → tolerance = 0.20%%.
        For low-price assets (PEPE): tick fraction can be > 0.20%% → tolerance = 1-tick fraction.
        This guarantees 1-tick drift is always absorbed for all instruments."""
        tick = self._tick_for_price(ref)
        tick_pct = tick / abs(ref) if ref != 0 else 0.0
        return max(0.002, tick_pct)  # max(0.20%%, 1-tick %%)

    def _within_noise_tolerance(self, lb: float, hb: float, sb: float,
                                  la: float, ha: float, sa: float) -> bool:
        """True when ALL three fields have |drift| < max(0.20%%, 1-tick%%).
        Used to guarantee that sub-threshold drift always produces identical hash
        regardless of quantum grid boundary crossings for high-precision instruments."""
        tol_low  = self._field_tolerance(lb)
        tol_high = self._field_tolerance(hb)
        tol_sl   = self._field_tolerance(sb)
        d_low  = self._drift_pct(lb, la)
        d_high = self._drift_pct(hb, ha)
        d_sl   = self._drift_pct(sb, sa)
        return (d_low < tol_low and d_high < tol_high and d_sl < tol_sl)

    def normalize_risk_flag(self, value: Any) -> str:
        return str(value or "").strip().lower()

    def normalized_risk_flags(self, values: Any) -> List[str]:
        if not isinstance(values, list):
            return []
        normalized: List[str] = []
        for value in values:
            flag = self.normalize_risk_flag(value)
            if flag:
                normalized.append(flag)
        return normalized

    def setup_state_version(self, row: Dict[str, Any]) -> str:
        execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
        all_flags = sorted(self.normalized_risk_flags(execution.get("riskFlags", []) or []))[:4]
        # PATCH-2: only execution-critical flags bump version; contextual flags are display-only
        critical_flags = [f for f in all_flags if f in self.EXECUTION_CRITICAL_RISK_FLAGS]
        payload = {
            "symbol": row.get("symbol"),
            "direction": row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0)),
            "grade": row.get("grade"),
            # PATCH-2: removed alertEligible, watchlistEligible (too noisy, display-only)
            # PATCH-2: bucket bullScore to halve sensitivity (e.g. 74->37, 75->37)
            "bullScoreBucket": int(int(row.get("bullScore", 0) or 0) // 2),
            "cleanBucket": int(float(row.get("cleanScore", 0.0) or 0.0) // 2),
            "riskFlags": critical_flags,  # PATCH-2: execution-critical only
            "disqualified": bool(row.get("disqualified")),
        }
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha1(encoded).hexdigest()[:12]

    def canonical_setup_state(self, value: Any) -> Optional[str]:
        return _canonical_setup_state(value)

    def legacy_setup_state(self, value: Any) -> Optional[str]:
        state = self.canonical_setup_state(value)
        if not state:
            return None
        return CANONICAL_TO_LEGACY_SETUP_STATE_MAP.get(state, state)

    def canonical_readiness_state(self, value: Any) -> Optional[str]:
        return _canonical_readiness_state(value)

    def legacy_readiness_state(self, value: Any) -> Optional[str]:
        state = self.canonical_readiness_state(value)
        if not state:
            return None
        return CANONICAL_TO_LEGACY_READINESS_STATE_MAP.get(state, state)

    def canonical_runtime_state(self, value: Any) -> Optional[str]:
        return _canonical_runtime_state(value)

    def canonical_delivery_state(self, value: Any) -> Optional[str]:
        return _canonical_delivery_state(value)

    def legacy_runtime_state(self, value: Any) -> Optional[str]:
        state = self.canonical_runtime_state(value)
        if not state:
            return None
        return CANONICAL_TO_LEGACY_RUNTIME_STATE_MAP.get(state, state)

    def setup_lifecycle_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        return _setup_lifecycle_state(record)

    def setup_readiness_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        return _setup_readiness_state(record)

    def monitor_lifecycle_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        return _monitor_lifecycle_state(record)

    def monitor_readiness_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        return _monitor_readiness_state(record)

    def delivery_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        return _delivery_state(record)

    def runtime_state(self, record: Optional[Dict[str, Any]]) -> Optional[str]:
        if not isinstance(record, dict):
            return None
        runtime = record.get("runtime") if isinstance(record.get("runtime"), dict) else {}
        state = runtime.get("state")
        if state:
            return self.canonical_runtime_state(state)
        return None

    # ─── B1: Readiness Hysteresis ─────────────────────────────────────────────

    def apply_readiness_hysteresis(
        self,
        observed_state: Optional[str],
        previous_record: Dict[str, Any],
        lifecycle_state: str,
    ) -> str:
        """
        B1: Apply flip-flop protection to readiness state transitions.

        State model:
          observed_state = raw scanner output (actionable.currentStatus canonicalized)
          stable_state   = hysteresis-applied; this is what gets persisted

        Rules:
          1. HARD INVALIDATION (lifecycle=invalidated): bypass hysteresis, go to standby immediately
          2. observed == stable: reset hysteresis counters (nothing to flip)
          3. observed = retest_ready: IMMEDIATE promote, reset hysteresis
          4. observed = standby (any subtype) + stable = retest_ready:
               start/continue DEMOTION CONFIRMATION
               demotion commits after CONFIRM_DEMOTIONS consecutive same-observed scans
          5. observed = standby (different subtype) + stable = standby:
               switch subtype immediately (different strategies)
        """
        if observed_state is None:
            return None

        prev_readiness_raw = previous_record.get("readiness")
        if not isinstance(prev_readiness_raw, dict):
            prev_readiness_raw = {}
            previous_record["readiness"] = prev_readiness_raw
        prev_readiness = prev_readiness_raw

        stable_state = prev_readiness.get("state")
        hysteresis_confirmations = int(prev_readiness.get(HYSTERESIS_CONFIRMATIONS_FIELD, 0) or 0)
        hysteresis_target = prev_readiness.get(HYSTERESIS_TARGET_FIELD)

        # Rule 1: Hard invalidation — bypass hysteresis
        if lifecycle_state == "invalidated":
            prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = 0
            prev_readiness[HYSTERESIS_TARGET_FIELD] = None
            prev_readiness["state"] = "standby"
            return "standby"

        # Rule 2: Observed same as stable — nothing to flip, reset hysteresis
        if observed_state == stable_state:
            prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = 0
            prev_readiness[HYSTERESIS_TARGET_FIELD] = None
            return stable_state

        # Rule 3: Promotion to retest_ready — immediate, reset hysteresis
        if observed_state == "retest_ready":
            prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = 0
            prev_readiness[HYSTERESIS_TARGET_FIELD] = None
            prev_readiness["state"] = "retest_ready"
            return "retest_ready"

        # Rule 4 & 5: Observed is standby, stable is different
        if hysteresis_target == observed_state and stable_state == "retest_ready":
            # Continue demotion confirmation
            hysteresis_confirmations += 1
            prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = hysteresis_confirmations
            prev_readiness[HYSTERESIS_TARGET_FIELD] = observed_state
            if hysteresis_confirmations >= CONFIRM_DEMOTIONS:
                # Demotion confirmed — commit to new stable state
                prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = 0
                prev_readiness[HYSTERESIS_TARGET_FIELD] = None
                prev_readiness["state"] = observed_state
                return observed_state
            # Still confirming — hold stable at retest_ready
            return "retest_ready"
        else:
            # New observation / different standby subtype
            hysteresis_confirmations = 1
            prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = hysteresis_confirmations
            prev_readiness[HYSTERESIS_TARGET_FIELD] = observed_state
            if stable_state == "retest_ready":
                # Start demotion confirmation — hold stable at retest_ready
                return "retest_ready"
            else:
                # stable was different standby subtype — switch immediately
                prev_readiness[HYSTERESIS_CONFIRMATIONS_FIELD] = 0
                prev_readiness[HYSTERESIS_TARGET_FIELD] = None
                prev_readiness["state"] = observed_state
                return observed_state

    def set_actionable_readiness_alias(self, actionable: Optional[Dict[str, Any]], readiness_state: Optional[str]) -> Optional[Dict[str, Any]]:
        # P4: writing currentStatus into actionableAlert is no longer done.
        # Canonical readiness.state is the only persisted truth.
        # Output adapters that need currentStatus for display should add it
        # directly to their output row, not mutate the underlying object.
        return actionable

    def readiness_version_from_actionable(
        self,
        symbol: str,
        side: str,
        actionable: Optional[Dict[str, Any]],
        stable_readiness_state: Optional[str] = None,  # B1.1: hysteresis-applied stable state
        prev_quantized_zone: Optional[Dict[str, float]] = None,  # PATCH-1: use prev q-zone when within tolerance
        prev_quantized_sl: Optional[float] = None,  # PATCH-1: use prev q-sl when within tolerance
    ) -> Optional[str]:
        """
        B1.1: readinessVersion is now based on STABLE readiness state (hysteresis-applied),
        NOT the raw observed scanner state. This prevents duplicate alerts and false
        readiness_changed signals when the scanner flips between observed states but
        stable state has not actually changed.

        stable_readiness_state: hysteresis-applied state from apply_readiness_hysteresis().
                                If None, falls back to actionable.currentStatus (backward compat).
        """
        if not isinstance(actionable, dict):
            return None
        zone = self._normalized_zone(actionable.get("entryZone"))
        stop_loss_raw = self._safe_float(actionable.get("stopLoss"))
        # B1.1: prefer stable_readiness_state (hysteresis-applied) over raw observed
        readiness_state = stable_readiness_state or self.canonical_readiness_state(actionable.get("currentStatus"))
        if not symbol or side not in {"long", "short"} or not readiness_state or zone is None or stop_loss_raw is None:
            return None
        # PATCH-1: quantize entryZone.low, entryZone.high, stopLoss to noise-absorption grid.
        # Quantum = max(0.20%% * |ref|, 1 tick). Anchored to BEFORE reference value.
        zone_low_raw  = zone.get("low") or 0
        zone_high_raw = zone.get("high") or 0
        zone_low_q  = self._quantized_for_hash(zone_low_raw,  zone_low_raw)
        zone_high_q = self._quantized_for_hash(zone_high_raw, zone_high_raw)
        stop_loss_q = self._quantized_for_hash(stop_loss_raw, stop_loss_raw)
        # PATCH-1: if previous quantized zone/sl provided and drift is within tolerance,
        # use previous quantized values to guarantee identical hash (noise absorbed).
        if (prev_quantized_zone is not None and prev_quantized_sl is not None and
                self._within_noise_tolerance(
                    prev_quantized_zone.get("low") or 0,
                    prev_quantized_zone.get("high") or 0,
                    prev_quantized_sl,
                    zone_low_raw, zone_high_raw, stop_loss_raw)):
            zone_low_q  = prev_quantized_zone.get("low") or 0
            zone_high_q = prev_quantized_zone.get("high") or 0
            stop_loss_q = prev_quantized_sl
        payload = {
            "symbol": str(symbol).upper(),
            "side": str(side).lower(),
            "readinessState": readiness_state,  # B1.1: stable state, not observed
            "entryZone": {
                "low": zone_low_q,
                "high": zone_high_q,
            },
            "stopLoss": stop_loss_q,
        }
        encoded = json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")
        return hashlib.sha1(encoded).hexdigest()[:12]

    def transition_log_entry(
        self,
        *,
        setup_key: str,
        layer: str,
        from_state: Optional[str],
        to_state: Optional[str],
        reason_code: str,
        event_type: str,
        changed_at: Optional[str],
        actor: str,
        setup_version: Optional[str] = None,
        readiness_version: Optional[str] = None,
        payload: Optional[Dict[str, Any]] = None,
        symbol: Optional[str] = None,
        side: Optional[str] = None,
    ) -> Dict[str, Any]:
        compact_payload = payload if isinstance(payload, dict) else {}
        return {
            "setupKey": setup_key,
            "symbol": symbol,
            "side": side,
            "layer": layer,
            "fromState": from_state,
            "toState": to_state,
            "reasonCode": reason_code,
            "eventType": event_type,
            "changedAt": changed_at or utc_now_iso(),
            "setupVersion": setup_version,
            "readinessVersion": readiness_version,
            "actor": actor,
            "payload": compact_payload,
        }

    def append_transition_log(self, recent: List[Dict[str, Any]], entry: Dict[str, Any], max_items: int = 200) -> None:
        recent.append(entry)
        del recent[:-max_items]

    def _migration_empty_counts(self) -> Dict[str, int]:
        return {bucket: 0 for bucket in MIGRATION_DIAGNOSTIC_BUCKETS}

    def _migration_classification(self, *, canonical_complete: bool, alias_present: bool, alias_used: bool, malformed: bool, conflict: bool = False) -> str:
        if malformed:
            return "malformed"
        if canonical_complete and not alias_used and not conflict:
            return "canonical"
        if alias_present and (alias_used or conflict):
            return "mixed" if canonical_complete else "old-only"
        if canonical_complete:
            return "canonical"
        return "malformed"

    def _migration_changed_at(self, *values: Optional[Any]) -> Optional[str]:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value
        return None

    def _setup_key_parts(self, setup_key: str) -> Dict[str, Optional[str]]:
        if not isinstance(setup_key, str) or ":" not in setup_key:
            return {"symbol": None, "side": None}
        symbol, side = setup_key.split(":", 1)
        side = side.lower().strip()
        return {
            "symbol": symbol.upper().strip() or None,
            "side": side if side in {"long", "short"} else None,
        }

    def _normalized_actionable_alias(self, actionable: Any, readiness_state: Optional[str]) -> Optional[Dict[str, Any]]:
        # P4: canonical readiness.state is the source of truth; currentStatus is
        # output-only. This function returns a clean actionable dict without writing
        # legacy aliases into the persisted object.
        if isinstance(actionable, dict):
            return dict(actionable)
        if readiness_state:
            # Only for non-dict actionable (edge case) — create minimal output stub
            return {"readiness": {"state": readiness_state}}
        return actionable if isinstance(actionable, dict) else None

    def normalize_watchlist_setup_record(self, setup_key: str, record: Any) -> Dict[str, Any]:
        warnings: List[str] = []
        source = record if isinstance(record, dict) else {}
        malformed = not isinstance(record, dict)
        if malformed:
            warnings.append(f"watchlist:{setup_key}: record is not an object, defaulting to empty dict")

        key_parts = self._setup_key_parts(setup_key)
        symbol = str(source.get("symbol") or key_parts.get("symbol") or "").upper() or None
        side_raw = str(source.get("side") or key_parts.get("side") or "").lower().strip()
        side = side_raw if side_raw in {"long", "short"} else None

        lifecycle_raw = source.get("lifecycle") if isinstance(source.get("lifecycle"), dict) else {}
        readiness_raw = source.get("readiness") if isinstance(source.get("readiness"), dict) else {}
        actionable_raw = source.get("actionableAlert") if isinstance(source.get("actionableAlert"), dict) else None

        lifecycle_nested = self.canonical_setup_state(lifecycle_raw.get("state"))
        lifecycle_alias = self.canonical_setup_state(source.get("state"))
        lifecycle_conflict = bool(lifecycle_nested and lifecycle_alias and lifecycle_nested != lifecycle_alias)
        if lifecycle_conflict:
            warnings.append(f"watchlist:{setup_key}: lifecycle nested state '{lifecycle_nested}' overrides alias '{lifecycle_alias}'")
        lifecycle_state = lifecycle_nested or lifecycle_alias
        lifecycle_alias_used = not lifecycle_nested and bool(lifecycle_alias)
        if not lifecycle_state:
            if bool(source.get("disqualified")) or source.get("grade") == "NO_TRADE":
                lifecycle_state = "invalidated"
                warnings.append(f"watchlist:{setup_key}: inferred lifecycle.state=invalidated from disqualification fields")
            elif bool(source.get("alertEligible")):
                lifecycle_state = "actionable"
                warnings.append(f"watchlist:{setup_key}: inferred lifecycle.state=actionable from alertEligible")
            elif bool(source.get("watchlistEligible")):
                lifecycle_state = "watchlist"
                warnings.append(f"watchlist:{setup_key}: inferred lifecycle.state=watchlist from watchlistEligible")
            elif symbol and side:
                lifecycle_state = "candidate"
                warnings.append(f"watchlist:{setup_key}: inferred lifecycle.state=candidate from setup identity")
            else:
                lifecycle_state = "stale"
                malformed = True
                warnings.append(f"watchlist:{setup_key}: unable to derive lifecycle.state cleanly, defaulting to stale")

        readiness_nested = self.canonical_readiness_state(readiness_raw.get("state"))
        readiness_alias = self.canonical_readiness_state((actionable_raw or {}).get("currentStatus") if actionable_raw else None)
        if lifecycle_state in {"stale", "invalidated"} and readiness_alias and not readiness_nested:
            warnings.append(f"watchlist:{setup_key}: ignored alias-derived readiness '{readiness_alias}' because lifecycle.state={lifecycle_state}")
            readiness_alias = None
        readiness_conflict = bool(readiness_nested and readiness_alias and readiness_nested != readiness_alias)
        if readiness_conflict:
            warnings.append(f"watchlist:{setup_key}: readiness nested state '{readiness_nested}' overrides alias '{readiness_alias}'")
        readiness_state = readiness_nested or readiness_alias
        readiness_alias_used = not readiness_nested and bool(readiness_alias)

        readiness_version_nested = readiness_raw.get("readinessVersion") if isinstance(readiness_raw.get("readinessVersion"), str) and readiness_raw.get("readinessVersion") else None
        readiness_version_alias = source.get("readinessVersion") if isinstance(source.get("readinessVersion"), str) and source.get("readinessVersion") else None
        readiness_version = readiness_version_nested or readiness_version_alias
        readiness_version_alias_used = not readiness_version_nested and bool(readiness_version_alias)

        actionable = self._normalized_actionable_alias(actionable_raw, readiness_state)
        if not readiness_version and symbol and side and isinstance(actionable, dict):
            readiness_version = self.readiness_version_from_actionable(symbol, side, actionable)

        if lifecycle_state in {"actionable", "notified"} and not readiness_state:
            malformed = True
            warnings.append(f"watchlist:{setup_key}: actionable/notified lifecycle without readiness.state")

        lifecycle_changed_at = self._migration_changed_at(
            lifecycle_raw.get("changedAt") if isinstance(lifecycle_raw, dict) else None,
            source.get("lastTransitionAt"),
            source.get("updatedAt"),
            source.get("lastSeenAt"),
            source.get("firstSeenAt"),
        )
        readiness_changed_at = self._migration_changed_at(
            readiness_raw.get("changedAt") if isinstance(readiness_raw, dict) else None,
            source.get("lastTransitionAt"),
            source.get("updatedAt"),
            source.get("lastSeenAt"),
        )

        lifecycle_reason = lifecycle_raw.get("reasonCode") if isinstance(lifecycle_raw.get("reasonCode"), str) and lifecycle_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if lifecycle_alias_used else "MIGRATION_INFERRED")
        readiness_reason = readiness_raw.get("reasonCode") if isinstance(readiness_raw.get("reasonCode"), str) and readiness_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if readiness_alias_used or readiness_version_alias_used else ("READINESS_NONE" if not readiness_state else "MIGRATION_INFERRED"))

        active_value = source.get("active")
        if isinstance(active_value, bool):
            active = active_value
        else:
            active = lifecycle_state in CANONICAL_SETUP_ACTIVE_STATES

        canonical_complete = bool(lifecycle_nested) and (bool(readiness_nested) or lifecycle_state in {"candidate", "watchlist", "invalidated", "stale"})
        alias_present = bool(lifecycle_alias or readiness_alias or readiness_version_alias)
        alias_used = bool(lifecycle_alias_used or readiness_alias_used or readiness_version_alias_used)

        normalized = dict(source)
        # P3: strip legacy alias fields — canonical nested fields are the only source of truth
        for _f in ("state", "pending", "currentStatus"):
            normalized.pop(_f, None)
        normalized["setupKey"] = source.get("setupKey") or setup_key
        if symbol:
            normalized["symbol"] = symbol
        if side:
            normalized["side"] = side
        normalized["active"] = active
        normalized["lifecycle"] = {
            **(lifecycle_raw if isinstance(lifecycle_raw, dict) else {}),
            "state": lifecycle_state,
            "reasonCode": lifecycle_reason,
            "changedAt": lifecycle_changed_at,
        }
        normalized["readiness"] = {
            **(readiness_raw if isinstance(readiness_raw, dict) else {}),
            "state": readiness_state,
            "reasonCode": readiness_reason,
            "changedAt": readiness_changed_at,
            "readinessVersion": readiness_version,
        }
        normalized["readinessVersion"] = readiness_version
        if isinstance(actionable, dict):
            normalized["actionableAlert"] = actionable

        classification = self._migration_classification(
            canonical_complete=canonical_complete,
            alias_present=alias_present,
            alias_used=alias_used,
            malformed=malformed,
            conflict=lifecycle_conflict or readiness_conflict,
        )
        return {"record": normalized, "classification": classification, "warnings": warnings}

    def normalize_alert_record(self, setup_key: str, record: Any) -> Dict[str, Any]:
        warnings: List[str] = []
        source = record if isinstance(record, dict) else {}
        malformed = not isinstance(record, dict)
        if malformed:
            warnings.append(f"alerts:{setup_key}: record is not an object, defaulting to empty dict")

        key_parts = self._setup_key_parts(setup_key)
        symbol = str(source.get("symbol") or key_parts.get("symbol") or "").upper() or None
        side_raw = str(source.get("side") or key_parts.get("side") or "").lower().strip()
        side = side_raw if side_raw in {"long", "short"} else None

        delivery_raw = source.get("delivery") if isinstance(source.get("delivery"), dict) else {}
        delivery_nested = self.canonical_delivery_state(delivery_raw.get("state"))
        delivery_alias = self.canonical_delivery_state(source.get("state"))
        pending_alias = bool(source.get("pending")) if source.get("pending") is not None else None
        inferred_from_pending = "pending" if pending_alias else None
        delivery_conflict = bool(delivery_nested and delivery_alias and delivery_nested != delivery_alias)
        if delivery_conflict:
            warnings.append(f"alerts:{setup_key}: delivery nested state '{delivery_nested}' overrides alias '{delivery_alias}'")

        delivery_state = delivery_nested or delivery_alias or inferred_from_pending
        delivery_alias_used = not delivery_nested and bool(delivery_alias or inferred_from_pending)
        if not delivery_state:
            if source.get("lastVersionSent") or source.get("lastSentAt"):
                delivery_state = "sent"
                warnings.append(f"alerts:{setup_key}: inferred delivery.state=sent from last sent markers")
            else:
                delivery_state = "idle"

        last_sent_readiness_version = None
        if isinstance(delivery_raw.get("lastSentReadinessVersion"), str) and delivery_raw.get("lastSentReadinessVersion"):
            last_sent_readiness_version = delivery_raw.get("lastSentReadinessVersion")
        elif isinstance(source.get("lastSentReadinessVersion"), str) and source.get("lastSentReadinessVersion"):
            last_sent_readiness_version = source.get("lastSentReadinessVersion")

        changed_at = self._migration_changed_at(
            delivery_raw.get("changedAt") if isinstance(delivery_raw, dict) else None,
            source.get("updatedAt"),
            source.get("lastSentAt"),
            source.get("lastSeenAt"),
            source.get("firstSeenAt"),
        )
        reason_code = delivery_raw.get("reasonCode") if isinstance(delivery_raw.get("reasonCode"), str) and delivery_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if delivery_alias_used else "MIGRATION_INFERRED")

        active_value = source.get("active")
        active = active_value if isinstance(active_value, bool) else delivery_state in {"pending", "sent"}

        canonical_complete = bool(delivery_nested)
        alias_present = bool(delivery_alias or pending_alias is not None or source.get("lastSentReadinessVersion"))
        alias_used = bool(delivery_alias_used or (not delivery_raw.get("lastSentReadinessVersion") and source.get("lastSentReadinessVersion")))

        normalized = dict(source)
        # P3: strip legacy alias fields — canonical nested fields are the only source of truth
        for _f in ("state", "pending", "currentStatus"):
            normalized.pop(_f, None)
        normalized["setupKey"] = source.get("setupKey") or setup_key
        if symbol:
            normalized["symbol"] = symbol
        if side:
            normalized["side"] = side
        normalized["active"] = active
        normalized["delivery"] = {
            **(delivery_raw if isinstance(delivery_raw, dict) else {}),
            "state": delivery_state,
            "reasonCode": reason_code,
            "changedAt": changed_at,
            "lastSentReadinessVersion": last_sent_readiness_version,
        }
        normalized["lastSentReadinessVersion"] = last_sent_readiness_version

        classification = self._migration_classification(
            canonical_complete=canonical_complete,
            alias_present=alias_present,
            alias_used=alias_used,
            malformed=malformed,
            conflict=delivery_conflict,
        )
        return {"record": normalized, "classification": classification, "warnings": warnings}

    def normalize_runtime_monitor(self, setup_key: str, record: Any) -> Dict[str, Any]:
        warnings: List[str] = []
        source = record if isinstance(record, dict) else {}
        malformed = not isinstance(record, dict)
        if malformed:
            warnings.append(f"runtime:{setup_key}: monitor is not an object, defaulting to empty dict")

        key_parts = self._setup_key_parts(setup_key)
        symbol = str(source.get("symbol") or key_parts.get("symbol") or "").upper() or None
        side_raw = str(source.get("side") or key_parts.get("side") or "").lower().strip()
        side = side_raw if side_raw in {"long", "short"} else None

        runtime_raw = source.get("runtime") if isinstance(source.get("runtime"), dict) else {}
        runtime_nested = self.canonical_runtime_state(runtime_raw.get("state"))
        runtime_alias = self.canonical_runtime_state(source.get("state"))
        runtime_conflict = bool(runtime_nested and runtime_alias and runtime_nested != runtime_alias)
        if runtime_conflict:
            warnings.append(f"runtime:{setup_key}: runtime nested state '{runtime_nested}' overrides alias '{runtime_alias}'")
        runtime_state = runtime_nested or runtime_alias or "inactive"
        runtime_alias_used = not runtime_nested and bool(runtime_alias)

        lifecycle_raw = source.get("lifecycle") if isinstance(source.get("lifecycle"), dict) else {}
        lifecycle_nested = self.canonical_setup_state(lifecycle_raw.get("state"))
        lifecycle_alias = self.canonical_setup_state(source.get("lifecycleState"))
        lifecycle_conflict = bool(lifecycle_nested and lifecycle_alias and lifecycle_nested != lifecycle_alias)
        if lifecycle_conflict:
            warnings.append(f"runtime:{setup_key}: lifecycle nested state '{lifecycle_nested}' overrides alias '{lifecycle_alias}'")
        lifecycle_state = lifecycle_nested or lifecycle_alias
        lifecycle_alias_used = not lifecycle_nested and bool(lifecycle_alias)

        readiness_raw = source.get("readiness") if isinstance(source.get("readiness"), dict) else {}
        readiness_nested = self.canonical_readiness_state(readiness_raw.get("state"))
        readiness_alias = self.canonical_readiness_state(source.get("readinessState"))
        if lifecycle_state in {"stale", "invalidated"} and readiness_alias and not readiness_nested:
            warnings.append(f"runtime:{setup_key}: ignored alias-derived readiness '{readiness_alias}' because lifecycle.state={lifecycle_state}")
            readiness_alias = None
        readiness_conflict = bool(readiness_nested and readiness_alias and readiness_nested != readiness_alias)
        if readiness_conflict:
            warnings.append(f"runtime:{setup_key}: readiness nested state '{readiness_nested}' overrides alias '{readiness_alias}'")
        readiness_state = readiness_nested or readiness_alias
        readiness_alias_used = not readiness_nested and bool(readiness_alias)

        readiness_version_nested = readiness_raw.get("readinessVersion") if isinstance(readiness_raw.get("readinessVersion"), str) and readiness_raw.get("readinessVersion") else None
        readiness_version_alias = source.get("readinessVersion") if isinstance(source.get("readinessVersion"), str) and source.get("readinessVersion") else None
        readiness_version = readiness_version_nested or readiness_version_alias

        changed_at = self._migration_changed_at(
            runtime_raw.get("changedAt") if isinstance(runtime_raw, dict) else None,
            source.get("updatedAt"),
            source.get("cooldownUntil"),
            source.get("triggeredAt"),
            source.get("armedAt"),
            source.get("inactiveAt"),
        )
        reason_code = runtime_raw.get("reasonCode") if isinstance(runtime_raw.get("reasonCode"), str) and runtime_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if runtime_alias_used else "MIGRATION_INFERRED")
        event_type = runtime_raw.get("eventType") if isinstance(runtime_raw.get("eventType"), str) and runtime_raw.get("eventType") else "MIGRATION_LOAD"

        lifecycle_changed_at = self._migration_changed_at(
            lifecycle_raw.get("changedAt") if isinstance(lifecycle_raw, dict) else None,
            source.get("updatedAt"),
            source.get("inactiveAt"),
            source.get("triggeredAt"),
            source.get("armedAt"),
        )
        lifecycle_reason = lifecycle_raw.get("reasonCode") if isinstance(lifecycle_raw.get("reasonCode"), str) and lifecycle_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if lifecycle_alias_used else ("MIGRATION_INFERRED" if lifecycle_state else None))

        readiness_changed_at = self._migration_changed_at(
            readiness_raw.get("changedAt") if isinstance(readiness_raw, dict) else None,
            source.get("updatedAt"),
            source.get("inactiveAt"),
            source.get("triggeredAt"),
            source.get("armedAt"),
        )
        readiness_reason = readiness_raw.get("reasonCode") if isinstance(readiness_raw.get("reasonCode"), str) and readiness_raw.get("reasonCode") else ("MIGRATED_FROM_ALIAS" if readiness_alias_used else ("READINESS_NONE" if not readiness_state else "MIGRATION_INFERRED"))

        normalized = dict(source)
        # P3: strip legacy alias fields — canonical nested fields are the only source of truth
        for _f in ("state", "pending", "currentStatus"):
            normalized.pop(_f, None)
        normalized["setupKey"] = source.get("setupKey") or setup_key
        if symbol:
            normalized["symbol"] = symbol
        if side:
            normalized["side"] = side
        normalized["runtime"] = {
            **(runtime_raw if isinstance(runtime_raw, dict) else {}),
            "state": runtime_state,
            "reasonCode": reason_code,
            "eventType": event_type,
            "changedAt": changed_at,
        }
        if lifecycle_state:
            normalized["lifecycle"] = {
                **(lifecycle_raw if isinstance(lifecycle_raw, dict) else {}),
                "state": lifecycle_state,
                "reasonCode": lifecycle_reason,
                "changedAt": lifecycle_changed_at,
            }
        elif "lifecycle" in normalized and not isinstance(normalized.get("lifecycle"), dict):
            normalized.pop("lifecycle", None)
        if readiness_state or readiness_version:
            normalized["readiness"] = {
                **(readiness_raw if isinstance(readiness_raw, dict) else {}),
                "state": readiness_state,
                "reasonCode": readiness_reason,
                "changedAt": readiness_changed_at,
                "readinessVersion": readiness_version,
            }
            normalized["readinessVersion"] = readiness_version
        elif "readiness" in normalized and not isinstance(normalized.get("readiness"), dict):
            normalized.pop("readiness", None)

        active_value = source.get("active")
        if isinstance(active_value, bool):
            normalized["active"] = active_value
        else:
            normalized["active"] = runtime_state != "inactive" or bool(source.get("trackingOnly"))

        canonical_complete = bool(runtime_nested)
        alias_present = bool(runtime_alias or lifecycle_alias or readiness_alias)
        alias_used = bool(runtime_alias_used or lifecycle_alias_used or readiness_alias_used)
        classification = self._migration_classification(
            canonical_complete=canonical_complete,
            alias_present=alias_present,
            alias_used=alias_used,
            malformed=malformed,
            conflict=runtime_conflict or lifecycle_conflict or readiness_conflict,
        )
        return {"record": normalized, "classification": classification, "warnings": warnings}

    def _normalize_watchlist_state_payload(self, raw_state: Any) -> Dict[str, Any]:
        default_state = {"updatedAt": None, "lastScanAt": None, "scanCount": 0, "setups": {}, "recent": []}
        diagnostics = {
            "scope": "watchlist",
            "counts": self._migration_empty_counts(),
            "totalRecords": 0,
            "warnings": [],
            "precedence": "authoritative nested fields win; legacy aliases are fallback-only during migration",
        }
        source = raw_state if isinstance(raw_state, dict) else {}
        if not isinstance(raw_state, dict):
            diagnostics["warnings"].append("watchlist: top-level payload is not an object, defaulted to empty state")
            diagnostics["counts"]["malformed"] += 1
        setups_raw = source.get("setups") if isinstance(source.get("setups"), dict) else {}
        if source.get("setups") is not None and not isinstance(source.get("setups"), dict):
            diagnostics["warnings"].append("watchlist: setups payload is not an object, defaulted to empty map")
        normalized_setups: Dict[str, Any] = {}
        for setup_key, record in setups_raw.items():
            result = self.normalize_watchlist_setup_record(str(setup_key), record)
            normalized_setups[str(setup_key)] = result["record"]
            diagnostics["counts"][result["classification"]] += 1
            diagnostics["warnings"].extend(result["warnings"])
        diagnostics["totalRecords"] = len(normalized_setups)
        state = {**default_state, **source}
        state["scanCount"] = int(state.get("scanCount", 0) or 0)
        state["setups"] = normalized_setups
        state["recent"] = state.get("recent") if isinstance(state.get("recent"), list) else []
        state["migrationDiagnostics"] = diagnostics
        return state

    def _normalize_alert_state_payload(self, raw_state: Any) -> Dict[str, Any]:
        default_state = {"updatedAt": None, "lastSyncAt": None, "setups": {}, "recent": []}
        diagnostics = {
            "scope": "alerts",
            "counts": self._migration_empty_counts(),
            "totalRecords": 0,
            "warnings": [],
            "precedence": "authoritative nested fields win; legacy aliases are fallback-only during migration",
        }
        source = raw_state if isinstance(raw_state, dict) else {}
        if not isinstance(raw_state, dict):
            diagnostics["warnings"].append("alerts: top-level payload is not an object, defaulted to empty state")
            diagnostics["counts"]["malformed"] += 1
        setups_raw = source.get("setups") if isinstance(source.get("setups"), dict) else {}
        if source.get("setups") is not None and not isinstance(source.get("setups"), dict):
            diagnostics["warnings"].append("alerts: setups payload is not an object, defaulted to empty map")
        normalized_setups: Dict[str, Any] = {}
        for setup_key, record in setups_raw.items():
            result = self.normalize_alert_record(str(setup_key), record)
            normalized_setups[str(setup_key)] = result["record"]
            diagnostics["counts"][result["classification"]] += 1
            diagnostics["warnings"].extend(result["warnings"])
        diagnostics["totalRecords"] = len(normalized_setups)
        state = {**default_state, **source}
        state["setups"] = normalized_setups
        state["recent"] = state.get("recent") if isinstance(state.get("recent"), list) else []
        state["migrationDiagnostics"] = diagnostics
        return state

    def _normalize_live_trigger_state_payload(self, raw_state: Any) -> Dict[str, Any]:
        default_state = {
            "updatedAt": None,
            "startedAt": None,
            "lastHeartbeatAt": None,
            "lastWsMessageAt": None,
            "status": "idle",
            "config": {},
            "monitors": {},
            "recent": [],
            "lastError": None,
            "metrics": {},
        }
        diagnostics = {
            "scope": "runtime",
            "counts": self._migration_empty_counts(),
            "totalRecords": 0,
            "warnings": [],
            "precedence": "authoritative nested fields win; legacy aliases are fallback-only during migration",
        }
        source = raw_state if isinstance(raw_state, dict) else {}
        if not isinstance(raw_state, dict):
            diagnostics["warnings"].append("runtime: top-level payload is not an object, defaulted to empty state")
            diagnostics["counts"]["malformed"] += 1
        monitors_raw = source.get("monitors") if isinstance(source.get("monitors"), dict) else {}
        if source.get("monitors") is not None and not isinstance(source.get("monitors"), dict):
            diagnostics["warnings"].append("runtime: monitors payload is not an object, defaulted to empty map")
        normalized_monitors: Dict[str, Any] = {}
        for setup_key, record in monitors_raw.items():
            result = self.normalize_runtime_monitor(str(setup_key), record)
            normalized_monitors[str(setup_key)] = result["record"]
            diagnostics["counts"][result["classification"]] += 1
            diagnostics["warnings"].extend(result["warnings"])
        diagnostics["totalRecords"] = len(normalized_monitors)
        state = {**default_state, **source}
        state["config"] = state.get("config") if isinstance(state.get("config"), dict) else {}
        state["monitors"] = normalized_monitors
        state["recent"] = state.get("recent") if isinstance(state.get("recent"), list) else []
        state["metrics"] = state.get("metrics") if isinstance(state.get("metrics"), dict) else {}
        state["migrationDiagnostics"] = diagnostics
        return state

    def state_migration_preview(self, scope: str = "all", limit: int = 3) -> Dict[str, Any]:
        scope = str(scope or "all").strip().lower()
        if scope not in {"watchlist", "alerts", "runtime", "all"}:
            raise RuntimeError("scope must be one of: watchlist, alerts, runtime, all")

        preview: Dict[str, Any] = {"precedenceRule": "authoritative nested field wins over legacy alias; alias is fallback-only during migration; normalized in-memory objects are completed in canonical schema"}

        def build_examples(raw_map: Any, normalized_map: Dict[str, Any], normalizer_name: str) -> List[Dict[str, Any]]:
            examples: List[Dict[str, Any]] = []
            if not isinstance(raw_map, dict):
                return examples
            for setup_key, raw_record in list(raw_map.items())[: max(limit * 3, limit)]:
                normalizer = getattr(self, normalizer_name)
                result = normalizer(str(setup_key), raw_record)
                if raw_record != result["record"] or result["classification"] != "canonical":
                    examples.append({
                        "setupKey": setup_key,
                        "classification": result["classification"],
                        "warnings": result["warnings"],
                        "before": raw_record,
                        "after": normalized_map.get(str(setup_key)),
                    })
                if len(examples) >= limit:
                    break
            return examples

        if scope in {"watchlist", "all"}:
            raw_watchlist = load_json_file(WATCHLIST_STATE_PATH, {"setups": {}})
            normalized_watchlist = self._normalize_watchlist_state_payload(raw_watchlist)
            preview["watchlist"] = {
                "path": str(WATCHLIST_STATE_PATH),
                "diagnostics": normalized_watchlist.get("migrationDiagnostics"),
                "examples": build_examples(raw_watchlist.get("setups") if isinstance(raw_watchlist, dict) else {}, normalized_watchlist.get("setups", {}), "normalize_watchlist_setup_record"),
            }

        if scope in {"alerts", "all"}:
            raw_alerts = load_json_file(ALERT_STATE_PATH, {"setups": {}})
            normalized_alerts = self._normalize_alert_state_payload(raw_alerts)
            preview["alerts"] = {
                "path": str(ALERT_STATE_PATH),
                "diagnostics": normalized_alerts.get("migrationDiagnostics"),
                "examples": build_examples(raw_alerts.get("setups") if isinstance(raw_alerts, dict) else {}, normalized_alerts.get("setups", {}), "normalize_alert_record"),
            }

        if scope in {"runtime", "all"}:
            raw_runtime = load_json_file(LIVE_TRIGGER_STATE_PATH, {"monitors": {}})
            normalized_runtime = self._normalize_live_trigger_state_payload(raw_runtime)
            preview["runtime"] = {
                "path": str(LIVE_TRIGGER_STATE_PATH),
                "diagnostics": normalized_runtime.get("migrationDiagnostics"),
                "examples": build_examples(raw_runtime.get("monitors") if isinstance(raw_runtime, dict) else {}, normalized_runtime.get("monitors", {}), "normalize_runtime_monitor"),
            }
        return preview

    def _safe_float(self, value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except Exception:
            return None

    def _normalized_zone(self, zone: Any) -> Optional[Dict[str, float]]:
        if not isinstance(zone, dict):
            return None
        low = self._safe_float(zone.get("low"))
        high = self._safe_float(zone.get("high"))
        if low is None or high is None:
            return None
        if low > high:
            low, high = high, low
        return {"low": low, "high": high}

    def _price_in_zone(self, price: Any, zone: Any) -> bool:
        current = self._safe_float(price)
        normalized = self._normalized_zone(zone)
        if current is None or normalized is None:
            return False
        return normalized["low"] <= current <= normalized["high"]

    def _zones_similar(self, left: Any, right: Any) -> bool:
        zone_left = self._normalized_zone(left)
        zone_right = self._normalized_zone(right)
        if zone_left is None or zone_right is None:
            return False
        width_left = max(zone_left["high"] - zone_left["low"], 1e-12)
        width_right = max(zone_right["high"] - zone_right["low"], 1e-12)
        overlap = max(0.0, min(zone_left["high"], zone_right["high"]) - max(zone_left["low"], zone_right["low"]))
        overlap_ratio = overlap / min(width_left, width_right)
        midpoint_left = (zone_left["low"] + zone_left["high"]) / 2.0
        midpoint_right = (zone_right["low"] + zone_right["high"]) / 2.0
        midpoint_gap = abs(midpoint_left - midpoint_right)
        return overlap_ratio >= 0.6 and midpoint_gap <= max(width_left, width_right) * 0.75

    def _same_alert_context(self, alert_previous: Dict[str, Any], actionable: Dict[str, Any]) -> bool:
        if not isinstance(alert_previous, dict) or not isinstance(actionable, dict):
            return False
        if self.delivery_state(alert_previous) != "sent":
            return False
        # Authoritative readiness state — from nested readiness object (not legacy currentStatus alias)
        current_status = (actionable.get("readiness") or {}).get("state")
        previous_status = (alert_previous.get("delivery") or {}).get("lastSentStatus")
        if not current_status or not previous_status or current_status != previous_status:
            return False
        current_zone = actionable.get("entryZone")
        previous_zone = alert_previous.get("lastSentEntryZone")
        if not self._zones_similar(previous_zone, current_zone):
            return False
        return self._price_in_zone(actionable.get("currentPrice"), previous_zone)

    def derive_setup_state(self, row: Dict[str, Any], already_alerted: bool = False) -> Dict[str, Any]:
        direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
        if direction not in {"long", "short"}:
            return {"lifecycleState": "stale", "legacyState": "stale", "active": False}
        if row.get("grade") == "NO_TRADE" or row.get("disqualified"):
            return {"lifecycleState": "invalidated", "legacyState": "invalidated", "active": False}
        if row.get("alertEligible"):
            lifecycle_state = "notified" if already_alerted else "actionable"
            return {"lifecycleState": lifecycle_state, "legacyState": self.legacy_setup_state(lifecycle_state), "active": True}
        if row.get("watchlistEligible"):
            return {"lifecycleState": "watchlist", "legacyState": "watchlist", "active": True}
        return {"lifecycleState": "candidate", "legacyState": "candidate", "active": True}

    def apply_setup_state_preview_to_row(self, row: Dict[str, Any], alert_record: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
        if direction not in {"long", "short"}:
            row["setupKey"] = None
            row["setupVersion"] = None
            row["readinessVersion"] = None
            row["state"] = None
            row["stateActive"] = False
            row["lifecycle"] = {"state": None}
            row["readiness"] = {"state": None}
            return row
        key = self.setup_state_key(str(row.get("symbol")), direction)
        version = self.setup_state_version(row)
        actionable = row.get("actionableAlert") if isinstance(row.get("actionableAlert"), dict) else None
        readiness_state = self.canonical_readiness_state((actionable or {}).get("currentStatus"))
        readiness_version = self.readiness_version_from_actionable(
            str(row.get("symbol") or "").upper(), direction, actionable,
            stable_readiness_state=readiness_state,  # B1.1: pass stable (this adapter IS the stable state)
        )
        current_delivery_state = self.delivery_state(alert_record)
        already_alerted = bool(
            isinstance(alert_record, dict)
            and current_delivery_state == "sent"
            and (
                alert_record.get("lastSentReadinessVersion") == readiness_version
                or alert_record.get("lastVersionSent") == version
            )
        )
        derived = self.derive_setup_state(row, already_alerted=already_alerted)
        row["setupKey"] = key
        row["setupVersion"] = version
        row["readinessVersion"] = readiness_version
        row["state"] = derived["legacyState"]
        row["stateActive"] = bool(derived["active"])
        row["lifecycle"] = {
            "state": derived["lifecycleState"],
        }
        row["readiness"] = {
            "state": readiness_state,
            "readinessVersion": readiness_version,
        }
        # currentStatus is a display-only field; add it to the output row
        # without mutating the underlying persisted object
        row["currentStatus"] = self.legacy_readiness_state(readiness_state) if readiness_state else None
        return row

    def readiness_render_observability(self, record: Optional[Dict[str, Any]], monitor: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        readiness_state = self.setup_readiness_state(record)
        lifecycle_state = self.setup_lifecycle_state(record)
        lane = self.notification_lane_for_setup(record or {}, monitor=monitor) if readiness_state else None
        blocked_by = None
        renderable = False
        if readiness_state == "retest_ready":
            if lifecycle_state in ("actionable", "notified"):
                if lane in {"LIVE", "READY"}:
                    renderable = True
                else:
                    blocked_by = "other_blockers"
            elif lifecycle_state == "stale":
                blocked_by = "stale"
            elif lifecycle_state == "invalidated":
                blocked_by = "invalidated"
            elif lifecycle_state == "candidate":
                blocked_by = "candidate_only"
            else:
                blocked_by = "other_blockers"
        elif readiness_state in LIVE_TRIGGER_STANDBY_READINESS_STATES:
            if lifecycle_state in CANONICAL_SETUP_ACTIVE_STATES:
                if lane == "STANDBY":
                    renderable = True
                else:
                    blocked_by = "other_blockers"
            elif lifecycle_state == "stale":
                blocked_by = "stale"
            elif lifecycle_state == "invalidated":
                blocked_by = "invalidated"
            else:
                blocked_by = "other_blockers"
        return {
            "readinessState": readiness_state,
            "lifecycleState": lifecycle_state,
            "wouldRenderLane": lane,
            "renderable": renderable,
            "blockedBy": blocked_by,
        }

    def setup_state_status(self, symbol: Optional[str] = None) -> Dict[str, Any]:
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        live_state = self.load_live_trigger_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        alert_setups = alerts.get("setups", {}) if isinstance(alerts, dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        if symbol:
            symbol = symbol.upper()
            return {
                "watchlistPath": str(WATCHLIST_STATE_PATH),
                "alertPath": str(ALERT_STATE_PATH),
                "symbol": symbol,
                "watchlistMigration": watchlist.get("migrationDiagnostics"),
                "alertMigration": alerts.get("migrationDiagnostics"),
                "setups": {key: value for key, value in setups.items() if isinstance(value, dict) and value.get("symbol") == symbol},
                "alerts": {key: value for key, value in alert_setups.items() if isinstance(value, dict) and value.get("symbol") == symbol},
            }

        state_counts: Dict[str, int] = {}
        readiness_counts_all: Dict[str, int] = {}
        readiness_counts_renderable: Dict[str, int] = {}
        blocked_readiness_counts: Dict[str, int] = {
            "stale": 0,
            "invalidated": 0,
            "candidate_only": 0,
            "other_blockers": 0,
        }
        for setup_key, record in setups.items():
            if not isinstance(record, dict):
                continue
            lifecycle_state = self.setup_lifecycle_state(record) or "unknown"
            readiness_state = self.setup_readiness_state(record) or "none"
            state_counts[lifecycle_state] = state_counts.get(lifecycle_state, 0) + 1
            readiness_counts_all[readiness_state] = readiness_counts_all.get(readiness_state, 0) + 1
            monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
            readiness_observability = self.readiness_render_observability(record, monitor=monitor)
            if readiness_observability.get("renderable") and readiness_state != "none":
                readiness_counts_renderable[readiness_state] = readiness_counts_renderable.get(readiness_state, 0) + 1
            blocked_by = readiness_observability.get("blockedBy")
            if blocked_by in blocked_readiness_counts:
                blocked_readiness_counts[blocked_by] += 1
        pending_alerts = sum(1 for record in alert_setups.values() if isinstance(record, dict) and self.delivery_state(record) == "pending")
        sent_alerts = sum(1 for record in alert_setups.values() if isinstance(record, dict) and self.delivery_state(record) == "sent")
        active_setups = sum(1 for record in setups.values() if isinstance(record, dict) and record.get("active"))
        return {
            "watchlistPath": str(WATCHLIST_STATE_PATH),
            "alertPath": str(ALERT_STATE_PATH),
            "updatedAt": watchlist.get("updatedAt"),
            "lastScanAt": watchlist.get("lastScanAt"),
            "scanCount": watchlist.get("scanCount", 0),
            "setupsTracked": len(setups),
            "activeSetups": active_setups,
            "stateCounts": state_counts,
            "readinessCounts": readiness_counts_renderable,
            "readinessCounts_all": readiness_counts_all,
            "readinessCounts_renderable": readiness_counts_renderable,
            "blockedReadinessCounts": blocked_readiness_counts,
            "pendingAlerts": pending_alerts,
            "sentAlerts": sent_alerts,
            "watchlistMigration": watchlist.get("migrationDiagnostics"),
            "alertMigration": alerts.get("migrationDiagnostics"),
            "recentTransitions": (watchlist.get("recent") or [])[-10:],
        }

    def pending_alerts(self, limit: int = 10, mark_sent: bool = False, notes: Optional[str] = None) -> Dict[str, Any]:
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        alert_setups = alerts.get("setups", {}) if isinstance(alerts, dict) else {}
        rows: List[Dict[str, Any]] = []
        for key, record in alert_setups.items():
            if not isinstance(record, dict) or self.delivery_state(record) != "pending":
                continue
            setup = setups.get(key, {}) if isinstance(setups.get(key), dict) else {}
            merged = {
                "setupKey": key,
                "symbol": record.get("symbol"),
                "side": record.get("side"),
                "grade": setup.get("grade") or record.get("grade"),
                "cleanScore": setup.get("cleanScore"),
                "gradeScore": setup.get("gradeScore"),
                "state": self.setup_lifecycle_state(setup),
                "lifecycleState": self.setup_lifecycle_state(setup),
                "deliveryState": self.delivery_state(record),
                "readinessState": self.setup_readiness_state(setup),
                "setupVersion": record.get("setupVersion"),
                "readinessVersion": setup.get("readinessVersion") or record.get("lastSentReadinessVersion"),
                "lastSeenAt": setup.get("lastSeenAt") or record.get("lastSeenAt"),
                "managementGuidance": setup.get("managementGuidance", []),
                "gradeReasons": setup.get("gradeReasons", []),
                "riskFlags": setup.get("riskFlags", []),
                "actionableAlert": setup.get("actionableAlert") if isinstance(setup.get("actionableAlert"), dict) else None,
            }
            rows.append(merged)
        rows = sorted(rows, key=lambda row: (self.grade_priority(row.get("grade")), float(row.get("cleanScore", 0.0) or 0.0), float(row.get("gradeScore", 0.0) or 0.0)), reverse=True)
        selected = rows[:limit]
        marked_sent = []
        if mark_sent:
            for row in selected:
                try:
                    self.mark_alert_sent(str(row.get("symbol") or "").upper(), side=str(row.get("side") or "").lower(), setup_version=row.get("setupVersion"), notes=notes)
                    marked_sent.append({
                        "setupKey": row.get("setupKey"),
                        "setupVersion": row.get("setupVersion"),
                    })
                except Exception:
                    continue
        return {
            "alertPath": str(ALERT_STATE_PATH),
            "watchlistPath": str(WATCHLIST_STATE_PATH),
            "pendingCount": len(rows),
            "pending": selected,
            "markedSent": marked_sent,
        }

    def sync_setup_state(self, rows: List[Dict[str, Any]], source: str = "scan") -> Dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        setups = watchlist.setdefault("setups", {})
        setup_recent = watchlist.setdefault("recent", [])
        alert_setups = alerts.setdefault("setups", {})
        alert_recent = alerts.setdefault("recent", [])

        rows = [row for row in rows if isinstance(row, dict) and row.get("symbol")]
        seen_symbols = {str(row.get("symbol")).upper() for row in rows}
        updated_keys = set()
        transitions = 0

        for row in rows:
            direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
            if direction not in {"long", "short"}:
                continue
            symbol = str(row.get("symbol")).upper()
            key = self.setup_state_key(symbol, direction)
            updated_keys.add(key)

            previous = setups.get(key, {}) if isinstance(setups.get(key), dict) else {}
            alert_previous = alert_setups.get(key, {}) if isinstance(alert_setups.get(key), dict) else {}
            setup_version = self.setup_state_version(row)
            actionable = row.get("actionableAlert") if isinstance(row.get("actionableAlert"), dict) else {}
            previous_delivery_state = self.delivery_state(alert_previous) or "idle"
            previous_sent_readiness_version = alert_previous.get("lastSentReadinessVersion")
            derived = self.derive_setup_state(row, already_alerted=False)
            lifecycle_state = derived["lifecycleState"]
            state = derived["legacyState"]
            active = bool(derived["active"])
            previous_lifecycle_state = self.setup_lifecycle_state(previous)
            previous_readiness_state = self.setup_readiness_state(previous)
            lifecycle_changed = previous_lifecycle_state != lifecycle_state or previous.get("setupVersion") != setup_version

            # B1: Apply hysteresis before computing stable readiness_state
            # stable_readiness_state is what gets persisted as readiness.state
            # readiness_version must be based on stable state (B1.1 fix)
            readiness_observed = self.canonical_readiness_state((actionable or {}).get("currentStatus"))
            stable_readiness_state = self.apply_readiness_hysteresis(
                observed_state=readiness_observed,
                previous_record=previous,
                lifecycle_state=lifecycle_state,
            )

            # B1.1: compute readiness_version from STABLE state (hysteresis-applied)
            # This ensures readinessVersion tracks confirmed readiness, not raw scanner flips
            readiness_version = self.readiness_version_from_actionable(
                symbol, direction, actionable, stable_readiness_state=stable_readiness_state
            )

            # B1: same_alert_context and already_alerted depend on stable readiness_version
            same_alert_context = self._same_alert_context(alert_previous, actionable)
            already_alerted = bool(
                previous_delivery_state == "sent"
                and previous_sent_readiness_version == readiness_version
                and same_alert_context
            )

            # readiness_changed: compare stable (hysteresis-applied) against previous stable,
            # AND bump version if hysteresis caused a state change
            hysteresis_caused_change = stable_readiness_state != readiness_observed
            readiness_changed = (
                previous_readiness_state != stable_readiness_state
                or previous.get("readinessVersion") != readiness_version
                or hysteresis_caused_change
            )

            lifecycle_reason = "SCAN_SET_CANDIDATE"
            if lifecycle_state == "watchlist":
                lifecycle_reason = "SCAN_PROMOTE_WATCHLIST"
            elif lifecycle_state == "actionable":
                lifecycle_reason = "SCAN_PROMOTE_ACTIONABLE"
            elif lifecycle_state == "notified":
                lifecycle_reason = "DELIVERY_SENT_ACTIVE"
            elif lifecycle_state == "invalidated":
                lifecycle_reason = "SCAN_INVALIDATE_DISQUALIFIED" if row.get("disqualified") or row.get("grade") == "NO_TRADE" else "SCAN_INVALIDATE_THRESHOLD_LOSS"

            # B1: readiness_reason is based on stable (hysteresis-applied) state
            readiness_reason = "READINESS_NONE"
            if stable_readiness_state == "retest_ready":
                readiness_reason = "SCAN_PROMOTE_RETEST_READY" if previous_readiness_state != "retest_ready" else "SCAN_REAFFIRM_RETEST_READY"
            elif stable_readiness_state in LIVE_TRIGGER_STANDBY_READINESS_STATES:
                readiness_reason = "SCAN_DEMOTE_STANDBY" if previous_readiness_state == "retest_ready" else ("SCAN_SET_STANDBY" if previous_readiness_state != stable_readiness_state else "SCAN_REAFFIRM_STANDBY")

            # W1: Map lifecycle_state → INVALIDATION_TAXONOMY type and cascade via apply_invalidation
            # This is the canonical scanner invalidation entry point (W1).
            # For invalidated/stale: applies to watchlist (setups) and alert (alert_setups) state.
            # Runtime state changes are handled separately in reconcile_live_trigger_targets.
            invalidation_type = None
            if lifecycle_state == "invalidated":
                invalidation_type = "SCAN_INVALIDATE_DISQUALIFIED" if (row.get("disqualified") or row.get("grade") == "NO_TRADE") else "SCAN_INVALIDATE_THRESHOLD_LOSS"
            elif lifecycle_state == "stale":
                invalidation_type = "SCAN_STALE_NOT_REAFFIRMED"
            if invalidation_type and previous_lifecycle_state not in (lifecycle_state, None):
                # Cascade via apply_invalidation for cross-layer effects (watchlist + alert)
                self.apply_invalidation(
                    {"setups": setups, "recent": setup_recent},
                    key,
                    invalidation_type,
                    actor="sync_setup_state",
                    extra_payload={"source": source, "rowGrade": row.get("grade"), "rowDisqualified": row.get("disqualified")},
                )

            if lifecycle_changed:
                transitions += 1
                self.append_transition_log(setup_recent, self.transition_log_entry(
                    setup_key=key,
                    symbol=symbol,
                    side=direction,
                    layer="lifecycle",
                    from_state=previous_lifecycle_state,
                    to_state=lifecycle_state,
                    reason_code=lifecycle_reason,
                    event_type="SCAN_COMMIT",
                    changed_at=now,
                    setup_version=setup_version,
                    readiness_version=readiness_version,
                    actor="sync_setup_state",
                    payload={"source": source, "legacyState": state},
                ), max_items=100)
            if readiness_changed:
                transitions += 1
                self.append_transition_log(setup_recent, self.transition_log_entry(
                    setup_key=key,
                    symbol=symbol,
                    side=direction,
                    layer="readiness",
                    from_state=previous_readiness_state,
                    to_state=stable_readiness_state,
                    reason_code=readiness_reason,
                    event_type="SCAN_COMMIT",
                    changed_at=now,
                    setup_version=setup_version,
                    readiness_version=readiness_version,
                    actor="sync_setup_state",
                    payload={"source": source, "observedState": readiness_observed, "legacyStatus": actionable.get("currentStatus") if actionable else None},
                ), max_items=100)

            setups[key] = {
                "setupKey": key,
                "symbol": symbol,
                "side": direction,
                "active": active,
                "source": source,
                "grade": row.get("grade"),
                "gradeScore": row.get("gradeScore"),
                "cleanScore": row.get("cleanScore"),
                "bullScore": row.get("bullScore"),
                "quoteVolume": row.get("quoteVolume"),
                "setupVersion": setup_version,
                "readinessVersion": readiness_version,
                "alertEligible": bool(row.get("alertEligible")),
                "watchlistEligible": bool(row.get("watchlistEligible")),
                "disqualified": bool(row.get("disqualified")),
                "disqualifiers": row.get("disqualifiers", []),
                "gradeReasons": row.get("gradeReasons", []),
                "managementGuidance": row.get("managementGuidance", []),
                "actionableAlert": actionable if isinstance(actionable, dict) else None,
                "riskFlags": ((row.get("executionFilter") or {}).get("riskFlags", []) if isinstance(row.get("executionFilter"), dict) else []),
                "lifecycle": {
                    "state": lifecycle_state,
                    "reasonCode": lifecycle_reason,
                    "changedAt": now if lifecycle_changed else ((previous.get("lifecycle") or {}).get("changedAt") if isinstance(previous.get("lifecycle"), dict) else previous.get("lastTransitionAt") or now),
                },
                "readiness": {
                    "state": stable_readiness_state,  # B1: hysteresis-applied stable state
                    "reasonCode": readiness_reason,
                    "changedAt": now if readiness_changed else ((previous.get("readiness") or {}).get("changedAt") if isinstance(previous.get("readiness"), dict) else previous.get("lastTransitionAt") or now),
                    "readinessVersion": readiness_version,
                    "observedState": readiness_observed,  # B1: raw scanner state this scan
                    HYSTERESIS_CONFIRMATIONS_FIELD: previous.get("readiness", {}).get(HYSTERESIS_CONFIRMATIONS_FIELD, 0) if isinstance(previous.get("readiness"), dict) else 0,
                    HYSTERESIS_TARGET_FIELD: None,  # B1: updated in apply_readiness_hysteresis
                },
                "firstSeenAt": previous.get("firstSeenAt") or now,
                "lastSeenAt": now,
                "updatedAt": now,
                "lastTransitionAt": now if (lifecycle_changed or readiness_changed) else previous.get("lastTransitionAt") or now,
                "timesSeen": int(previous.get("timesSeen", 0) or 0) + 1,
            }

            # W2: RECOVERY LIFECYCLE CONTRACT
            # Detect recovery from {invalidated, stale} → actionable.
            # Recovery = ONLY legal path for lifecycle to exit invalid/stale states.
            # Requires: new setupVersion + retest_ready readiness.
            prev_lc = previous_lifecycle_state
            prev_rv = previous.get("readinessVersion") if isinstance(previous, dict) else None
            prev_sv = previous.get("setupVersion") if isinstance(previous, dict) else None
            prev_rs = previous.get("readiness", {}).get("state") if isinstance(previous.get("readiness"), dict) else None
            recovery_key = (prev_lc, prev_rs, lifecycle_state, stable_readiness_state)
            recovery_info = RECOVERY_TRANSITIONS.get(recovery_key)
            # W2: Recovery requires NEW setupVersion (not same as previous invalidating version)
            if recovery_info and setup_version != prev_sv:
                # New setupVersion confirmed — this is a real recovery
                recovery_entry = {
                    "setupKey": key,
                    "symbol": symbol,
                    "side": direction,
                    "from_lifecycle": prev_lc,
                    "to_lifecycle": lifecycle_state,
                    "from_readiness": prev_rs,
                    "to_readiness": stable_readiness_state,
                    "setupVersion": setup_version,
                    "readinessVersion": readiness_version,
                    "reason_code": recovery_info["reason_code"],
                    "runtime_target": recovery_info["runtime_target"],
                    "delivery_reset": recovery_info["delivery_reset"],
                }
                watchlist.setdefault("recoveryLog", []).append(recovery_entry)
                # W2: Reset delivery.sent for retest_ready recovery with delivery_reset=True
                # Only applies when previous delivery was {cleared, idle} and delivery_reset=True
                if recovery_info["delivery_reset"]:
                    prev_ds = previous.get("delivery", {}).get("state") if isinstance(previous.get("delivery"), dict) else None
                    if prev_ds in {"cleared", "idle"}:
                        setups[key]["delivery"] = {
                            "state": "pending",
                            "reasonCode": recovery_info["reason_code"],
                            "changedAt": now,
                        }


            previous_alert_state = previous_delivery_state
            if row.get("alertEligible"):
                if previous_delivery_state == "sent" and previous_sent_readiness_version == readiness_version and same_alert_context:
                    alert_state = "sent"
                    delivery_reason = "DELIVERY_RETAIN_SENT"
                elif previous_delivery_state == "sent" and previous_sent_readiness_version == readiness_version and not same_alert_context:
                    alert_state = "pending"
                    delivery_reason = "DELIVERY_RESET_CONTEXT_CHANGED"
                elif previous_delivery_state == "sent" and previous_sent_readiness_version != readiness_version:
                    alert_state = "pending"
                    delivery_reason = "DELIVERY_RESET_READINESS_VERSION_CHANGED"
                elif previous_delivery_state == "pending":
                    alert_state = "pending"
                    delivery_reason = "DELIVERY_RETAIN_PENDING"
                else:
                    alert_state = "pending"
                    delivery_reason = "DELIVERY_PENDING_NEW"
            elif lifecycle_state in {"invalidated", "stale"}:
                if previous_delivery_state in {"pending", "sent"}:
                    alert_state = "cleared"
                    delivery_reason = "DELIVERY_CLEAR_LIFECYCLE_EXIT"
                else:
                    alert_state = "idle"
                    delivery_reason = "DELIVERY_IDLE_LIFECYCLE_EXIT"
                # W1: Cascade to alert state via apply_invalidation (alert_setups)
                inv_type = "SCAN_INVALIDATE_DISQUALIFIED" if (row.get("disqualified") or row.get("grade") == "NO_TRADE") else ("SCAN_STALE_NOT_REAFFIRMED" if lifecycle_state == "stale" else "SCAN_INVALIDATE_THRESHOLD_LOSS")
                self.apply_invalidation(
                    {"setups": alert_setups, "recent": alert_recent},
                    key,
                    inv_type,
                    actor="sync_setup_state.alert",
                    extra_payload={"source": source},
                )
            elif previous_delivery_state in {"pending", "sent"}:
                alert_state = "cleared"
                delivery_reason = "DELIVERY_CLEAR_NOT_ACTIONABLE"
            else:
                alert_state = previous_delivery_state or "idle"
                delivery_reason = "DELIVERY_IDLE_NOOP"
            alert_pending = alert_state == "pending"
            if previous_alert_state != alert_state or alert_previous.get("setupVersion") != setup_version or alert_previous.get("lastSentReadinessVersion") != readiness_version:
                self.append_transition_log(alert_recent, self.transition_log_entry(
                    setup_key=key,
                    symbol=symbol,
                    side=direction,
                    layer="delivery",
                    from_state=previous_alert_state,
                    to_state=alert_state,
                    reason_code=delivery_reason,
                    event_type="SCAN_COMMIT",
                    changed_at=now,
                    setup_version=setup_version,
                    readiness_version=readiness_version,
                    actor="sync_setup_state",
                    payload={"source": source, "sameAlertContext": same_alert_context},
                ), max_items=100)
            alert_setups[key] = {
                "setupKey": key,
                "symbol": symbol,
                "side": direction,
                "state": alert_state,
                "active": active,
                "pending": alert_pending,
                "alertEligible": bool(row.get("alertEligible")),
                "watchlistEligible": bool(row.get("watchlistEligible")),
                "grade": row.get("grade"),
                "setupVersion": setup_version,
                "firstSeenAt": alert_previous.get("firstSeenAt") or now,
                "lastSeenAt": now,
                "updatedAt": now,
                "delivery": {
                    "state": alert_state,
                    "reasonCode": delivery_reason,
                    "changedAt": now if previous_alert_state != alert_state else ((alert_previous.get("delivery") or {}).get("changedAt") if isinstance(alert_previous.get("delivery"), dict) else alert_previous.get("updatedAt") or now),
                    "lastSentReadinessVersion": alert_previous.get("lastSentReadinessVersion"),
                },
                "lastVersionSent": alert_previous.get("lastVersionSent"),
                "lastSentReadinessVersion": alert_previous.get("lastSentReadinessVersion"),
                "lastSentAt": alert_previous.get("lastSentAt"),
                "lastSentEntryZone": alert_previous.get("lastSentEntryZone"),
                "lastSentStatus": alert_previous.get("lastSentStatus"),
                "lastSentEntryStyle": alert_previous.get("lastSentEntryStyle"),
                "lastSentDoNotChase": alert_previous.get("lastSentDoNotChase"),
                "lastSentPrice": alert_previous.get("lastSentPrice"),
                "lastClearedAt": now if alert_state == "cleared" else alert_previous.get("lastClearedAt"),
            }

        for key, previous in list(setups.items()):
            if key in updated_keys or not isinstance(previous, dict):
                continue
            symbol = str(previous.get("symbol") or "").upper()
            next_lifecycle_state = "invalidated" if symbol in seen_symbols else "stale"
            next_state = self.legacy_setup_state(next_lifecycle_state)
            was_active = bool(previous.get("active"))
            previous_lifecycle_state = self.setup_lifecycle_state(previous)
            previous_readiness_state = self.setup_readiness_state(previous)
            if previous_lifecycle_state != next_lifecycle_state or was_active:
                transitions += 1
                self.append_transition_log(setup_recent, self.transition_log_entry(
                    setup_key=key,
                    symbol=symbol,
                    side=previous.get("side"),
                    layer="lifecycle",
                    from_state=previous_lifecycle_state,
                    to_state=next_lifecycle_state,
                    reason_code="SCAN_STALE_NOT_REAFFIRMED" if next_lifecycle_state == "stale" else "SCAN_INVALIDATE_THRESHOLD_LOSS",
                    event_type="SCAN_COMMIT",
                    changed_at=now,
                    setup_version=previous.get("setupVersion"),
                    readiness_version=previous.get("readinessVersion"),
                    actor="sync_setup_state",
                    payload={"source": source},
                ), max_items=100)
            if previous_readiness_state is not None:
                transitions += 1
                self.append_transition_log(setup_recent, self.transition_log_entry(
                    setup_key=key,
                    symbol=symbol,
                    side=previous.get("side"),
                    layer="readiness",
                    from_state=previous_readiness_state,
                    to_state=None,
                    reason_code="READINESS_CLEARED_LIFECYCLE_EXIT",
                    event_type="SCAN_COMMIT",
                    changed_at=now,
                    setup_version=previous.get("setupVersion"),
                    readiness_version=previous.get("readinessVersion"),
                    actor="sync_setup_state",
                    payload={"source": source},
                ), max_items=100)
            # W1: Cascade stale/invalidated to watchlist via apply_invalidation (unseen loop)
            if next_lifecycle_state in ("stale", "invalidated") and previous_lifecycle_state != next_lifecycle_state:
                self.apply_invalidation(
                    {"setups": setups, "recent": setup_recent},
                    key,
                    "SCAN_STALE_NOT_REAFFIRMED" if next_lifecycle_state == "stale" else "SCAN_INVALIDATE_THRESHOLD_LOSS",
                    actor="sync_setup_state.unseen",
                    extra_payload={"source": source},
                )
            previous["active"] = False
            previous["updatedAt"] = now
            previous["lastTransitionAt"] = now
            previous["inactiveReason"] = "symbol missing from active scan universe" if next_state == "stale" else "setup no longer passed current thresholds"
            previous["lifecycle"] = {
                "state": next_lifecycle_state,
                "reasonCode": "SCAN_STALE_NOT_REAFFIRMED" if next_lifecycle_state == "stale" else "SCAN_INVALIDATE_THRESHOLD_LOSS",
                "changedAt": now,
            }
            previous["readiness"] = {
                "state": None,
                "reasonCode": "READINESS_CLEARED_LIFECYCLE_EXIT",
                "changedAt": now,
                "readinessVersion": None,
            }
            previous["readinessVersion"] = None
            setups[key] = previous

            alert_previous = alert_setups.get(key, {}) if isinstance(alert_setups.get(key), dict) else {}
            if alert_previous:
                previous_delivery_state = self.delivery_state(alert_previous) or "idle"
                next_alert_state = "cleared" if previous_delivery_state in {"pending", "sent"} else "idle"
                if previous_delivery_state != next_alert_state:
                    self.append_transition_log(alert_recent, self.transition_log_entry(
                        setup_key=key,
                        symbol=symbol,
                        side=previous.get("side"),
                        layer="delivery",
                        from_state=previous_delivery_state,
                        to_state=next_alert_state,
                        reason_code="DELIVERY_CLEAR_LIFECYCLE_EXIT",
                        event_type="SCAN_COMMIT",
                        changed_at=now,
                        setup_version=previous.get("setupVersion"),
                        readiness_version=alert_previous.get("lastSentReadinessVersion") or previous.get("readinessVersion"),
                        actor="sync_setup_state",
                        payload={"source": source},
                    ), max_items=100)
                alert_previous["active"] = False
                alert_previous["updatedAt"] = now
                alert_previous["delivery"] = {
                    "state": next_alert_state,
                    "reasonCode": "DELIVERY_CLEAR_LIFECYCLE_EXIT" if next_alert_state == "cleared" else "DELIVERY_IDLE_LIFECYCLE_EXIT",
                    "changedAt": now,
                    "lastSentReadinessVersion": alert_previous.get("lastSentReadinessVersion"),
                }
                if next_alert_state == "cleared":
                    alert_previous["lastClearedAt"] = now
                alert_setups[key] = alert_previous

        watchlist["lastScanAt"] = now
        watchlist["scanCount"] = int(watchlist.get("scanCount", 0) or 0) + 1
        watchlist["recent"] = setup_recent[-100:]
        alerts["lastSyncAt"] = now
        alerts["recent"] = alert_recent[-100:]

        self.save_watchlist_state(watchlist)
        self.save_alert_state(alerts)
        status = self.setup_state_status()
        return {
            "saved": True,
            "watchlistPath": str(WATCHLIST_STATE_PATH),
            "alertPath": str(ALERT_STATE_PATH),
            "lastScanAt": now,
            "transitions": transitions,
            "trackedSetups": status.get("setupsTracked"),
            "activeSetups": status.get("activeSetups"),
            "pendingAlerts": status.get("pendingAlerts"),
            "stateCounts": status.get("stateCounts"),
        }

    def mark_alert_sent(self, symbol: str, side: str, setup_version: Optional[str] = None, notes: Optional[str] = None) -> Dict[str, Any]:
        symbol = symbol.upper()
        side = side.lower().strip()
        if side not in {"long", "short"}:
            raise RuntimeError("side must be long or short")
        key = self.setup_state_key(symbol, side)
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        alert_setups = alerts.setdefault("setups", {})
        record = alert_setups.get(key)
        if not isinstance(record, dict):
            raise RuntimeError(f"No alert state found for {key}")
        now = datetime.now(timezone.utc).isoformat()
        version = setup_version or record.get("setupVersion")
        setups = watchlist.setdefault("setups", {})
        setup_record = setups.get(key)
        actionable = setup_record.get("actionableAlert") if isinstance(setup_record, dict) and isinstance(setup_record.get("actionableAlert"), dict) else {}
        readiness_state = self.setup_readiness_state(setup_record)
        readiness_version = self.readiness_version_from_actionable(symbol, side, actionable)
        previous_delivery_state = self.delivery_state(record)
        previous_lifecycle_state = self.setup_lifecycle_state(setup_record)

        record["active"] = True
        record["lastVersionSent"] = version
        record["lastSentReadinessVersion"] = readiness_version
        record["lastSentAt"] = now
        record["updatedAt"] = now
        record["lastSentEntryZone"] = self._normalized_zone(actionable.get("entryZone")) if actionable else record.get("lastSentEntryZone")
        record["lastSentStatus"] = readiness_state
        record["lastSentEntryStyle"] = actionable.get("entryStyle") if actionable else record.get("lastSentEntryStyle")
        record["lastSentDoNotChase"] = actionable.get("doNotChase") if actionable else record.get("lastSentDoNotChase")
        record["lastSentPrice"] = self._safe_float(actionable.get("currentPrice")) if actionable else record.get("lastSentPrice")
        record["delivery"] = {
            "state": "sent",
            "reasonCode": "DELIVERY_SENT",
            "changedAt": now,
            "lastSentReadinessVersion": readiness_version,
            "lastSentStatus": readiness_state,
        }
        if notes:
            record["notes"] = notes
        alert_setups[key] = record
        self.append_transition_log(alerts.setdefault("recent", []), self.transition_log_entry(
            setup_key=key,
            symbol=symbol,
            side=side,
            layer="delivery",
            from_state=previous_delivery_state,
            to_state="sent",
            reason_code="DELIVERY_SENT",
            event_type="DELIVERY_ACK",
            changed_at=now,
            setup_version=version,
            readiness_version=readiness_version,
            actor="mark_alert_sent",
            payload={"notes": notes or ""},
        ), max_items=100)
        self.save_alert_state(alerts)

        if isinstance(setup_record, dict) and setup_record.get("setupVersion") == version and setup_record.get("alertEligible"):
            setup_record["active"] = True
            setup_record["updatedAt"] = now
            setup_record["lastTransitionAt"] = now
            setup_record["lifecycle"] = {
                "state": "notified",
                "reasonCode": "DELIVERY_SENT",
                "changedAt": now,
            }
            setup_record["readiness"] = {
                **(setup_record.get("readiness") if isinstance(setup_record.get("readiness"), dict) else {}),
                "state": readiness_state,
                "reasonCode": "SCAN_REAFFIRM_RETEST_READY" if readiness_state == "retest_ready" else ((setup_record.get("readiness") or {}).get("reasonCode") if isinstance(setup_record.get("readiness"), dict) else None),
                "changedAt": ((setup_record.get("readiness") or {}).get("changedAt") if isinstance(setup_record.get("readiness"), dict) else now),
                "readinessVersion": readiness_version,
            }
            setup_record["readinessVersion"] = readiness_version
            setups[key] = setup_record
            self.append_transition_log(watchlist.setdefault("recent", []), self.transition_log_entry(
                setup_key=key,
                symbol=symbol,
                side=side,
                layer="lifecycle",
                from_state=previous_lifecycle_state,
                to_state="notified",
                reason_code="DELIVERY_SENT",
                event_type="DELIVERY_ACK",
                changed_at=now,
                setup_version=version,
                readiness_version=readiness_version,
                actor="mark_alert_sent",
                payload={"source": "manual_mark_alert_sent"},
            ), max_items=100)
            self.save_watchlist_state(watchlist)

        return {
            "saved": True,
            "setupKey": key,
            "setupVersion": version,
            "alertState": record,
            "setupState": self.setup_state_status(symbol),
        }

    # ─── Live Trigger Engine ───────────────────────────────────────────────

    def load_live_trigger_state(self) -> Dict[str, Any]:
        raw_state = load_json_file(LIVE_TRIGGER_STATE_PATH, {
            "updatedAt": None,
            "startedAt": None,
            "lastHeartbeatAt": None,
            "lastWsMessageAt": None,
            "status": "idle",
            "config": {},
            "monitors": {},
            "recent": [],
            "lastError": None,
            "metrics": {},
        })
        return self._normalize_live_trigger_state_payload(raw_state)

    def clean_live_trigger_state_payload(self, state: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, int]]:
        payload = dict(state)
        payload.pop("migrationDiagnostics", None)
        counts = {
            "runtimeStateAliasRemoved": 0,
            "runtimeInactiveReasonAliasRemoved": 0,
            "lifecycleStateAliasRemoved": 0,
            "readinessStateAliasRemoved": 0,
        }
        monitors = payload.get("monitors") if isinstance(payload.get("monitors"), dict) else {}
        if monitors:
            cleaned_monitors: Dict[str, Any] = {}
            for setup_key, record in monitors.items():
                monitor = dict(record) if isinstance(record, dict) else {}
                if "state" in monitor:
                    counts["runtimeStateAliasRemoved"] += 1
                    monitor.pop("state", None)
                runtime = monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}
                if runtime.get("reasonCode") and "inactiveReason" in monitor:
                    counts["runtimeInactiveReasonAliasRemoved"] += 1
                    monitor.pop("inactiveReason", None)
                if "lifecycleState" in monitor:
                    counts["lifecycleStateAliasRemoved"] += 1
                    monitor.pop("lifecycleState", None)
                if "readinessState" in monitor:
                    counts["readinessStateAliasRemoved"] += 1
                    monitor.pop("readinessState", None)
                cleaned_monitors[str(setup_key)] = monitor
            payload["monitors"] = cleaned_monitors
        return payload, counts

    def save_live_trigger_state(self, state: Dict[str, Any]) -> None:
        payload, _counts = self.clean_live_trigger_state_payload(state)
        payload["updatedAt"] = utc_now_iso()
        state["updatedAt"] = payload["updatedAt"]
        save_json_file(LIVE_TRIGGER_STATE_PATH, payload)

    def live_trigger_transition(
        self,
        state: Dict[str, Any],
        setup_key: str,
        new_state: str,
        payload: Optional[Dict[str, Any]] = None,
        reason_code: str = "RUNTIME_TRANSITION",
        event_type: str = "RUNTIME_EVENT",
        actor: str = "live_trigger_transition",
    ) -> bool:
        """
        B2: Enforce allowed runtime transitions.

        Returns True if transition was applied, False if rejected/coerced.

        Self-loop (previous == new_state): update metadata only, NO transition log.
        Illegal transition: REJECT + LOG illegal attempt, DO NOT apply new state.
        "COERCE_inactive": apply transition to inactive instead.
        "HARD_INVALIDATION": any → inactive is always allowed.
        "SETUP_CHANGED": notified → inactive when lifecycle/readiness changed after alert.
        "ZONE_ESCAPE": zone_touched → armed when price exits zone.
        "ALERT_EXPIRED": notified → armed when alert is old (recycle).
        "COOLDOWN_EXPIRED": cooldown → armed when cooldown passed and target still valid.
        "TARGET_REMOVED": cooldown → inactive when target is removed from watchlist.
        "TARGET_REARMED": inactive → armed when setup re-enters watchlist.
        "ZONE_TOUCH_CONFIRMED": armed → zone_touched.
        "TRIGGER_CONFIRMED": zone_touched → confirmed.
        "DELIVERY_SENT": confirmed → notified.
        "COOLDOWN_STARTED": notified → cooldown.
        "SELF-LOOP": same state, update metadata only.
        "ILLEGAL_TRANSITION": logged and rejected.
        Note: transitions NOT explicitly listed in the matrix are treated as ILLEGAL (whitelist).
        """
        monitors = state.setdefault("monitors", {})
        monitor = monitors.setdefault(setup_key, {})
        previous = self.runtime_state(monitor) or "inactive"
        canonical_new_state = self.canonical_runtime_state(new_state) or "inactive"
        changed_at = utc_now_iso()

        # SELF-LOOP: same state — update metadata only, no transition log
        if previous == canonical_new_state:
            runtime = monitor.setdefault("runtime", {})
            runtime.update({
                "state": canonical_new_state,
                "reasonCode": reason_code,
                "eventType": event_type,
                "changedAt": changed_at,
            })
            monitor["updatedAt"] = utc_now_iso()
            if payload:
                monitor.update(payload)
            return True  # applied as self-loop

        # Look up transition policy
        # B2: transitions NOT explicitly listed in the matrix for a given from_state
        # are treated as ILLEGAL (REJECT). This is a whitelist approach.
        from_row = RUNTIME_ALLOWED_TRANSITIONS.get(previous, {})
        policy = from_row.get(canonical_new_state)

        # Special case: zone_touched→armed with ZONE_ESCAPE is allowed (price escaped zone)
        if (policy == "REJECT" and previous == "zone_touched" and canonical_new_state == "armed"
                and reason_code == "ZONE_ESCAPE"):
            monitor["runtime"] = {
                **(monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}),
                "state": canonical_new_state,
                "reasonCode": reason_code,
                "eventType": event_type,
                "changedAt": changed_at,
            }
            monitor["updatedAt"] = changed_at
            if payload:
                monitor.update(payload)
            self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                setup_key=setup_key,
                symbol=monitor.get("symbol"),
                side=monitor.get("side"),
                layer="runtime",
                from_state=previous,
                to_state=canonical_new_state,
                reason_code=reason_code,
                event_type=event_type,
                changed_at=changed_at,
                setup_version=monitor.get("setupVersion"),
                readiness_version=monitor.get("readinessVersion"),
                actor=actor,
                payload=payload or {},
            ), max_items=200)
            return True

        # Special case: notified→armed with ALERT_EXPIRED is an allowed escape (alert recycled)
        if (policy == "REJECT" and previous == "notified" and canonical_new_state == "armed"
                and reason_code == "ALERT_EXPIRED"):
            monitor["runtime"] = {
                **(monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}),
                "state": canonical_new_state,
                "reasonCode": reason_code,
                "eventType": event_type,
                "changedAt": changed_at,
            }
            monitor["updatedAt"] = changed_at
            if payload:
                monitor.update(payload)
            self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                setup_key=setup_key,
                symbol=monitor.get("symbol"),
                side=monitor.get("side"),
                layer="runtime",
                from_state=previous,
                to_state=canonical_new_state,
                reason_code=reason_code,
                event_type=event_type,
                changed_at=changed_at,
                setup_version=monitor.get("setupVersion"),
                readiness_version=monitor.get("readinessVersion"),
                actor=actor,
                payload=payload or {},
            ), max_items=200)
            return True

        # policy is None means transition not in matrix → REJECT
        if policy is None or policy == "REJECT" or policy == "ILLEGAL_TRANSITION":
            self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                setup_key=setup_key,
                symbol=monitor.get("symbol"),
                side=monitor.get("side"),
                layer="runtime",
                from_state=previous,
                to_state=canonical_new_state,
                reason_code=f"ILLEGAL_TRANSITION:{reason_code}",
                event_type="TRANSITION_REJECTED",
                changed_at=changed_at,
                setup_version=monitor.get("setupVersion"),
                readiness_version=monitor.get("readinessVersion"),
                actor=actor,
                payload={"attempted": canonical_new_state, **(payload or {})},
            ), max_items=200)
            return False

        # COERCE_inactive: apply transition to 'inactive' instead
        # Exception: ZONE_ESCAPE and ALERT_EXPIRED are allowed escapes from zone_touched/notified
        if policy == "COERCE_inactive":
            if reason_code in ("ZONE_ESCAPE", "ALERT_EXPIRED"):
                # These special reason codes are allowed escapes — apply as normal transition
                monitor["runtime"] = {
                    **(monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}),
                    "state": canonical_new_state,
                    "reasonCode": reason_code,
                    "eventType": event_type,
                    "changedAt": changed_at,
                }
                monitor["updatedAt"] = changed_at
                if payload:
                    monitor.update(payload)
                self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                    setup_key=setup_key,
                    symbol=monitor.get("symbol"),
                    side=monitor.get("side"),
                    layer="runtime",
                    from_state=previous,
                    to_state=canonical_new_state,
                    reason_code=reason_code,
                    event_type=event_type,
                    changed_at=changed_at,
                    setup_version=monitor.get("setupVersion"),
                    readiness_version=monitor.get("readinessVersion"),
                    actor=actor,
                    payload=payload or {},
                ), max_items=200)
                return True
            coerced_state = "inactive"
            coerced_reason = f"COERCED_FROM_{previous.upper()}:{reason_code}"
            monitor["runtime"] = {
                **(monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}),
                "state": coerced_state,
                "reasonCode": coerced_reason,
                "eventType": "COERCED_TRANSITION",
                "changedAt": changed_at,
            }
            monitor["updatedAt"] = changed_at
            if payload:
                monitor.update(payload)
            self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                setup_key=setup_key,
                symbol=monitor.get("symbol"),
                side=monitor.get("side"),
                layer="runtime",
                from_state=previous,
                to_state=coerced_state,
                reason_code=coerced_reason,
                event_type="COERCED_TRANSITION",
                changed_at=changed_at,
                setup_version=monitor.get("setupVersion"),
                readiness_version=monitor.get("readinessVersion"),
                actor=actor,
                payload={"originalAttempted": canonical_new_state, **(payload or {})},
            ), max_items=200)
            return False

        # ALLOWED: apply the transition normally
        monitor["runtime"] = {
            **(monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}),
            "state": canonical_new_state,
            "reasonCode": reason_code,
            "eventType": event_type,
            "changedAt": changed_at,
        }
        monitor["updatedAt"] = changed_at
        if payload:
            monitor.update(payload)
        self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
            setup_key=setup_key,
            symbol=monitor.get("symbol"),
            side=monitor.get("side"),
            layer="runtime",
            from_state=previous,
            to_state=canonical_new_state,
            reason_code=reason_code,
            event_type=event_type,
            changed_at=changed_at,
            setup_version=monitor.get("setupVersion"),
            readiness_version=monitor.get("readinessVersion"),
            actor=actor,
            payload=payload or {},
        ), max_items=200)
        return True

    def evaluate_cooldown_exit(
        self,
        state: Dict[str, Any],
        setup_key: str,
        cooldown_seconds: int,
        target_record: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        B3: Deterministic cooldown exit evaluation.

        Evaluates cooldown exit conditions for a monitor in 'cooldown' state and
        performs the appropriate state transition (or self-loop if conditions not met).

        Returns the actual new runtime state after evaluation.

        EXIT PRECEDENCE (first match wins):
          1. HARD_INVALIDATION: lifecycle state is 'invalidated' → inactive
          2. TARGET_REMOVED: setup_key not in target_map → inactive
          3. READINESS_STANDBY: readiness state not in COOLDOWN_EXIT_READINESS_ALLOWED → inactive
          4. COOLDOWN_EXPIRED: cooldownUntil passed + lifecycle valid + readiness=retest_ready → armed
          5. SELF-LOOP: cooldownUntil not yet passed → cooldown (no change)

        Args:
            state: Full live_trigger state dict (mutated in-place on transition)
            setup_key: Monitor setup_key to evaluate
            cooldown_seconds: Cooldown duration in seconds (used if cooldownUntil is missing)
            target_record: Optional target record from watchlist scan (for lifecycle/readiness check)

        Returns:
            The resulting runtime state after evaluation ('cooldown', 'armed', or 'inactive')
        """
        monitors = state.get("monitors", {})
        monitor = monitors.get(setup_key, {}) if isinstance(monitors.get(setup_key), dict) else {}

        if self.runtime_state(monitor) != "cooldown":
            return self.runtime_state(monitor) or "inactive"

        now = datetime.now(timezone.utc)
        cooldown_until = parse_iso_timestamp(monitor.get("cooldownUntil"))

        # Compute effective cooldown_until (fallback to cooldownStarted + seconds)
        if not cooldown_until:
            started = parse_iso_timestamp(monitor.get("cooldownStartedAt") or monitor.get("changedAt")) or now
            cooldown_until = started + timedelta(seconds=cooldown_seconds)

        # PRECEDENCE 5: SELF-LOOP — cooldownUntil not yet reached
        if cooldown_until > now:
            # Not yet expired — self-loop: update cooldownStatus but stay in cooldown
            monitor.setdefault("runtime", {})["cooldownStatus"] = "active"
            monitor["updatedAt"] = utc_now_iso()
            return "cooldown"

        # At this point: cooldown has expired (cooldown_until <= now)
        # Determine exit path based on precedence

        # PRECEDENCE 1: HARD_INVALIDATION — lifecycle invalidated
        # target_record has flat lifecycleState; monitor has nested lifecycle.state
        if target_record:
            lifecycle_state = target_record.get("lifecycleState") or target_record.get("state") or "open"
        else:
            lifecycle_state = self.setup_lifecycle_state(monitor)
        if lifecycle_state == "invalidated":
            self.live_trigger_transition(state, setup_key, "inactive", {
                "inactiveAt": utc_now_iso(),
                "cooldownUntil": None,
                "cooldownExpiredAt": utc_now_iso(),
                "cooldownStatus": "exited_hard_invalidation",
            }, reason_code="HARD_INVALIDATION", event_type="COOLDOWN_EXIT", actor="evaluate_cooldown_exit")
            return "inactive"

        # PRECEDENCE 2: TARGET_REMOVED handled in caller (reconcile_live_trigger_targets)
        # Check readiness state for PRECEDENCE 3 and 4
        if target_record:
            readiness_state = target_record.get("readinessState") or "pullback_standby"
        else:
            readiness_state = self.setup_readiness_state(monitor)

        # PRECEDENCE 3: READINESS_STANDBY — readiness not in retest_ready
        if readiness_state in COOLDOWN_EXIT_READINESS_BLOCKED:
            self.live_trigger_transition(state, setup_key, "inactive", {
                "inactiveAt": utc_now_iso(),
                "cooldownUntil": None,
                "cooldownExpiredAt": utc_now_iso(),
                "cooldownStatus": f"exited_readiness_{readiness_state}",
            }, reason_code=f"READINESS_{readiness_state.upper()}", event_type="COOLDOWN_EXIT", actor="evaluate_cooldown_exit")
            return "inactive"

        # PRECEDENCE 4: COOLDOWN_EXPIRED — cooldown passed, lifecycle valid, readiness=retest_ready
        # Clear trigger-related fields and re-arm
        self.live_trigger_mark_reset(monitor, "COOLDOWN_EXPIRED")
        self.live_trigger_transition(state, setup_key, "armed", {
            "armedAt": utc_now_iso(),
            "cooldownUntil": None,
            "cooldownExpiredAt": utc_now_iso(),
            "cooldownStatus": "exited_normal",
            "triggeredAt": None,
            "lastTriggeredPrice": None,
            "lastTriggerCandle": None,
            "triggerReason": None,
            "zoneTouchedAt": None,
            "deliveredEventKeys": [],
            "notificationStatus": None,
            "notificationError": None,
            "lastDeliveryError": None,
            "notificationAttemptedAt": None,
            "notificationSentAt": None,
            "pendingDeliveryKey": None,
            "pendingDeliveryAt": None,
        }, reason_code="COOLDOWN_EXPIRED", event_type="COOLDOWN_EXIT", actor="evaluate_cooldown_exit")
        return "armed"

    def apply_invalidation(
        self,
        state: Dict[str, Any],
        setup_key: str,
        invalidation_type: str,
        actor: str = "apply_invalidation",
        extra_payload: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        B4: Single canonical entry point for ALL invalidation events.

        Cascades invalidation effects across all affected layers:
        - lifecycle.state  (INVALIDATION_TAXONOMY[invalidation_type]["lifecycle"])
        - readiness.state  (INVALIDATION_TAXONOMY[invalidation_type]["readiness"])
        - delivery.state   (INVALIDATION_TAXONOMY[invalidation_type]["delivery"])
        - runtime.state    (INVALIDATION_TAXONOMY[invalidation_type]["runtime"])

        Precedence rules (first match wins):
          1. HARD_INVALIDATION, ZONE_BROKEN, CONTEXT_BROKEN > all others
          2. STALE / TARGET_REMOVED > SOFT_DEMOTION
          3. SOFT_DEMOTION (reversible, non-sticky)

        All sticky invalidations require a new setupVersion to recover.

        Args:
            state: Full live_trigger state dict (mutated in-place)
            setup_key: Monitor/setup key to invalidate
            invalidation_type: Key from INVALIDATION_TAXONOMY
            actor: Who is calling this (for transition log)
            extra_payload: Additional fields to merge into the transition payload

        Returns:
            True if any layer changed, False if no-op (unknown invalidation_type)
        """
        taxonomy = INVALIDATION_TAXONOMY.get(invalidation_type)
        if not taxonomy:
            return False

        # W1: Support both monitors (live_trigger state) and setups (watchlist/alert state)
        monitors = state.get("monitors", {})
        if setup_key not in monitors or not isinstance(monitors.get(setup_key), dict):
            # Fall back to setups (watchlist/alert state) for W1 scanner integration
            monitors = state.get("setups", {})
        monitor = monitors.get(setup_key, {}) if isinstance(monitors.get(setup_key), dict) else {}
        monitor = monitors.get(setup_key, {}) if isinstance(monitors.get(setup_key), dict) else {}
        now = utc_now_iso()
        payload = extra_payload or {}
        payload["invalidationType"] = invalidation_type
        payload["invalidationSticky"] = taxonomy.get("sticky", False)
        changed = False
        self.live_trigger_mark_reset(monitor, invalidation_type)
        self._final_confirmation_reset_on_invalidation(monitor, invalidation_type)

        # --- LIFECYCLE LAYER ---
        lifecycle_effect = taxonomy.get("lifecycle")
        if lifecycle_effect is not None:
            prev_lifecycle_state = self.setup_lifecycle_state(monitor)
            if prev_lifecycle_state != lifecycle_effect:
                self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                    setup_key=setup_key,
                    symbol=monitor.get("symbol"),
                    side=monitor.get("side"),
                    layer="lifecycle",
                    from_state=prev_lifecycle_state,
                    to_state=lifecycle_effect,
                    reason_code=invalidation_type,
                    event_type="INVALIDATION",
                    changed_at=now,
                    setup_version=monitor.get("setupVersion"),
                    readiness_version=monitor.get("readinessVersion"),
                    actor=actor,
                    payload=payload,
                ), max_items=200)
                # Write to monitor's nested lifecycle record
                nested = monitor.setdefault("lifecycle", {})
                nested["state"] = lifecycle_effect
                nested["invalidationReason"] = invalidation_type
                nested["invalidatedAt"] = now
                nested["changedAt"] = now
                changed = True

        # --- READINESS LAYER ---
        readiness_effect = taxonomy.get("readiness")
        if "readiness" in taxonomy:  # always process (None=clear, str=transition, None vs absent matters)
            prev_readiness_state = self.setup_readiness_state(monitor)
            target_readiness = readiness_effect  # None = clear readiness
            if prev_readiness_state != target_readiness:
                self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
                    setup_key=setup_key,
                    symbol=monitor.get("symbol"),
                    side=monitor.get("side"),
                    layer="readiness",
                    from_state=prev_readiness_state,
                    to_state=target_readiness,
                    reason_code=invalidation_type,
                    event_type="INVALIDATION",
                    changed_at=now,
                    setup_version=monitor.get("setupVersion"),
                    readiness_version=monitor.get("readinessVersion"),
                    actor=actor,
                    payload=payload,
                ), max_items=200)
            if target_readiness is None:
                # Hard/sticky: clear readiness record entirely
                if "readiness" in monitor:
                    del monitor["readiness"]
                if invalidation_type in INVALIDATION_STICKY:
                    monitor["readinessVersion"] = None
            else:
                nested = monitor.setdefault("readiness", {})
                nested["state"] = target_readiness
                nested["invalidationReason"] = invalidation_type
                nested["changedAt"] = now
                if invalidation_type in INVALIDATION_STICKY:
                    monitor["readinessVersion"] = None
                    nested["readinessVersion"] = None
            changed = True

        # --- DELIVERY LAYER ---
        if invalidation_type in INVALIDATION_CLEAR_DELIVERY:
            prev_delivery_state = self.delivery_state(monitor)
            if prev_delivery_state not in (None, "cleared"):
                nested = monitor.setdefault("delivery", {})
                nested["state"] = "cleared"
                nested["reasonCode"] = invalidation_type
                nested["changedAt"] = now
                monitor["notificationStatus"] = None
                monitor["notificationError"] = None
                monitor["lastDeliveryError"] = None
                monitor["notificationAttemptedAt"] = None
                monitor["notificationSentAt"] = None
                monitor["pendingDeliveryKey"] = None
                monitor["pendingDeliveryAt"] = None
                changed = True

        # --- RUNTIME LAYER ---
        # Always force runtime to inactive on ANY hard/sticky invalidation
        # Also handle SOFT_DEMOTION which sets runtime → inactive
        runtime_effect = taxonomy.get("runtime")
        if runtime_effect == "inactive":
            current_runtime = self.runtime_state(monitor)
            # B4: For invalidation types, force runtime → inactive regardless of current state.
            # All INVALIDATION_STICKY types override matrix policy to HARD_INVALIDATION.
            if current_runtime != "inactive":  # already inactive — no transition needed
                if current_runtime in RUNTIME_MID_STATES | {"armed"}:
                    policy = RUNTIME_ALLOWED_TRANSITIONS.get(current_runtime, {}).get("inactive", "REJECT")
                    # Override policy for invalidation-triggered transitions
                    if invalidation_type in INVALIDATION_STICKY:
                        policy = "HARD_INVALIDATION"
                    if policy in ("HARD_INVALIDATION",):
                        self.live_trigger_transition(state, setup_key, "inactive", {
                            "inactiveAt": now,
                            "cooldownUntil": None,
                            "armedAt": None,
                            "zoneTouchedAt": None,
                            "triggeredAt": None,
                            "lastAlertAt": None,
                            "lastTriggeredPrice": None,
                            "lastTriggerCandle": None,
                            "triggerReason": None,
                            "notificationStatus": None,
                            "notificationError": None,
                            "lastDeliveryError": None,
                            "notificationAttemptedAt": None,
                            "notificationSentAt": None,
                            "pendingDeliveryKey": None,
                            "pendingDeliveryAt": None,
                        }, reason_code=invalidation_type, event_type="INVALIDATION", actor=actor)
                        changed = True
                elif current_runtime == "cooldown":
                    # Cooldown state: hard invalidation always allowed (B2 matrix entry)
                    if invalidation_type in INVALIDATION_STICKY:
                        self.live_trigger_transition(state, setup_key, "inactive", {
                            "inactiveAt": now,
                            "cooldownUntil": None,
                            "armedAt": None,
                            "zoneTouchedAt": None,
                            "triggeredAt": None,
                            "lastAlertAt": None,
                            "lastTriggeredPrice": None,
                            "lastTriggerCandle": None,
                            "triggerReason": None,
                            "notificationStatus": None,
                            "notificationError": None,
                            "lastDeliveryError": None,
                            "notificationAttemptedAt": None,
                            "notificationSentAt": None,
                            "pendingDeliveryKey": None,
                            "pendingDeliveryAt": None,
                        }, reason_code=invalidation_type, event_type="INVALIDATION", actor=actor)
                        changed = True
        return changed

    def live_trigger_metric_add(self, state: Dict[str, Any], name: str, delta: int = 1) -> None:
        metrics = state.setdefault("metrics", {})
        try:
            metrics[name] = int(metrics.get(name, 0) or 0) + int(delta)
        except Exception:
            metrics[name] = int(delta)

    def live_trigger_delivery_key(self, monitor: Dict[str, Any], trigger_candle: Optional[Dict[str, Any]], delivery_mode: str = "ws", event_type: str = "trigger", event_name: str = "default") -> str:
        candle_close_time = None
        if isinstance(trigger_candle, dict):
            candle_close_time = trigger_candle.get("closeTime")
        return "|".join([
            str(event_type or "trigger"),
            str(event_name or "default"),
            str(monitor.get("setupKey") or "unknown"),
            str(monitor.get("setupVersion") or "unknown"),
            str(candle_close_time or "noclose"),
            str(delivery_mode or "ws"),
        ])

    def live_trigger_record_evaluation(
        self,
        monitor: Dict[str, Any],
        *,
        phase: Optional[str] = None,
        outcome: Optional[str] = None,
        reason: Optional[str] = None,
        trigger_candle: Optional[Dict[str, Any]] = None,
        context_outcome: Optional[Dict[str, Any]] = None,
        regime: Optional[Dict[str, Any]] = None,
    ) -> None:
        evaluation = monitor.setdefault("evaluation", {}) if isinstance(monitor, dict) else {}
        now = utc_now_iso()
        evaluation["lastEvaluationAt"] = now
        monitor["lastEvaluation"] = now
        if phase is not None:
            evaluation["lastEvaluationPhase"] = phase
        if trigger_candle is not None:
            evaluation["lastTriggerCandidateCandle"] = trigger_candle
        if context_outcome is not None:
            evaluation["lastContextOutcome"] = context_outcome
        if regime is not None:
            evaluation["lastEvaluationRegime"] = regime
        outcome_changed = False
        if outcome is not None and evaluation.get("lastEvaluationOutcome") != outcome:
            outcome_changed = True
            evaluation["lastEvaluationOutcome"] = outcome
        if reason is not None and evaluation.get("lastEvaluationReason") != reason:
            outcome_changed = True
            evaluation["lastEvaluationReason"] = reason
        if outcome_changed:
            evaluation["lastOutcomeChangedAt"] = now

    def live_trigger_mark_reset(self, monitor: Dict[str, Any], reset_reason: str) -> None:
        evaluation = monitor.setdefault("evaluation", {}) if isinstance(monitor, dict) else {}
        evaluation["lastResetReason"] = str(reset_reason or "unknown")
        evaluation["lastResetAt"] = utc_now_iso()

    def final_confirmation_create(self, monitor: Dict[str, Any]) -> None:
        """Create sticky final confirmation record at confirmed ledger moment.

        Called when trigger|confirmed ledger entry is created.
        Record is anchored to the monitor's setupVersion + readinessVersion at confirm time.
        """
        now = utc_now_iso()
        trigger_candle = (monitor.get("evaluation", {}).get("lastTriggerCandidateCandle")
                          if isinstance(monitor.get("evaluation"), dict) else None)
        entry_price = None
        if isinstance(trigger_candle, dict):
            entry_price = trigger_candle.get("close")
        if entry_price is None:
            entry_price = monitor.get("lastTriggeredPrice")

        monitor["finalConfirmation"] = {
            "state": "active",
            "setupVersion": monitor.get("setupVersion"),
            "readinessVersion": monitor.get("readinessVersion"),
            "confirmedAt": now,
            "triggerReason": monitor.get("triggerReason") or "unknown",
            "entryPriceEstimate": entry_price,
            "sourceRuntimeState": "confirmed",
            "deliveryStatus": "pending",
            "deliveryStatusAt": now,
            "deliveryError": None,
            "notificationSentAt": None,
            "transportMetadata": monitor.get("lastDeliveryTransport") or {},
            "expiresAt": None,
            "expired": False,
            "resetReason": None,
            "resetAt": None,
        }

    def final_confirmation_update_delivery(
        self,
        monitor: Dict[str, Any],
        status: str,
        error: Optional[str] = None,
        notification_sent_at: Optional[str] = None,
        transport: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Update delivery outcome of an existing finalConfirmation.

        status must be "sent" or "failed".
        Does NOT reset or overwrite the record — only patches delivery fields.
        """
        fc = monitor.get("finalConfirmation")
        if not isinstance(fc, dict) or fc.get("state") != "active":
            return
        if status not in ("sent", "failed"):
            return
        now = utc_now_iso()
        fc["deliveryStatus"] = status
        fc["deliveryStatusAt"] = now
        if error is not None:
            fc["deliveryError"] = error
        if notification_sent_at is not None:
            fc["notificationSentAt"] = notification_sent_at
            fc["deliveryStatusAt"] = notification_sent_at
        if transport is not None:
            fc["transportMetadata"] = transport

    def final_confirmation_reset(self, monitor: Dict[str, Any], reason: str) -> None:
        """Reset finalConfirmation to reset state with reason.

        Does NOT create a new record — only transitions state to reset.
        New record creation happens only when a new confirmed ledger entry is created.
        Persists resetAt so later forensic replay can prove the exact reset time.
        """
        fc = monitor.get("finalConfirmation")
        if not isinstance(fc, dict):
            return
        if fc.get("state") == "reset":
            return  # already reset, don't touch
        fc["state"] = "reset"
        fc["resetReason"] = str(reason)
        fc["resetAt"] = utc_now_iso()

    def final_confirmation_maybe_create_or_keep(self, monitor: Dict[str, Any]) -> None:
        """Called at confirmed ledger creation: create if not exists and not active.

        If finalConfirmation.state is "reset" or None → create new.
        If finalConfirmation.state is "active" → do nothing (already confirmed for this version).
        """
        fc = monitor.get("finalConfirmation")
        if isinstance(fc, dict) and fc.get("state") == "active":
            return
        self.final_confirmation_create(monitor)

    def _final_confirmation_reset_on_version_change(self, monitor: Dict[str, Any], reason: str) -> bool:
        """Reset sticky finalConfirmation only when setup identity changes.

        Important: readinessVersion drift alone must NOT reset a committed
        confirmation. readinessVersion is scanner/readiness metadata, while
        setupVersion is the execution identity boundary for committed runtime.

        Returns True if reset was triggered.
        """
        fc = monitor.get("finalConfirmation")
        if not isinstance(fc, dict) or fc.get("state") != "active":
            return False
        sv = monitor.get("setupVersion")
        if fc.get("setupVersion") == sv:
            return False
        self.final_confirmation_reset(monitor, reason)
        return True

    def final_confirmation_identity(self, monitor: Optional[Dict[str, Any]], active_only: bool = False) -> Dict[str, Any]:
        return _final_confirmation_identity(monitor, active_only)

    def _final_confirmation_reset_on_invalidation(self, monitor: Dict[str, Any], reason: str) -> None:
        """Hard invalidation paths: reset finalConfirmation if active.
        """
        fc = monitor.get("finalConfirmation")
        if isinstance(fc, dict) and fc.get("state") == "active":
            self.final_confirmation_reset(monitor, reason)

    def _final_confirmation_reset_on_soft_trigger(self, monitor: Dict[str, Any], reason: str) -> None:
        """Soft reset triggers (context_broken, zone_reclaimed per conservative default).
        """
        fc = monitor.get("finalConfirmation")
        if isinstance(fc, dict) and fc.get("state") == "active":
            self.final_confirmation_reset(monitor, reason)

    def live_trigger_log_delivery_event(
        self,
        state: Dict[str, Any],
        monitor: Dict[str, Any],
        *,
        from_state: Optional[str],
        to_state: Optional[str],
        reason_code: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
        actor: str,
    ) -> None:
        self.append_transition_log(state.setdefault("recent", []), self.transition_log_entry(
            setup_key=monitor.get("setupKey"),
            symbol=monitor.get("symbol"),
            side=monitor.get("side"),
            layer="delivery",
            from_state=from_state,
            to_state=to_state,
            reason_code=reason_code,
            event_type=event_type,
            changed_at=utc_now_iso(),
            setup_version=monitor.get("setupVersion"),
            readiness_version=monitor.get("readinessVersion"),
            actor=actor,
            payload=payload or {},
        ), max_items=200)

    def live_trigger_prepare_delivery(self, state: Dict[str, Any], monitor: Dict[str, Any], delivery_key: str) -> Optional[Dict[str, Any]]:
        delivery_ledger = monitor.setdefault("deliveryLedger", {})
        delivered_keys = [str(key) for key in (monitor.get("deliveredEventKeys") or []) if key]
        previous_status = None
        if isinstance(delivery_ledger.get(delivery_key), dict):
            previous_status = delivery_ledger.get(delivery_key, {}).get("status")
        if delivery_key in delivered_keys or monitor.get("pendingDeliveryKey") == delivery_key or previous_status in {"pending", "sent"}:
            monitor["notificationStatus"] = "suppressed"
            monitor["notificationError"] = None
            self.live_trigger_record_evaluation(monitor, phase="delivery")
            self.live_trigger_log_delivery_event(
                state,
                monitor,
                from_state="sending",
                to_state="suppressed",
                reason_code="DUPLICATE_SUPPRESSED",
                event_type="DELIVERY_SUPPRESSED",
                payload={"deliveryKey": delivery_key},
                actor="live_trigger_prepare_delivery",
            )
            self.live_trigger_metric_add(state, "notificationsSuppressed", 1)
            return {
                "sent": False,
                "deliveryKey": delivery_key,
                "reason": "duplicate_suppressed",
                "message": monitor.get("lastMessage"),
            }

        monitor["pendingDeliveryKey"] = delivery_key
        monitor["pendingDeliveryAt"] = utc_now_iso()
        monitor["notificationAttemptedAt"] = monitor.get("pendingDeliveryAt")
        monitor["notificationStatus"] = "sending"
        monitor["notificationError"] = None
        delivery_ledger[delivery_key] = {
            "status": "pending",
            "preparedAt": monitor.get("pendingDeliveryAt"),
        }
        self.live_trigger_record_evaluation(monitor, phase="delivery")
        self.live_trigger_log_delivery_event(
            state,
            monitor,
            from_state=None,
            to_state="sending",
            reason_code="DELIVERY_ATTEMPTED",
            event_type="DELIVERY_ATTEMPTED",
            payload={"deliveryKey": delivery_key},
            actor="live_trigger_prepare_delivery",
        )
        self.save_live_trigger_state(state)
        return None

    def live_trigger_finalize_delivery(self, state: Dict[str, Any], monitor: Dict[str, Any], delivery_key: str, message: str, metric_name: str = "notificationsSent", transport: Optional[Dict[str, Any]] = None) -> None:
        delivery_ledger = monitor.setdefault("deliveryLedger", {})
        delivered_keys = [str(key) for key in (monitor.get("deliveredEventKeys") or []) if key]
        delivered_keys.append(delivery_key)
        monitor["deliveredEventKeys"] = delivered_keys[-20:]
        monitor["lastDeliveredKey"] = delivery_key
        monitor["pendingDeliveryKey"] = None
        monitor["pendingDeliveryAt"] = None
        monitor["notificationStatus"] = "sent"
        monitor["notificationError"] = None
        monitor["lastDeliveryError"] = None
        monitor["notificationSentAt"] = utc_now_iso()
        monitor["lastMessage"] = message
        if transport is not None:
            monitor["lastDeliveryTransport"] = transport
        delivery_ledger[delivery_key] = {
            **(delivery_ledger.get(delivery_key) if isinstance(delivery_ledger.get(delivery_key), dict) else {}),
            "status": "sent",
            "sentAt": monitor.get("notificationSentAt"),
            "message": message,
            "transport": transport,
        }
        self.live_trigger_record_evaluation(monitor, phase="delivery")
        self.live_trigger_log_delivery_event(
            state,
            monitor,
            from_state="sending",
            to_state="sent",
            reason_code="DELIVERY_SENT",
            event_type="DELIVERY_SENT",
            payload={"deliveryKey": delivery_key, "transport": transport},
            actor="live_trigger_finalize_delivery",
        )
        self.live_trigger_metric_add(state, metric_name, 1)

    def live_trigger_fail_delivery(self, state: Dict[str, Any], monitor: Dict[str, Any], delivery_key: str, error: str, transport: Optional[Dict[str, Any]] = None) -> None:
        delivery_ledger = monitor.setdefault("deliveryLedger", {})
        monitor["pendingDeliveryKey"] = None
        monitor["pendingDeliveryAt"] = None
        monitor["notificationStatus"] = "failed"
        monitor["notificationError"] = error
        monitor["lastDeliveryError"] = error
        if transport is not None:
            monitor["lastDeliveryTransport"] = transport
        delivery_ledger[delivery_key] = {
            **(delivery_ledger.get(delivery_key) if isinstance(delivery_ledger.get(delivery_key), dict) else {}),
            "status": "failed",
            "failedAt": utc_now_iso(),
            "error": error,
            "transport": transport,
        }
        self.live_trigger_record_evaluation(monitor, phase="delivery")
        self.live_trigger_log_delivery_event(
            state,
            monitor,
            from_state="sending",
            to_state="failed",
            reason_code="DELIVERY_FAILED",
            event_type="DELIVERY_FAILED",
            payload={"deliveryKey": delivery_key, "error": error, "transport": transport},
            actor="live_trigger_fail_delivery",
        )
        self.live_trigger_metric_add(state, "notificationFailures", 1)

    def live_trigger_lifecycle_state(self, monitor: Dict[str, Any]) -> Optional[str]:
        lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
        state = lifecycle.get("state")
        return str(state) if state else None

    def live_trigger_lifecycle_open(self, monitor: Dict[str, Any]) -> bool:
        return self.live_trigger_lifecycle_state(monitor) in LIVE_TRIGGER_LIFECYCLE_OPEN_STATES

    def live_trigger_tracking_rows(self, state: Dict[str, Any], targets: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        seen: set = set()
        for row in targets:
            setup_key = row.get("setupKey")
            if not setup_key:
                continue
            rows.append(dict(row))
            seen.add(setup_key)

        for setup_key, monitor in (state.get("monitors") or {}).items():
            if setup_key in seen:
                continue
            if not isinstance(monitor, dict) or not self.live_trigger_lifecycle_open(monitor):
                continue
            symbol = str(monitor.get("symbol") or "").upper()
            if not symbol:
                continue
            rows.append({
                "setupKey": setup_key,
                "symbol": symbol,
                "side": str(monitor.get("side") or "").lower(),
                "setupVersion": monitor.get("setupVersion"),
                "grade": monitor.get("grade"),
                "state": monitor.get("setupState") or "tracking_only",
                "riskFlags": list(monitor.get("riskFlags", []) or []),
                "managementGuidance": list(monitor.get("managementGuidance", []) or []),
                "actionableAlert": monitor.get("actionableAlert") if isinstance(monitor.get("actionableAlert"), dict) else {},
                "trackingOnly": True,
            })
        return rows

    def live_trigger_expected_symbol_count(self, state: Dict[str, Any]) -> int:
        state = state if isinstance(state, dict) else {}
        monitors = state.get("monitors", {}) if isinstance(state.get("monitors"), dict) else {}
        symbols = set()
        for monitor in monitors.values():
            if not isinstance(monitor, dict):
                continue
            if not monitor.get("active") and not self.live_trigger_lifecycle_open(monitor):
                continue
            symbol = str(monitor.get("symbol") or "").upper()
            if symbol:
                symbols.add(symbol)
        return len(symbols)

    def live_trigger_ws_stale_threshold_seconds(self, poll_seconds: int) -> int:
        try:
            poll_seconds = int(poll_seconds)
        except Exception:
            poll_seconds = LIVE_TRIGGER_DEFAULT_POLL_SECONDS
        return max(poll_seconds * 4, 60)

    def live_trigger_raise_on_starved_ws(
        self,
        tracked_rows: List[Dict[str, Any]],
        last_ws_recv_monotonic: float,
        poll_seconds: int,
        *,
        now_monotonic: Optional[float] = None,
    ) -> None:
        symbols = {
            str(row.get("symbol") or "").upper()
            for row in (tracked_rows or [])
            if isinstance(row, dict) and row.get("symbol")
        }
        if not symbols:
            return
        try:
            last_ws_recv_monotonic = float(last_ws_recv_monotonic)
        except Exception:
            return
        if now_monotonic is None:
            now_monotonic = time.monotonic()
        threshold = self.live_trigger_ws_stale_threshold_seconds(poll_seconds)
        gap_seconds = max(0.0, float(now_monotonic) - last_ws_recv_monotonic)
        if gap_seconds > threshold:
            raise RuntimeError(f"ws_message_gap:{gap_seconds:.0f}s")

    def live_trigger_prepare_running_state(
        self,
        state: Dict[str, Any],
        *,
        channel: str,
        target: str,
        trigger_timeframe: str,
        context_timeframe: str,
        poll_seconds: int,
        cooldown_seconds: int,
    ) -> Dict[str, Any]:
        state = dict(state) if isinstance(state, dict) else {}
        state["status"] = "running"
        state["startedAt"] = utc_now_iso()
        state["lastHeartbeatAt"] = None
        state["lastWsMessageAt"] = None
        state["lastErrorAt"] = None
        state["lastRecoveredAt"] = None
        state["recoveryState"] = "none"
        state["config"] = {
            "channel": channel,
            "target": target,
            "triggerTimeframe": trigger_timeframe,
            "contextTimeframe": context_timeframe,
            "pollSeconds": poll_seconds,
            "cooldownSeconds": cooldown_seconds,
        }
        state.setdefault("metrics", {})
        return state

    def live_trigger_update_lifecycle_excursion(self, monitor: Dict[str, Any], candle: Dict[str, Any]) -> None:
        lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
        entry_reference = lifecycle.get("entryReferencePrice")
        if entry_reference in (None, 0, ""):
            return
        try:
            entry_reference = float(entry_reference)
            candle_high = float(candle.get("high") or 0.0)
            candle_low = float(candle.get("low") or 0.0)
        except Exception:
            return
        side = str(monitor.get("side") or "").lower()
        if side == "long":
            favorable_price = candle_high
            adverse_price = candle_low
            favorable_bps = ((favorable_price - entry_reference) / entry_reference) * 10000.0
            adverse_bps = ((entry_reference - adverse_price) / entry_reference) * 10000.0
        elif side == "short":
            favorable_price = candle_low
            adverse_price = candle_high
            favorable_bps = ((entry_reference - favorable_price) / entry_reference) * 10000.0
            adverse_bps = ((adverse_price - entry_reference) / entry_reference) * 10000.0
        else:
            return

        lifecycle["maxFavorablePrice"] = favorable_price if lifecycle.get("maxFavorablePrice") is None else (
            max(float(lifecycle.get("maxFavorablePrice") or favorable_price), favorable_price) if side == "long"
            else min(float(lifecycle.get("maxFavorablePrice") or favorable_price), favorable_price)
        )
        lifecycle["maxAdversePrice"] = adverse_price if lifecycle.get("maxAdversePrice") is None else (
            min(float(lifecycle.get("maxAdversePrice") or adverse_price), adverse_price) if side == "long"
            else max(float(lifecycle.get("maxAdversePrice") or adverse_price), adverse_price)
        )
        lifecycle["maxFavorableBps"] = round(max(float(lifecycle.get("maxFavorableBps") or 0.0), favorable_bps), 3)
        lifecycle["maxAdverseBps"] = round(max(float(lifecycle.get("maxAdverseBps") or 0.0), adverse_bps), 3)

    def initialize_live_trigger_lifecycle(self, monitor: Dict[str, Any], trigger_candle: Optional[Dict[str, Any]]) -> None:
        lifecycle = monitor.setdefault("lifecycle", {})
        entry_reference_price = monitor.get("lastTriggeredPrice")
        if entry_reference_price is None and isinstance(trigger_candle, dict):
            entry_reference_price = trigger_candle.get("close")
        close_time = trigger_candle.get("closeTime") if isinstance(trigger_candle, dict) else None
        lifecycle.update({
            "state": "entered",
            "mode": "simulated_after_alert",
            "assumed": True,
            "startedAt": utc_now_iso(),
            "startCandleCloseTime": close_time,
            "lastProcessedCloseTime": close_time,
            "entryReferencePrice": entry_reference_price,
            "entryReferenceMode": "trigger_close_after_alert",
            "tp1HitAt": None,
            "tp2HitAt": None,
            "tp3HitAt": None,
            "stopHitAt": None,
            "closedAt": None,
            "closeReason": None,
            "lastEvent": "entered",
            "lastEventAt": utc_now_iso(),
            "lastEventPrice": entry_reference_price,
            "lastEventCandleCloseTime": close_time,
            "maxFavorablePrice": entry_reference_price,
            "maxAdversePrice": entry_reference_price,
            "maxFavorableBps": 0.0,
            "maxAdverseBps": 0.0,
        })

    def evaluate_live_trigger_lifecycle(self, monitor: Dict[str, Any], candle: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
        lifecycle_state = str(lifecycle.get("state") or "")
        if lifecycle_state not in LIVE_TRIGGER_LIFECYCLE_OPEN_STATES:
            return None
        if not isinstance(candle, dict) or not candle.get("closed"):
            return None

        close_time = candle.get("closeTime")
        start_close_time = lifecycle.get("startCandleCloseTime")
        if start_close_time and close_time and int(close_time) <= int(start_close_time):
            return None
        last_processed = lifecycle.get("lastProcessedCloseTime")
        if last_processed and close_time and int(close_time) <= int(last_processed):
            return None
        lifecycle["lastProcessedCloseTime"] = close_time
        self.live_trigger_update_lifecycle_excursion(monitor, candle)

        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        tp = alert.get("takeProfit", {}) if isinstance(alert.get("takeProfit"), dict) else {}
        stop_loss = alert.get("stopLoss")
        tp1 = tp.get("tp1")
        tp2 = tp.get("tp2")
        tp3 = tp.get("tp3")
        if stop_loss is None or tp1 is None or tp2 is None or tp3 is None:
            return None

        side = str(monitor.get("side") or "").lower()
        candle_high = float(candle.get("high") or 0.0)
        candle_low = float(candle.get("low") or 0.0)
        candle_close = float(candle.get("close") or 0.0)

        if side == "long":
            stop_hit = candle_low <= float(stop_loss)
            tp1_hit = candle_high >= float(tp1)
            tp2_hit = candle_high >= float(tp2)
            tp3_hit = candle_high >= float(tp3)
        elif side == "short":
            stop_hit = candle_high >= float(stop_loss)
            tp1_hit = candle_low <= float(tp1)
            tp2_hit = candle_low <= float(tp2)
            tp3_hit = candle_low <= float(tp3)
        else:
            return None

        progress_hits = []
        if lifecycle_state == "entered":
            if tp1_hit:
                progress_hits.append("tp1")
            if tp2_hit:
                progress_hits.append("tp2")
            if tp3_hit:
                progress_hits.append("tp3")
        elif lifecycle_state == "tp1":
            if tp2_hit:
                progress_hits.append("tp2")
            if tp3_hit:
                progress_hits.append("tp3")
        elif lifecycle_state == "tp2":
            if tp3_hit:
                progress_hits.append("tp3")

        if stop_hit and progress_hits:
            lifecycle.update({
                "state": "closed",
                "closedAt": utc_now_iso(),
                "closeReason": "ambiguous_stop_vs_target_same_candle",
                "stopHitAt": utc_now_iso(),
                "lastEvent": "closed",
                "lastEventAt": utc_now_iso(),
                "lastEventPrice": candle_close,
                "lastEventCandleCloseTime": close_time,
            })
            return {
                "milestone": "closed",
                "reason": "ambiguous_stop_vs_target_same_candle",
                "price": candle_close,
                "candle": candle,
                "note": "Candle yang sama menyentuh stop dan target, jadi lifecycle ditutup secara konservatif sebagai ambiguous.",
            }

        if stop_hit:
            lifecycle.update({
                "state": "closed",
                "closedAt": utc_now_iso(),
                "closeReason": "stop_loss_hit",
                "stopHitAt": utc_now_iso(),
                "lastEvent": "closed",
                "lastEventAt": utc_now_iso(),
                "lastEventPrice": candle_close,
                "lastEventCandleCloseTime": close_time,
            })
            return {
                "milestone": "closed",
                "reason": "stop_loss_hit",
                "price": candle_close,
                "candle": candle,
                "note": "Stop loss tersentuh pada candle trigger timeframe.",
            }

        if not progress_hits:
            return None

        furthest = progress_hits[-1]
        now_iso = utc_now_iso()
        if furthest == "tp1":
            lifecycle.update({
                "state": "tp1",
                "tp1HitAt": lifecycle.get("tp1HitAt") or now_iso,
                "lastEvent": "tp1",
                "lastEventAt": now_iso,
                "lastEventPrice": candle_close,
                "lastEventCandleCloseTime": close_time,
            })
            return {
                "milestone": "tp1",
                "reason": "tp1_hit",
                "price": candle_close,
                "candle": candle,
                "note": "TP1 tersentuh. Secara workflow manual, idealnya partial pertama sudah diambil.",
            }
        if furthest == "tp2":
            lifecycle.update({
                "state": "tp2",
                "tp1HitAt": lifecycle.get("tp1HitAt") or now_iso,
                "tp2HitAt": lifecycle.get("tp2HitAt") or now_iso,
                "lastEvent": "tp2",
                "lastEventAt": now_iso,
                "lastEventPrice": candle_close,
                "lastEventCandleCloseTime": close_time,
            })
            return {
                "milestone": "tp2",
                "reason": "tp2_hit",
                "price": candle_close,
                "candle": candle,
                "note": "TP2 tersentuh. Tracking ini mengasumsikan TP1 juga sudah tercapai lebih dulu atau pada candle yang sama.",
            }

        lifecycle.update({
            "state": "closed",
            "tp1HitAt": lifecycle.get("tp1HitAt") or now_iso,
            "tp2HitAt": lifecycle.get("tp2HitAt") or now_iso,
            "tp3HitAt": lifecycle.get("tp3HitAt") or now_iso,
            "closedAt": now_iso,
            "closeReason": "tp3_hit",
            "lastEvent": "closed",
            "lastEventAt": now_iso,
            "lastEventPrice": candle_close,
            "lastEventCandleCloseTime": close_time,
        })
        return {
            "milestone": "closed",
            "reason": "tp3_hit",
            "price": candle_close,
            "candle": candle,
            "note": "TP3 tersentuh, jadi lifecycle setup ini ditutup sebagai target completion.",
        }

    def live_trigger_target_lane(self, record: Dict[str, Any]) -> Optional[str]:
        if not isinstance(record, dict):
            return None
        if not record.get("active"):
            return None
        lifecycle_state = self.setup_lifecycle_state(record)
        if lifecycle_state not in {"actionable", "notified"}:
            return None
        readiness_state = self.setup_readiness_state(record)
        if readiness_state in LIVE_TRIGGER_PRIMARY_READINESS_STATES:
            return "primary"
        if readiness_state in LIVE_TRIGGER_STANDBY_READINESS_STATES:
            return "standby"
        return None

    def live_trigger_targets(self, lane: str = "primary") -> List[Dict[str, Any]]:
        watchlist = self.load_watchlist_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        rows: List[Dict[str, Any]] = []
        for key, record in setups.items():
            if not isinstance(record, dict):
                continue
            monitor_lane = self.live_trigger_target_lane(record)
            if monitor_lane != lane:
                continue
            actionable = record.get("actionableAlert") if isinstance(record.get("actionableAlert"), dict) else None
            if not actionable:
                continue
            rows.append({
                "setupKey": key,
                "symbol": str(record.get("symbol") or "").upper(),
                "side": str(record.get("side") or "").lower(),
                "setupVersion": record.get("setupVersion"),
                "readinessVersion": record.get("readinessVersion") or ((record.get("readiness") or {}).get("readinessVersion") if isinstance(record.get("readiness"), dict) else None),
                "grade": record.get("grade"),
                "state": self.setup_lifecycle_state(record),
                "lifecycleState": self.setup_lifecycle_state(record),
                "riskFlags": list(record.get("riskFlags", []) or []),
                "managementGuidance": list(record.get("managementGuidance", []) or []),
                "actionableAlert": actionable,
                "currentStatus": self.legacy_readiness_state(self.setup_readiness_state(record)),
                "readinessState": self.setup_readiness_state(record),
                "monitorLane": monitor_lane,
                "lastSeenAt": record.get("lastSeenAt"),
            })
        rows = sorted(rows, key=lambda row: (self.grade_priority(row.get("grade")), row.get("lastSeenAt") or ""), reverse=True)
        return rows

    def reconcile_live_trigger_targets(self, state: Dict[str, Any], targets: List[Dict[str, Any]], cooldown_seconds: int) -> Dict[str, Dict[str, Any]]:
        now = datetime.now(timezone.utc)
        monitors = state.setdefault("monitors", {})
        target_map = {row["setupKey"]: row for row in targets if row.get("setupKey")}

        for setup_key, target in target_map.items():
            previous = monitors.get(setup_key, {}) if isinstance(monitors.get(setup_key), dict) else {}
            previous_state = self.runtime_state(previous)
            version_changed = previous.get("setupVersion") != target.get("setupVersion")
            readiness_changed = previous.get("readinessVersion") != target.get("readinessVersion")
            monitor = {
                **previous,
                "setupKey": setup_key,
                "symbol": target.get("symbol"),
                "side": target.get("side"),
                "setupVersion": target.get("setupVersion"),
                "readinessVersion": target.get("readinessVersion"),
                "grade": target.get("grade"),
                "riskFlags": target.get("riskFlags", []),
                "managementGuidance": target.get("managementGuidance", []),
                "actionableAlert": target.get("actionableAlert"),
                "setupState": target.get("state"),
                "lifecycleState": target.get("lifecycleState"),
                "currentStatus": target.get("currentStatus"),
                "readinessState": target.get("readinessState"),
                "monitorLane": target.get("monitorLane") or "primary",
                "active": True,
                "trackingOnly": False,
                "removedFromWatchlistAt": None,
                "inactiveAt": None,
                "cooldownSeconds": cooldown_seconds,
                "runtime": previous.get("runtime") if isinstance(previous.get("runtime"), dict) else {},
            }
            monitors[setup_key] = monitor

            # B2: Safe transition enforcement in reconcile
            # Rule: Never force re-arm from mid-committed states (zone_touched/confirmed/notified).
            # If those states need to change, use the proper transition path.
            # Safe transitions from reconcile:
            #   - new monitor (not previous): inactive → armed (TARGET_REARMED)
            #   - version/readiness changed while in inactive or unknown: → armed (TARGET_REARMED),
            #       unless scan truth explicitly demoted readiness from retest_ready to standby
            #   - armed self-loop reaffirm: → armed (TARGET_REARMED)
            #   - zone_touched/confirmed/notified + setupVersion changed:
            #       reset finalConfirmation + inactive (new execution identity)
            #   - zone_touched/confirmed/notified + readinessVersion changed only:
            #       preserve committed runtime + sticky finalConfirmation
            #   - cooldown + cooldown expired: handled separately below
            is_new = not previous
            is_mid_state = previous_state in RUNTIME_MID_STATES
            is_inactive_or_unknown = previous_state in ("inactive", None) or previous_state not in RUNTIME_ALLOWED_TRANSITIONS
            prev_rs = self.monitor_readiness_state(previous)
            curr_rs = self.canonical_readiness_state(target.get("readinessState")) if isinstance(target, dict) else None

            if is_new:
                # Case 1a: Brand-new monitor arms normally, even if current scan is standby.
                self.live_trigger_transition(state, setup_key, "armed", {
                    "armedAt": utc_now_iso(),
                    "zoneTouchedAt": None,
                    "triggeredAt": None,
                    "lastAlertAt": None,
                    "lastTriggeredPrice": None,
                    "lastMessage": None,
                    "lastTriggerCandle": None,
                    "lastContextCandle": None,
                    "cooldownUntil": None,
                    "cooldownExpiredAt": None,
                    "lastEvaluation": None,
                    "triggerReason": None,
                    "inactiveAt": None,
                    "trackingOnly": False,
                    "removedFromWatchlistAt": None,
                    "lifecycle": {},
                    "deliveredEventKeys": [],
                    "notificationStatus": None,
                    "notificationError": None,
                    "lastDeliveryError": None,
                    "notificationAttemptedAt": None,
                    "notificationSentAt": None,
                    "pendingDeliveryKey": None,
                    "pendingDeliveryAt": None,
                }, reason_code="TARGET_REARMED", event_type="SCAN_COMMIT", actor="reconcile_live_trigger_targets")
            elif is_mid_state and version_changed:
                # Case 2: Committed mid-state + setup identity changed → inactive.
                # confirmed/notified must not silently continue under a new
                # execution identity.
                self.live_trigger_mark_reset(monitor, "SETUP_CHANGED_INACTIVE")
                self._final_confirmation_reset_on_version_change(monitor, "setup_version_changed")
                self.live_trigger_transition(state, setup_key, "inactive", {
                    "inactiveAt": utc_now_iso(),
                    "armedAt": None,
                    "zoneTouchedAt": None,
                    "triggeredAt": None,
                    "lastAlertAt": None,
                    "lastTriggeredPrice": None,
                    "lastMessage": None,
                    "lastTriggerCandle": None,
                    "lastContextCandle": None,
                    "cooldownUntil": None,
                    "cooldownExpiredAt": None,
                    "lastEvaluation": None,
                    "triggerReason": None,
                    "lifecycle": {},
                    "deliveredEventKeys": [],
                    "notificationStatus": None,
                    "notificationError": None,
                    "lastDeliveryError": None,
                    "notificationAttemptedAt": None,
                    "notificationSentAt": None,
                    "pendingDeliveryKey": None,
                    "pendingDeliveryAt": None,
                }, reason_code="SETUP_CHANGED_INACTIVE", event_type="SCAN_COMMIT", actor="reconcile_live_trigger_targets")
            elif is_mid_state and readiness_changed:
                # Case 2b: readiness metadata drift only.
                # Preserve sticky confirmation and committed runtime. The
                # latest readiness/readinessVersion stays observable on the
                # monitor, but this alone must not demote LIVE/CONFIRMED to
                # READY for the same setup identity.
                monitor["updatedAt"] = utc_now_iso()
            elif is_inactive_or_unknown and not is_mid_state:
                # Case 1b: previously inactive/unknown monitor. Respect explicit
                # scan demotion from retest_ready → standby before generic re-arm.
                is_demotion = (
                    prev_rs == "retest_ready"
                    and curr_rs in LIVE_TRIGGER_STANDBY_READINESS_STATES
                    and readiness_changed
                )
                if is_demotion:
                    self.apply_invalidation(
                        state,
                        setup_key,
                        "SCAN_DEMOTE_STANDBY",
                        actor="reconcile_live_trigger_targets",
                        extra_payload={
                            "source": "scan_reconcile",
                            "prevReadiness": prev_rs,
                            "currReadiness": curr_rs,
                        },
                    )
                else:
                    self.live_trigger_mark_reset(monitor, "TARGET_REARMED")
                    self.live_trigger_transition(state, setup_key, "armed", {
                        "armedAt": utc_now_iso(),
                        "zoneTouchedAt": None,
                        "triggeredAt": None,
                        "lastAlertAt": None,
                        "lastTriggeredPrice": None,
                        "lastMessage": None,
                        "lastTriggerCandle": None,
                        "lastContextCandle": None,
                        "cooldownUntil": None,
                        "cooldownExpiredAt": None,
                        "lastEvaluation": None,
                        "triggerReason": None,
                        "inactiveAt": None,
                        "trackingOnly": False,
                        "removedFromWatchlistAt": None,
                        "lifecycle": {},
                        "deliveredEventKeys": [],
                        "notificationStatus": None,
                        "notificationError": None,
                        "lastDeliveryError": None,
                        "notificationAttemptedAt": None,
                        "notificationSentAt": None,
                        "pendingDeliveryKey": None,
                        "pendingDeliveryAt": None,
                    }, reason_code="TARGET_REARMED", event_type="SCAN_COMMIT", actor="reconcile_live_trigger_targets")

        for setup_key, monitor in list(monitors.items()):
            if setup_key in target_map:
                continue
            if not isinstance(monitor, dict):
                continue
            if self.live_trigger_lifecycle_open(monitor):
                monitor["active"] = True
                monitor["trackingOnly"] = True
                monitor["removedFromWatchlistAt"] = monitor.get("removedFromWatchlistAt") or utc_now_iso()
                continue
            if self.runtime_state(monitor) != "inactive" or monitor.get("active"):
                monitor["active"] = False
                monitor["trackingOnly"] = False
                self.live_trigger_mark_reset(monitor, "TARGET_REMOVED")
                self.live_trigger_transition(state, setup_key, "inactive", {
                    "inactiveAt": utc_now_iso(),
                    "notificationStatus": None,
                    "notificationError": None,
                    "lastDeliveryError": None,
                    "notificationAttemptedAt": None,
                    "notificationSentAt": None,
                    "pendingDeliveryKey": None,
                    "pendingDeliveryAt": None,
                }, reason_code="TARGET_REMOVED", event_type="SCAN_COMMIT", actor="reconcile_live_trigger_targets")
            else:
                monitor["active"] = False
                monitor["trackingOnly"] = False

        # B3: Evaluate cooldown exit for monitors still in watchlist
        for setup_key, monitor in monitors.items():
            if not isinstance(monitor, dict):
                continue
            if self.runtime_state(monitor) != "cooldown":
                continue
            if setup_key not in target_map:
                # TARGET_REMOVED already handled above — skip
                continue
            # B3: deterministic cooldown exit evaluation
            target_record = target_map.get(setup_key)
            self.evaluate_cooldown_exit(state, setup_key, cooldown_seconds, target_record)

        return {key: value for key, value in target_map.items()}

    def live_trigger_message_text(self, monitor: Dict[str, Any], trigger_timeframe: str = "5m", context_timeframe: str = "15m") -> str:
        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        entry_zone = alert.get("entryZone", {}) if isinstance(alert.get("entryZone"), dict) else {}
        tp = alert.get("takeProfit", {}) if isinstance(alert.get("takeProfit"), dict) else {}
        crowding = alert.get("crowdingSignals", {}) if isinstance(alert.get("crowdingSignals"), dict) else {}
        quality_gate = monitor.get("lastQualityGate", {}) if isinstance(monitor.get("lastQualityGate"), dict) else {}
        risk_flags = list(alert.get("riskFlags", []) or monitor.get("riskFlags", []) or [])
        risk_text = ", ".join(risk_flags) if risk_flags else "none"
        trigger_reason = monitor.get("triggerReason") or alert.get("trigger") or "trigger confirmed"
        trigger_candle = monitor.get("lastTriggerCandle") if isinstance(monitor.get("lastTriggerCandle"), dict) else {}
        trigger_close_time = unix_ms_to_iso(trigger_candle.get("closeTime"))
        quality_parts: List[str] = []
        if quality_gate.get("spreadBps") is not None:
            quality_parts.append(f"spread {quality_gate.get('spreadBps')}bps")
        if quality_gate.get("depthNotionalTop10") is not None:
            quality_parts.append(f"depth {quality_gate.get('depthNotionalTop10')}")
        if quality_gate.get("quoteVolume") is not None:
            quality_parts.append(f"qv {quality_gate.get('quoteVolume')}")
        if quality_gate.get("wsAgeSec") is not None:
            quality_parts.append(f"wsAge {quality_gate.get('wsAgeSec')}s")
        context_candle = monitor.get("lastContextCandle") if isinstance(monitor.get("lastContextCandle"), dict) else {}
        context_close = context_candle.get("close")
        context_verdict = monitor.get("contextVerdict") or quality_gate.get("contextVerdict")
        delivery_mode = str(monitor.get("lastDeliveryMode") or quality_gate.get("deliveryMode") or "ws").upper()
        fallback_flag = bool(quality_gate.get("fallbackMode"))
        extra_note_parts = []
        crowding_note = str(crowding.get("crowdingNote") or "").strip()
        if crowding_note:
            extra_note_parts.append(crowding_note)
        caution_notes = quality_gate.get("cautionNotes") if isinstance(quality_gate, dict) else []
        if caution_notes:
            extra_note_parts.extend([str(note) for note in caution_notes[:2] if note])
        if quality_parts:
            extra_note_parts.append("quality " + ", ".join(quality_parts))
        return (
            f"{monitor.get('symbol')} | {str(monitor.get('side') or '').upper()} TRIGGER CONFIRMED | "
            f"grade {monitor.get('grade')} | mode {delivery_mode} | fallbackMode {fallback_flag} | entryStyle {alert.get('entryStyle')} | "
            f"triggerPrice {monitor.get('lastTriggeredPrice')} | candleCloseTime {trigger_close_time} | triggerTf {trigger_timeframe} | contextTf {context_timeframe} | contextClose {context_close} | "
            f"entryZone {entry_zone.get('low')}-{entry_zone.get('high')} | stopLoss {alert.get('stopLoss')} | "
            f"tp1 {tp.get('tp1')} | tp2 {tp.get('tp2')} | tp3 {tp.get('tp3')} | "
            f"doNotChase {alert.get('doNotChase')} | risk flags {risk_text} | invalidation: {alert.get('invalidation')} | "
            f"whyConfirmed: {trigger_reason} | contextVerdict: {context_verdict} | "
            f"note: {alert.get('executionNote') or ''} {' | '.join(extra_note_parts)} Alert ini konfirmasi trigger, bukan fill manual.".strip()
        )

    def evaluate_live_trigger_context(self, monitor: Dict[str, Any], context_candle: Optional[Dict[str, Any]], context_timeframe: str) -> Dict[str, Any]:
        if not isinstance(context_candle, dict) or not context_candle.get("closed"):
            return {
                "ok": False,
                "reason": f"{context_timeframe} context candle belum tersedia atau belum close",
            }

        side = str(monitor.get("side") or "").lower()
        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        zone = alert.get("entryZone", {}) if isinstance(alert.get("entryZone"), dict) else {}
        if zone.get("low") is None or zone.get("high") is None:
            return {"ok": False, "reason": "entry zone tidak tersedia untuk context check"}

        zone_low = float(zone.get("low"))
        zone_high = float(zone.get("high"))
        zone_mid = (zone_low + zone_high) / 2
        o = float(context_candle.get("open") or 0.0)
        c = float(context_candle.get("close") or 0.0)

        if side == "short":
            if c > zone_high:
                return {"ok": False, "reason": f"{context_timeframe} context close di atas zone high, reclaim belum gagal"}
            if c < o or c <= zone_mid:
                return {"ok": True, "reason": f"{context_timeframe} context masih bearish/di bawah mid-zone"}
            return {"ok": False, "reason": f"{context_timeframe} context belum cukup bearish"}

        if side == "long":
            if c < zone_low:
                return {"ok": False, "reason": f"{context_timeframe} context close di bawah zone low, breakdown belum gagal"}
            if c > o or c >= zone_mid:
                return {"ok": True, "reason": f"{context_timeframe} context masih bullish/di atas mid-zone"}
            return {"ok": False, "reason": f"{context_timeframe} context belum cukup bullish"}

        return {"ok": False, "reason": "side tidak valid untuk context check"}

    def live_trigger_quality_gate(self, symbol: str, trigger_candle: Optional[Dict[str, Any]], context_candle: Optional[Dict[str, Any]], trigger_timeframe: str, context_timeframe: str, state: Optional[Dict[str, Any]] = None, delivery_mode: str = "ws", poll_seconds: int = LIVE_TRIGGER_DEFAULT_POLL_SECONDS, context_verdict: Optional[str] = None) -> Dict[str, Any]:
        hard_block_reasons: List[str] = []
        caution_notes: List[str] = []
        now = datetime.now(timezone.utc)

        trigger_seconds = max(interval_to_seconds(trigger_timeframe), 60)
        context_seconds = max(interval_to_seconds(context_timeframe), trigger_seconds) if context_timeframe else trigger_seconds

        def candle_age_seconds(candle: Optional[Dict[str, Any]]) -> Optional[float]:
            if not isinstance(candle, dict):
                return None
            close_time = candle.get("closeTime")
            if close_time in {None, 0}:
                return None
            try:
                return max(0.0, now.timestamp() - (float(close_time) / 1000.0))
            except Exception:
                return None

        trigger_age = candle_age_seconds(trigger_candle)
        context_age = candle_age_seconds(context_candle)
        if trigger_age is None or trigger_age > max(trigger_seconds * 3, 180):
            hard_block_reasons.append(f"trigger candle {trigger_timeframe} stale")
        if context_timeframe and (context_age is None or context_age > max(context_seconds * 3, 480)):
            hard_block_reasons.append(f"context candle {context_timeframe} stale")

        ws_age_sec = None
        ws_stale = False
        last_ws_message_at = None
        if isinstance(state, dict):
            last_ws_message_at = parse_iso_timestamp(state.get("lastWsMessageAt"))
        if last_ws_message_at:
            ws_age_sec = max(0.0, (now - last_ws_message_at).total_seconds())
        if str(delivery_mode or "ws").lower() == "ws":
            ws_threshold = max(float(poll_seconds) * 4.0, 45.0)
            if ws_age_sec is None or ws_age_sec > ws_threshold:
                ws_stale = True
                hard_block_reasons.append("websocket stream stale")
        elif str(delivery_mode or "ws").lower() == "fallback":
            caution_notes.append("fallback mode aktif, konfirmasi berasal dari REST fallback")

        ticker24h = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["ticker_24h"], {"symbol": symbol.upper()})
        book = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["book_ticker"], {"symbol": symbol.upper()})
        depth = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["depth"], {"symbol": symbol.upper(), "limit": 20})

        try:
            bid = float(book.get("bidPrice", 0))
            ask = float(book.get("askPrice", 0))
            mid = (bid + ask) / 2 if bid and ask else 0.0
            spread_bps = 0.0 if mid == 0 else ((ask - bid) / mid) * 10000
        except Exception:
            spread_bps = None

        try:
            quote_volume = float(ticker24h.get("quoteVolume", 0))
        except Exception:
            quote_volume = None

        depth_notional_top10 = None
        depth_imbalance = None
        try:
            bids = depth.get("bids", []) if isinstance(depth, dict) else []
            asks = depth.get("asks", []) if isinstance(depth, dict) else []
            bid_notional = sum(float(price) * float(size) for price, size in bids[:10])
            ask_notional = sum(float(price) * float(size) for price, size in asks[:10])
            depth_notional_top10 = bid_notional + ask_notional
            depth_imbalance = 0.0 if depth_notional_top10 == 0 else (bid_notional - ask_notional) / depth_notional_top10
        except Exception:
            depth_notional_top10 = None
            depth_imbalance = None

        if spread_bps is None:
            hard_block_reasons.append("spread check unavailable")
        elif spread_bps > 12:
            hard_block_reasons.append("spread too wide for trigger execution")
        elif spread_bps > 6:
            caution_notes.append("spread mulai melebar")

        if depth_notional_top10 is None:
            hard_block_reasons.append("depth check unavailable")
        elif depth_notional_top10 < 1_000_000:
            hard_block_reasons.append("depth terlalu tipis untuk trigger execution")
        elif depth_notional_top10 < 2_000_000:
            caution_notes.append("depth masih tergolong tipis")

        if quote_volume is None:
            caution_notes.append("quote volume unavailable")
        elif quote_volume < 80_000_000:
            hard_block_reasons.append("quote volume terlalu tipis")
        elif quote_volume < 150_000_000:
            caution_notes.append("quote volume tidak terlalu besar")

        return {
            "ok": not hard_block_reasons,
            "checkedAt": utc_now_iso(),
            "deliveryMode": str(delivery_mode or "ws").lower(),
            "fallbackMode": str(delivery_mode or "ws").lower() == "fallback",
            "contextVerdict": context_verdict,
            "triggerCandleAgeSec": round(trigger_age, 2) if trigger_age is not None else None,
            "contextCandleAgeSec": round(context_age, 2) if context_age is not None else None,
            "wsAgeSec": round(ws_age_sec, 2) if ws_age_sec is not None else None,
            "wsStale": ws_stale,
            "spreadBps": round(spread_bps, 2) if spread_bps is not None else None,
            "quoteVolume": round(quote_volume, 2) if quote_volume is not None else None,
            "depthNotionalTop10": round(depth_notional_top10, 2) if depth_notional_top10 is not None else None,
            "depthImbalance": round(depth_imbalance, 4) if depth_imbalance is not None else None,
            "hardBlockReasons": hard_block_reasons,
            "cautionNotes": caution_notes,
        }

    def send_live_trigger_notification(self, state: Dict[str, Any], monitor: Dict[str, Any], channel: str, target: str, trigger_candle: Optional[Dict[str, Any]], trigger_timeframe: str, context_timeframe: str, delivery_mode: str = "ws") -> Dict[str, Any]:
        delivery_key = self.live_trigger_delivery_key(monitor, trigger_candle, delivery_mode=delivery_mode, event_type="trigger", event_name="confirmed")
        suppressed = self.live_trigger_prepare_delivery(state, monitor, delivery_key)
        if suppressed:
            return suppressed

        message = self.live_trigger_message_text(monitor, trigger_timeframe=trigger_timeframe, context_timeframe=context_timeframe)
        command = self.openclaw_message_send_command(channel=channel, target=target, message=message, refresh=False)
        if not command:
            error = self.openclaw_transport_error or "openclaw transport unavailable"
            transport = self.openclaw_transport_info(error=error)
            self.live_trigger_fail_delivery(state, monitor, delivery_key, error, transport=transport)
            self.save_live_trigger_state(state)
            return {
                "sent": False,
                "deliveryKey": delivery_key,
                "reason": "transport_unavailable",
                "error": error,
                "message": message,
                "transport": transport,
            }
        try:
            output = self.run_command(command, expect_json=False)
        except Exception as exc:
            transport = self.openclaw_transport_info(error=str(exc))
            self.live_trigger_fail_delivery(state, monitor, delivery_key, str(exc), transport=transport)
            self.save_live_trigger_state(state)
            return {
                "sent": False,
                "deliveryKey": delivery_key,
                "reason": "send_failed",
                "error": str(exc),
                "message": message,
                "transport": transport,
            }
        transport = self.openclaw_transport_info(error=None)
        self.live_trigger_finalize_delivery(state, monitor, delivery_key, message, metric_name="triggerNotificationsSent", transport=transport)
        self.live_trigger_metric_add(state, "notificationsSent", 1)
        return {
            "sent": True,
            "deliveryKey": delivery_key,
            "message": message,
            "result": output,
            "transport": transport,
        }

    def live_trigger_lifecycle_message_text(self, monitor: Dict[str, Any], event: Dict[str, Any], trigger_timeframe: str = "5m") -> str:
        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        tp = alert.get("takeProfit", {}) if isinstance(alert.get("takeProfit"), dict) else {}
        lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
        milestone = str(event.get("milestone") or "lifecycle").upper()
        reason = str(event.get("reason") or "")
        note = str(event.get("note") or "").strip()
        return (
            f"{monitor.get('symbol')} | {str(monitor.get('side') or '').upper()} LIFECYCLE {milestone} | "
            f"mode {lifecycle.get('mode') or 'unknown'} | assumedFill {lifecycle.get('assumed')} | "
            f"entryRef {lifecycle.get('entryReferencePrice')} | stopLoss {alert.get('stopLoss')} | "
            f"tp1 {tp.get('tp1')} | tp2 {tp.get('tp2')} | tp3 {tp.get('tp3')} | "
            f"eventPrice {event.get('price')} | triggerTf {trigger_timeframe} | reason {reason} | "
            f"note: {note} Ini tracking heuristik pasca-alert, bukan konfirmasi fill manual."
        )

    def send_live_trigger_lifecycle_notification(self, state: Dict[str, Any], monitor: Dict[str, Any], event: Dict[str, Any], channel: str, target: str, trigger_candle: Optional[Dict[str, Any]], trigger_timeframe: str, delivery_mode: str = "ws") -> Dict[str, Any]:
        delivery_key = self.live_trigger_delivery_key(
            monitor,
            trigger_candle,
            delivery_mode=delivery_mode,
            event_type="lifecycle",
            event_name=str(event.get("reason") or event.get("milestone") or "event"),
        )
        suppressed = self.live_trigger_prepare_delivery(state, monitor, delivery_key)
        if suppressed:
            return suppressed

        message = self.live_trigger_lifecycle_message_text(monitor, event, trigger_timeframe=trigger_timeframe)
        command = self.openclaw_message_send_command(channel=channel, target=target, message=message, refresh=False)
        if not command:
            error = self.openclaw_transport_error or "openclaw transport unavailable"
            transport = self.openclaw_transport_info(error=error)
            self.live_trigger_fail_delivery(state, monitor, delivery_key, error, transport=transport)
            self.save_live_trigger_state(state)
            return {
                "sent": False,
                "deliveryKey": delivery_key,
                "reason": "transport_unavailable",
                "error": error,
                "message": message,
                "transport": transport,
            }
        try:
            output = self.run_command(command, expect_json=False)
        except Exception as exc:
            transport = self.openclaw_transport_info(error=str(exc))
            self.live_trigger_fail_delivery(state, monitor, delivery_key, str(exc), transport=transport)
            self.save_live_trigger_state(state)
            return {
                "sent": False,
                "deliveryKey": delivery_key,
                "reason": "send_failed",
                "error": str(exc),
                "message": message,
                "transport": transport,
            }
        transport = self.openclaw_transport_info(error=None)
        self.live_trigger_finalize_delivery(state, monitor, delivery_key, message, metric_name="lifecycleNotificationsSent", transport=transport)
        self.live_trigger_metric_add(state, "notificationsSent", 1)
        return {
            "sent": True,
            "deliveryKey": delivery_key,
            "message": message,
            "result": output,
            "transport": transport,
        }

    def live_trigger_zone_touched(self, monitor: Dict[str, Any], candle: Dict[str, Any]) -> bool:
        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        zone = alert.get("entryZone", {}) if isinstance(alert.get("entryZone"), dict) else {}
        low = zone.get("low")
        high = zone.get("high")
        if low is None or high is None:
            return False
        candle_high = float(candle.get("high") or 0.0)
        candle_low = float(candle.get("low") or 0.0)
        return candle_high >= float(low) and candle_low <= float(high)

    def evaluate_live_trigger_candle(self, monitor: Dict[str, Any], candle: Dict[str, Any], context_candle: Optional[Dict[str, Any]] = None, context_timeframe: str = "15m") -> Dict[str, Any]:
        side = str(monitor.get("side") or "").lower()
        alert = monitor.get("actionableAlert", {}) if isinstance(monitor.get("actionableAlert"), dict) else {}
        zone = alert.get("entryZone", {}) if isinstance(alert.get("entryZone"), dict) else {}
        if zone.get("low") is None or zone.get("high") is None:
            return {
                "ok": False,
                "confirmed": False,
                "outcome": "structure_fail",
                "reason": "entry zone tidak lengkap",
                "candle": candle,
                "contextCandle": context_candle,
                "contextOutcome": None,
            }

        zone_low = float(zone.get("low"))
        zone_high = float(zone.get("high"))
        zone_mid = (zone_low + zone_high) / 2
        o = float(candle.get("open") or 0.0)
        h = float(candle.get("high") or 0.0)
        l = float(candle.get("low") or 0.0)
        c = float(candle.get("close") or 0.0)
        closed = bool(candle.get("closed"))
        touched = self.live_trigger_zone_touched(monitor, candle)
        body = abs(c - o)
        upper_wick = max(0.0, h - max(o, c))
        lower_wick = max(0.0, min(o, c) - l)

        if not touched:
            return {
                "ok": False,
                "confirmed": False,
                "outcome": "no_touch",
                "reason": "candle belum menyentuh entry zone",
                "candle": candle,
                "contextCandle": context_candle,
                "contextOutcome": None,
            }
        if not closed:
            return {
                "ok": False,
                "confirmed": False,
                "outcome": "structure_fail",
                "reason": "candle trigger belum close",
                "candle": candle,
                "contextCandle": context_candle,
                "contextOutcome": None,
            }

        if side == "short":
            bearish_close = c < o
            zone_range = zone_high - zone_low
            # Momentum check — filter doji / flat candles
            high_low_range = h - l if h > l else 0.0
            body_ratio = (abs(c - o) / high_low_range) if high_low_range > 0 else 0.0
            momentum_ok = (body_ratio >= MOMENTUM_BODY_RATIO_MIN) if CONFIRMATION_RELAX_ENABLED.get("momentum_check") else True
            # Soft failed reclaim: close within 20% above zone_low (was: close ≤ zone_low)
            if CONFIRMATION_RELAX_ENABLED.get("soft_failed_reclaim"):
                soft_zone_top = zone_low + zone_range * 0.20
                failed_reclaim = c <= soft_zone_top
            else:
                failed_reclaim = c <= zone_low
            # Soft tested deep: price reached 70% into zone (was: high ≥ zone_mid)
            if CONFIRMATION_RELAX_ENABLED.get("soft_tested_deep"):
                zone_70 = zone_low + zone_range * 0.70
                tested_deep = h >= zone_70
            else:
                tested_deep = h >= zone_mid
            # Minimal wick: any upper wick present (was: upper_wick ≥ zone_range * 0.12)
            if CONFIRMATION_RELAX_ENABLED.get("minimal_wick"):
                wick_ok = upper_wick > 0
            else:
                wick_ok = upper_wick >= max(body * 0.5, zone_range * 0.12)
            if bearish_close and failed_reclaim and tested_deep and wick_ok and momentum_ok:
                context_result = self.evaluate_live_trigger_context(monitor, context_candle, context_timeframe)
                if not context_result.get("ok"):
                    return {
                        "ok": False,
                        "confirmed": False,
                        "outcome": "context_fail",
                        "reason": context_result.get("reason") or "context timeframe tidak lolos",
                        "candle": candle,
                        "contextCandle": context_candle,
                        "contextOutcome": context_result,
                    }
                return {
                    "ok": True,
                    "confirmed": True,
                    "outcome": "confirmed",
                    "price": c,
                    "reason": f"Zone touched, 5m retest gagal reclaim, candle close bearish kembali di bawah area entry, dan {context_result.get('reason')}",
                    "contextVerdict": context_result.get("reason"),
                    "candle": candle,
                    "contextCandle": context_candle,
                    "contextOutcome": context_result,
                }
            short_failures: List[str] = []
            if not bearish_close:
                short_failures.append("close tidak bearish")
            if not failed_reclaim:
                soft_top = (zone_low + zone_range * 0.20) if CONFIRMATION_RELAX_ENABLED.get("soft_failed_reclaim") else zone_low
                short_failures.append(f"close belum reclaim di bawah {soft_top:.4f}")
            if not tested_deep:
                target = (zone_low + zone_range * 0.70) if CONFIRMATION_RELAX_ENABLED.get("soft_tested_deep") else zone_mid
                short_failures.append(f"retest belum cukup dalam (butuh h ≥ {target:.4f})")
            if not wick_ok:
                short_failures.append("upper wick belum cukup valid")
            if not momentum_ok:
                short_failures.append(f"body ratio terlalu kecil ({body_ratio:.1%} < {int(MOMENTUM_BODY_RATIO_MIN*100)}%)")
            return {
                "ok": False,
                "confirmed": False,
                "outcome": "structure_fail",
                "reason": "; ".join(short_failures) or "struktur candle short belum valid",
                "candle": candle,
                "contextCandle": context_candle,
                "contextOutcome": None,
            }
        if side == "long":
            bullish_close = c > o
            zone_range = zone_high - zone_low
            # Momentum check — filter doji / flat candles
            high_low_range = h - l if h > l else 0.0
            body_ratio = (abs(c - o) / high_low_range) if high_low_range > 0 else 0.0
            momentum_ok = (body_ratio >= MOMENTUM_BODY_RATIO_MIN) if CONFIRMATION_RELAX_ENABLED.get("momentum_check") else True
            # Soft failed retest: close within 20% below zone_high (was: close ≥ zone_high)
            if CONFIRMATION_RELAX_ENABLED.get("soft_failed_reclaim"):
                soft_zone_bottom = zone_high - zone_range * 0.20
                failed_retest = c >= soft_zone_bottom
            else:
                failed_retest = c >= zone_high
            # Soft tested deep: price entered zone from above (for long: low dips into zone)
            # Original: l <= zone_mid (low within lower half of zone)
            # Relaxed: l <= zone_high - range*0.20 (low entered zone, within 20% of top)
            if CONFIRMATION_RELAX_ENABLED.get("soft_tested_deep"):
                zone_entered = zone_high - zone_range * 0.20
                tested_deep = l <= zone_entered
            else:
                tested_deep = l <= zone_mid
            # Minimal wick: any lower wick present (was: lower_wick ≥ zone_range * 0.12)
            if CONFIRMATION_RELAX_ENABLED.get("minimal_wick"):
                wick_ok = lower_wick > 0
            else:
                wick_ok = lower_wick >= max(body * 0.5, zone_range * 0.12)
            if bullish_close and failed_retest and tested_deep and wick_ok and momentum_ok:
                context_result = self.evaluate_live_trigger_context(monitor, context_candle, context_timeframe)
                if not context_result.get("ok"):
                    return {
                        "ok": False,
                        "confirmed": False,
                        "outcome": "context_fail",
                        "reason": context_result.get("reason") or "context timeframe tidak lolos",
                        "candle": candle,
                        "contextCandle": context_candle,
                        "contextOutcome": context_result,
                    }
                return {
                    "ok": True,
                    "confirmed": True,
                    "outcome": "confirmed",
                    "price": c,
                    "reason": f"Zone touched, 5m retest gagal breakdown, candle close bullish kembali di atas area entry, dan {context_result.get('reason')}",
                    "contextVerdict": context_result.get("reason"),
                    "candle": candle,
                    "contextCandle": context_candle,
                    "contextOutcome": context_result,
                }
            long_failures: List[str] = []
            if not bullish_close:
                long_failures.append("close tidak bullish")
            if not failed_retest:
                soft_bottom = (zone_high - zone_range * 0.20) if CONFIRMATION_RELAX_ENABLED.get("soft_failed_reclaim") else zone_high
                long_failures.append(f"close belum reclaim di atas {soft_bottom:.4f}")
            if not tested_deep:
                target = (zone_low + zone_range * 0.70) if CONFIRMATION_RELAX_ENABLED.get("soft_tested_deep") else zone_mid
                long_failures.append(f"retest belum cukup dalam (butuh l ≥ {target:.4f})")
            if not wick_ok:
                long_failures.append("lower wick belum cukup valid")
            if not momentum_ok:
                long_failures.append(f"body ratio terlalu kecil ({body_ratio:.1%} < {int(MOMENTUM_BODY_RATIO_MIN*100)}%)")
            return {
                "ok": False,
                "confirmed": False,
                "outcome": "structure_fail",
                "reason": "; ".join(long_failures) or "struktur candle long belum valid",
                "candle": candle,
                "contextCandle": context_candle,
                "contextOutcome": None,
            }
        return {
            "ok": False,
            "confirmed": False,
            "outcome": "structure_fail",
            "reason": f"side {side or 'unknown'} tidak didukung untuk evaluasi trigger",
            "candle": candle,
            "contextCandle": context_candle,
            "contextOutcome": None,
        }

    def rest_fallback_candle(self, symbol: str, interval: str) -> Optional[Dict[str, Any]]:
        rows = self.get(BINANCE_REST, ENDPOINT_MAP["binance_rest"]["klines"], params={
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": 2,
        })
        if not isinstance(rows, list) or not rows:
            return None
        row = rows[-2] if len(rows) >= 2 else rows[-1]
        return {
            "openTime": int(row[0]),
            "closeTime": int(row[6]),
            "open": float(row[1]),
            "high": float(row[2]),
            "low": float(row[3]),
            "close": float(row[4]),
            "closed": True,
        }

    def live_trigger_engine_status(self) -> Dict[str, Any]:
        state = self.load_live_trigger_state()
        pid_from_file = read_pid_file(LIVE_TRIGGER_PID_PATH)
        running_pids = live_trigger_running_pids()
        pid = pid_from_file if pid_from_file in running_pids else (running_pids[0] if running_pids else pid_from_file)
        monitors = state.get("monitors", {}) if isinstance(state.get("monitors"), dict) else {}
        active = [row for row in monitors.values() if isinstance(row, dict) and row.get("active") and self.runtime_state(row) != "inactive"]
        open_lifecycle = [row for row in monitors.values() if isinstance(row, dict) and self.live_trigger_lifecycle_open(row)]
        standby_targets = self.live_trigger_targets(lane="standby")
        derived_status = self._live_trigger_derive_status(state, running_pids)
        return {
            "statePath": str(LIVE_TRIGGER_STATE_PATH),
            "pidPath": str(LIVE_TRIGGER_PID_PATH),
            "logPath": str(LIVE_TRIGGER_LOG_PATH),
            "running": bool(running_pids),
            "pid": pid,
            "pids": running_pids,
            "duplicatePids": running_pids[1:] if len(running_pids) > 1 else [],
            "status": derived_status,
            "persistedStatus": state.get("status"),
            "startedAt": state.get("startedAt"),
            "lastHeartbeatAt": state.get("lastHeartbeatAt"),
            "lastWsMessageAt": state.get("lastWsMessageAt"),
            "lastError": state.get("lastError"),
            "lastErrorAt": state.get("lastErrorAt"),
            "lastRecoveredAt": state.get("lastRecoveredAt"),
            "recoveryState": state.get("recoveryState", "none"),
            "config": state.get("config", {}),
            "metrics": state.get("metrics", {}),
            "activeMonitors": len(active),
            "openLifecycleMonitors": len(open_lifecycle),
            "standbyTargets": len(standby_targets),
            "monitors": active[:20],
            "standby": standby_targets[:20],
            "migrationDiagnostics": state.get("migrationDiagnostics"),
            "recent": (state.get("recent") or [])[-20:],
            "derivedStatus": derived_status,
            "liveTrust": self.live_trigger_live_trust(state, running_pids),
        }

    def live_trigger_live_trust(self, state: Optional[Dict[str, Any]] = None, running_pids: Optional[List[int]] = None) -> Dict[str, Any]:
        state = state if isinstance(state, dict) else self.load_live_trigger_state()
        running = list(running_pids) if isinstance(running_pids, list) else live_trigger_running_pids()
        derived_status = self._live_trigger_derive_status(state, running)
        now = datetime.now(timezone.utc)
        started_at = parse_iso_timestamp(state.get("startedAt"))
        heartbeat = parse_iso_timestamp(state.get("lastHeartbeatAt"))
        ws_msg = parse_iso_timestamp(state.get("lastWsMessageAt"))
        recovered_at = parse_iso_timestamp(state.get("lastRecoveredAt"))
        error_at = parse_iso_timestamp(state.get("lastErrorAt"))
        recovery_state = str(state.get("recoveryState") or "none")
        expected_symbols = self.live_trigger_expected_symbol_count(state)
        heartbeat_stale_sec = 90
        ws_stale_sec = max((state.get("config", {}).get("pollSeconds") or 15) * 4, 60)
        engine_age_sec = (now - started_at).total_seconds() if started_at else None

        heartbeat_missing = heartbeat is None and (engine_age_sec is None or engine_age_sec > heartbeat_stale_sec)
        heartbeat_stale = bool(heartbeat and (now - heartbeat).total_seconds() > heartbeat_stale_sec)
        ws_missing = expected_symbols > 0 and ws_msg is None and (engine_age_sec is None or engine_age_sec > ws_stale_sec)
        ws_stale = expected_symbols > 0 and bool(ws_msg and (now - ws_msg).total_seconds() > ws_stale_sec)
        unrecovered_error = bool(error_at and (not recovered_at or recovered_at < error_at))

        reason = None
        if not running:
            reason = "engine_down"
        elif heartbeat_missing:
            reason = "heartbeat_missing"
        elif heartbeat_stale:
            reason = "heartbeat_stale"
        elif ws_missing:
            reason = "ws_missing"
        elif ws_stale:
            reason = "ws_stale"
        elif recovery_state == "active_error":
            reason = "active_error"
        elif unrecovered_error:
            reason = "unrecovered_error"
        elif derived_status != "healthy":
            reason = f"derived_status_{derived_status}"

        trust_live = derived_status == "healthy"
        return {
            "trustLive": trust_live,
            "blockLive": not trust_live,
            "reason": reason,
            "derivedStatus": derived_status,
            "expectedSymbols": expected_symbols,
            "heartbeatFresh": bool(heartbeat and not heartbeat_stale),
            "wsFresh": True if expected_symbols == 0 else bool(ws_msg and not ws_stale),
            "heartbeatStaleSeconds": heartbeat_stale_sec,
            "wsStaleSeconds": ws_stale_sec,
            "lastHeartbeatAt": state.get("lastHeartbeatAt"),
            "lastWsMessageAt": state.get("lastWsMessageAt"),
            "lastErrorAt": state.get("lastErrorAt"),
            "lastRecoveredAt": state.get("lastRecoveredAt"),
            "recoveryState": recovery_state,
        }

    def _live_trigger_derive_status(self, state: Dict[str, Any], running_pids: List[int]) -> str:
        """Compute current health from live signals, independent of raw lastError."""
        now = datetime.now(timezone.utc)
        started_at = parse_iso_timestamp(state.get("startedAt"))
        heartbeat = parse_iso_timestamp(state.get("lastHeartbeatAt"))
        ws_msg = parse_iso_timestamp(state.get("lastWsMessageAt"))
        recovered_at = parse_iso_timestamp(state.get("lastRecoveredAt"))
        error_at = parse_iso_timestamp(state.get("lastErrorAt"))
        recovery_state = str(state.get("recoveryState") or "none")
        expected_symbols = self.live_trigger_expected_symbol_count(state)

        # Process check
        if not running_pids:
            return "down"

        # Staleness thresholds (seconds)
        heartbeat_stale_sec = 90
        ws_stale_sec = max((state.get("config", {}).get("pollSeconds") or 15) * 4, 60)

        engine_age_sec = (now - started_at).total_seconds() if started_at else None

        # Heartbeat staleness / absence
        if heartbeat is None:
            if engine_age_sec is None or engine_age_sec > heartbeat_stale_sec:
                return "down"
            heartbeat_fresh = False
        else:
            heartbeat_fresh = (now - heartbeat).total_seconds() <= heartbeat_stale_sec

        # Websocket freshness / absence
        if expected_symbols == 0:
            ws_fresh = True
        elif ws_msg is None:
            ws_fresh = False if engine_age_sec is None or engine_age_sec > ws_stale_sec else True
        else:
            ws_fresh = (now - ws_msg).total_seconds() <= ws_stale_sec

        # Active impairment: heartbeat or ws stale
        if not heartbeat_fresh or not ws_fresh:
            return "degraded"

        if recovery_state == "active_error":
            return "degraded"

        # Active impairment: recent un-recovered error (lastError with no recovery timestamp, or error too recent)
        if error_at:
            # If error happened but no recovery recorded yet, degraded until proven recovered
            if not recovered_at or (recovered_at and recovered_at < error_at):
                return "degraded"
            # If last error is older than recovery, check recovery freshness
            recovery_window_sec = 120
            if recovered_at and (now - recovered_at).total_seconds() > recovery_window_sec:
                # Recovery is stale; current signals take precedence
                pass
            elif (now - error_at).total_seconds() < 60 and not recovered_at:
                return "degraded"

        # Recovery state: if we recovered recently and signals are fresh, treat as healthy
        # even if lastError is non-empty (historical evidence only)
        if recovered_at:
            recovery_window_sec = 120
            if (now - recovered_at).total_seconds() <= recovery_window_sec:
                return "healthy"

        # All signals fresh and no active impairment → healthy
        return "healthy"

    def _live_trigger_fallback_poll(
        self,
        state: Dict[str, Any],
        targets: List[Dict[str, Any]],
        trigger_timeframe: str,
        context_timeframe: str,
        poll_seconds: int,
        cooldown_seconds: int,
    ) -> bool:
        """
        Fallback REST polling when WebSocket is down.
        Returns True if at least one target was processed successfully.
        On any exception during fallback, returns False (caller will keep degraded state).
        """
        try:
            tracking_rows = self.live_trigger_tracking_rows(state, targets)
            for target_row in tracking_rows:
                setup_key = target_row.get("setupKey")
                monitor = (state.get("monitors") or {}).get(setup_key, {}) if isinstance(state.get("monitors"), dict) else {}
                if not isinstance(monitor, dict):
                    continue
                candle = self.rest_fallback_candle(target_row["symbol"], trigger_timeframe)
                if not candle:
                    continue
                lifecycle_event = self.evaluate_live_trigger_lifecycle(monitor, candle)
                if lifecycle_event:
                    milestone = str(lifecycle_event.get("milestone") or "lifecycle")
                    metric_name = {
                        "tp1": "lifecycleTp1",
                        "tp2": "lifecycleTp2",
                        "closed": "lifecycleClosed",
                    }.get(milestone, "lifecycleEvents")
                    self.live_trigger_metric_add(state, "lifecycleEvents", 1)
                    self.live_trigger_metric_add(state, metric_name, 1)
                    if lifecycle_event.get("reason") == "stop_loss_hit":
                        self.live_trigger_metric_add(state, "lifecycleStopLoss", 1)
                    if lifecycle_event.get("reason") == "ambiguous_stop_vs_target_same_candle":
                        self.live_trigger_metric_add(state, "lifecycleAmbiguous", 1)
                    delivered_lifecycle = self.send_live_trigger_lifecycle_notification(
                        state,
                        monitor,
                        lifecycle_event,
                        channel=str((state.get("config") or {}).get("channel") or "telegram"),
                        target=str((state.get("config") or {}).get("target") or ""),
                        trigger_candle=candle,
                        trigger_timeframe=trigger_timeframe,
                        delivery_mode="fallback",
                    )
                    if delivered_lifecycle.get("sent"):
                        self.live_trigger_metric_add(state, "lifecycleDelivered", 1)
                    self.production_eval_log_lifecycle_event(setup_key, monitor, lifecycle_event)
            self.live_trigger_metric_add(state, "fallbackPollSuccess", 1)
            self.production_eval_log_funnel_by_regime(state, source="fallback_poll")
            return True
        except Exception:
            self.live_trigger_metric_add(state, "fallbackPollFailure", 1)
            return False

    async def live_trigger_run(self, channel: str, target: str, trigger_timeframe: str = "5m", context_timeframe: str = "15m", poll_seconds: int = LIVE_TRIGGER_DEFAULT_POLL_SECONDS, cooldown_seconds: int = LIVE_TRIGGER_DEFAULT_COOLDOWN_SECONDS) -> None:
        if websockets is None:
            raise RuntimeError("websockets package is not available")

        state = self.live_trigger_prepare_running_state(
            self.load_live_trigger_state(),
            channel=channel,
            target=target,
            trigger_timeframe=trigger_timeframe,
            context_timeframe=context_timeframe,
            poll_seconds=poll_seconds,
            cooldown_seconds=cooldown_seconds,
        )
        self.save_live_trigger_state(state)

        backoff = 3
        while True:
            try:
                state["lastHeartbeatAt"] = utc_now_iso()
                targets = self.live_trigger_targets()
                self.reconcile_live_trigger_targets(state, targets, cooldown_seconds)
                tracked_rows = self.live_trigger_tracking_rows(state, targets)
                self.save_live_trigger_state(state)

                symbols = sorted({row["symbol"].lower() for row in tracked_rows if row.get("symbol")})
                if not symbols:
                    await asyncio.sleep(poll_seconds)
                    continue

                stream_names = {f"{symbol}@kline_{trigger_timeframe}" for symbol in symbols}
                if context_timeframe and context_timeframe != trigger_timeframe:
                    stream_names.update(f"{symbol}@kline_{context_timeframe}" for symbol in symbols)
                streams = "/".join(sorted(stream_names))
                ws_url = f"wss://fstream.binance.com/stream?streams={streams}"
                self.live_trigger_metric_add(state, "wsSessionsStarted", 1)
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                    backoff = 3
                    last_refresh = time.time()
                    last_ws_recv_monotonic = time.monotonic()
                    while True:
                        if time.time() - last_refresh >= poll_seconds:
                            refreshed_targets = self.live_trigger_targets()
                            self.reconcile_live_trigger_targets(state, refreshed_targets, cooldown_seconds)
                            refreshed_rows = self.live_trigger_tracking_rows(state, refreshed_targets)
                            refreshed_symbols = sorted({row["symbol"].lower() for row in refreshed_rows if row.get("symbol")})
                            state["lastHeartbeatAt"] = utc_now_iso()
                            self.production_eval_log_funnel_by_regime(state, source="ws_refresh")
                            self.save_live_trigger_state(state)
                            if refreshed_symbols != symbols:
                                break
                            tracked_rows = refreshed_rows
                            symbols = refreshed_symbols
                            last_refresh = time.time()

                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            self.live_trigger_raise_on_starved_ws(
                                tracked_rows,
                                last_ws_recv_monotonic,
                                poll_seconds,
                            )
                            continue

                        payload = json.loads(raw)
                        last_ws_recv_monotonic = time.monotonic()
                        state["lastWsMessageAt"] = utc_now_iso()
                        self.live_trigger_metric_add(state, "wsMessages", 1)
                        data = payload.get("data", payload) if isinstance(payload, dict) else {}
                        candle_payload = data.get("k") if isinstance(data, dict) else None
                        if not isinstance(candle_payload, dict):
                            continue
                        symbol = str(candle_payload.get("s") or "").upper()
                        candle = {
                            "openTime": int(candle_payload.get("t") or 0),
                            "closeTime": int(candle_payload.get("T") or 0),
                            "open": float(candle_payload.get("o") or 0.0),
                            "high": float(candle_payload.get("h") or 0.0),
                            "low": float(candle_payload.get("l") or 0.0),
                            "close": float(candle_payload.get("c") or 0.0),
                            "closed": bool(candle_payload.get("x")),
                        }
                        interval = str(candle_payload.get("i") or "")

                        for setup_key, monitor in list((state.get("monitors") or {}).items()):
                            if not isinstance(monitor, dict):
                                continue
                            if not monitor.get("active"):
                                continue
                            if str(monitor.get("symbol") or "").upper() != symbol:
                                continue
                            if context_timeframe and context_timeframe != trigger_timeframe and interval == context_timeframe and candle.get("closed"):
                                monitor["lastContextCandle"] = candle
                                self.live_trigger_metric_add(state, "contextUpdates", 1)
                                continue
                            if interval != trigger_timeframe:
                                continue

                            regime = self.production_eval_regime_from_row(monitor)

                            lifecycle_event = self.evaluate_live_trigger_lifecycle(monitor, candle)
                            if lifecycle_event:
                                milestone = str(lifecycle_event.get("milestone") or "lifecycle")
                                metric_name = {
                                    "tp1": "lifecycleTp1",
                                    "tp2": "lifecycleTp2",
                                    "closed": "lifecycleClosed",
                                }.get(milestone, "lifecycleEvents")
                                self.live_trigger_metric_add(state, "lifecycleEvents", 1)
                                self.live_trigger_metric_add(state, metric_name, 1)
                                if lifecycle_event.get("reason") == "stop_loss_hit":
                                    self.live_trigger_metric_add(state, "lifecycleStopLoss", 1)
                                if lifecycle_event.get("reason") == "ambiguous_stop_vs_target_same_candle":
                                    self.live_trigger_metric_add(state, "lifecycleAmbiguous", 1)
                                delivered_lifecycle = self.send_live_trigger_lifecycle_notification(
                                    state,
                                    monitor,
                                    lifecycle_event,
                                    channel=channel,
                                    target=target,
                                    trigger_candle=candle,
                                    trigger_timeframe=trigger_timeframe,
                                    delivery_mode="ws",
                                )
                                if delivered_lifecycle.get("sent"):
                                    monitor["lastLifecycleMessage"] = delivered_lifecycle.get("message")
                                    monitor["lastLifecycleEvent"] = lifecycle_event
                                    monitor["lastLifecycleEventAt"] = utc_now_iso()
                                self.production_eval_log_lifecycle_event(setup_key, monitor, lifecycle_event)
                                self.save_live_trigger_state(state)

                            if self.runtime_state(monitor) not in LIVE_TRIGGER_EVALUATION_STATES:
                                continue

                            self.live_trigger_record_evaluation(monitor, phase="trigger_eval", trigger_candle=candle, regime=regime)
                            self.live_trigger_metric_add(state, "triggerEvaluations", 1)
                            if self.live_trigger_zone_touched(monitor, candle) and self.runtime_state(monitor) == "armed":
                                self.live_trigger_transition(state, setup_key, "zone_touched", {
                                    "zoneTouchedAt": utc_now_iso(),
                                    "lastTouchedPrice": candle.get("close"),
                                }, reason_code="ZONE_TOUCH_CONFIRMED", event_type="TRIGGER_ZONE_TOUCH", actor="live_trigger_run.ws")
                                self.live_trigger_record_evaluation(monitor, phase="zone_touch", outcome="touch_detected", reason="zone touched", trigger_candle=candle, regime=regime)

                            context_candle = monitor.get("lastContextCandle") if isinstance(monitor.get("lastContextCandle"), dict) else None
                            if context_timeframe and context_timeframe != trigger_timeframe and not context_candle:
                                context_candle = self.rest_fallback_candle(symbol, context_timeframe)
                                if context_candle:
                                    monitor["lastContextCandle"] = context_candle

                            evaluated = self.evaluate_live_trigger_candle(monitor, candle, context_candle=context_candle, context_timeframe=context_timeframe)
                            self.live_trigger_record_evaluation(
                                monitor,
                                phase="trigger_eval",
                                outcome=evaluated.get("outcome"),
                                reason=evaluated.get("reason"),
                                trigger_candle=evaluated.get("candle") or candle,
                                context_outcome=evaluated.get("contextOutcome"),
                                regime=regime,
                            )
                            if not evaluated.get("confirmed"):
                                # DEBUG: log where confirmation fails
                                import sys
                                debug_msg = (f"[DEBUG] {setup_key} confirmed=False "
                                    f"outcome={evaluated.get('outcome')} "
                                    f"reason={evaluated.get('reason', '')[:80]}")
                                sys.stdout.write(debug_msg + '\n')
                                sys.stdout.flush()
                                # B4: evaluate zone_broken and context_broken
                                current_rt = self.runtime_state(monitor)
                                if current_rt in RUNTIME_MID_STATES | {"armed"}:
                                    # Check context broken (context candle outside zone = hard invalidation)
                                    context_result = evaluated.get("contextOutcome") if isinstance(evaluated.get("contextOutcome"), dict) else self.evaluate_live_trigger_context(monitor, context_candle, context_timeframe)
                                    ctx_ok = context_result.get("ok") if isinstance(context_result, dict) else None
                                    ctx_reason = context_result.get("reason") if isinstance(context_result, dict) else '?'
                                    sys.stdout.write(f"[DEBUG] {setup_key} context_result ok={ctx_ok} reason={str(ctx_reason)[:60]}\n")
                                    sys.stdout.flush()
                                    if context_result is not None and not context_result.get("ok"):
                                        # B4: context broken at candle close = hard invalidation
                                        self.live_trigger_mark_reset(monitor, "CONTEXT_BROKEN")
                                        self._final_confirmation_reset_on_soft_trigger(monitor, "context_broken")
                                        self.apply_invalidation(state, setup_key, "CONTEXT_BROKEN",
                                            actor="live_trigger_run.ws",
                                            extra_payload={"breakReason": context_result.get("reason")})
                                        self.live_trigger_metric_add(state, "invalidationsContextBroken", 1)
                                        self.save_live_trigger_state(state)
                                        continue
                                    # Check zone broken (zone was touched but conditions not met for confirmed)
                                    # If we are in zone_touched+ and evaluation returns None → zone conditions failed
                                    # This is a ZONE_RECLAIMED soft demotion (not hard invalidation)
                                    if current_rt in RUNTIME_MID_STATES:
                                        self.live_trigger_mark_reset(monitor, "ZONE_RECLAIMED")
                                        self._final_confirmation_reset_on_soft_trigger(monitor, "zone_reclaimed")
                                        self.apply_invalidation(state, setup_key, "ZONE_RECLAIMED",
                                            actor="live_trigger_run.ws",
                                            extra_payload={"breakNote": "zone conditions not met for confirmed"})
                                        self.live_trigger_metric_add(state, "invalidationsZoneReclaimed", 1)
                                        self.save_live_trigger_state(state)
                                continue

                            quality_gate = self.live_trigger_quality_gate(
                                symbol,
                                evaluated.get("candle"),
                                evaluated.get("contextCandle"),
                                trigger_timeframe,
                                context_timeframe,
                                state=state,
                                delivery_mode="ws",
                                poll_seconds=poll_seconds,
                                context_verdict=evaluated.get("contextVerdict"),
                            )
                            monitor["lastQualityGate"] = quality_gate
                            if not quality_gate.get("ok"):
                                hard_reasons = quality_gate.get("hardBlockReasons") or []
                                self.live_trigger_record_evaluation(
                                    monitor,
                                    phase="quality_gate",
                                    outcome="quality_gate_blocked",
                                    reason="; ".join(hard_reasons) or "quality gate blocked",
                                    context_outcome={
                                        "ok": bool(quality_gate.get("ok")),
                                        "reason": quality_gate.get("contextVerdict") or "; ".join(hard_reasons) or "quality gate blocked",
                                    },
                                    regime=regime,
                                )
                                # B4: stale candle = context broken hard invalidation
                                stale_candle = any("stale" in r.lower() for r in hard_reasons)
                                if stale_candle:
                                    self.live_trigger_mark_reset(monitor, "CONTEXT_BROKEN")
                                    self.apply_invalidation(state, setup_key, "CONTEXT_BROKEN",
                                        actor="live_trigger_run.ws",
                                        extra_payload={"breakReason": "; ".join(hard_reasons)})
                                    self.live_trigger_metric_add(state, "invalidationsStaleCandle", 1)
                                    self.save_live_trigger_state(state)
                                    continue
                                monitor["lastBlockedReason"] = "; ".join(hard_reasons)
                                self.live_trigger_metric_add(state, "qualityGateBlocked", 1)
                                self.save_live_trigger_state(state)
                                continue
                            monitor["lastBlockedReason"] = None

                            self.live_trigger_transition(state, setup_key, "confirmed", {
                                "triggeredAt": utc_now_iso(),
                                "lastTriggeredPrice": evaluated.get("price"),
                                "lastTriggerCandle": evaluated.get("candle"),
                                "lastContextCandle": evaluated.get("contextCandle"),
                                "lastQualityGate": quality_gate,
                                "contextVerdict": evaluated.get("contextVerdict"),
                                "lastDeliveryMode": "ws",
                                "triggerReason": evaluated.get("reason"),
                            }, reason_code="TRIGGER_CONFIRMED", event_type="TRIGGER_CANDLE_CLOSE", actor="live_trigger_run.ws")
                            self.live_trigger_record_evaluation(
                                monitor,
                                phase="trigger_eval",
                                outcome="confirmed",
                                reason=evaluated.get("reason"),
                                trigger_candle=evaluated.get("candle"),
                                context_outcome=evaluated.get("contextOutcome"),
                                regime=regime,
                            )
                            self.live_trigger_metric_add(state, "triggerConfirmed", 1)
                            self.final_confirmation_maybe_create_or_keep(monitor)
                            self.live_trigger_record_evaluation(monitor, phase="delivery", regime=regime)
                            delivered = self.send_live_trigger_notification(state, monitor, channel=channel, target=target, trigger_candle=evaluated.get("candle"), trigger_timeframe=trigger_timeframe, context_timeframe=context_timeframe, delivery_mode="ws")
                            if not delivered.get("sent"):
                                self.final_confirmation_update_delivery(
                                    monitor, "failed",
                                    error=monitor.get("notificationError") or delivered.get("error") or "delivery_failed",
                                    transport=monitor.get("lastDeliveryTransport"),
                                )
                                self.save_live_trigger_state(state)
                                continue
                            self.final_confirmation_update_delivery(
                                monitor, "sent",
                                notification_sent_at=monitor.get("notificationSentAt"),
                                transport=monitor.get("lastDeliveryTransport"),
                            )
                            self.live_trigger_transition(state, setup_key, "notified", {
                                "lastAlertAt": utc_now_iso(),
                                "lastMessage": delivered.get("message"),
                                "lastDeliveryKey": delivered.get("deliveryKey"),
                            }, reason_code="DELIVERY_SENT", event_type="DELIVERY_ACK", actor="live_trigger_run.ws")
                            self.initialize_live_trigger_lifecycle(monitor, evaluated.get("candle"))
                            self.live_trigger_metric_add(state, "lifecycleEntered", 1)
                            cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=cooldown_seconds)
                            self.live_trigger_transition(state, setup_key, "cooldown", {
                                "cooldownUntil": cooldown_until.isoformat(),
                            }, reason_code="COOLDOWN_STARTED", event_type="DELIVERY_ACK", actor="live_trigger_run.ws")
                            self.save_live_trigger_state(state)
            except Exception as exc:
                state["status"] = "degraded"
                state["lastError"] = str(exc)
                state["lastErrorAt"] = utc_now_iso()
                state["lastHeartbeatAt"] = utc_now_iso()
                state["recoveryState"] = "active_error"
                self.save_live_trigger_state(state)
                # Attempt fallback REST polling to maintain lifecycle while WS is down
                fallback_ok = self._live_trigger_fallback_poll(state, targets, trigger_timeframe, context_timeframe, poll_seconds, cooldown_seconds)
                if fallback_ok:
                    # Fallback succeeded — record recovery and keep running
                    state["recoveryState"] = "recovered"
                    state["lastRecoveredAt"] = utc_now_iso()
                    state["lastError"] = str(exc)
                    state["lastHeartbeatAt"] = utc_now_iso()
                    self.save_live_trigger_state(state)
                    # Fallback maintained lifecycle; WS will be retried on next poll cycle
                    await asyncio.sleep(poll_seconds)
                    continue
                # Fallback also failed — degraded, continue with degraded fallback path
                state["lastError"] = str(exc)
                state["lastHeartbeatAt"] = utc_now_iso()
                self.save_live_trigger_state(state)
                try:
                    targets = self.live_trigger_targets()
                    self.reconcile_live_trigger_targets(state, targets, cooldown_seconds)
                    tracking_rows = self.live_trigger_tracking_rows(state, targets)
                    for target_row in tracking_rows:
                        setup_key = target_row.get("setupKey")
                        monitor = (state.get("monitors") or {}).get(setup_key, {}) if isinstance(state.get("monitors"), dict) else {}
                        if not isinstance(monitor, dict):
                            continue
                        candle = self.rest_fallback_candle(target_row["symbol"], trigger_timeframe)
                        if not candle:
                            continue
                        regime = self.production_eval_regime_from_row(monitor)
                        lifecycle_event = self.evaluate_live_trigger_lifecycle(monitor, candle)
                        if lifecycle_event:
                            milestone = str(lifecycle_event.get("milestone") or "lifecycle")
                            metric_name = {
                                "tp1": "lifecycleTp1",
                                "tp2": "lifecycleTp2",
                                "closed": "lifecycleClosed",
                            }.get(milestone, "lifecycleEvents")
                            self.live_trigger_metric_add(state, "lifecycleEvents", 1)
                            self.live_trigger_metric_add(state, metric_name, 1)
                            if lifecycle_event.get("reason") == "stop_loss_hit":
                                self.live_trigger_metric_add(state, "lifecycleStopLoss", 1)
                            if lifecycle_event.get("reason") == "ambiguous_stop_vs_target_same_candle":
                                self.live_trigger_metric_add(state, "lifecycleAmbiguous", 1)
                            delivered_lifecycle = self.send_live_trigger_lifecycle_notification(
                                state,
                                monitor,
                                lifecycle_event,
                                channel=channel,
                                target=target,
                                trigger_candle=candle,
                                trigger_timeframe=trigger_timeframe,
                                delivery_mode="fallback",
                            )
                            if delivered_lifecycle.get("sent"):
                                monitor["lastLifecycleMessage"] = delivered_lifecycle.get("message")
                                monitor["lastLifecycleEvent"] = lifecycle_event
                                monitor["lastLifecycleEventAt"] = utc_now_iso()
                            self.production_eval_log_lifecycle_event(setup_key, monitor, lifecycle_event)
                        if self.runtime_state(monitor) not in LIVE_TRIGGER_EVALUATION_STATES:
                            continue
                        self.live_trigger_record_evaluation(monitor, phase="trigger_eval", trigger_candle=candle, regime=regime)
                        self.live_trigger_metric_add(state, "fallbackEvaluations", 1)
                        if self.live_trigger_zone_touched(monitor, candle) and self.runtime_state(monitor) == "armed":
                            self.live_trigger_transition(state, setup_key, "zone_touched", {
                                "zoneTouchedAt": utc_now_iso(),
                                "lastTouchedPrice": candle.get("close"),
                            }, reason_code="ZONE_TOUCH_CONFIRMED", event_type="TRIGGER_ZONE_TOUCH", actor="live_trigger_run.fallback")
                            self.live_trigger_record_evaluation(monitor, phase="zone_touch", outcome="touch_detected", reason="zone touched", trigger_candle=candle, regime=regime)
                        context_candle = None
                        if context_timeframe:
                            context_candle = self.rest_fallback_candle(target_row["symbol"], context_timeframe)
                            if context_candle:
                                monitor["lastContextCandle"] = context_candle
                        evaluated = self.evaluate_live_trigger_candle(monitor, candle, context_candle=context_candle, context_timeframe=context_timeframe)
                        self.live_trigger_record_evaluation(
                            monitor,
                            phase="trigger_eval",
                            outcome=evaluated.get("outcome"),
                            reason=evaluated.get("reason"),
                            trigger_candle=evaluated.get("candle") or candle,
                            context_outcome=evaluated.get("contextOutcome"),
                            regime=regime,
                        )
                        if not evaluated.get("confirmed"):
                            continue
                        quality_gate = self.live_trigger_quality_gate(
                            target_row["symbol"],
                            evaluated.get("candle"),
                            evaluated.get("contextCandle"),
                            trigger_timeframe,
                            context_timeframe,
                            state=state,
                            delivery_mode="fallback",
                            poll_seconds=poll_seconds,
                            context_verdict=evaluated.get("contextVerdict"),
                        )
                        monitor["lastQualityGate"] = quality_gate
                        if not quality_gate.get("ok"):
                            monitor["lastBlockedReason"] = "; ".join(quality_gate.get("hardBlockReasons") or [])
                            self.live_trigger_record_evaluation(
                                monitor,
                                phase="quality_gate",
                                outcome="quality_gate_blocked",
                                reason=monitor.get("lastBlockedReason") or "quality gate blocked",
                                context_outcome={
                                    "ok": bool(quality_gate.get("ok")),
                                    "reason": quality_gate.get("contextVerdict") or monitor.get("lastBlockedReason") or "quality gate blocked",
                                },
                                regime=regime,
                            )
                            self.live_trigger_metric_add(state, "qualityGateBlocked", 1)
                            continue
                        monitor["lastBlockedReason"] = None
                        self.live_trigger_transition(state, setup_key, "confirmed", {
                            "triggeredAt": utc_now_iso(),
                            "lastTriggeredPrice": evaluated.get("price"),
                            "lastTriggerCandle": evaluated.get("candle"),
                            "lastContextCandle": evaluated.get("contextCandle"),
                            "lastQualityGate": quality_gate,
                            "contextVerdict": evaluated.get("contextVerdict"),
                            "lastDeliveryMode": "fallback",
                            "triggerReason": f"Fallback mode: {evaluated.get('reason')}",
                        }, reason_code="TRIGGER_CONFIRMED", event_type="TRIGGER_CANDLE_CLOSE", actor="live_trigger_run.fallback")
                        self.live_trigger_record_evaluation(
                            monitor,
                            phase="trigger_eval",
                            outcome="confirmed",
                            reason=f"Fallback mode: {evaluated.get('reason')}",
                            trigger_candle=evaluated.get("candle"),
                            context_outcome=evaluated.get("contextOutcome"),
                            regime=regime,
                        )
                        self.live_trigger_metric_add(state, "triggerConfirmed", 1)
                        self.final_confirmation_maybe_create_or_keep(monitor)
                        self.live_trigger_record_evaluation(monitor, phase="delivery", regime=regime)
                        delivered = self.send_live_trigger_notification(state, monitor, channel=channel, target=target, trigger_candle=evaluated.get("candle"), trigger_timeframe=trigger_timeframe, context_timeframe=context_timeframe, delivery_mode="fallback")
                        if not delivered.get("sent"):
                            self.final_confirmation_update_delivery(
                                monitor, "failed",
                                error=monitor.get("notificationError") or delivered.get("error") or "delivery_failed",
                                transport=monitor.get("lastDeliveryTransport"),
                            )
                            continue
                        self.final_confirmation_update_delivery(
                            monitor, "sent",
                            notification_sent_at=monitor.get("notificationSentAt"),
                            transport=monitor.get("lastDeliveryTransport"),
                        )
                        self.live_trigger_transition(state, setup_key, "notified", {
                            "lastAlertAt": utc_now_iso(),
                            "lastMessage": delivered.get("message"),
                            "lastDeliveryKey": delivered.get("deliveryKey"),
                        }, reason_code="DELIVERY_SENT", event_type="DELIVERY_ACK", actor="live_trigger_run.fallback")
                        self.initialize_live_trigger_lifecycle(monitor, evaluated.get("candle"))
                        self.live_trigger_metric_add(state, "lifecycleEntered", 1)
                        cooldown_until = datetime.now(timezone.utc) + timedelta(seconds=cooldown_seconds)
                        self.live_trigger_transition(state, setup_key, "cooldown", {
                            "cooldownUntil": cooldown_until.isoformat(),
                        }, reason_code="COOLDOWN_STARTED", event_type="DELIVERY_ACK", actor="live_trigger_run.fallback")
                    self.save_live_trigger_state(state)
                except Exception as fallback_exc:
                    state["lastError"] = f"{exc} | fallback: {fallback_exc}"
                    self.save_live_trigger_state(state)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                self.live_trigger_metric_add(state, "wsReconnects", 1)

    def live_trigger_start(self, channel: str, target: str, trigger_timeframe: str = "5m", context_timeframe: str = "15m", poll_seconds: int = LIVE_TRIGGER_DEFAULT_POLL_SECONDS, cooldown_seconds: int = LIVE_TRIGGER_DEFAULT_COOLDOWN_SECONDS) -> Dict[str, Any]:
        pid_from_file = read_pid_file(LIVE_TRIGGER_PID_PATH)
        running_pids = live_trigger_running_pids()
        if running_pids:
            pid = pid_from_file if pid_from_file in running_pids else running_pids[0]
            LIVE_TRIGGER_PID_PATH.write_text(str(pid))
            return {
                "started": False,
                "reason": "duplicate_running" if len(running_pids) > 1 else "already_running",
                "pid": pid,
                "pids": running_pids,
                "status": self.live_trigger_engine_status(),
            }

        cmd = [
            sys.executable,
            str(Path(__file__).resolve()),
            "live-trigger-run",
            "--channel", channel,
            "--target", target,
            "--trigger-timeframe", trigger_timeframe,
            "--context-timeframe", context_timeframe,
            "--poll-seconds", str(poll_seconds),
            "--cooldown-seconds", str(cooldown_seconds),
        ]
        LIVE_TRIGGER_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with LIVE_TRIGGER_LOG_PATH.open("a") as log_handle:
            proc = subprocess.Popen(cmd, stdout=log_handle, stderr=log_handle, start_new_session=True)
        LIVE_TRIGGER_PID_PATH.write_text(str(proc.pid))
        return {
            "started": True,
            "pid": proc.pid,
            "logPath": str(LIVE_TRIGGER_LOG_PATH),
            "status": self.live_trigger_engine_status(),
        }

    def live_trigger_stop(self) -> Dict[str, Any]:
        pid_from_file = read_pid_file(LIVE_TRIGGER_PID_PATH)
        running_pids = live_trigger_running_pids()
        target_pids = sorted(set(([pid_from_file] if pid_from_file else []) + running_pids))
        if not target_pids:
            if LIVE_TRIGGER_PID_PATH.exists():
                LIVE_TRIGGER_PID_PATH.unlink()
            state = self.load_live_trigger_state()
            state["status"] = "stopped"
            self.save_live_trigger_state(state)
            return {"stopped": False, "reason": "not_running", "status": self.live_trigger_engine_status()}
        for pid in target_pids:
            try:
                os.kill(pid, signal.SIGTERM)
            except Exception:
                continue
        deadline = time.time() + 4.0
        while time.time() < deadline and any(process_is_alive(pid) for pid in target_pids):
            time.sleep(0.2)
        for pid in target_pids:
            if process_is_alive(pid):
                try:
                    os.kill(pid, signal.SIGKILL)
                except Exception:
                    pass
        if LIVE_TRIGGER_PID_PATH.exists():
            LIVE_TRIGGER_PID_PATH.unlink()
        state = self.load_live_trigger_state()
        state["status"] = "stopped"
        state["lastHeartbeatAt"] = utc_now_iso()
        self.save_live_trigger_state(state)
        return {"stopped": True, "pid": target_pids[0], "pids": target_pids, "status": self.live_trigger_engine_status()}

    def load_liquidation_state(self) -> Dict[str, Any]:
        state = load_json_file(LIQUIDATION_STATE_PATH, {"updatedAt": None, "captures": {}, "recent": []})
        if not isinstance(state, dict):
            state = {"updatedAt": None, "captures": {}, "recent": []}
        state.setdefault("captures", {})
        state.setdefault("recent", [])
        return state

    def save_liquidation_state(self, state: Dict[str, Any]) -> None:
        state["updatedAt"] = datetime.now(timezone.utc).isoformat()
        save_json_file(LIQUIDATION_STATE_PATH, state)

    def symbol_reference_price(self, snapshot: Dict[str, Any]) -> float:
        binance = snapshot.get("binance", {}) if isinstance(snapshot, dict) else {}
        book = binance.get("bookTicker", {}) if isinstance(binance, dict) else {}
        ticker = binance.get("ticker24h", {}) if isinstance(binance, dict) else {}
        try:
            bid = float(book.get("bidPrice", 0) or 0)
            ask = float(book.get("askPrice", 0) or 0)
            if bid > 0 and ask > 0:
                return (bid + ask) / 2
        except Exception:
            pass
        try:
            last = float(ticker.get("lastPrice", 0) or 0)
            if last > 0:
                return last
        except Exception:
            pass
        return 0.0

    def summarize_liquidation_events(self, events: List[Dict[str, Any]], reference_price: Optional[float] = None) -> Dict[str, Any]:
        clean_events = [row for row in events if isinstance(row, dict) and float(row.get("notional", 0) or 0) > 0 and float(row.get("price", 0) or 0) > 0]
        if not clean_events:
            return {
                "eventCount": 0,
                "buyCount": 0,
                "sellCount": 0,
                "buyNotional": 0.0,
                "sellNotional": 0.0,
                "dominantSide": None,
                "dominantImbalance": 0.0,
                "largestEvent": None,
                "buyTopCluster": None,
                "sellTopCluster": None,
                "nearbyBuyNotional": 0.0,
                "nearbySellNotional": 0.0,
                "nearbyRangePct": 0.5,
            }

        ref_price = float(reference_price or 0.0)
        if ref_price <= 0:
            ref_price = float(clean_events[-1].get("price", 0) or 0)
        band_size = max(ref_price * 0.0025, 1e-9)
        buckets: Dict[str, Dict[str, Any]] = {}
        buy_notional = 0.0
        sell_notional = 0.0
        buy_count = 0
        sell_count = 0
        nearby_range_pct = 0.5
        nearby_buy = 0.0
        nearby_sell = 0.0

        for event in clean_events:
            side = str(event.get("side") or "").upper()
            price = float(event.get("price", 0) or 0)
            notional = float(event.get("notional", 0) or 0)
            bucket_key = str(int(round(price / band_size)))
            bucket = buckets.setdefault(bucket_key, {
                "bucket": bucket_key,
                "centerPrice": 0.0,
                "minPrice": price,
                "maxPrice": price,
                "eventCount": 0,
                "buyCount": 0,
                "sellCount": 0,
                "buyNotional": 0.0,
                "sellNotional": 0.0,
                "totalNotional": 0.0,
                "lastEventAt": event.get("ts"),
            })
            bucket["eventCount"] += 1
            bucket["minPrice"] = min(float(bucket.get("minPrice", price) or price), price)
            bucket["maxPrice"] = max(float(bucket.get("maxPrice", price) or price), price)
            bucket["totalNotional"] = float(bucket.get("totalNotional", 0.0) or 0.0) + notional
            bucket["lastEventAt"] = event.get("ts")
            if side == "BUY":
                buy_notional += notional
                buy_count += 1
                bucket["buyCount"] += 1
                bucket["buyNotional"] = float(bucket.get("buyNotional", 0.0) or 0.0) + notional
            else:
                sell_notional += notional
                sell_count += 1
                bucket["sellCount"] += 1
                bucket["sellNotional"] = float(bucket.get("sellNotional", 0.0) or 0.0) + notional
            if ref_price > 0:
                distance_pct = ((price - ref_price) / ref_price) * 100
                if abs(distance_pct) <= nearby_range_pct:
                    if side == "BUY":
                        nearby_buy += notional
                    else:
                        nearby_sell += notional

        clusters = []
        for bucket in buckets.values():
            center_price = ((float(bucket.get("minPrice", 0) or 0) + float(bucket.get("maxPrice", 0) or 0)) / 2) if bucket.get("eventCount") else 0.0
            total_notional = float(bucket.get("totalNotional", 0.0) or 0.0)
            distance_pct = 0.0 if ref_price <= 0 else ((center_price - ref_price) / ref_price) * 100
            dominant_side = "BUY" if float(bucket.get("buyNotional", 0.0) or 0.0) >= float(bucket.get("sellNotional", 0.0) or 0.0) else "SELL"
            clusters.append({
                "centerPrice": round(center_price, 8),
                "distancePct": round(distance_pct, 4),
                "eventCount": int(bucket.get("eventCount", 0) or 0),
                "buyCount": int(bucket.get("buyCount", 0) or 0),
                "sellCount": int(bucket.get("sellCount", 0) or 0),
                "buyNotional": round(float(bucket.get("buyNotional", 0.0) or 0.0), 2),
                "sellNotional": round(float(bucket.get("sellNotional", 0.0) or 0.0), 2),
                "totalNotional": round(total_notional, 2),
                "dominantSide": dominant_side,
                "lastEventAt": bucket.get("lastEventAt"),
            })
        clusters = sorted(clusters, key=lambda row: (float(row.get("totalNotional", 0.0) or 0.0), int(row.get("eventCount", 0) or 0)), reverse=True)
        buy_clusters = sorted(clusters, key=lambda row: (float(row.get("buyNotional", 0.0) or 0.0), float(row.get("totalNotional", 0.0) or 0.0)), reverse=True)
        sell_clusters = sorted(clusters, key=lambda row: (float(row.get("sellNotional", 0.0) or 0.0), float(row.get("totalNotional", 0.0) or 0.0)), reverse=True)
        total_notional = buy_notional + sell_notional
        dominant_side = None
        if total_notional > 0:
            dominant_side = "BUY" if buy_notional > sell_notional else "SELL" if sell_notional > buy_notional else "BALANCED"
        dominant_imbalance = 0.0 if total_notional == 0 else abs(buy_notional - sell_notional) / total_notional
        largest_event = max(clean_events, key=lambda row: float(row.get("notional", 0.0) or 0.0)) if clean_events else None
        if isinstance(largest_event, dict):
            largest_event = {
                "symbol": largest_event.get("symbol"),
                "side": largest_event.get("side"),
                "price": round(float(largest_event.get("price", 0.0) or 0.0), 8),
                "qty": round(float(largest_event.get("qty", 0.0) or 0.0), 6),
                "notional": round(float(largest_event.get("notional", 0.0) or 0.0), 2),
                "ts": largest_event.get("ts"),
            }
        return {
            "eventCount": len(clean_events),
            "buyCount": buy_count,
            "sellCount": sell_count,
            "buyNotional": round(buy_notional, 2),
            "sellNotional": round(sell_notional, 2),
            "dominantSide": dominant_side,
            "dominantImbalance": round(dominant_imbalance, 4),
            "largestEvent": largest_event,
            "buyTopCluster": buy_clusters[0] if buy_clusters and float(buy_clusters[0].get("buyNotional", 0.0) or 0.0) > 0 else None,
            "sellTopCluster": sell_clusters[0] if sell_clusters and float(sell_clusters[0].get("sellNotional", 0.0) or 0.0) > 0 else None,
            "topClusters": clusters[:5],
            "nearbyBuyNotional": round(nearby_buy, 2),
            "nearbySellNotional": round(nearby_sell, 2),
            "nearbyRangePct": nearby_range_pct,
            "referencePrice": round(ref_price, 8) if ref_price > 0 else None,
        }

    async def _capture_binance_force_orders(self, symbols: List[str], seconds: int, max_events: int, min_notional: float) -> Dict[str, Any]:
        if websockets is None:
            raise RuntimeError("websockets package is not available")
        tracked = [str(symbol or "").upper().strip() for symbol in symbols if str(symbol or "").upper().strip()]
        if not tracked:
            raise RuntimeError("At least one symbol is required")
        streams = "/".join(f"{symbol.lower()}@forceOrder" for symbol in tracked)
        url = f"wss://fstream.binance.com/stream?streams={streams}"
        deadline = asyncio.get_running_loop().time() + max(int(seconds), 1)
        events: List[Dict[str, Any]] = []
        per_symbol: Dict[str, List[Dict[str, Any]]] = {symbol: [] for symbol in tracked}
        while asyncio.get_running_loop().time() < deadline and len(events) < max_events:
            remaining = max(0.25, deadline - asyncio.get_running_loop().time())
            try:
                async with websockets.connect(url, ping_interval=20, ping_timeout=20, close_timeout=5, max_size=2_000_000) as ws:
                    while asyncio.get_running_loop().time() < deadline and len(events) < max_events:
                        timeout = min(2.0, max(0.25, deadline - asyncio.get_running_loop().time()))
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
                        except asyncio.TimeoutError:
                            continue
                        payload = json.loads(raw)
                        event = payload.get("data") if isinstance(payload, dict) and payload.get("data") else payload
                        order = event.get("o", {}) if isinstance(event, dict) else {}
                        symbol = str(order.get("s") or event.get("s") or "").upper().strip()
                        if symbol not in per_symbol:
                            continue
                        side = str(order.get("S") or event.get("S") or "").upper().strip() or "UNKNOWN"
                        try:
                            qty = float(order.get("z") or order.get("q") or 0)
                            price = float(order.get("ap") or order.get("p") or 0)
                        except Exception:
                            continue
                        notional = qty * price
                        if notional < float(min_notional or 0):
                            continue
                        normalized = {
                            "symbol": symbol,
                            "side": side,
                            "price": round(price, 8),
                            "qty": round(qty, 6),
                            "notional": round(notional, 2),
                            "ts": int(order.get("T") or event.get("E") or int(time.time() * 1000)),
                            "status": order.get("X"),
                        }
                        events.append(normalized)
                        per_symbol[symbol].append(normalized)
            except Exception:
                if asyncio.get_running_loop().time() >= deadline:
                    break
                await asyncio.sleep(0.5)
        return {"symbols": tracked, "events": events, "perSymbol": per_symbol, "stream": url}

    def liquidation_capture(self, symbols: List[str], seconds: int = 20, max_events: int = 200, min_notional: float = 50_000, persist: bool = True) -> Dict[str, Any]:
        tracked = [str(symbol or "").upper().strip() for symbol in symbols if str(symbol or "").upper().strip()]
        if not tracked:
            raise RuntimeError("At least one symbol is required")
        captured_at = datetime.now(timezone.utc).isoformat()
        raw = asyncio.run(self._capture_binance_force_orders(tracked, seconds=seconds, max_events=max_events, min_notional=min_notional))
        state = self.load_liquidation_state()
        captures = state.setdefault("captures", {})
        recent = state.setdefault("recent", [])
        symbol_payloads: Dict[str, Any] = {}
        for symbol in tracked:
            events = (raw.get("perSymbol") or {}).get(symbol, []) if isinstance(raw, dict) else []
            summary = self.summarize_liquidation_events(events)
            payload = {
                "symbol": symbol,
                "capturedAt": captured_at,
                "windowSeconds": int(seconds),
                "minNotional": float(min_notional),
                "eventCount": len(events),
                "events": events[-LIQUIDATION_STATE_MAX_EVENTS:],
                "summary": summary,
            }
            symbol_payloads[symbol] = payload
            if persist:
                captures[symbol] = payload
                recent.append({
                    "symbol": symbol,
                    "capturedAt": captured_at,
                    "windowSeconds": int(seconds),
                    "eventCount": len(events),
                    "dominantSide": summary.get("dominantSide"),
                    "largestEvent": summary.get("largestEvent"),
                })
        if persist:
            state["recent"] = recent[-60:]
            self.save_liquidation_state(state)
        return {
            "saved": persist,
            "statePath": str(LIQUIDATION_STATE_PATH),
            "capturedAt": captured_at,
            "windowSeconds": int(seconds),
            "minNotional": float(min_notional),
            "stream": raw.get("stream") if isinstance(raw, dict) else None,
            "symbols": tracked,
            "eventCount": len(raw.get("events", [])) if isinstance(raw, dict) else 0,
            "bySymbol": symbol_payloads,
        }

    def liquidation_state_status(self, symbol: Optional[str] = None, reference_price: Optional[float] = None) -> Dict[str, Any]:
        state = self.load_liquidation_state()
        captures = state.get("captures", {}) if isinstance(state, dict) else {}
        if symbol:
            symbol = symbol.upper()
            capture = captures.get(symbol, {}) if isinstance(captures.get(symbol), dict) else {}
            captured_at = capture.get("capturedAt")
            age_minutes = None
            if captured_at:
                try:
                    age_minutes = round((datetime.now(timezone.utc) - datetime.fromisoformat(str(captured_at))).total_seconds() / 60, 2)
                except Exception:
                    age_minutes = None
            events = capture.get("events", []) if isinstance(capture.get("events"), list) else []
            live_summary = self.summarize_liquidation_events(events, reference_price=reference_price)
            return {
                "path": str(LIQUIDATION_STATE_PATH),
                "updatedAt": state.get("updatedAt"),
                "symbol": symbol,
                "capture": capture,
                "ageMinutes": age_minutes,
                "stale": age_minutes is None or age_minutes > LIQUIDATION_STALE_MINUTES,
                "summary": live_summary,
            }
        tracked = sorted([key for key, value in captures.items() if isinstance(value, dict)])
        return {
            "path": str(LIQUIDATION_STATE_PATH),
            "updatedAt": state.get("updatedAt"),
            "symbolsTracked": len(tracked),
            "symbols": tracked,
            "recentCount": len(state.get("recent") or []),
            "recent": (state.get("recent") or [])[-20:],
        }

    def liquidation_overlay(self, symbol: str, side: str, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        reference_price = self.symbol_reference_price(snapshot)
        status = self.liquidation_state_status(symbol.upper(), reference_price=reference_price)
        summary = status.get("summary", {}) if isinstance(status, dict) else {}
        capture = status.get("capture", {}) if isinstance(status, dict) else {}
        default = {
            "symbol": symbol.upper(),
            "side": side,
            "available": False,
            "stale": True,
            "cleanScoreDelta": 0.0,
            "riskFlags": [],
            "notes": [],
            "supportBias": None,
            "statePath": str(LIQUIDATION_STATE_PATH),
            "summary": summary,
            "capture": {
                "capturedAt": capture.get("capturedAt") if isinstance(capture, dict) else None,
                "windowSeconds": capture.get("windowSeconds") if isinstance(capture, dict) else None,
                "eventCount": capture.get("eventCount") if isinstance(capture, dict) else None,
                "minNotional": capture.get("minNotional") if isinstance(capture, dict) else None,
            },
        }
        if side not in {"long", "short"}:
            default["notes"] = ["Liquidation context tersedia, tetapi bias watch tidak memakai directional scoring"]
            return default
        if not capture:
            default["notes"] = ["Tidak ada state liquidation lokal untuk simbol ini"]
            return default
        age_minutes = status.get("ageMinutes")
        stale = bool(status.get("stale"))
        clean_delta = 0.0
        notes: List[str] = []
        risk_flags: List[str] = []
        support_bias = None
        nearby_buy = float(summary.get("nearbyBuyNotional", 0.0) or 0.0)
        nearby_sell = float(summary.get("nearbySellNotional", 0.0) or 0.0)
        buy_cluster = summary.get("buyTopCluster") if isinstance(summary.get("buyTopCluster"), dict) else None
        sell_cluster = summary.get("sellTopCluster") if isinstance(summary.get("sellTopCluster"), dict) else None

        if stale:
            notes.append("State liquidation tersedia tetapi sudah stale")
        elif int(summary.get("eventCount", 0) or 0) == 0:
            notes.append("Capture liquidation terakhir tidak menemukan event di atas threshold")
        else:
            if side == "long":
                if sell_cluster and nearby_sell >= 250_000 and float(sell_cluster.get("distancePct", 999) or 999) <= 0.0:
                    clean_delta += 0.7
                    support_bias = "long"
                    notes.append("Ada cluster SELL liquidation di bawah atau dekat harga sekarang, jadi downside flush bisa sudah dibersihkan")
                if buy_cluster and nearby_buy >= max(250_000, nearby_sell * 1.1):
                    clean_delta -= 0.9
                    risk_flags.append("recent_short_squeeze")
                    notes.append("BUY liquidation dekat harga menandakan short squeeze baru terjadi, jadi long berisiko jadi chase")
            if side == "short":
                if buy_cluster and nearby_buy >= 250_000 and float(buy_cluster.get("distancePct", -999) or -999) >= 0.0:
                    clean_delta += 0.7
                    support_bias = "short"
                    notes.append("Ada cluster BUY liquidation di atas atau dekat harga sekarang, jadi upside squeeze bisa mulai kehabisan tenaga")
                if sell_cluster and nearby_sell >= max(250_000, nearby_buy * 1.1):
                    clean_delta -= 0.9
                    risk_flags.append("recent_long_flush")
                    notes.append("SELL liquidation dekat harga menandakan long flush baru terjadi, jadi short berisiko terlambat")
            if float(summary.get("dominantImbalance", 0.0) or 0.0) >= 0.65 and summary.get("dominantSide") in {"BUY", "SELL"}:
                notes.append(f"Dominasi liquidation {summary.get('dominantSide')} cukup ekstrem dan layak dipakai sebagai context, bukan sinyal tunggal")

        default.update({
            "available": True,
            "stale": stale,
            "ageMinutes": age_minutes,
            "cleanScoreDelta": round(max(-1.5, min(1.5, clean_delta)), 2),
            "riskFlags": risk_flags,
            "notes": notes,
            "supportBias": support_bias,
            "summary": summary,
            "capture": {
                "capturedAt": capture.get("capturedAt"),
                "windowSeconds": capture.get("windowSeconds"),
                "eventCount": capture.get("eventCount"),
                "minNotional": capture.get("minNotional"),
            },
        })
        return default

    def latest_ratio_value(self, payload: Any, keys: List[str]) -> Optional[float]:
        rows = payload if isinstance(payload, list) else []
        if not rows:
            return None
        row = rows[-1]
        if not isinstance(row, dict):
            return None
        for key in keys:
            try:
                if row.get(key) is not None:
                    return float(row.get(key))
            except Exception:
                continue
        return None

    def direction_label(self, bull_score: int) -> str:
        if bull_score >= 2:
            return "long"
        if bull_score <= -2:
            return "short"
        return "watch"

    def grade_priority(self, grade: Optional[str]) -> int:
        return GRADE_PRIORITY.get(str(grade or "NO_TRADE"), 0)

    def scan_sort_key(self, row: Dict[str, Any]) -> Any:
        return (
            self.grade_priority(row.get("grade")),
            float(row.get("cleanScore", 0.0)),
            abs(int(row.get("bullScore", 0) or 0)),
            float(row.get("quoteVolume", 0.0) or 0.0),
        )

    def is_hard_disqualified(self, row: Dict[str, Any]) -> Dict[str, Any]:
        execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
        risk_flags = set(execution.get("riskFlags", []) or [])
        disqualifiers: List[str] = []

        try:
            execution_delta = float(execution.get("cleanScoreDelta", 0.0) or 0.0)
        except Exception:
            execution_delta = 0.0
        try:
            bull_score = int(row.get("bullScore", 0) or 0)
        except Exception:
            bull_score = 0
        try:
            pos_15m = float((((row.get("features") or {}).get("15m") or {}).get("pos", 0.5)) or 0.5)
        except Exception:
            pos_15m = 0.5

        negative_keywords = set(((row.get("headlineFlags") or {}).get("negativeKeywords") or []))
        severe_headlines = sorted(negative_keywords & SEVERE_HEADLINE_KEYWORDS)

        if "wide_spread" in risk_flags:
            disqualifiers.append("Spread terlalu lebar untuk setup high-grade")
        if {"thin_quote_volume", "thin_depth"}.issubset(risk_flags):
            disqualifiers.append("Likuiditas terlalu tipis, volume dan depth sama-sama lemah")
        if {"thin_depth", "low_trade_count"}.issubset(risk_flags):
            disqualifiers.append("Depth tipis dan trade count rendah membuat tape terlalu rapuh")
        if execution_delta <= -4.0:
            disqualifiers.append("Execution penalty terlalu berat")
        if "hot_oi" in risk_flags and "thin_depth" in risk_flags and ("short_squeeze_risk" in risk_flags or "long_crowding_risk" in risk_flags):
            disqualifiers.append("OI panas plus crowding dan depth tipis terlalu berbahaya")
        if bull_score >= 2 and "long_crowding_risk" in risk_flags and pos_15m >= 0.9:
            disqualifiers.append("Long terlalu crowded dan harga sudah terlalu dekat area atas")
        if bull_score <= -2 and "short_squeeze_risk" in risk_flags and pos_15m <= 0.1:
            disqualifiers.append("Short terlalu squeeze-prone dan harga sudah terlalu dekat area bawah")
        if severe_headlines:
            disqualifiers.append(f"Headline risk berat terdeteksi: {', '.join(severe_headlines)}")

        return {
            "disqualified": bool(disqualifiers),
            "disqualifiers": disqualifiers,
        }

    def grade_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        clean_score = round(float(row.get("cleanScore", 0.0) or 0.0), 2)
        bull_score = int(row.get("bullScore", 0) or 0)
        direction = self.direction_label(bull_score)
        execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
        risk_flags = list(execution.get("riskFlags", []) or [])
        risk_flag_set = set(risk_flags)
        execution_delta = round(float(execution.get("cleanScoreDelta", 0.0) or 0.0), 2)
        severe_risk_count = len(risk_flag_set & SEVERE_RISK_FLAGS)
        grade_reasons: List[str] = []

        disqualification = self.is_hard_disqualified(row)
        disqualified = bool(disqualification.get("disqualified"))
        disqualifiers = list(disqualification.get("disqualifiers") or [])

        if abs(bull_score) >= 7:
            grade_reasons.append("Directional alignment multi-timeframe sangat kuat")
        elif abs(bull_score) >= 5:
            grade_reasons.append("Directional alignment multi-timeframe cukup kuat")
        elif abs(bull_score) >= 4:
            grade_reasons.append("Bias arah ada, tapi conviction belum elite")
        elif abs(bull_score) >= 2:
            grade_reasons.append("Bias arah masih ada, tapi belum cukup kuat untuk actionable setup")
        else:
            grade_reasons.append("Bias arah terlalu campuran untuk setup aktif")

        if clean_score >= 97:
            grade_reasons.append("Clean score masuk tier tertinggi")
        elif clean_score >= 93:
            grade_reasons.append("Clean score cukup tinggi untuk setup actionable")
        elif clean_score >= 88:
            grade_reasons.append("Clean score cukup untuk watchlist")
        elif clean_score >= 80:
            grade_reasons.append("Clean score borderline dan perlu disiplin")
        else:
            grade_reasons.append("Clean score terlalu rendah")

        if execution_delta >= 0:
            grade_reasons.append("Execution filter mendukung, bukan menghambat")
        elif execution_delta > -1.5:
            grade_reasons.append("Execution quality masih layak walau ada sedikit friksi")
        else:
            grade_reasons.append("Execution quality menurunkan conviction")

        if risk_flags:
            grade_reasons.append(f"Risk flags aktif: {', '.join(risk_flags[:3])}")
        else:
            grade_reasons.append("Tidak ada risk flag utama yang aktif")

        grade = "NO_TRADE"
        if not disqualified:
            if clean_score >= 97 and abs(bull_score) >= 7 and execution_delta > -1.5 and severe_risk_count == 0:
                grade = "A+"
            elif clean_score >= 93 and abs(bull_score) >= 5 and execution_delta > -2.5 and severe_risk_count <= 1:
                grade = "A"
            elif clean_score >= 88 and abs(bull_score) >= 4:
                grade = "B"
            elif clean_score >= 80 and abs(bull_score) >= 2:
                grade = "C"

        if grade == "NO_TRADE":
            if disqualified:
                grade_reasons.extend(disqualifiers)
            else:
                grade_reasons.append("Belum lolos threshold grade formal")

        return {
            **row,
            "direction": direction,
            "grade": grade,
            "gradeScore": clean_score,
            "alertEligible": grade in ALERT_GRADES and not disqualified,
            "watchlistEligible": grade in WATCHLIST_GRADES and not disqualified,
            "disqualified": disqualified,
            "disqualifiers": disqualifiers,
            "gradeReasons": grade_reasons[:6],
        }

    def apply_grade_to_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        graded = self.grade_row(row)
        row.update(graded)
        self.apply_setup_state_preview_to_row(row)
        return row

    def execution_filter_overlay(self, symbol: str, bull_score: int, snapshot: Dict[str, Any]) -> Dict[str, Any]:
        clean_delta = 0.0
        notes: List[str] = []
        management: List[str] = []
        risk_flags: List[str] = []
        side = "long" if bull_score >= 2 else "short" if bull_score <= -2 else "watch"

        binance = snapshot.get("binance", {}) if isinstance(snapshot, dict) else {}
        ticker24h = binance.get("ticker24h", {}) if isinstance(binance, dict) else {}
        depth = binance.get("depth", {}) if isinstance(binance, dict) else {}
        agg_trades = binance.get("aggTrades", []) if isinstance(binance, dict) else []
        book = binance.get("bookTicker", {}) if isinstance(binance, dict) else {}
        premium = binance.get("premiumIndex", {}) if isinstance(binance, dict) else {}
        open_interest = binance.get("openInterest", {}) if isinstance(binance, dict) else {}
        oi_hist = binance.get("openInterestHist", []) if isinstance(binance, dict) else []
        cvd = binance.get("cvd", {}) if isinstance(binance, dict) else {}
        klines = binance.get("klines", {}) if isinstance(binance, dict) else {}

        try:
            bid = float(book.get("bidPrice", 0))
            ask = float(book.get("askPrice", 0))
            mid = (bid + ask) / 2 if bid and ask else 0.0
            spread_bps = 0.0 if mid == 0 else ((ask - bid) / mid) * 10000
        except Exception:
            spread_bps = 0.0
        try:
            quote_volume = float(ticker24h.get("quoteVolume", 0))
        except Exception:
            quote_volume = 0.0
        try:
            trade_count = int(ticker24h.get("count", 0))
        except Exception:
            trade_count = 0
        try:
            funding = float(premium.get("lastFundingRate", 0))
        except Exception:
            funding = 0.0
        try:
            oi_now = float(open_interest.get("openInterest", 0))
            oi_old = float(oi_hist[0].get("sumOpenInterest", 0)) if isinstance(oi_hist, list) and oi_hist else 0.0
            oi_change_pct = 0.0 if oi_old == 0 else ((oi_now - oi_old) / oi_old) * 100
        except Exception:
            oi_change_pct = 0.0

        bid_notional = 0.0
        ask_notional = 0.0
        try:
            bids = depth.get("bids", []) if isinstance(depth, dict) else []
            asks = depth.get("asks", []) if isinstance(depth, dict) else []
            bid_notional = sum(float(price) * float(size) for price, size in bids[:10])
            ask_notional = sum(float(price) * float(size) for price, size in asks[:10])
        except Exception:
            bid_notional = 0.0
            ask_notional = 0.0
        depth_total = bid_notional + ask_notional
        depth_imbalance = 0.0 if depth_total == 0 else (bid_notional - ask_notional) / depth_total

        buy_aggression = 0.0
        try:
            if isinstance(agg_trades, list) and agg_trades:
                buy_count = sum(1 for row in agg_trades if not row.get("m", False))
                buy_aggression = buy_count / len(agg_trades)
        except Exception:
            buy_aggression = 0.0

        try:
            cvd_delta_ratio = float(cvd.get("deltaRatio", 0.0) or 0.0)
        except Exception:
            cvd_delta_ratio = 0.0
        try:
            recent_cvd_delta_ratio = float(cvd.get("recentDeltaRatio", 0.0) or 0.0)
        except Exception:
            recent_cvd_delta_ratio = 0.0
        try:
            cvd_efficiency = float(cvd.get("efficiency", 0.0) or 0.0)
        except Exception:
            cvd_efficiency = 0.0
        cvd_divergence = cvd.get("divergence") if isinstance(cvd, dict) else None
        cvd_bias = cvd.get("bias") if isinstance(cvd, dict) else None
        liquidation = self.liquidation_overlay(symbol, side=side, snapshot=snapshot)

        recent_location_15m = None
        recent_location_1h = None
        try:
            rows_15m = klines.get("15m", []) if isinstance(klines, dict) else []
            if rows_15m:
                highs = [float(row[2]) for row in rows_15m[-8:]]
                lows = [float(row[3]) for row in rows_15m[-8:]]
                closes = [float(row[4]) for row in rows_15m[-8:]]
                denom = (max(highs) - min(lows)) + 1e-12
                recent_location_15m = (closes[-1] - min(lows)) / denom
        except Exception:
            recent_location_15m = None
        try:
            rows_1h = klines.get("1h", []) if isinstance(klines, dict) else []
            if rows_1h:
                highs = [float(row[2]) for row in rows_1h[-8:]]
                lows = [float(row[3]) for row in rows_1h[-8:]]
                closes = [float(row[4]) for row in rows_1h[-8:]]
                denom = (max(highs) - min(lows)) + 1e-12
                recent_location_1h = (closes[-1] - min(lows)) / denom
        except Exception:
            recent_location_1h = None

        global_ratio = self.latest_ratio_value(binance.get("globalLongShortAccountRatio"), ["longShortRatio"])
        top_account_ratio = self.latest_ratio_value(binance.get("topLongShortAccountRatio"), ["longShortRatio"])
        top_position_ratio = self.latest_ratio_value(binance.get("topLongShortPositionRatio"), ["longShortRatio"])
        taker_ratio = self.latest_ratio_value(binance.get("takerLongShortRatio"), ["buySellRatio", "buyVolSellVolRatio"])

        if quote_volume >= 1_000_000_000:
            clean_delta += 1.25
            notes.append("Execution quality gets a boost from very high quote volume")
        elif quote_volume >= 300_000_000:
            clean_delta += 0.55
            notes.append("Execution quality is supported by healthy quote volume")
        elif quote_volume < 80_000_000:
            clean_delta -= 1.5
            risk_flags.append("thin_quote_volume")
            notes.append("Execution quality is penalized because quote volume is too thin")

        if spread_bps > 12:
            clean_delta -= 3.0
            risk_flags.append("wide_spread")
            notes.append("Spread is too wide for clean execution")
        elif spread_bps > 6:
            clean_delta -= 1.4
            risk_flags.append("medium_spread")
            notes.append("Spread is wide enough to reduce execution quality")
        elif spread_bps < 2:
            clean_delta += 0.4
            notes.append("Tight spread supports cleaner execution")

        if depth_total >= 8_000_000:
            clean_delta += 1.0
            notes.append("Top-of-book depth is strong")
        elif depth_total >= 3_000_000:
            clean_delta += 0.4
            notes.append("Top-of-book depth is acceptable")
        elif depth_total < 1_000_000:
            clean_delta -= 1.8
            risk_flags.append("thin_depth")
            notes.append("Top-of-book depth is too thin")

        if trade_count < 120_000:
            clean_delta -= 0.85
            risk_flags.append("low_trade_count")
            notes.append("Low trade count suggests weaker tape quality")
        elif trade_count > 300_000:
            clean_delta += 0.35

        if abs(oi_change_pct) > 22:
            clean_delta -= 0.9
            risk_flags.append("hot_oi")
            notes.append("Open interest expansion is hot enough to raise execution risk")

        clean_delta += float(liquidation.get("cleanScoreDelta", 0.0) or 0.0)
        risk_flags.extend([flag for flag in (liquidation.get("riskFlags") or []) if flag not in risk_flags])
        notes.extend([note for note in (liquidation.get("notes") or []) if note not in notes])

        if side == "long":
            if cvd_bias == "bullish" and recent_cvd_delta_ratio > 0.08:
                clean_delta += 0.75
                notes.append("CVD dari aggTrades mendukung long dengan delta beli yang konsisten")
            elif cvd_divergence == "bearish":
                clean_delta -= 1.1
                risk_flags.append("bearish_cvd_divergence")
                notes.append("CVD menunjukkan bearish divergence terhadap ide long")
            elif cvd_delta_ratio < -0.06 and cvd_efficiency > 0.2:
                clean_delta -= 0.6
                notes.append("CVD bersih condong jual, jadi long kehilangan dukungan tape")
            if recent_cvd_delta_ratio < -0.25 and cvd_efficiency > 0.18:
                clean_delta -= 1.0
                risk_flags.append("recent_sell_pressure")
                notes.append("Recent tape masih sell-heavy, jadi long pullback seperti AAVE-type setup harus ditahan lebih ketat")
            if recent_location_15m is not None and recent_location_1h is not None and recent_location_15m > 0.78 and recent_location_1h > 0.62 and recent_cvd_delta_ratio < 0.12:
                clean_delta -= 1.1
                risk_flags.append("late_long_location")
                notes.append("Long terlihat terlalu tinggi di range intraday tanpa tape yang cukup kuat, jadi risiko telat masuk meningkat")

        if side == "short":
            if cvd_bias == "bearish" and recent_cvd_delta_ratio < -0.08:
                clean_delta += 0.75
                notes.append("CVD dari aggTrades mendukung short dengan delta jual yang konsisten")
            elif cvd_divergence == "bullish":
                clean_delta -= 1.1
                risk_flags.append("bullish_cvd_divergence")
                notes.append("CVD menunjukkan bullish divergence terhadap ide short")
            elif cvd_delta_ratio > 0.06 and cvd_efficiency > 0.2:
                clean_delta -= 0.6
                notes.append("CVD bersih condong beli, jadi short kehilangan dukungan tape")
            if recent_cvd_delta_ratio > 0.25 and cvd_efficiency > 0.18:
                clean_delta -= 1.0
                risk_flags.append("recent_buy_pressure")
                notes.append("Recent tape masih buy-heavy, jadi short seperti ADA-type setup harus lebih selektif")
            if recent_location_15m is not None and recent_location_1h is not None and recent_location_15m < 0.22 and recent_location_1h < 0.38 and recent_cvd_delta_ratio > -0.15:
                clean_delta -= 1.1
                risk_flags.append("late_short_location")
                notes.append("Short terlihat terlalu dekat low range tanpa continuation tape yang cukup, jadi risiko telat masuk meningkat")

        if side == "short":
            short_squeeze_risk = 0
            if funding < -0.0005:
                short_squeeze_risk += 1
                notes.append("Negative funding increases short crowding risk")
            if any(r is not None and r < 0.9 for r in [global_ratio, top_account_ratio, top_position_ratio]):
                short_squeeze_risk += 1
                notes.append("Positioning ratios suggest the short side is already crowded")
            if taker_ratio is not None and taker_ratio > 1.08:
                short_squeeze_risk += 1
                notes.append("Recent taker flow leans buy-heavy against the short idea")
            if buy_aggression > 0.6 and depth_imbalance > 0.08:
                short_squeeze_risk += 1
                notes.append("Aggressive buy flow and bid-side depth raise squeeze risk")
            if quote_volume < 150_000_000 or depth_total < 2_000_000:
                short_squeeze_risk += 1
                notes.append("Lower-liquidity short candidate gets extra squeeze penalty")
            if short_squeeze_risk >= 3:
                clean_delta -= 2.8
                risk_flags.append("short_squeeze_risk")
            elif short_squeeze_risk == 2:
                clean_delta -= 1.6
                risk_flags.append("short_squeeze_risk")
            if short_squeeze_risk > 0:
                management.append("Kalau tetap short, wajib ambil partial lebih cepat dan pindah stop ke break-even setelah TP1 atau +1R")

        if side == "long":
            long_crowding_risk = 0
            if funding > 0.0008:
                long_crowding_risk += 1
                notes.append("Positive funding raises long crowding risk")
            if any(r is not None and r > 1.25 for r in [global_ratio, top_account_ratio, top_position_ratio]):
                long_crowding_risk += 1
                notes.append("Positioning ratios show long crowding")
            if taker_ratio is not None and taker_ratio < 0.92:
                long_crowding_risk += 1
                notes.append("Recent taker flow leans sell-heavy against the long idea")
            if buy_aggression < 0.4 and depth_imbalance < -0.08:
                long_crowding_risk += 1
                notes.append("Offer-side pressure hurts long execution quality")
            if long_crowding_risk >= 3:
                clean_delta -= 2.2
                risk_flags.append("long_crowding_risk")
            elif long_crowding_risk == 2:
                clean_delta -= 1.2
                risk_flags.append("long_crowding_risk")

        feedback = ((self.load_execution_feedback_state().get("symbols") or {}).get(symbol.upper()) or {})
        side_feedback = ((feedback.get("sides") or {}).get(side) or {}) if side in {"long", "short"} else {}
        side_total = int(side_feedback.get("total", 0) or 0)
        side_outcomes = side_feedback.get("outcomes", {}) if isinstance(side_feedback, dict) else {}
        overall_total = int(feedback.get("total", 0) or 0)
        if side_total >= 2:
            stop_count = int(side_outcomes.get("stop_loss", 0) or 0)
            no_fill_count = int(side_outcomes.get("no_fill", 0) or 0)
            runner_count = int(side_outcomes.get("runner", 0) or 0)
            tp_count = int(side_outcomes.get("take_profit", 0) or 0)
            stop_rate = stop_count / side_total
            no_fill_rate = no_fill_count / side_total
            if stop_rate >= 0.6:
                clean_delta -= 2.0
                risk_flags.append("bad_symbol_history")
                notes.append("Historical feedback for this symbol and side is poor")
            elif stop_rate >= 0.4:
                clean_delta -= 1.0
                notes.append("Historical feedback shows this symbol is not consistently executable")
            if no_fill_rate >= 0.4:
                clean_delta -= 0.75
                risk_flags.append("no_fill_history")
                management.append("Nama ini sering no-fill, jadi hindari limit terlalu pasif dan tunggu reclaim atau rejection yang benar-benar jelas")
            if runner_count > tp_count and runner_count >= 1:
                management.append("Runner history suggests ambil partial lebih cepat, lalu geser stop ke break-even")
        elif overall_total >= 2:
            notes.append("Historical feedback exists, but not enough on this exact side yet")

        if side in {"long", "short"} and not management:
            management.append("Default management: ambil partial di TP1 atau sekitar +1R, lalu geser stop ke break-even bila tape tetap mendukung")

        # ─── NEW INDICATORS: MACD, ATR, VWAP ─────────────────────────────────────
        macd_histogram = None
        macd_signal = None
        macd_line = None
        atr_14 = None
        vwap_value = None
        macd_note = None
        atr_note = None
        vwap_note = None
        macd_warn = False
        atr_warn = False
        vwap_warn = False

        if NEW_INDICATOR_ENABLED.get("macd", False) or NEW_INDICATOR_ENABLED.get("atr", False) or NEW_INDICATOR_ENABLED.get("vwap", False):
            try:
                rows_1h = klines.get("1h", []) if isinstance(klines, dict) else []
                if rows_1h and len(rows_1h) >= 26:
                    c_1h = [float(r[4]) for r in rows_1h]
                    h_1h = [float(r[2]) for r in rows_1h]
                    l_1h = [float(r[3]) for r in rows_1h]
                    v_1h = [float(r[5]) for r in rows_1h]

                    # MACD: EMA(12) - EMA(26)
                    def _ema(vals, period):
                        if len(vals) < period:
                            return None
                        mult = 2.0 / (period + 1)
                        e = sum(vals[:period]) / period
                        for val in vals[period:]:
                            e = (val - e) * mult + e
                        return e

                    ema_12 = _ema(c_1h, 12)
                    ema_26 = _ema(c_1h, 26)
                    if ema_12 is not None and ema_26 is not None:
                        macd_line = round(ema_12 - ema_26, 8)
                        # Signal = EMA(9) of MACD line — compute from MACD series
                        macd_series = []
                        for i in range(26, len(c_1h)):
                            e12 = _ema(c_1h[:i+1], 12) if i >= 11 else None
                            e26 = _ema(c_1h[:i+1], 26) if i >= 25 else None
                            if e12 is not None and e26 is not None:
                                macd_series.append(e12 - e26)
                        if len(macd_series) >= 9:
                            sig = sum(macd_series[:9]) / 9
                            mult = 2.0 / 10.0
                            for h_val in macd_series[9:]:
                                sig = (h_val - sig) * mult + sig
                            macd_signal = round(sig, 8)
                        if macd_line is not None and macd_signal is not None:
                            macd_histogram = round(macd_line - macd_signal, 8)
                        # Warn if MACD histogram is unusually large vs price
                        if macd_histogram is not None and c_1h[-1] > 0:
                            if abs(macd_histogram) / c_1h[-1] > MACD_PRICE_RATIO_MAX:
                                macd_warn = True

                    # ATR(14) from 1h klines
                    if len(c_1h) >= 15:
                        trs = []
                        for i in range(-14, 0):
                            hl = h_1h[i] - l_1h[i]
                            hc = abs(h_1h[i] - c_1h[i-1])
                            lc = abs(l_1h[i] - c_1h[i-1])
                            trs.append(max(hl, hc, lc))
                        if trs:
                            atr_14 = round(sum(trs) / len(trs), 8)
                            if c_1h[-1] > 0 and atr_14 / c_1h[-1] > ATR_PRICE_RATIO_MAX:
                                atr_warn = True

                    # VWAP from 1h klines (rolling, not session-anchored)
                    if v_1h and sum(v_1h[-24:]) > 0:
                        typ = [(h_1h[i] + l_1h[i] + c_1h[i]) / 3 for i in range(len(c_1h))]
                        pv = [typ[i] * v_1h[i] for i in range(len(typ))]
                        total_vol = sum(v_1h[-24:])
                        if total_vol > 0:
                            vwap_value = round(sum(pv[-24:]) / total_vol, 8)
                            if c_1h[-1] > 0 and vwap_value > 0:
                                if abs(c_1h[-1] - vwap_value) / c_1h[-1] > VWAP_PRICE_DIVERGENCE_MAX:
                                    vwap_warn = True

                    # Build macdNote if enabled (NEVER changes grade/cleanScore/readiness)
                    if NEW_INDICATOR_ENABLED.get("macd", False) and macd_histogram is not None:
                        direction = "bullish" if macd_histogram > 0 else "bearish"
                        macd_note = f"MACD histogram {direction} ({macd_histogram:.6f})"
                        if macd_warn:
                            macd_note += " [unusual magnitude]"
                    if NEW_INDICATOR_ENABLED.get("atr", False) and atr_14 is not None:
                        atr_note = f"ATR(14)={atr_14:.4f}"
                        if atr_warn:
                            atr_note += " [unusual volatility]"
                    if NEW_INDICATOR_ENABLED.get("vwap", False) and vwap_value is not None:
                        above = c_1h[-1] > vwap_value
                        vwap_dir = "above" if above else "below"
                        vwap_note = f"Price {vwap_dir} VWAP ({vwap_value:.4f})"
                        if vwap_warn:
                            vwap_note += " [unusual divergence]"
                        notes.append(vwap_note)
                        if macd_note:
                            notes.append(macd_note)
                        if atr_note:
                            notes.append(atr_note)
            except Exception:
                # Never let new indicators crash execution filter
                pass

        clean_delta = max(-6.0, min(2.5, round(clean_delta, 2)))
        return {
            "symbol": symbol.upper(),
            "side": side,
            "cleanScoreDelta": clean_delta,
            "quoteVolume": round(quote_volume, 2),
            "tradeCount": trade_count,
            "depthNotionalTop10": round(depth_total, 2),
            "depthImbalance": round(depth_imbalance, 4),
            "buyAggression": round(buy_aggression, 4),
            "cvd": cvd,
            "liquidation": liquidation,
            "funding": round(funding, 6),
            "oiChangePct": round(oi_change_pct, 2),
            "ratios": {
                "globalLongShort": global_ratio,
                "topAccounts": top_account_ratio,
                "topPositions": top_position_ratio,
                "taker": taker_ratio,
            },
            "riskFlags": risk_flags,
            "notes": notes,
            "managementGuidance": management,
            "feedbackSummary": feedback,
            "recentLocation": {
                "m15": round(recent_location_15m, 4) if recent_location_15m is not None else None,
                "h1": round(recent_location_1h, 4) if recent_location_1h is not None else None,
            },
            # New indicators: stored but NEVER used for grade/cleanScore/readiness decisions
            "macdHistogram": macd_histogram,
            "macdSignal": macd_signal,
            "macdLine": macd_line,
            "atr14": atr_14,
            "vwap": vwap_value,
            "_indicatorFlags": {
                "macdWarn": macd_warn,
                "atrWarn": atr_warn,
                "vwapWarn": vwap_warn,
            },
        }

    def apply_execution_filter_to_row(self, row: Dict[str, Any], execution_overlay: Dict[str, Any]) -> Dict[str, Any]:
        row["executionFilter"] = execution_overlay
        row["cleanScore"] = round(max(0.0, min(100.0, float(row.get("cleanScore", 0.0)) + float(execution_overlay.get("cleanScoreDelta", 0.0)))), 2)
        row["managementGuidance"] = execution_overlay.get("managementGuidance", [])
        self.apply_grade_to_row(row)
        return row

    def price_decimals(self, price: Optional[float]) -> int:
        value = abs(float(price or 0.0))
        if value >= 1000:
            return 1
        if value >= 100:
            return 2
        if value >= 10:
            return 3
        if value >= 1:
            return 4
        if value >= 0.1:
            return 5
        if value >= 0.01:
            return 6
        return 7

    def round_price(self, price: Optional[float], reference: Optional[float] = None) -> Optional[float]:
        if price is None:
            return None
        ref = reference if reference not in {None, 0} else price
        return round(float(price), self.price_decimals(ref))

    def normalize_take_profit_ladder(self, side: str, tp1: float, tp2: float, tp3: float) -> Tuple[float, float, float]:
        values = [float(tp1), float(tp2), float(tp3)]
        if str(side or "").lower().strip() == "short":
            values = sorted(values, reverse=True)
        else:
            values = sorted(values)
        return values[0], values[1], values[2]

    def actionable_setup_plan(self, row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        side = str(row.get("direction") or "").lower().strip()
        if side not in {"long", "short"}:
            return None
        if row.get("grade") == "NO_TRADE" or row.get("disqualified"):
            return None

        features = row.get("features", {}) if isinstance(row.get("features"), dict) else {}
        execution = row.get("executionFilter", {}) if isinstance(row.get("executionFilter"), dict) else {}
        m15 = features.get("15m", {}) if isinstance(features.get("15m"), dict) else {}
        h1 = features.get("1h", {}) if isinstance(features.get("1h"), dict) else {}
        last = float(m15.get("last") or row.get("lastPrice") or 0.0)
        if last <= 0:
            return None

        m15_high = float(m15.get("recentHigh") or last)
        m15_low = float(m15.get("recentLow") or last)
        h1_high = float(h1.get("recentHigh") or m15_high)
        h1_low = float(h1.get("recentLow") or m15_low)
        swing_high = max(m15_high, h1_high, last)
        swing_low = min(m15_low, h1_low, last)
        range_15 = max(m15_high - m15_low, last * 0.0025)
        range_1h = max(h1_high - h1_low, last * 0.004)
        ref_cap = max(range_15 * 2.5, last * 0.02)
        ref_range = min(max(range_15, range_1h * 0.3, last * 0.003), ref_cap)

        recent_location = execution.get("recentLocation", {}) if isinstance(execution.get("recentLocation"), dict) else {}
        loc15 = recent_location.get("m15")
        loc1h = recent_location.get("h1")
        risk_flags = list(execution.get("riskFlags", []) or [])
        management = list(row.get("managementGuidance", []) or [])

        # ─── SUPERTREND: ATR-based stop zone ─────────────────────────────────────
        # Read ATR(14) from execution_filter_overlay output
        atr_14 = None
        atr_stop_multiplier = 2.0  # Supertrend standard multiplier
        supertrend_note = None
        if NEW_INDICATOR_ENABLED.get("supertrend", False):
            try:
                atr_14 = execution.get("atr14")
                atr_warn = execution.get("_indicatorFlags", {}).get("atrWarn", False)
                if atr_14 is not None and atr_14 > 0 and last > 0:
                    # Validate ATR is reasonable
                    atr_ratio = atr_14 / last
                    if atr_ratio <= ATR_PRICE_RATIO_MAX:
                        supertrend_note = f"ATR(14)={atr_14:.4f} stop buffer active"
                        if atr_warn:
                            supertrend_note += " [check volatility]"
            except Exception:
                pass

        def _atr_stop(buffer_abs: float, atr: Optional[float], multiplier: float) -> float:
            """Return max of range-based buffer or ATR-based buffer."""
            if atr is not None and atr > 0:
                return max(buffer_abs, multiplier * atr)
            return buffer_abs

        late_short = side == "short" and (
            "late_short_location" in risk_flags
            or (loc15 is not None and float(loc15) <= 0.18)
            or (loc1h is not None and float(loc1h) <= 0.12)
        )
        late_long = side == "long" and (
            "late_long_location" in risk_flags
            or (loc15 is not None and float(loc15) >= 0.82)
            or (loc1h is not None and float(loc1h) >= 0.88)
        )
        do_not_chase = late_short or late_long

        if side == "short":
            entry_style = "short on bounce" if do_not_chase else "short on rejection"
            entry_low = last + (ref_range * 0.18 if do_not_chase else -ref_range * 0.03)
            entry_high = last + (ref_range * 0.5 if do_not_chase else ref_range * 0.18)
            stop_buffer = _atr_stop(ref_range * 0.18, atr_14, atr_stop_multiplier)
            stop_loss = max(m15_high, entry_high) + stop_buffer
            raw_tp1 = min(last - ref_range * 0.35, max(0.0, swing_low + ref_range * 0.04))
            raw_tp2 = max(0.0, last - ref_range * 0.75)
            raw_tp3 = max(0.0, last - ref_range * 1.15)
            trigger = "Tunggu bounce ke entry zone lalu cari rejection candle 5m/15m" if do_not_chase else "Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah"
            invalidation = "Batalkan short bila 15m close kuat di atas stop atau reclaim high lokal bertahan"
            current_status = "wait_bounce" if do_not_chase else "ready_on_retest"
        else:
            entry_style = "long on dip" if do_not_chase else "long on reclaim"
            entry_low = last - (ref_range * 0.5 if do_not_chase else ref_range * 0.18)
            entry_high = last + (ref_range * 0.03 if do_not_chase else ref_range * 0.12)
            stop_buffer = _atr_stop(ref_range * 0.18, atr_14, atr_stop_multiplier)
            stop_loss = min(m15_low, entry_low) - stop_buffer
            raw_tp1 = max(last + ref_range * 0.35, swing_high - ref_range * 0.04)
            raw_tp2 = last + ref_range * 0.75
            raw_tp3 = last + ref_range * 1.15
            trigger = "Tunggu dip ke entry zone lalu cari reclaim candle 5m/15m" if do_not_chase else "Eksekusi hanya jika hold area entry tetap kuat dan reclaim terjaga"
            invalidation = "Batalkan long bila 15m close kuat di bawah stop atau breakdown low lokal bertahan"
            current_status = "wait_pullback" if do_not_chase else "ready_on_retest"

        tp1, tp2, tp3 = self.normalize_take_profit_ladder(side, raw_tp1, raw_tp2, raw_tp3)

        if entry_low > entry_high:
            entry_low, entry_high = entry_high, entry_low

        execution_note = management[0] if management else "Ambil partial di TP1 atau sekitar +1R, lalu geser stop ke break-even bila tape tetap mendukung"
        if do_not_chase:
            execution_note = "Jangan entry market sekarang, tunggu retest ke zona entry yang lebih bersih"

        return {
            "currentPrice": self.round_price(last, reference=last),
            "entryStyle": entry_style,
            "entryZone": {
                "low": self.round_price(entry_low, reference=last),
                "high": self.round_price(entry_high, reference=last),
            },
            "stopLoss": self.round_price(stop_loss, reference=last),
            "takeProfit": {
                "tp1": self.round_price(tp1, reference=last),
                "tp2": self.round_price(tp2, reference=last),
                "tp3": self.round_price(tp3, reference=last),
            },
            "trigger": trigger,
            "invalidation": invalidation,
            "doNotChase": do_not_chase,
            "currentStatus": current_status,
            "executionNote": execution_note,
            "supertrendNote": supertrend_note,
            "riskFlags": risk_flags,
            "crowdingSignals": self._build_crowding_signals(execution, side),
        }

    def _build_crowding_signals(self, execution: Dict[str, Any], side: str) -> Dict[str, Any]:
        """Build crowding/detection signals from top trader L/S data for actionable alert."""
        ratios = execution.get("ratios", {}) if isinstance(execution, dict) else {}
        top_positions_ratio = ratios.get("topPositions")
        top_accounts_ratio = ratios.get("topAccounts")
        global_ratio = ratios.get("globalLongShort")

        def safe_float(v):
            try:
                return float(v) if v is not None else None
            except Exception:
                return None

        top_pos = safe_float(top_positions_ratio)
        top_acc = safe_float(top_accounts_ratio)
        glob = safe_float(global_ratio)

        # Determine dominant side of top traders
        if top_pos is not None:
            if top_pos > 1.5:
                top_trader_bias = "long_crowded"
            elif top_pos < 0.67:
                top_trader_bias = "short_crowded"
            else:
                top_trader_bias = "balanced"
        else:
            top_trader_bias = "unknown"

        # Squeeze risk for this setup's side
        squeeze_risk = "none"
        if side == "short":
            if top_pos is not None and top_pos > 2.0:
                squeeze_risk = "high"
            elif top_pos is not None and top_pos > 1.5:
                squeeze_risk = "medium"
        elif side == "long":
            if top_pos is not None and top_pos < 0.5:
                squeeze_risk = "high"
            elif top_pos is not None and top_pos < 0.67:
                squeeze_risk = "medium"

        return {
            "topPositionsRatio": top_pos,
            "topAccountsRatio": top_acc,
            "globalRatio": glob,
            "topTraderBias": top_trader_bias,
            "squeezeRisk": squeeze_risk,
            "crowdingNote": self._crowding_note(top_trader_bias, squeeze_risk, side),
        }

    def _crowding_note(self, bias: str, squeeze: str, side: str) -> str:
        if squeeze == "high":
            if side == "short":
                return "WARNING: Top traders are heavily long-crowded. Shorts face high squeeze risk if price bounces."
            return "WARNING: Top traders are heavily short-crowded. Longs face high squeeze risk if price drops."
        elif squeeze == "medium":
            if side == "short":
                return "CAUTION: Top traders lean long. Shorts should manage risk tightly and take partials early."
            return "CAUTION: Top traders lean short. Longs should manage risk tightly and take partials early."
        elif bias == "balanced":
            return "Top traders are balanced — no extreme crowding detected."
        elif bias == "long_crowded":
            return "Top traders lean long in this pair. Longs face moderate crowding risk; take partials early."
        elif bias == "short_crowded":
            return "Top traders lean short in this pair. Shorts face moderate crowding risk; take partials early."
        return ""

    def apply_actionable_setup_plan(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row["actionableAlert"] = self.actionable_setup_plan(row)
        return row

    def _score_symbol(self, symbol: str, include_auth_providers: bool = False, prod_mode: bool = False) -> Dict[str, Any]:
        snapshot = self.snapshot(symbol, include_auth_providers=include_auth_providers)
        binance = snapshot["binance"]
        klines = binance["klines"]

        def features(rows: List[List[Any]]) -> Dict[str, float]:
            closes = [float(r[4]) for r in rows]
            highs = [float(r[2]) for r in rows]
            lows = [float(r[3]) for r in rows]
            vols = [float(r[5]) for r in rows]
            last = closes[-1]
            # True EMA (Wilder smoothing, same as TradingView/TAAPI)
            # Fall back to SMA if not enough candles
            def _ema(vals: List[float], period: int) -> float:
                if len(vals) < period:
                    return sum(vals) / len(vals)
                mult = 2.0 / (period + 1)
                ema_val = sum(vals[:period]) / period  # seed with SMA
                for v in vals[period:]:
                    ema_val = (v - ema_val) * mult + ema_val
                return ema_val

            ema20 = _ema(closes, 20) if NEW_INDICATOR_ENABLED.get("ema", False) else sum(closes[-20:]) / 20
            ema50 = _ema(closes, 50) if NEW_INDICATOR_ENABLED.get("ema", False) else sum(closes[-50:]) / 50
            recent_high = max(highs[-24:]) if len(highs) >= 24 else max(highs)
            recent_low = min(lows[-24:]) if len(lows) >= 24 else min(lows)
            gains, losses = [], []
            for idx in range(-14, 0):
                change = closes[idx] - closes[idx - 1]
                gains.append(max(change, 0))
                losses.append(max(-change, 0))
            avg_gain = sum(gains) / 14
            avg_loss = sum(losses) / 14
            rsi = 100 if avg_loss == 0 else 100 - (100 / (1 + (avg_gain / avg_loss)))
            pos = (last - recent_low) / (recent_high - recent_low + 1e-12)
            return {
                "last": last,
                "recentHigh": recent_high,
                "recentLow": recent_low,
                "above20": 1.0 if last > ema20 else -1.0,
                "above50": 1.0 if last > ema50 else -1.0,
                "rsi": rsi,
                "pos": pos,
                "vol": vols[-1] / ((sum(vols[-20:]) / 20) + 1e-12),
            }

        f = {interval: features(klines[interval]) for interval in DEFAULT_INTERVALS}
        book = binance["bookTicker"]
        bid = float(book["bidPrice"])
        ask = float(book["askPrice"])
        spread_bps = ((ask - bid) / ((ask + bid) / 2)) * 10000
        funding = float(binance["premiumIndex"]["lastFundingRate"])
        oi_now = float(binance["openInterest"]["openInterest"])
        oi_old = float(binance["openInterestHist"][0]["sumOpenInterest"])
        oi_change_pct = ((oi_now - oi_old) / (oi_old + 1e-12)) * 100
        bull_score = int(
            f["15m"]["above20"] + f["1h"]["above20"] + f["4h"]["above20"] +
            f["15m"]["above50"] + f["1h"]["above50"] + f["4h"]["above50"] +
            (1 if f["15m"]["rsi"] > 52 else -1 if f["15m"]["rsi"] < 48 else 0) +
            (1 if f["1h"]["rsi"] > 52 else -1 if f["1h"]["rsi"] < 48 else 0) +
            (1 if f["4h"]["rsi"] > 52 else -1 if f["4h"]["rsi"] < 48 else 0)
        )
        clean_score = 100.0
        clean_score -= min(spread_bps * 2.5, 40)
        clean_score -= 20 if abs(funding) > 0.0008 else 0
        clean_score -= 15 if abs(oi_change_pct) > 20 else 0
        clean_score += min((f["15m"]["vol"] + f["1h"]["vol"]) * 5, 10)
        clean_score = max(0.0, min(100.0, clean_score))
        ticker24h = binance.get("ticker24h", {}) if isinstance(binance, dict) else {}
        try:
            quote_volume = float(ticker24h.get("quoteVolume", 0.0) or 0.0)
        except Exception:
            quote_volume = 0.0
        try:
            price_change_percent = float(ticker24h.get("priceChangePercent", 0.0) or 0.0)
        except Exception:
            price_change_percent = 0.0

        news_titles = []
        if isinstance(snapshot.get("news"), list):
            news_titles = [row.get("title") for row in snapshot["news"][:3]]
        flags = snapshot.get("headlineFlags", {}) if isinstance(snapshot.get("headlineFlags"), dict) else {}
        # Prod mode: skip news/social keyword scoring to keep production path deterministic.
        # Research/snapshot paths (prod_mode=False) include this enrichment for context.
        if not prod_mode:
            negative_count = int(flags.get("negativeCount", 0))
            positive_count = int(flags.get("positiveCount", 0))
            clean_score -= min(negative_count * 4, 12)
            clean_score += min(positive_count * 2, 6)
        clean_score = max(0.0, min(100.0, clean_score))
        base_clean_score = round(clean_score, 2)
        execution_overlay = self.execution_filter_overlay(symbol, bull_score=bull_score, snapshot=snapshot)
        clean_score = max(0.0, min(100.0, clean_score + float(execution_overlay.get("cleanScoreDelta", 0.0))))

        scored = {
            "symbol": symbol,
            "baseCleanScore": base_clean_score,
            "cleanScore": round(clean_score, 2),
            "bullScore": bull_score,
            "spreadBps": round(spread_bps, 3),
            "funding": round(funding, 6),
            "oi6hChangePct": round(oi_change_pct, 2),
            "quoteVolume": quote_volume,
            "priceChangePercent": price_change_percent,
            "lastPrice": float(ticker24h.get("lastPrice") or f["15m"]["last"] or 0.0),
            "features": {
                interval: {
                    "last": round(f[interval]["last"], 8),
                    "recentHigh": round(f[interval]["recentHigh"], 8),
                    "recentLow": round(f[interval]["recentLow"], 8),
                    "rsi": round(f[interval]["rsi"], 2),
                    "pos": round(f[interval]["pos"], 3),
                    "volVs20": round(f[interval]["vol"], 2),
                }
                for interval in DEFAULT_INTERVALS
            },
            "news": news_titles,
            "headlineFlags": flags,
            "executionFilter": execution_overlay,
            "managementGuidance": execution_overlay.get("managementGuidance", []),
        }
        return self.apply_grade_to_row(scored)

    def apply_provider_enrichment_to_row(self, row: Dict[str, Any], enrichment: Dict[str, Any]) -> Dict[str, Any]:
        overlay = self.provider_overlay(row["symbol"], int(row.get("bullScore", 0)), enrichment)
        row["providerOverlay"] = overlay
        row["providerEnrichment"] = enrichment
        row["cleanScore"] = round(max(0.0, min(100.0, float(row.get("cleanScore", 0.0)) + float(overlay.get("cleanScoreDelta", 0.0)))), 2)
        self.apply_grade_to_row(row)
        return row

    def score_symbols(self, symbols: List[str], top: int = 5, enrich_top: int = 2, tradingview_top: int = 0, persist_state: bool = False, source: str = "symbol_scan", mark_alerts_sent: bool = False, alert_limit: int = 10, delivery_note: Optional[str] = None, prod_mode: bool = False) -> Dict[str, Any]:
        requested = []
        seen = set()
        for symbol in symbols:
            symbol = str(symbol or "").upper().strip()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            requested.append(symbol)

        scored: List[Dict[str, Any]] = []
        for symbol in requested:
            try:
                # prod_mode: skip news keyword scoring to keep production path deterministic
                score = self._score_symbol(symbol, include_auth_providers=False, prod_mode=prod_mode)
                self.apply_grade_to_row(score)
                scored.append(score)
            except Exception as exc:
                scored.append({"symbol": symbol, "error": str(exc)})

        tradable = [row for row in scored if "error" not in row]
        massive_grouped_daily = None
        effective_enrich_top = 0 if prod_mode else enrich_top
        effective_tradingview_top = 0 if prod_mode else tradingview_top
        if effective_enrich_top > 0 and tradable:
            try:
                massive_grouped_daily = self.massive_grouped_daily()
            except Exception:
                massive_grouped_daily = None
            candidates = sorted(tradable, key=self.scan_sort_key, reverse=True)
            selected_symbols = [row["symbol"] for row in candidates[:effective_enrich_top]]
            for row in tradable:
                if row["symbol"] not in selected_symbols:
                    continue
                enrichment = self.provider_enrichment(row["symbol"], massive_grouped_daily=massive_grouped_daily)
                self.apply_provider_enrichment_to_row(row, enrichment)

        if effective_tradingview_top > 0 and tradable:
            candidates = sorted(tradable, key=self.scan_sort_key, reverse=True)
            selected_symbols = [row["symbol"] for row in candidates[:effective_tradingview_top]]
            for row in tradable:
                if row["symbol"] not in selected_symbols:
                    continue
                confirmation = self.tradingview_confirmation(row["symbol"], bull_score=int(row.get("bullScore", 0)))
                self.apply_tradingview_enrichment_to_row(row, confirmation)

        alert_state_map = (self.load_alert_state().get("setups") or {})
        for row in tradable:
            self.apply_grade_to_row(row)
            # prod_mode: freeze feedback influence until minimum sample threshold is met
            self._apply_feedback_v2_to_row(row, prod_mode=prod_mode)
            self.apply_actionable_setup_plan(row)
            direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
            alert_record = alert_state_map.get(self.setup_state_key(row["symbol"], direction)) if direction in {"long", "short"} else None
            self.apply_setup_state_preview_to_row(row, alert_record=alert_record)

        output_tradable = tradable
        pruned_lifecycle_rows: List[str] = []
        if prod_mode:
            output_tradable = []
            for row in tradable:
                lifecycle_state = self.setup_lifecycle_state(row)
                if lifecycle_state in {"invalidated", "stale"}:
                    pruned_lifecycle_rows.append(str(row.get("symbol") or ""))
                    continue
                output_tradable.append(row)

        longs = sorted([r for r in output_tradable if r["bullScore"] >= 2 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]
        shorts = sorted([r for r in output_tradable if r["bullScore"] <= -2 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]
        watch = sorted([r for r in output_tradable if -1 <= r["bullScore"] <= 1 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]

        result = {
            "generatedAt": int(time.time()),
            "source": source,
            "prodMode": prod_mode,
            "requestedSymbols": requested,
            "resolvedTradable": [row["symbol"] for row in output_tradable],
            "resolvedTradableAll": [row["symbol"] for row in tradable],
            "errors": [row for row in scored if "error" in row],
            "massiveGroupedDailyDate": massive_grouped_daily.get("date") if isinstance(massive_grouped_daily, dict) else None,
            "executionFeedbackPath": str(EXECUTION_FEEDBACK_STATE_PATH),
            "productionSignalEvalLogPath": str(PRODUCTION_SIGNAL_EVAL_LOG_PATH),
            "watchlistStatePath": str(WATCHLIST_STATE_PATH),
            "alertStatePath": str(ALERT_STATE_PATH),
            "authProvidersAppliedTo": [row["symbol"] for row in tradable if "providerOverlay" in row],
            "tradingviewAppliedTo": [row["symbol"] for row in tradable if "tradingviewOverlay" in row],
            "productionPruned": {
                "providerEnrichmentDisabled": bool(prod_mode),
                "tradingviewDisabled": bool(prod_mode),
                "requestedEnrichTop": enrich_top,
                "requestedTradingviewTop": tradingview_top,
                "effectiveEnrichTop": effective_enrich_top,
                "effectiveTradingviewTop": effective_tradingview_top,
                "lifecycleHiddenSymbols": pruned_lifecycle_rows,
            },
            "tradingviewStatus": self.tradingview_status(),
            "alertEligible": [] if mark_alerts_sent else [row["symbol"] for row in output_tradable if row.get("alertEligible")],
            "watchlistEligible": [] if mark_alerts_sent else [row["symbol"] for row in output_tradable if row.get("watchlistEligible")],
            "topLongs": longs,
            "topShorts": shorts,
            "topWatch": watch,
        }
        self.production_eval_log_scan_batch(result, tradable)
        if persist_state:
            result["stateSync"] = self.sync_setup_state(tradable, source=source)
            result["pendingAlerts"] = self.pending_alerts(limit=alert_limit, mark_sent=mark_alerts_sent, notes=delivery_note)
            result["stateStatus"] = self.setup_state_status()
        return result

    def watchlist_refresh(self, top: int = 5, max_symbols: int = 12, enrich_top: int = 2, tradingview_top: int = 0, persist_state: bool = True, mark_alerts_sent: bool = False, alert_limit: int = 10, delivery_note: Optional[str] = None, prod_mode: bool = True) -> Dict[str, Any]:
        # prod_mode=True by default: production watchlist path is Binance-native and
        # deterministic. News keyword scoring and low-sample feedback influence are disabled.
        watchlist = self.load_watchlist_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        active_records = []
        for record in setups.values():
            if not isinstance(record, dict):
                continue
            if not record.get("active"):
                continue
            if self.setup_lifecycle_state(record) not in CANONICAL_SETUP_ACTIVE_STATES:
                continue
            active_records.append(record)
        active_records = sorted(active_records, key=lambda record: (self.grade_priority(record.get("grade")), float(record.get("cleanScore", 0.0) or 0.0), float(record.get("gradeScore", 0.0) or 0.0)), reverse=True)
        selected_symbols = []
        seen = set()
        for record in active_records:
            symbol = str(record.get("symbol") or "").upper()
            if not symbol or symbol in seen:
                continue
            seen.add(symbol)
            selected_symbols.append(symbol)
            if len(selected_symbols) >= max_symbols:
                break
        result = self.score_symbols(selected_symbols, top=top, enrich_top=enrich_top, tradingview_top=tradingview_top, persist_state=persist_state, source="watchlist_refresh", mark_alerts_sent=mark_alerts_sent, alert_limit=alert_limit, delivery_note=delivery_note, prod_mode=prod_mode)
        result["selectedSetupCount"] = len(active_records)
        return result

    # ─── Deterministic Notification Renderer ──────────────────────────────

    def load_notification_render_state(self) -> Dict[str, Any]:
        return load_json_file(NOTIFICATION_RENDER_STATE_PATH, {
            "updatedAt": None,
            "lastCycleAt": None,
            "lastKind": None,
            "lastMode": None,
            "pairs": {},
        })

    def save_notification_render_state(self, state: Dict[str, Any]) -> None:
        payload = dict(state)
        payload["updatedAt"] = utc_now_iso()
        save_json_file(NOTIFICATION_RENDER_STATE_PATH, payload)

    def notification_format_value(self, value: Any) -> str:
        return _notification_format_value(value)

    def notification_join_flags(self, values: Any) -> str:
        return _notification_join_flags(values)

    def notification_crowding_note(self, actionable: Dict[str, Any]) -> str:
        return _notification_crowding_note(actionable)

    def notification_lane_for_setup(self, setup: Dict[str, Any], monitor: Optional[Dict[str, Any]] = None, live_trust: Optional[Dict[str, Any]] = None) -> Optional[str]:
        return _notification_lane_for_setup(setup, monitor=monitor, live_trust=live_trust)

    def notification_entry_from_setup(self, setup_key: str, setup: Dict[str, Any], alert: Optional[Dict[str, Any]] = None, monitor: Optional[Dict[str, Any]] = None, live_trust: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        return _notification_entry_from_setup(setup_key, setup, alert=alert, monitor=monitor, live_trust=live_trust)

    def notification_entry_signature(self, entry: Dict[str, Any]) -> str:
        return _notification_entry_signature(entry)

    def notification_update_reasons(self, entry: Dict[str, Any], previous: Dict[str, Any]) -> List[str]:
        return _notification_update_reasons(entry, previous)

    def notification_sort_key(self, entry: Dict[str, Any]) -> Any:
        return _notification_sort_key(entry)

    def notification_snapshot(self, max_live: int = 3, max_standby: int = 5) -> Dict[str, Any]:
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        live_state = self.load_live_trigger_state()
        live_trust = self.live_trigger_live_trust(live_state)
        setups = watchlist.get("setups", {}) if isinstance(watchlist, dict) else {}
        alert_setups = alerts.get("setups", {}) if isinstance(alerts, dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        entries: List[Dict[str, Any]] = []
        for setup_key, setup in setups.items():
            if not isinstance(setup, dict):
                continue
            monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
            entry = self.notification_entry_from_setup(setup_key, setup, alert_setups.get(setup_key), monitor=monitor, live_trust=live_trust)
            if entry:
                entries.append(entry)
        entries = sorted(entries, key=self.notification_sort_key, reverse=True)
        live = [entry for entry in entries if entry.get("lane") == "LIVE"]
        ready = [entry for entry in entries if entry.get("lane") == "READY"]
        confirmed = [entry for entry in entries if entry.get("lane") == "CONFIRMED"]
        standby = [entry for entry in entries if entry.get("lane") == "STANDBY"]
        return {
            "generatedAt": watchlist.get("updatedAt") or utc_now_iso(),
            "allEntries": entries,
            "live": live[:max_live],
            "ready": ready[:max_live],
            "confirmed": confirmed[:max_live],
            "standby": standby[:max_standby],
            "liveTotal": len(live),
            "readyTotal": len(ready),
            "confirmedTotal": len(confirmed),
            "standbyTotal": len(standby),
            "hiddenLive": max(0, len(live) - max_live),
            "hiddenReady": max(0, len(ready) - max_live),
            "hiddenConfirmed": max(0, len(confirmed) - max_live),
            "hiddenStandby": max(0, len(standby) - max_standby),
            "liveTrust": live_trust,
            "setups": setups,
            "alerts": alert_setups,
            "watchlist": watchlist,
        }

    def notification_recent_deltas(self, since_iso: Optional[str]) -> List[Dict[str, Any]]:
        if not since_iso:
            return []
        since_dt = parse_iso_timestamp(since_iso)
        if not since_dt:
            return []
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        recent = []
        for source_name, rows in (("watchlist", watchlist.get("recent", [])), ("alerts", alerts.get("recent", []))):
            if not isinstance(rows, list):
                continue
            for row in rows:
                if not isinstance(row, dict):
                    continue
                changed_at = parse_iso_timestamp(row.get("changedAt"))
                if not changed_at or changed_at <= since_dt:
                    continue
                layer = row.get("layer")
                to_state = row.get("toState")
                if layer != "lifecycle":
                    continue
                if to_state not in {"invalidated", "stale"}:
                    continue
                recent.append({
                    "source": source_name,
                    "setupKey": row.get("setupKey"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "layer": layer,
                    "fromState": row.get("fromState"),
                    "toState": to_state,
                    "reasonCode": row.get("reasonCode"),
                    "eventType": row.get("eventType"),
                    "changedAt": row.get("changedAt"),
                    "setupVersion": row.get("setupVersion"),
                    "readinessVersion": row.get("readinessVersion"),
                })
        recent = sorted(recent, key=lambda item: str(item.get("changedAt") or ""))
        deduped: List[Dict[str, Any]] = []
        seen = set()
        for item in recent:
            key = (
                item.get("setupKey"),
                item.get("layer"),
                item.get("toState"),
                item.get("reasonCode"),
                item.get("changedAt"),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
        return deduped

    def notification_material_changes(self, snapshot: Dict[str, Any], previous_pairs: Dict[str, Any], since_iso: Optional[str]) -> List[Dict[str, Any]]:
        changes: List[Dict[str, Any]] = []
        current_pairs: Dict[str, Dict[str, Any]] = {}
        for entry in snapshot.get("allEntries", []):
            setup_key = str(entry.get("setupKey") or "")
            if not setup_key:
                continue
            signature = self.notification_entry_signature(entry)
            current_pairs[setup_key] = {
                "signature": signature,
                "lane": entry.get("lane"),
                "setupVersion": entry.get("setupVersion"),
                "readinessVersion": entry.get("readinessVersion"),
                "entry": entry,
            }
            previous = previous_pairs.get(setup_key) if isinstance(previous_pairs, dict) else None
            if not isinstance(previous, dict):
                changes.append({"kind": "PAIR_NEW", "entry": entry})
                continue
            # PATCH-1: before flagging PAIR_UPDATED for readinessVersion change,
            # check if the zone/sl drift is within noise tolerance.
            # If so, use prev quantized values → identical hash, skip PAIR_UPDATED.
            noise_absorbed = False
            if previous.get("readinessVersion") != entry.get("readinessVersion"):
                prev_entry = previous.get("entry") if isinstance(previous.get("entry"), dict) else {}
                pz_low  = float(prev_entry.get("entryZoneLow") or 0)
                pz_high = float(prev_entry.get("entryZoneHigh") or 0)
                ps_low  = float(prev_entry.get("stopLoss") or 0)
                cz_low  = float(entry.get("entryZoneLow") or 0)
                cz_high = float(entry.get("entryZoneHigh") or 0)
                cs_low  = float(entry.get("stopLoss") or 0)
                if self._within_noise_tolerance(pz_low, pz_high, ps_low, cz_low, cz_high, cs_low):
                    # Within tolerance — absorb noise; mark as no-change for readinessVersion
                    entry = dict(entry)
                    entry["readinessVersion"] = previous.get("readinessVersion")
                    signature = self.notification_entry_signature(entry)
                    noise_absorbed = True
            if previous.get("signature") != signature or previous.get("lane") != entry.get("lane"):
                reasons = self.notification_update_reasons(entry, previous)
                if noise_absorbed and "READINESS_VERSION_CHANGED" in reasons:
                    reasons = [r for r in reasons if r != "READINESS_VERSION_CHANGED"]
                changes.append({
                    "kind": "PAIR_UPDATED",
                    "entry": entry,
                    "previous": previous,
                    "reasons": reasons,
                })

        setups = snapshot.get("setups", {}) if isinstance(snapshot.get("setups"), dict) else {}
        for setup_key, previous in (previous_pairs or {}).items():
            if setup_key in current_pairs:
                continue
            setup = setups.get(setup_key, {}) if isinstance(setups.get(setup_key), dict) else {}
            changes.append({
                "kind": "PAIR_EXITED",
                "setupKey": setup_key,
                "symbol": setup.get("symbol") or previous.get("symbol"),
                "side": setup.get("side") or previous.get("side"),
                "lane": previous.get("lane"),
                "lifecycleState": self.setup_lifecycle_state(setup) or previous.get("lifecycleState"),
                "reasonCode": ((setup.get("lifecycle") or {}).get("reasonCode") if isinstance(setup.get("lifecycle"), dict) else None),
                "setupVersion": setup.get("setupVersion") or previous.get("setupVersion"),
                "readinessVersion": setup.get("readinessVersion") or previous.get("readinessVersion"),
            })

        exited_keys = {change.get("setupKey") for change in changes if change.get("kind") == "PAIR_EXITED"}
        for delta in self.notification_recent_deltas(since_iso):
            if delta.get("setupKey") in exited_keys:
                continue
            changes.append({"kind": "TRANSITION", "delta": delta})
        return changes

    def _entry_badges(self, entry: Dict[str, Any]) -> str:
        """Build badges string for entry: ENTRY_CONFIRMED + delivery status."""
        badges: List[str] = []
        # ENTRY_CONFIRMED badge: LIVE lane + fresh confirmed (< 10min)
        if entry.get("lane") == "LIVE" and entry.get("isFresh"):
            badges.append("ENTRY_CONFIRMED")
        # Delivery status badge
        ds = entry.get("deliveryStatus")
        if ds == "sent":
            badges.append("✅ Delivered")
        elif ds == "failed":
            badges.append("⚠️ Failed")
        elif ds == "pending":
            badges.append("⏳ Pending")
        return " | " + ", ".join(badges) if badges else ""

    def notification_render_entry_compact(self, entry: Dict[str, Any]) -> str:
        return _notification_render_entry_compact(entry)

    def notification_render_entry_detailed(self, entry: Dict[str, Any]) -> str:
        return _notification_render_entry_detailed(entry)

    def notification_render_delta(self, change: Dict[str, Any]) -> str:
        return _notification_render_delta(change)

    def notification_render(self, mode: str = "compact", cycle_kind: str = "watchlist", max_live: int = 3, max_standby: int = 5, persist_render_state: bool = False) -> str:
        snapshot = self.notification_snapshot(max_live=max_live, max_standby=max_standby)
        render_state = self.load_notification_render_state()
        previous_pairs = render_state.get("pairs", {}) if isinstance(render_state.get("pairs"), dict) else {}
        last_cycle_at = render_state.get("lastCycleAt")
        changes = self.notification_material_changes(snapshot, previous_pairs, last_cycle_at)

        next_pairs: Dict[str, Any] = {}
        for entry in snapshot.get("allEntries", []):
            next_pairs[entry.get("setupKey")] = {
                "symbol": entry.get("symbol"),
                "side": entry.get("side"),
                "lane": entry.get("lane"),
                "lifecycleState": entry.get("lifecycleState"),
                "setupVersion": entry.get("setupVersion"),
                "readinessVersion": entry.get("readinessVersion"),
                "signature": self.notification_entry_signature(entry),
            }

        if persist_render_state:
            render_state["lastCycleAt"] = snapshot.get("generatedAt")
            render_state["lastKind"] = cycle_kind
            render_state["lastMode"] = mode
            render_state["pairs"] = next_pairs
            self.save_notification_render_state(render_state)

        if mode == "material" and not changes:
            return "NO_REPLY"

        lines = [
            f"[{cycle_kind.upper()} {str(mode).upper()}] {self.notification_format_value(snapshot.get('generatedAt'))}",
            f"snapshot: LIVE={snapshot.get('liveTotal', 0)}, READY={snapshot.get('readyTotal', 0)}, CONFIRMED={snapshot.get('confirmedTotal', 0)}, STANDBY={snapshot.get('standbyTotal', 0)}, materialChanges={len(changes)}",
        ]
        if mode in {"compact", "detailed"}:
            for section_key, section_label in (("live", "LIVE"), ("ready", "READY"), ("confirmed", "CONFIRMED"), ("standby", "STANDBY")):
                entries_list = snapshot.get(section_key, [])
                if entries_list:
                    lines.append("")
                    lines.append(section_label)
                    for entry in entries_list:
                        rendered = self.notification_render_entry_detailed(entry) if mode == "detailed" else self.notification_render_entry_compact(entry)
                        lines.append(f"- {rendered}" if mode == "compact" else rendered)
                    hidden_key = f"hidden{section_key.capitalize()}"
                    if snapshot.get(hidden_key, 0):
                        lines.append(f"- +{snapshot.get(hidden_key)} more {section_label} pair(s)")
                else:
                    lines.append("")
                    lines.append(section_label)
                    lines.append("- none")

        lines.append("")
        lines.append("DELTA")
        if changes:
            for change in changes:
                lines.append(f"- {self.notification_render_delta(change)}")
        else:
            lines.append("- NO_MATERIAL_CHANGE")
        return "\n".join(lines).strip()

    def entry_ready_price_in_zone(self, price: Any, low: Any, high: Any) -> bool:
        try:
            if price is None or low is None or high is None:
                return False
            return float(low) <= float(price) <= float(high)
        except Exception:
            return False

    def entry_ready_event_priority(self, event_type: str) -> int:
        return _entry_ready_event_priority(event_type)

    def entry_ready_sort_key(self, event: EntryReadyEvent) -> Any:
        return _entry_ready_sort_key(event)

    def entry_ready_build_event(self, setup_key: str, setup: Dict[str, Any], monitor: Optional[Dict[str, Any]], event_type: str, simulated: bool = False, cancel_reason: Optional[str] = None) -> EntryReadyEvent:
        if EntryReadyEvent is None:
            raise RuntimeError("telegram_execution_cards module unavailable")
        source = setup if isinstance(setup, dict) and setup else (monitor if isinstance(monitor, dict) else {})
        actionable = source.get("actionableAlert") if isinstance(source.get("actionableAlert"), dict) else {}
        if not actionable and isinstance(monitor, dict):
            actionable = monitor.get("actionableAlert") if isinstance(monitor.get("actionableAlert"), dict) else {}
        take_profit = actionable.get("takeProfit") if isinstance(actionable.get("takeProfit"), dict) else {}
        fc = monitor.get("finalConfirmation") if isinstance(monitor, dict) and isinstance(monitor.get("finalConfirmation"), dict) else {}
        fc_identity = self.final_confirmation_identity(monitor, active_only=True)
        metadata_setup_version = source.get("setupVersion") or (monitor.get("setupVersion") if isinstance(monitor, dict) else None)
        metadata_readiness_version = source.get("readinessVersion") or (monitor.get("readinessVersion") if isinstance(monitor, dict) else None)
        if event_type == "LIVE_STRICT" and fc_identity:
            metadata_setup_version = fc_identity.get("setupVersion") or metadata_setup_version
            metadata_readiness_version = fc_identity.get("readinessVersion") or metadata_readiness_version
        metadata = {
            "cleanScore": source.get("cleanScore") if isinstance(source, dict) else None,
            "contextVerdict": (monitor.get("contextVerdict") if isinstance(monitor, dict) else None),
            "triggerReason": ((fc.get("triggerReason") or monitor.get("triggerReason")) if isinstance(monitor, dict) else None),
            "setupVersion": metadata_setup_version,
            "readinessVersion": metadata_readiness_version,
        }
        return EntryReadyEvent(
            event_type=event_type,
            symbol=str(source.get("symbol") or (monitor.get("symbol") if isinstance(monitor, dict) else "")),
            side=str(source.get("side") or (monitor.get("side") if isinstance(monitor, dict) else "")),
            grade=source.get("grade") or (monitor.get("grade") if isinstance(monitor, dict) else None),
            entry_low=((actionable.get("entryZone") or {}).get("low") if isinstance(actionable.get("entryZone"), dict) else None),
            entry_high=((actionable.get("entryZone") or {}).get("high") if isinstance(actionable.get("entryZone"), dict) else None),
            stop_loss=actionable.get("stopLoss"),
            tp1=take_profit.get("tp1"),
            tp2=take_profit.get("tp2"),
            tp3=take_profit.get("tp3"),
            current_price=actionable.get("currentPrice"),
            confirmed_at=(fc.get("confirmedAt") or (monitor.get("triggeredAt") if isinstance(monitor, dict) else None)),
            trigger=actionable.get("trigger"),
            invalidation=actionable.get("invalidation"),
            execution_note=actionable.get("executionNote"),
            risk_flags=(actionable.get("riskFlags") if isinstance(actionable.get("riskFlags"), list) else (source.get("riskFlags") if isinstance(source.get("riskFlags"), list) else [])),
            crowding_note=(((actionable.get("crowdingSignals") or {}).get("crowdingNote")) if isinstance(actionable.get("crowdingSignals"), dict) else None),
            current_status=actionable.get("currentStatus"),
            cancel_reason=cancel_reason,
            setup_key=setup_key,
            simulated=simulated,
            metadata=metadata,
        )

    def entry_ready_collect_events(self, limit: int = 10) -> List[EntryReadyEvent]:
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        live_state = self.load_live_trigger_state()
        live_trust = self.live_trigger_live_trust(live_state)
        setups = watchlist.get("setups", {}) if isinstance(watchlist.get("setups"), dict) else {}
        alert_setups = alerts.get("setups", {}) if isinstance(alerts.get("setups"), dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        events: List[EntryReadyEvent] = []
        for setup_key, setup in setups.items():
            if not isinstance(setup, dict):
                continue
            monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
            entry = self.notification_entry_from_setup(setup_key, setup, alert_setups.get(setup_key), monitor=monitor, live_trust=live_trust)
            if not isinstance(entry, dict):
                continue
            lane = entry.get("lane")
            event_type = None
            fc = monitor.get("finalConfirmation") if isinstance(monitor, dict) and isinstance(monitor.get("finalConfirmation"), dict) else None
            fc_active = isinstance(fc, dict) and fc.get("state") == "active"
            if lane == "LIVE" and fc_active:
                event_type = "LIVE_STRICT"
            elif lane == "READY" and not bool(entry.get("doNotChase")) and self.entry_ready_price_in_zone(entry.get("currentPrice"), entry.get("entryZoneLow"), entry.get("entryZoneHigh")):
                event_type = "READY_IN_ZONE"
            if not event_type:
                continue
            events.append(self.entry_ready_build_event(setup_key, setup, monitor, event_type=event_type))
        return sorted(events, key=self.entry_ready_sort_key, reverse=True)[:limit]

    def entry_ready_simulated_live_event(self) -> Optional[EntryReadyEvent]:
        live_state = self.load_live_trigger_state()
        live_trust = self.live_trigger_live_trust(live_state)
        if not bool(live_trust.get("trustLive", True)):
            return None
        watchlist = self.load_watchlist_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist.get("setups"), dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        for setup_key, monitor in monitors.items():
            if not isinstance(monitor, dict):
                continue
            fc = monitor.get("finalConfirmation") if isinstance(monitor.get("finalConfirmation"), dict) else None
            if not isinstance(fc, dict):
                continue
            if fc.get("state") != "active":
                continue
            if not fc.get("confirmedAt"):
                continue
            if self.runtime_state(monitor) == "inactive":
                continue
            return self.entry_ready_build_event(setup_key, {}, monitor, event_type="LIVE_STRICT", simulated=True)
        return None

    def entry_ready_simulated_cancel_event(self) -> Optional[EntryReadyEvent]:
        live_state = self.load_live_trigger_state()
        watchlist = self.load_watchlist_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist.get("setups"), dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        reason_map = {
            "ZONE_RECLAIMED": "price left the intended execution zone",
            "TARGET_REMOVED": "original setup was removed from the active watchlist",
        }
        for setup_key, monitor in monitors.items():
            if not isinstance(monitor, dict):
                continue
            inactive_reason = self.runtime_reason_code(monitor)
            if not inactive_reason:
                continue
            cancel_reason = reason_map.get(str(inactive_reason), "original setup no longer valid")
            return self.entry_ready_build_event(setup_key, {}, monitor, event_type="CANCEL", simulated=True, cancel_reason=cancel_reason)
        return None

    def entry_ready_preview(self, limit: int = 3, include_simulated_live: bool = True, include_simulated_cancel: bool = True) -> Dict[str, Any]:
        if TelegramExecutionCardFormatter is None:
            raise RuntimeError("telegram_execution_cards module unavailable")
        formatter = TelegramExecutionCardFormatter()
        ready_events = self.entry_ready_collect_events(limit=limit)
        cards: List[Dict[str, Any]] = []
        for event in ready_events:
            rendered, fallbacks = formatter.render(event)
            cards.append({
                "eventType": event.event_type,
                "setupKey": event.setup_key,
                "symbol": event.symbol,
                "side": event.side,
                "simulated": event.simulated,
                "charCount": len(rendered),
                "fallbacksUsed": fallbacks,
                "card": rendered,
            })
        if include_simulated_live:
            simulated_live = self.entry_ready_simulated_live_event()
            if simulated_live:
                rendered, fallbacks = formatter.render(simulated_live)
                cards.append({
                    "eventType": simulated_live.event_type,
                    "setupKey": simulated_live.setup_key,
                    "symbol": simulated_live.symbol,
                    "side": simulated_live.side,
                    "simulated": True,
                    "charCount": len(rendered),
                    "fallbacksUsed": fallbacks,
                    "card": rendered,
                })
        if include_simulated_cancel:
            simulated_cancel = self.entry_ready_simulated_cancel_event()
            if simulated_cancel:
                rendered, fallbacks = formatter.render(simulated_cancel)
                cards.append({
                    "eventType": simulated_cancel.event_type,
                    "setupKey": simulated_cancel.setup_key,
                    "symbol": simulated_cancel.symbol,
                    "side": simulated_cancel.side,
                    "simulated": True,
                    "charCount": len(rendered),
                    "fallbacksUsed": fallbacks,
                    "card": rendered,
                })
        return {
            "generatedAt": utc_now_iso(),
            "cards": cards,
        }

    def entry_ready_preview_text(self, limit: int = 3, include_simulated_live: bool = True, include_simulated_cancel: bool = True) -> str:
        preview = self.entry_ready_preview(limit=limit, include_simulated_live=include_simulated_live, include_simulated_cancel=include_simulated_cancel)
        cards = [row.get("card") for row in preview.get("cards", []) if isinstance(row, dict) and row.get("card")]
        return "\n\n-----\n\n".join(cards).strip() if cards else "NO_CARDS"

    def entry_ready_empty_state(self) -> Dict[str, Any]:
        return {
            "updatedAt": None,
            "activeBySetup": {},
            "ledger": {},
            "cancelLedger": {},
            "cycles": [],
            # ── C2 Anti-Spam Opportunity Gate ──────────────────────────────────
            # Tracks operator-facing opportunity identity and delivery state.
            # Replaces pure fingerprint-based dedup with opportunity semantics:
            #   - one READY per open opportunity
            #   - one CANCEL per open opportunity
            #   - suppress repeated READY/CANCEL for same opportunity
            #   - reopen only after material reset (not tiny drift)
            "opportunityGate": {},  # setupKey → {state, opportunityKey, deliveryKey, openedAt, cancelledAt, cooldownUntil, lastReadyFingerprint, lastCancelFingerprint, suppressReason}
        }

    def load_entry_ready_dry_run_state(self) -> Dict[str, Any]:
        return load_json_file(ENTRY_READY_DRY_RUN_STATE_PATH, self.entry_ready_empty_state())

    def save_entry_ready_dry_run_state(self, state: Dict[str, Any]) -> None:
        save_json_file(ENTRY_READY_DRY_RUN_STATE_PATH, state)

    def load_entry_ready_delivery_state(self) -> Dict[str, Any]:
        return load_json_file(ENTRY_READY_DELIVERY_STATE_PATH, self.entry_ready_empty_state())

    def save_entry_ready_delivery_state(self, state: Dict[str, Any]) -> None:
        save_json_file(ENTRY_READY_DELIVERY_STATE_PATH, state)

    def load_periodic_delivery_log(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        if not PERIODIC_DELIVERY_LOG_PATH.exists():
            return rows
        try:
            with PERIODIC_DELIVERY_LOG_PATH.open("r", encoding="utf-8") as handle:
                for raw in handle:
                    raw = str(raw or "").strip()
                    if not raw:
                        continue
                    try:
                        rows.append(json.loads(raw))
                    except Exception:
                        continue
        except Exception:
            return []
        if limit is not None and limit >= 0:
            return rows[-limit:]
        return rows

    def append_entry_ready_dry_run_log(self, row: Dict[str, Any]) -> None:
        ENTRY_READY_DRY_RUN_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        with ENTRY_READY_DRY_RUN_LOG_PATH.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")

    def reset_entry_ready_dry_run_state(self) -> Dict[str, Any]:
        state = self.entry_ready_empty_state()
        state["updatedAt"] = utc_now_iso()
        self.save_entry_ready_dry_run_state(state)
        return state

    def reset_entry_ready_delivery_state(self) -> Dict[str, Any]:
        state = self.entry_ready_empty_state()
        state["updatedAt"] = utc_now_iso()
        self.save_entry_ready_delivery_state(state)
        return state

    def execution_alert_fingerprint(self, event: EntryReadyEvent) -> str:
        """
        Notification identity for Telegram execution alerts.
        Captures the full execution plan user-facing: entry zone, stop loss, TP ladder, event type.
        Built from canonical rounded display values — NOT raw intermediate computed values.

        Separate from setupVersion / readinessVersion which serve the state machine contract.
        """
        parts = [
            str(event.setup_key or ""),
            str(event.event_type or ""),
            self._fp(self.round_price(event.entry_low, reference=event.entry_low or 1.0)),
            self._fp(self.round_price(event.entry_high, reference=event.entry_high or 1.0)),
            self._fp(self.round_price(event.stop_loss, reference=event.entry_low or 1.0)),
            self._fp(self.round_price(event.tp1, reference=event.entry_low or 1.0)),
            self._fp(self.round_price(event.tp2, reference=event.entry_low or 1.0)),
            self._fp(self.round_price(event.tp3, reference=event.entry_low or 1.0)),
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]

    def execution_alert_fingerprint_from_entry(self, entry: Dict[str, Any], event_type: str) -> str:
        """
        Same fingerprint logic as execution_alert_fingerprint(), but accepts an entry dict
        (canonical notification entry format) instead of an EntryReadyEvent object.
        Used by shadow validator and any external consumer that has entry dicts.
        """
        def fp_val(v: Any) -> str:
            if v is None:
                return "_"
            try:
                return f"{float(v):.10g}"
            except Exception:
                return str(v)

        def round_v(v: Any, ref: Any) -> str:
            if v is None:
                return "_"
            try:
                return f"{self.round_price(float(v), reference=float(ref) if ref else 1.0):.10g}"
            except Exception:
                return fp_val(v)

        entry_zone = entry.get("entryZone") or {}
        tp_data = entry.get("takeProfit") or {}
        return hashlib.sha256("|".join([
            str(entry.get("setupKey") or ""),
            str(event_type or ""),
            round_v(entry_zone.get("low"), entry_zone.get("low")),
            round_v(entry_zone.get("high"), entry_zone.get("high")),
            round_v(entry.get("stopLoss"), entry_zone.get("low")),
            round_v(tp_data.get("tp1"), entry_zone.get("low")),
            round_v(tp_data.get("tp2"), entry_zone.get("low")),
            round_v(tp_data.get("tp3"), entry_zone.get("low")),
        ]).encode("utf-8")).hexdigest()[:16]

    def _fp(self, value: Any) -> str:
        """Format a numeric value for fingerprint input — converts to string safely."""
        if value is None:
            return "_"
        try:
            return f"{float(value):.10g}"
        except Exception:
            return str(value)

    def entry_ready_delivery_key(self, event: EntryReadyEvent) -> str:
        """
        Delivery identity for ENTRY_READY execution alerts.
        Now uses execution_alert_fingerprint for identity, not setupVersion/readinessVersion.
        Those remain stored as metadata for observability but are NOT the delivery identity.
        """
        fingerprint = self.execution_alert_fingerprint(event)
        parts = [
            str(event.setup_key or ""),
            str(event.event_type or ""),
            fingerprint,
            str(event.cancel_reason or ""),
        ]
        return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()[:16]

    def runtime_reason_code(self, monitor: Optional[Dict[str, Any]]) -> Optional[str]:
        monitor = monitor if isinstance(monitor, dict) else {}
        runtime = monitor.get("runtime") if isinstance(monitor.get("runtime"), dict) else {}
        reason_code = runtime.get("reasonCode") if isinstance(runtime.get("reasonCode"), str) and runtime.get("reasonCode") else None
        if reason_code:
            return reason_code
        legacy = monitor.get("inactiveReason")
        return str(legacy) if isinstance(legacy, str) and legacy else None

    def entry_ready_cancel_context(self, setup_key: str, setup: Optional[Dict[str, Any]], monitor: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        setup = setup if isinstance(setup, dict) else {}
        monitor = monitor if isinstance(monitor, dict) else {}
        lifecycle_state = self.setup_lifecycle_state(setup)
        readiness_state = self.setup_readiness_state(setup)
        reason_code = self.runtime_reason_code(monitor)
        reason_family = None
        reason_text = None

        if lifecycle_state == "stale":
            reason_family = "STALE"
            reason_text = "the setup has gone stale"
            reason_code = reason_code or "STALE"
        elif lifecycle_state == "invalidated":
            if reason_code == "ZONE_RECLAIMED":
                reason_family = "ZONE_EXIT"
                reason_text = "price left the intended execution zone"
            elif reason_code == "TARGET_REMOVED":
                reason_family = "TARGET_REMOVED"
                reason_text = "the original setup was removed from the active watchlist"
            elif reason_code == "CONTEXT_BROKEN":
                reason_family = "CONTEXT_BROKEN"
                reason_text = "market context invalidated the original setup"
            else:
                reason_family = "INVALIDATED"
                reason_text = "the setup has been invalidated"
                reason_code = reason_code or "INVALIDATED"
        elif reason_code == "ZONE_RECLAIMED":
            reason_family = "ZONE_EXIT"
            reason_text = "price left the intended execution zone"
        elif reason_code == "TARGET_REMOVED":
            reason_family = "TARGET_REMOVED"
            reason_text = "the original setup was removed from the active watchlist"
        elif reason_code == "CONTEXT_BROKEN":
            reason_family = "CONTEXT_BROKEN"
            reason_text = "market context invalidated the original setup"

        if not reason_text:
            return None

        return {
            "text": reason_text,
            "reasonCode": reason_code,
            "reasonFamily": reason_family,
            "currentLifecycleState": lifecycle_state,
            "currentReadinessState": readiness_state,
            "currentSetupVersion": setup.get("setupVersion") or (monitor.get("setupVersion") if isinstance(monitor, dict) else None),
            "currentReadinessVersion": setup.get("readinessVersion") or (monitor.get("readinessVersion") if isinstance(monitor, dict) else None),
        }

    def entry_ready_cancel_reason(self, setup_key: str, setup: Optional[Dict[str, Any]], monitor: Optional[Dict[str, Any]]) -> Optional[str]:
        context = self.entry_ready_cancel_context(setup_key, setup, monitor)
        return context.get("text") if isinstance(context, dict) else None

    def entry_ready_cancel_anchor_event(
        self,
        setup_key: str,
        active_record: Optional[Dict[str, Any]],
        gate_record: Optional[Dict[str, Any]],
        current_setup: Optional[Dict[str, Any]],
        current_monitor: Optional[Dict[str, Any]],
        cancel_context: Dict[str, Any],
    ) -> EntryReadyEvent:
        active_record = active_record if isinstance(active_record, dict) else {}
        gate_record = gate_record if isinstance(gate_record, dict) else {}
        current_setup = current_setup if isinstance(current_setup, dict) else {}
        current_monitor = current_monitor if isinstance(current_monitor, dict) else {}

        actionable = active_record.get("actionableSnapshot") if isinstance(active_record.get("actionableSnapshot"), dict) else {}
        if not actionable and isinstance(gate_record.get("lastActionableSnapshot"), dict):
            actionable = gate_record.get("lastActionableSnapshot") or {}
        entry_zone = actionable.get("entryZone") if isinstance(actionable.get("entryZone"), dict) else {}
        take_profit = actionable.get("takeProfit") if isinstance(actionable.get("takeProfit"), dict) else {}
        anchor_setup_version = active_record.get("setupVersion") or gate_record.get("setupVersion")
        anchor_readiness_version = active_record.get("readinessVersion") or gate_record.get("readinessVersion")
        anchor_delivery_key = active_record.get("deliveryKey") or gate_record.get("deliveryKey")

        metadata = {
            "setupVersion": anchor_setup_version,
            "readinessVersion": anchor_readiness_version,
            "cancelAnchorDeliveryKey": anchor_delivery_key,
            "cancelAnchorSetupVersion": anchor_setup_version,
            "cancelAnchorReadinessVersion": anchor_readiness_version,
            "cancelAnchorSentAt": active_record.get("sentAt") or gate_record.get("openedAt"),
            "cancelAnchorEventType": active_record.get("eventType") or gate_record.get("lastReadyEventType"),
            "cancelCurrentSetupVersion": cancel_context.get("currentSetupVersion"),
            "cancelCurrentReadinessVersion": cancel_context.get("currentReadinessVersion"),
            "cancelCurrentLifecycleState": cancel_context.get("currentLifecycleState"),
            "cancelCurrentReadinessState": cancel_context.get("currentReadinessState"),
            "cancelReasonCode": cancel_context.get("reasonCode"),
            "cancelReasonFamily": cancel_context.get("reasonFamily"),
        }

        return EntryReadyEvent(
            event_type="CANCEL",
            symbol=str(active_record.get("symbol") or gate_record.get("symbol") or current_setup.get("symbol") or current_monitor.get("symbol") or ""),
            side=str(active_record.get("side") or gate_record.get("side") or current_setup.get("side") or current_monitor.get("side") or ""),
            grade=active_record.get("grade") or gate_record.get("grade") or current_setup.get("grade") or current_monitor.get("grade"),
            entry_low=(entry_zone.get("low") if isinstance(entry_zone, dict) else None),
            entry_high=(entry_zone.get("high") if isinstance(entry_zone, dict) else None),
            stop_loss=actionable.get("stopLoss"),
            tp1=(take_profit.get("tp1") if isinstance(take_profit, dict) else None),
            tp2=(take_profit.get("tp2") if isinstance(take_profit, dict) else None),
            tp3=(take_profit.get("tp3") if isinstance(take_profit, dict) else None),
            current_price=actionable.get("currentPrice"),
            confirmed_at=None,
            trigger=actionable.get("trigger"),
            invalidation=actionable.get("invalidation"),
            execution_note=actionable.get("executionNote"),
            risk_flags=(actionable.get("riskFlags") if isinstance(actionable.get("riskFlags"), list) else []),
            crowding_note=(((actionable.get("crowdingSignals") or {}).get("crowdingNote")) if isinstance(actionable.get("crowdingSignals"), dict) else None),
            current_status=actionable.get("currentStatus"),
            cancel_reason=str(cancel_context.get("text") or "original setup no longer valid"),
            setup_key=setup_key,
            simulated=False,
            metadata=metadata,
        )

    def entry_ready_render_record(self, formatter: TelegramExecutionCardFormatter, event: EntryReadyEvent, order_index: int, delivery_key: str, action: str) -> Dict[str, Any]:
        rendered, fallbacks = formatter.render(event)
        meta = event.metadata if isinstance(event.metadata, dict) else {}
        fingerprint = self.execution_alert_fingerprint(event)
        return {
            "order": order_index,
            "action": action,
            "eventType": event.event_type,
            "setupKey": event.setup_key,
            "symbol": event.symbol,
            "side": event.side,
            "deliveryKey": delivery_key,
            "executionFingerprint": fingerprint,
            "setupVersion": meta.get("setupVersion"),
            "readinessVersion": meta.get("readinessVersion"),
            "grade": event.grade,
            "entryZone": {"low": event.entry_low, "high": event.entry_high},
            "stopLoss": event.stop_loss,
            "takeProfit": {"tp1": event.tp1, "tp2": event.tp2, "tp3": event.tp3},
            "currentPrice": event.current_price,
            "trigger": event.trigger,
            "invalidation": event.invalidation,
            "executionNote": event.execution_note,
            "currentStatus": event.current_status,
            "riskFlags": list(event.risk_flags or []),
            "cancelReason": event.cancel_reason,
            "cancelAnchorDeliveryKey": meta.get("cancelAnchorDeliveryKey"),
            "cancelAnchorSetupVersion": meta.get("cancelAnchorSetupVersion"),
            "cancelAnchorReadinessVersion": meta.get("cancelAnchorReadinessVersion"),
            "cancelAnchorSentAt": meta.get("cancelAnchorSentAt"),
            "cancelAnchorEventType": meta.get("cancelAnchorEventType"),
            "cancelCurrentSetupVersion": meta.get("cancelCurrentSetupVersion"),
            "cancelCurrentReadinessVersion": meta.get("cancelCurrentReadinessVersion"),
            "cancelCurrentLifecycleState": meta.get("cancelCurrentLifecycleState"),
            "cancelCurrentReadinessState": meta.get("cancelCurrentReadinessState"),
            "cancelReasonCode": meta.get("cancelReasonCode"),
            "cancelReasonFamily": meta.get("cancelReasonFamily"),
            "charCount": len(rendered),
            "fallbacksUsed": fallbacks,
            "card": rendered,
        }

    # ── C2 Anti-Spam Opportunity Gate ────────────────────────────────────────
    # The C2 delivery layer converts from repeated-event semantics to opportunity
    # semantics: one READY per open opportunity, one CANCEL per cancelled one,
    # suppress repeats for same opportunity, reopen only after material reset.
    #
    # Glossary (delivery-layer, not engine-layer):
    #   idle        = no open C2 opportunity for this setupKey
    #   open        = READY/LIVE sent to Telegram, awaiting execution
    #   cancelled   = CANCEL sent; new READY blocked until reset gate passes
    #   cooldown    = soft suppress; reopen only on strong material reset
    #
    # The gate uses fingerprint for stable operator-facing opportunity identity,
    # not raw setupVersion which changes on every internal scan refresh.

    def entry_opportunity_key(self, event: EntryReadyEvent) -> str:
        """
        Stable operator-facing opportunity identity.
        Derived from the execution plan fields the operator actually cares about:
        symbol, side, entry zone, stop loss, TP ladder.
        Excludes cosmetic/trichy fields (crowding notes, micro price drift, wording).
        Two scans with the same operator-facing plan produce the same key.
        """
        return self.execution_alert_fingerprint(event)

    def entry_gate_state(self, state: Dict[str, Any], setup_key: str) -> Dict[str, Any]:
        """Current opportunity gate record for a setupKey."""
        gate = state.get("opportunityGate", {}) if isinstance(state, dict) else {}
        return gate.get(setup_key, {}) if isinstance(gate, dict) else {}

    def entry_gate_readiness(self, gate_record: Dict[str, Any]) -> str:
        """gate readiness state string or 'idle'."""
        if not isinstance(gate_record, dict):
            return "idle"
        return str(gate_record.get("state") or "idle")

    def entry_is_material_reset(self, prev_event: EntryReadyEvent, curr_event: EntryReadyEvent) -> Tuple[bool, str]:
        """
        Determine whether the difference between prev and curr constitutes a material reset
        that justifies reopening a cancelled opportunity.

        Returns (is_material, reason_code).
        Reason codes:
          'side_changed'           — long ↔ short
          'zone_moved_materially'  — entry zone shifted beyond tolerance
          'stop_moved_materially'  — stop loss moved meaningfully
          'execution_regime_changed' — reclaim vs rejection / execution shape change
          'setup_version_reaffirmed' — new setupVersion after prior invalidation
          'grade_crossed'          — grade bucket changed meaningfully
          'cooldown_expired'       — cooldown period passed + setup re-qualified
          'not_material'           — only tiny / cosmetic drift
        """
        prev_ok = isinstance(prev_event, EntryReadyEvent)
        curr_ok = isinstance(curr_event, EntryReadyEvent)
        if not prev_ok or not curr_ok:
            return (False, "not_material")

        # Side change is always material
        if str(prev_event.side or "") != str(curr_event.side or ""):
            return (True, "side_changed")

        # Side must match for rest
        side = str(prev_event.side or "")

        def _sig(v: Any, ref: Any) -> str:
            if v is None:
                return "_"
            try:
                return f"{self.round_price(float(v), reference=float(ref) if ref else 1.0):.10g}"
            except Exception:
                return "_"

        ref = prev_event.entry_low or 1.0
        prev_entry_low = _sig(prev_event.entry_low, ref)
        prev_entry_high = _sig(prev_event.entry_high, ref)
        prev_stop = _sig(prev_event.stop_loss, ref)
        curr_entry_low = _sig(curr_event.entry_low, ref)
        curr_entry_high = _sig(curr_event.entry_high, ref)
        curr_stop = _sig(curr_event.stop_loss, ref)

        # Entry zone material shift — exclude tiny drift (round to 6 sig figs)
        if prev_entry_low != curr_entry_low or prev_entry_high != curr_entry_high:
            try:
                def _pct(a: Any, b: Any) -> float:
                    aa = abs(float(a))
                    bb = abs(float(b))
                    if bb < 1e-12:
                        return 0.0
                    return abs(aa - bb) / bb

                delta_low = _pct(curr_event.entry_low, prev_event.entry_low)
                delta_high = _pct(curr_event.entry_high, prev_event.entry_high)
                # material = > 0.5% zone shift
                if delta_low > 0.005 or delta_high > 0.005:
                    return (True, "zone_moved_materially")
            except Exception:
                pass

        # Stop loss material shift — > 0.5%
        if prev_stop != curr_stop:
            try:
                ref_s = abs(float(prev_event.stop_loss or 1.0))
                curr_s = abs(float(curr_event.stop_loss or 1.0))
                if ref_s > 1e-12 and abs(curr_s - ref_s) / ref_s > 0.005:
                    return (True, "stop_moved_materially")
            except Exception:
                pass

        # Take-profit material shift — > 0.5% change in any TP level
        for tp_label in ("tp1", "tp2", "tp3"):
            prev_tp = getattr(prev_event, tp_label, None)
            curr_tp = getattr(curr_event, tp_label, None)
            if prev_tp is None and curr_tp is None:
                continue
            if prev_tp is None or curr_tp is None:
                return (True, f"tp_{tp_label}_changed_materially")
            try:
                ref_tp = abs(float(prev_tp))
                curr_tp_val = abs(float(curr_tp))
                if ref_tp > 1e-12:
                    delta = abs(curr_tp_val - ref_tp) / ref_tp
                    if delta > 0.005:
                        return (True, f"tp_{tp_label}_changed_materially")
            except Exception:
                pass

        return (False, "not_material")

    def entry_ready_plan_cycle(self, limit: int, formatter: TelegramExecutionCardFormatter, state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        # ── C2 Anti-Spam Opportunity Gate ─────────────────────────────────────────
        # Converts event-stream delivery to opportunity-stream delivery:
        #   Rule 1: one READY per open opportunity
        #   Rule 2: one CANCEL per cancelled opportunity
        #   Rule 3: suppress repeated READY for same opportunity (same fingerprint)
        #   Rule 4: suppress duplicate CANCEL for same cancelled opportunity
        #   Rule 5: reopen after cancel only on material reset gate
        #
        # The gate uses fingerprint for stable operator-facing opportunity identity.
        # State machine uses deliveryKey for fine-grained delivery dedup (same event
        # in same cycle). The opportunity gate is the coarser layer on top.
        # ───────────────────────────────────────────────────────────────────────
        state = dict(state or self.entry_ready_empty_state())
        watchlist = self.load_watchlist_state()
        live_state = self.load_live_trigger_state()
        setups = watchlist.get("setups", {}) if isinstance(watchlist.get("setups"), dict) else {}
        monitors = live_state.get("monitors", {}) if isinstance(live_state.get("monitors"), dict) else {}
        cycle_id = hashlib.sha256(f"{utc_now_iso()}|{time.time()}".encode("utf-8")).hexdigest()[:12]

        collected = self.entry_ready_collect_events(limit=limit)
        event_counts = {
            "READY_IN_ZONE": sum(1 for event in collected if event.event_type == "READY_IN_ZONE"),
            "LIVE_STRICT": sum(1 for event in collected if event.event_type == "LIVE_STRICT"),
            "CANCEL": 0,
        }

        # ── Load delivery-layer state ───────────────────────────────────────
        previous_active = dict(state.get("activeBySetup", {}) or {})
        cancel_ledger = dict(state.get("cancelLedger", {}) or {})
        # opportunityGate: setupKey → gate record
        opportunity_gate = dict(state.get("opportunityGate", {}) or {})
        would_send: List[Dict[str, Any]] = []
        suppressed: List[Dict[str, Any]] = []
        seen_setups = set()
        order_index = 0

        # ── Phase 1: READY/LIVE events (from entry_ready_collect_events) ────
        for event in collected:
            setup_key = str(event.setup_key or "")
            seen_setups.add(setup_key)
            delivery_key = self.entry_ready_delivery_key(event)
            fingerprint = self.execution_alert_fingerprint(event)
            gate_record = opportunity_gate.get(setup_key, {}) if isinstance(opportunity_gate.get(setup_key), dict) else {}
            gate_state = self.entry_gate_readiness(gate_record)

            # ── Gate: suppress READY if opportunity already open ──────────────
            if gate_state == "open":
                # Allow through if execution plan has materially changed
                prev_actionable = gate_record.get("lastActionableSnapshot") or {}
                prev_zone = prev_actionable.get("entryZone", {}) or {}
                prev_tp = prev_actionable.get("takeProfit", {}) or {}
                prev_for_comparison = EntryReadyEvent(
                    event_type=str(gate_record.get("lastReadyEventType") or "READY_IN_ZONE"),
                    symbol=str(gate_record.get("symbol") or ""),
                    side=str(gate_record.get("side") or ""),
                    grade=gate_record.get("grade"),
                    entry_low=(prev_zone.get("low") if isinstance(prev_zone, dict) else None),
                    entry_high=(prev_zone.get("high") if isinstance(prev_zone, dict) else None),
                    stop_loss=prev_actionable.get("stopLoss"),
                    tp1=(prev_tp.get("tp1") if isinstance(prev_tp, dict) else None),
                    tp2=(prev_tp.get("tp2") if isinstance(prev_tp, dict) else None),
                    tp3=(prev_tp.get("tp3") if isinstance(prev_tp, dict) else None),
                    current_price=prev_actionable.get("currentPrice"),
                    confirmed_at=None,
                    trigger=prev_actionable.get("trigger"),
                    invalidation=prev_actionable.get("invalidation"),
                    execution_note=prev_actionable.get("executionNote"),
                    risk_flags=prev_actionable.get("riskFlags", []) if isinstance(prev_actionable, dict) else [],
                    crowding_note=None,
                    current_status=prev_actionable.get("currentStatus"),
                    cancel_reason=None,
                    setup_key=setup_key,
                    simulated=True,
                    metadata={},
                )
                is_material, reset_reason = self.entry_is_material_reset(prev_for_comparison, event)
                if is_material:
                    # Material change → treat as new opportunity, update gate
                    gate_record["state"] = "open"
                    gate_record["opportunityKey"] = fingerprint
                    gate_record["deliveryKey"] = delivery_key
                    gate_record["lastReadyFingerprint"] = fingerprint
                    gate_record["lastReadyEventType"] = str(event.event_type or "")
                    gate_record["openedAt"] = utc_now_iso()
                    gate_record["cancelledAt"] = None
                    gate_record["cooldownUntil"] = None
                    gate_record["symbol"] = event.symbol
                    gate_record["side"] = event.side
                    gate_record["grade"] = event.grade
                    gate_record["lastActionableSnapshot"] = {
                        "entryZone": {"low": event.entry_low, "high": event.entry_high},
                        "stopLoss": event.stop_loss,
                        "takeProfit": {"tp1": event.tp1, "tp2": event.tp2, "tp3": event.tp3},
                        "currentPrice": event.current_price,
                        "trigger": event.trigger,
                        "invalidation": event.invalidation,
                        "executionNote": event.execution_note,
                        "currentStatus": event.current_status,
                        "riskFlags": event.risk_flags,
                    }
                    gate_record["reopenReason"] = reset_reason
                    gate_record.pop("suppressReason", None)
                    opportunity_gate[setup_key] = gate_record
                else:
                    suppressed.append({
                        "eventType": event.event_type,
                        "setupKey": setup_key,
                        "symbol": event.symbol,
                        "side": event.side,
                        "deliveryKey": delivery_key,
                        "executionFingerprint": fingerprint,
                        "reason": "opportunity_already_open",
                        "suppressReason": "same_opportunity_already_open",
                    })
                    continue

            # ── Gate: suppress READY if opportunity cancelled (requires reset) ─
            if gate_state == "cancelled":
                prev_event: Optional[EntryReadyEvent] = None
                prev_actionable = gate_record.get("lastActionableSnapshot") or {}
                if isinstance(prev_actionable, dict):
                    prev_zone = prev_actionable.get("entryZone", {}) or {}
                    prev_tp = prev_actionable.get("takeProfit", {}) or {}
                    prev_event = EntryReadyEvent(
                        event_type=str(gate_record.get("lastReadyEventType") or "READY_IN_ZONE"),
                        symbol=str(gate_record.get("symbol") or ""),
                        side=str(gate_record.get("side") or ""),
                        grade=gate_record.get("grade"),
                        entry_low=(prev_zone.get("low") if isinstance(prev_zone, dict) else None),
                        entry_high=(prev_zone.get("high") if isinstance(prev_zone, dict) else None),
                        stop_loss=prev_actionable.get("stopLoss"),
                        tp1=(prev_tp.get("tp1") if isinstance(prev_tp, dict) else None),
                        tp2=(prev_tp.get("tp2") if isinstance(prev_tp, dict) else None),
                        tp3=(prev_tp.get("tp3") if isinstance(prev_tp, dict) else None),
                        current_price=prev_actionable.get("currentPrice"),
                        confirmed_at=None,
                        trigger=prev_actionable.get("trigger"),
                        invalidation=prev_actionable.get("invalidation"),
                        execution_note=prev_actionable.get("executionNote"),
                        risk_flags=prev_actionable.get("riskFlags", []) if isinstance(prev_actionable, dict) else [],
                        crowding_note=None,
                        current_status=prev_actionable.get("currentStatus"),
                        cancel_reason=None,
                        setup_key=setup_key,
                        simulated=True,
                        metadata={},
                    )
                is_material, reset_reason = self.entry_is_material_reset(prev_event, event) if prev_event else (False, "no_prior_opportunity")
                if not is_material:
                    suppressed.append({
                        "eventType": event.event_type,
                        "setupKey": setup_key,
                        "symbol": event.symbol,
                        "side": event.side,
                        "deliveryKey": delivery_key,
                        "executionFingerprint": fingerprint,
                        "reason": "cancelled_opportunity_no_material_reset",
                        "suppressReason": f"cooldown_active_no_reset:{reset_reason}",
                    })
                    continue
                # Material reset — allow reopen
                gate_record["state"] = "open"
                gate_record["lastReadyFingerprint"] = fingerprint
                gate_record["deliveryKey"] = delivery_key
                gate_record["openedAt"] = utc_now_iso()
                gate_record["cancelledAt"] = None
                gate_record["cooldownUntil"] = None
                gate_record["reopenReason"] = reset_reason
                gate_record.pop("suppressReason", None)
                opportunity_gate[setup_key] = gate_record

            # ── Gate: idle — new opportunity, allow READY ───────────────────────
            order_index += 1
            would_send.append(self.entry_ready_render_record(formatter, event, order_index, delivery_key, action="would_send"))
            # Update opportunity gate
            gate_record = dict(opportunity_gate.get(setup_key, {}) or {})
            gate_record["state"] = "open"
            gate_record["opportunityKey"] = fingerprint
            gate_record["deliveryKey"] = delivery_key
            gate_record["lastReadyFingerprint"] = fingerprint
            gate_record["lastReadyEventType"] = str(event.event_type or "")
            gate_record["openedAt"] = utc_now_iso()
            gate_record["cancelledAt"] = None
            gate_record["cooldownUntil"] = None
            gate_record["symbol"] = event.symbol
            gate_record["side"] = event.side
            gate_record["grade"] = event.grade
            gate_record["lastActionableSnapshot"] = {
                "entryZone": {"low": event.entry_low, "high": event.entry_high},
                "stopLoss": event.stop_loss,
                "takeProfit": {"tp1": event.tp1, "tp2": event.tp2, "tp3": event.tp3},
                "currentPrice": event.current_price,
                "trigger": event.trigger,
                "invalidation": event.invalidation,
                "executionNote": event.execution_note,
                "currentStatus": event.current_status,
                "riskFlags": event.risk_flags,
            }
            gate_record.pop("suppressReason", None)
            gate_record.pop("reopenReason", None)
            opportunity_gate[setup_key] = gate_record

        # ── Phase 2: CANCEL events (previously active, now gone from scans) ───
        for setup_key, active_record in previous_active.items():
            if setup_key in seen_setups:
                continue
            if not isinstance(active_record, dict):
                continue
            setup = setups.get(setup_key) if isinstance(setups.get(setup_key), dict) else None
            monitor = monitors.get(setup_key) if isinstance(monitors.get(setup_key), dict) else None
            cancel_context = self.entry_ready_cancel_context(setup_key, setup, monitor)
            if not isinstance(cancel_context, dict):
                continue
            gate_record = opportunity_gate.get(setup_key, {}) if isinstance(opportunity_gate.get(setup_key), dict) else {}
            cancel_event = self.entry_ready_cancel_anchor_event(setup_key, active_record, gate_record, setup, monitor, cancel_context)
            delivery_key = self.entry_ready_delivery_key(cancel_event)
            fingerprint = self.execution_alert_fingerprint(cancel_event)
            event_counts["CANCEL"] += 1

            # ── Gate: suppress CANCEL if no open opportunity ───────────────────
            gate_state = self.entry_gate_readiness(gate_record)
            if gate_state != "open":
                suppressed.append({
                    "eventType": "CANCEL",
                    "setupKey": setup_key,
                    "symbol": cancel_event.symbol,
                    "side": cancel_event.side,
                    "deliveryKey": delivery_key,
                    "executionFingerprint": fingerprint,
                    "reason": "no_open_opportunity",
                    "suppressReason": "duplicate_cancel_no_open_opportunity",
                })
                continue
            anchor_delivery_key = active_record.get("deliveryKey") or gate_record.get("deliveryKey")
            prior_cancel = cancel_ledger.get(setup_key) if isinstance(cancel_ledger.get(setup_key), dict) else None
            if isinstance(prior_cancel, dict) and prior_cancel.get("anchorDeliveryKey") == anchor_delivery_key:
                suppressed.append({
                    "eventType": "CANCEL",
                    "setupKey": setup_key,
                    "symbol": cancel_event.symbol,
                    "side": cancel_event.side,
                    "deliveryKey": delivery_key,
                    "executionFingerprint": fingerprint,
                    "reason": "duplicate_cancel_context",
                    "suppressReason": "duplicate_cancel_same_anchor",
                })
                continue
            order_index += 1
            would_send.append(self.entry_ready_render_record(formatter, cancel_event, order_index, delivery_key, action="would_cancel"))
            # Update opportunity gate: mark cancelled
            gate_record = dict(opportunity_gate.get(setup_key, {}) or {})
            gate_record["state"] = "cancelled"
            gate_record["cancelledAt"] = utc_now_iso()
            gate_record["lastCancelFingerprint"] = fingerprint
            gate_record.pop("suppressReason", None)
            opportunity_gate[setup_key] = gate_record

        # ── Persist updated opportunity gate back into state ─────────────────
        state["opportunityGate"] = opportunity_gate

        return {
            "cycleId": cycle_id,
            "generatedAt": utc_now_iso(),
            "collection": {
                "totalCollected": len(collected) + event_counts["CANCEL"],
                "readyInZone": event_counts["READY_IN_ZONE"],
                "liveStrict": event_counts["LIVE_STRICT"],
                "cancel": event_counts["CANCEL"],
                "dedupSuppressed": len(suppressed),
            },
            "wouldSend": would_send,
            "suppressed": suppressed,
            "priorityOrder": [
                {
                    "order": row.get("order"),
                    "eventType": row.get("eventType"),
                    "setupKey": row.get("setupKey"),
                    "symbol": row.get("symbol"),
                    "side": row.get("side"),
                    "action": row.get("action"),
                }
                for row in would_send
            ],
            # Thread opportunityGate back to caller so state can be updated
            "opportunityGate": opportunity_gate,
        }

    def entry_ready_apply_delivery_record(self, state: Dict[str, Any], record: Dict[str, Any], applied_at: Optional[str] = None, action_override: Optional[str] = None) -> Dict[str, Any]:
        applied_at = applied_at or utc_now_iso()
        delivery_key = str(record.get("deliveryKey") or "")
        setup_key = str(record.get("setupKey") or "")
        event_type = str(record.get("eventType") or "")
        ledger = dict(state.get("ledger", {}) or {})
        active_by_setup = dict(state.get("activeBySetup", {}) or {})
        cancel_ledger = dict(state.get("cancelLedger", {}) or {})
        existing = ledger.get(delivery_key) if isinstance(ledger.get(delivery_key), dict) else {}
        last_action = action_override or str(record.get("action") or "would_send")

        ledger[delivery_key] = {
            "eventType": event_type,
            "setupKey": setup_key,
            "symbol": record.get("symbol"),
            "side": record.get("side"),
            "setupVersion": record.get("setupVersion"),
            "readinessVersion": record.get("readinessVersion"),
            "cancelAnchorDeliveryKey": record.get("cancelAnchorDeliveryKey"),
            "cancelCurrentSetupVersion": record.get("cancelCurrentSetupVersion"),
            "cancelReasonCode": record.get("cancelReasonCode"),
            "cancelReasonFamily": record.get("cancelReasonFamily"),
            "charCount": record.get("charCount"),
            "lastAction": last_action,
            "firstSeenAt": existing.get("firstSeenAt") or applied_at,
            "lastSeenAt": applied_at,
            "lastDeliveredAt": applied_at,
        }

        if event_type == "CANCEL":
            cancel_ledger[setup_key] = {
                "deliveryKey": delivery_key,
                "anchorDeliveryKey": record.get("cancelAnchorDeliveryKey"),
                "cancelReason": record.get("cancelReason"),
                "cancelReasonCode": record.get("cancelReasonCode"),
                "cancelReasonFamily": record.get("cancelReasonFamily"),
                "sentAt": applied_at,
            }
            active_by_setup.pop(setup_key, None)
        else:
            active_by_setup[setup_key] = {
                "deliveryKey": delivery_key,
                "eventType": event_type,
                "setupVersion": record.get("setupVersion"),
                "readinessVersion": record.get("readinessVersion"),
                "symbol": record.get("symbol"),
                "side": record.get("side"),
                "grade": record.get("grade"),
                "actionableSnapshot": {
                    "entryZone": dict(record.get("entryZone") or {}),
                    "stopLoss": record.get("stopLoss"),
                    "takeProfit": dict(record.get("takeProfit") or {}),
                    "currentPrice": record.get("currentPrice"),
                    "trigger": record.get("trigger"),
                    "invalidation": record.get("invalidation"),
                    "executionNote": record.get("executionNote"),
                    "currentStatus": record.get("currentStatus"),
                    "riskFlags": list(record.get("riskFlags") or []),
                },
                "sentAt": applied_at,
            }
            cancel_ledger.pop(setup_key, None)

        state["updatedAt"] = applied_at
        state["activeBySetup"] = active_by_setup
        state["ledger"] = ledger
        state["cancelLedger"] = cancel_ledger
        return state

    def entry_ready_record_cycle_summary(self, state: Dict[str, Any], cycle: Dict[str, Any], delivered_count: int, suppressed_count: int) -> Dict[str, Any]:
        state["updatedAt"] = cycle.get("generatedAt") or utc_now_iso()
        state["cycles"] = ((state.get("cycles") or []) + [
            {
                "cycleId": cycle.get("cycleId"),
                "generatedAt": cycle.get("generatedAt"),
                "wouldSendCount": delivered_count,
                "suppressedCount": suppressed_count,
            }
        ])[-20:]
        return state

    def forensic_event_lane_snapshot(self, event_type: Optional[str]) -> Dict[str, Any]:
        normalized = str(event_type or "").upper()
        if normalized == "LIVE_STRICT":
            return {"fcActive": True, "actionableNow": True, "lane": "LIVE"}
        if normalized == "READY_IN_ZONE":
            return {"fcActive": False, "actionableNow": True, "lane": "READY"}
        if normalized == "CANCEL":
            return {"fcActive": False, "actionableNow": False, "lane": "CANCEL"}
        return {"fcActive": None, "actionableNow": None, "lane": None}

    def forensic_replay(self, setup_key: str, from_iso: Optional[str] = None, to_iso: Optional[str] = None) -> Dict[str, Any]:
        setup_key = str(setup_key or "").strip()
        start_dt = parse_iso_timestamp(from_iso) if from_iso else None
        end_dt = parse_iso_timestamp(to_iso) if to_iso else None

        def in_window(ts: Optional[str]) -> bool:
            dt = parse_iso_timestamp(ts) if ts else None
            if dt is None:
                return False
            if start_dt and dt < start_dt:
                return False
            if end_dt and dt > end_dt:
                return False
            return True

        live_state = self.load_live_trigger_state()
        watchlist = self.load_watchlist_state()
        alerts = self.load_alert_state()
        entry_ready_state = self.load_entry_ready_delivery_state()
        periodic_rows = self.load_periodic_delivery_log()

        monitor = (live_state.get("monitors", {}) or {}).get(setup_key) if isinstance(live_state, dict) else None
        if not isinstance(monitor, dict):
            monitor = {}
        setup = (watchlist.get("setups", {}) or {}).get(setup_key) if isinstance(watchlist, dict) else None
        if not isinstance(setup, dict):
            setup = {}
        alert = (alerts.get("setups", {}) or {}).get(setup_key) if isinstance(alerts, dict) else None
        if not isinstance(alert, dict):
            alert = {}

        periodic_by_hash: Dict[str, Dict[str, Any]] = {}
        for row in periodic_rows:
            if not isinstance(row, dict):
                continue
            rendered_hash = str(row.get("renderedHash") or "")
            if rendered_hash:
                periodic_by_hash[rendered_hash] = row

        ledger = (entry_ready_state.get("ledger", {}) or {}) if isinstance(entry_ready_state, dict) else {}
        entry_ready_events: List[Dict[str, Any]] = []
        for delivery_key, rec in ledger.items():
            if not isinstance(rec, dict):
                continue
            if str(rec.get("setupKey") or "") != setup_key:
                continue
            ts = rec.get("lastDeliveredAt") or rec.get("firstSeenAt")
            if not in_window(ts):
                continue
            lane_snapshot = self.forensic_event_lane_snapshot(rec.get("eventType"))
            periodic = periodic_by_hash.get(str(delivery_key), {}) if isinstance(periodic_by_hash.get(str(delivery_key)), dict) else {}
            entry_ready_events.append({
                "timestamp": ts,
                "layer": "entry_ready",
                "deliveryKey": delivery_key,
                "eventType": rec.get("eventType"),
                "setupVersion": rec.get("setupVersion"),
                "readinessVersion": rec.get("readinessVersion"),
                "lastAction": rec.get("lastAction"),
                "lane": lane_snapshot.get("lane"),
                "fcActive": lane_snapshot.get("fcActive"),
                "actionableNow": lane_snapshot.get("actionableNow"),
                "renderedAt": periodic.get("renderedAt"),
                "attemptedAt": periodic.get("attemptedAt"),
                "messageIds": ((periodic.get("transportMetadata") or {}).get("messageIds") if isinstance(periodic.get("transportMetadata"), dict) else None),
            })
        entry_ready_events = sorted(entry_ready_events, key=lambda row: str(row.get("timestamp") or ""))

        runtime_confirm_events: List[Dict[str, Any]] = []
        delivery_ledger = monitor.get("deliveryLedger") if isinstance(monitor.get("deliveryLedger"), dict) else {}
        for delivery_key, rec in delivery_ledger.items():
            if not isinstance(rec, dict):
                continue
            ts = rec.get("sentAt") or rec.get("preparedAt")
            if not in_window(ts):
                continue
            if str(delivery_key).startswith(f"trigger|confirmed|{setup_key}|"):
                runtime_confirm_events.append({
                    "timestamp": ts,
                    "layer": "runtime",
                    "deliveryKey": delivery_key,
                    "eventType": "TRIGGER_CONFIRMED",
                    "status": rec.get("status"),
                    "preparedAt": rec.get("preparedAt"),
                    "sentAt": rec.get("sentAt"),
                    "message": rec.get("message"),
                })
        runtime_confirm_events = sorted(runtime_confirm_events, key=lambda row: str(row.get("timestamp") or ""))

        fc = monitor.get("finalConfirmation") if isinstance(monitor.get("finalConfirmation"), dict) else {}
        fc_current = dict(fc) if isinstance(fc, dict) else {}
        runtime_current = dict(monitor.get("runtime") or {}) if isinstance(monitor.get("runtime"), dict) else {}
        current_snapshot = {
            "runtimeState": self.runtime_state(monitor),
            "finalConfirmationActive": bool(isinstance(fc, dict) and fc.get("state") == "active"),
            "finalConfirmationConfirmedAt": fc.get("confirmedAt") if isinstance(fc, dict) else None,
            "finalConfirmationResetAt": fc.get("resetAt") if isinstance(fc, dict) else None,
            "lifecycleState": self.setup_lifecycle_state(setup),
            "readinessState": self.setup_readiness_state(setup),
            "deliveryState": self.delivery_state(alert),
            "setupVersion": setup.get("setupVersion") or monitor.get("setupVersion"),
            "readinessVersion": setup.get("readinessVersion") or monitor.get("readinessVersion"),
        }

        replay: List[Dict[str, Any]] = []
        if isinstance(fc, dict) and in_window(fc.get("confirmedAt")):
            replay.append({
                "timestamp": fc.get("confirmedAt"),
                "layer": "final_confirmation",
                "eventType": "FINAL_CONFIRMATION_ACTIVE",
                "setupVersion": fc.get("setupVersion"),
                "readinessVersion": fc.get("readinessVersion"),
                "fcActive": fc.get("state") == "active",
                "actionableNow": None,
                "lane": "LIVE_RUNTIME",
            })
        replay.extend(runtime_confirm_events)
        replay.extend(entry_ready_events)
        if isinstance(fc, dict) and in_window(fc.get("resetAt")):
            replay.append({
                "timestamp": fc.get("resetAt"),
                "layer": "final_confirmation",
                "eventType": "FINAL_CONFIRMATION_RESET",
                "setupVersion": fc.get("setupVersion"),
                "readinessVersion": fc.get("readinessVersion"),
                "resetReason": fc.get("resetReason"),
                "fcActive": False,
                "actionableNow": None,
                "lane": None,
            })
        replay = sorted(replay, key=lambda row: str(row.get("timestamp") or ""))

        return {
            "setupKey": setup_key,
            "window": {"from": from_iso, "to": to_iso},
            "current": current_snapshot,
            "finalConfirmationCurrent": fc_current,
            "runtimeCurrent": runtime_current,
            "entryReadyEvents": entry_ready_events,
            "runtimeConfirmedEvents": runtime_confirm_events,
            "replay": replay,
        }

    def entry_ready_dry_run_cycle(self, limit: int = 10, persist: bool = True, reset_state: bool = False) -> Dict[str, Any]:
        if TelegramExecutionCardFormatter is None:
            raise RuntimeError("telegram_execution_cards module unavailable")
        formatter = TelegramExecutionCardFormatter()
        state = self.reset_entry_ready_dry_run_state() if reset_state else self.load_entry_ready_dry_run_state()
        cycle = self.entry_ready_plan_cycle(limit=limit, formatter=formatter, state=state)
        cycle["statePath"] = str(ENTRY_READY_DRY_RUN_STATE_PATH)
        cycle["logPath"] = str(ENTRY_READY_DRY_RUN_LOG_PATH)

        # Merge opportunityGate from plan_cycle (which operated on a shallow copy)
        if isinstance(cycle.get("opportunityGate"), dict):
            state["opportunityGate"] = cycle["opportunityGate"]

        for record in cycle.get("wouldSend", []):
            self.entry_ready_apply_delivery_record(
                state,
                record,
                applied_at=cycle.get("generatedAt"),
                action_override=str(record.get("action") or "would_send"),
            )

        self.entry_ready_record_cycle_summary(
            state,
            cycle,
            delivered_count=len(cycle.get("wouldSend", [])),
            suppressed_count=len(cycle.get("suppressed", [])),
        )
        if persist:
            self.save_entry_ready_dry_run_state(state)
            self.append_entry_ready_dry_run_log(cycle)
        cycle["stateSummary"] = {
            "activeBySetup": len(state.get("activeBySetup", {}) or {}),
            "ledgerEntries": len(state.get("ledger", {}) or {}),
            "cancelLedgerEntries": len(state.get("cancelLedger", {}) or {}),
        }
        return cycle

    def entry_ready_refresh_state(self, kind: str, top: int, universe: int, max_symbols: int, enrich_top: int, tradingview_top: int, alert_limit: int, delivery_note: Optional[str]) -> Dict[str, Any]:
        if kind == "discovery":
            return self.scan(
                top=top,
                universe_size=universe,
                enrich_top=enrich_top,
                tradingview_top=tradingview_top,
                persist_state=True,
                mark_alerts_sent=True,
                alert_limit=alert_limit,
                delivery_note=delivery_note,
            )
        return self.watchlist_refresh(
            top=top,
            max_symbols=max_symbols,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            persist_state=True,
            mark_alerts_sent=True,
            alert_limit=alert_limit,
            delivery_note=delivery_note,
        )

    def _automation_cycle_send_entry_ready(
        self,
        kind: str,
        channel: str,
        target: str,
        top: int = 5,
        universe: int = 24,
        max_symbols: int = 12,
        enrich_top: int = 2,
        tradingview_top: int = 2,
        alert_limit: int = 3,
        delivery_note: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        ENTRY-READY execution-card delivery path.
        Refreshes canonical state first, then collects LIVE_STRICT / READY_IN_ZONE
        events, renders each as a Telegram execution card, sends individually,
        and persists delivery-ledger state for duplicate suppression.
        """
        if TelegramExecutionCardFormatter is None:
            return {"sent": False, "reason": "formatter_unavailable", "error": "telegram_execution_cards module not available"}
        formatter = TelegramExecutionCardFormatter()

        self.entry_ready_refresh_state(
            kind=kind,
            top=top,
            universe=universe,
            max_symbols=max_symbols,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            alert_limit=alert_limit,
            delivery_note=delivery_note,
        )

        state = self.load_entry_ready_delivery_state()
        cycle = self.entry_ready_plan_cycle(limit=top, formatter=formatter, state=state)
        # Merge opportunityGate from plan_cycle (which operated on a shallow copy)
        if isinstance(cycle.get("opportunityGate"), dict):
            state["opportunityGate"] = cycle["opportunityGate"]

        if not cycle.get("wouldSend"):
            rendered_at = utc_now_iso()
            reason = "duplicate_suppressed" if cycle.get("suppressed") else "no_reply"
            rendered_hash = (cycle.get("suppressed", [{}])[0] or {}).get("deliveryKey") if cycle.get("suppressed") else "no_qualifying_events"
            self._periodic_log_delivery(
                rendered_at=rendered_at,
                rendered_hash=rendered_hash or "no_qualifying_events",
                rendered_len=0,
                kind=kind,
                output_mode="entry-ready",
                channel=channel,
                target=target,
                sent=False,
                reason=reason,
                transport=None,
                error=None,
            )
            return {
                "sent": False,
                "reason": reason,
                "kind": kind,
                "outputMode": "entry-ready",
                "suppressedCount": len(cycle.get("suppressed", [])),
            }

        all_message_ids: List[str] = []
        send_error: Optional[str] = None
        sent_at = utc_now_iso()
        rendered_at = utc_now_iso()
        total_len = sum(int(row.get("charCount", 0) or 0) for row in cycle.get("wouldSend", []))
        sent_records = 0

        for card in cycle.get("wouldSend", []):
            try:
                all_sent, message_ids, err = self._send_long_message(
                    str(card.get("card") or ""), channel=channel, target=target
                )
                if all_sent:
                    all_message_ids.extend(message_ids)
                    sent_records += 1
                    self.entry_ready_apply_delivery_record(
                        state,
                        card,
                        applied_at=utc_now_iso(),
                        action_override="cancel_sent" if card.get("eventType") == "CANCEL" else "sent",
                    )
                    self.save_entry_ready_delivery_state(state)

                    transport_info = self.openclaw_transport_info(error=None)
                    transport_info["messageIds"] = message_ids
                    self._periodic_log_delivery(
                        rendered_at=rendered_at,
                        rendered_hash=str(card.get("deliveryKey") or ""),
                        rendered_len=int(card.get("charCount", 0) or 0),
                        kind=kind,
                        output_mode="entry-ready",
                        channel=channel,
                        target=target,
                        sent=True,
                        reason=None,
                        transport=transport_info,
                        error=None,
                        attempt_at=utc_now_iso(),
                        sent_at=sent_at,
                    )
                else:
                    send_error = err or "send_failed"
                    transport_info = self.openclaw_transport_info(error=send_error)
                    self._periodic_log_delivery(
                        rendered_at=rendered_at,
                        rendered_hash=str(card.get("deliveryKey") or ""),
                        rendered_len=int(card.get("charCount", 0) or 0),
                        kind=kind,
                        output_mode="entry-ready",
                        channel=channel,
                        target=target,
                        sent=False,
                        reason="send_failed",
                        transport=transport_info,
                        error=send_error,
                    )
                    break
            except Exception as exc:
                send_error = str(exc)
                transport_info = self.openclaw_transport_info(error=send_error)
                self._periodic_log_delivery(
                    rendered_at=rendered_at,
                    rendered_hash=str(card.get("deliveryKey") or ""),
                    rendered_len=int(card.get("charCount", 0) or 0),
                    kind=kind,
                    output_mode="entry-ready",
                    channel=channel,
                    target=target,
                    sent=False,
                    reason="send_failed",
                    transport=transport_info,
                    error=send_error,
                )
                break

        self.entry_ready_record_cycle_summary(
            state,
            cycle,
            delivered_count=sent_records,
            suppressed_count=len(cycle.get("suppressed", [])),
        )
        self.save_entry_ready_delivery_state(state)

        return {
            "sent": bool(all_message_ids),
            "kind": kind,
            "outputMode": "entry-ready",
            "channel": channel,
            "target": target,
            "cardCount": len(cycle.get("wouldSend", [])),
            "totalCharCount": total_len,
            "messageIds": [f"✅ Sent via Telegram. Message ID: {mid}" for mid in all_message_ids],
            "suppressedCount": len(cycle.get("suppressed", [])),
            "error": send_error,
            "transport": self.openclaw_transport_info(error=send_error) if send_error else self.openclaw_transport_info(error=None),
        }

    def automation_cycle(self, kind: str = "watchlist", output_mode: str = "material", top: int = 5, universe: int = 24, max_symbols: int = 12, enrich_top: int = 2, tradingview_top: int = 2, alert_limit: int = 3, delivery_note: Optional[str] = None, max_live: int = 3, max_standby: int = 5) -> str:
        if kind == "discovery":
            self.scan(top=top, universe_size=universe, enrich_top=enrich_top, tradingview_top=tradingview_top, persist_state=True, mark_alerts_sent=True, alert_limit=alert_limit, delivery_note=delivery_note)
        else:
            self.watchlist_refresh(top=top, max_symbols=max_symbols, enrich_top=enrich_top, tradingview_top=tradingview_top, persist_state=True, mark_alerts_sent=True, alert_limit=alert_limit, delivery_note=delivery_note)
        return self.notification_render(mode=output_mode, cycle_kind=kind, max_live=max_live, max_standby=max_standby, persist_render_state=True)

    def automation_cycle_send(
        self,
        kind: str = "watchlist",
        output_mode: str = "material",
        channel: str = "telegram",
        target: str = "851120836",
        top: int = 5,
        universe: int = 24,
        max_symbols: int = 12,
        enrich_top: int = 2,
        tradingview_top: int = 2,
        alert_limit: int = 3,
        delivery_note: Optional[str] = None,
        max_live: int = 3,
        max_standby: int = 5,
    ) -> Dict[str, Any]:
        """
        Direct periodic delivery: render canonical output, send to Telegram
        without any agent reformatting layer.

        output_mode "entry-ready" uses the TelegramExecutionCardFormatter for
        ENTRY-READY ONLY policy: LIVE_STRICT or READY_IN_ZONE only, no STANDBY,
        no material-change batch dump.

        This function is idempotent per output hash + kind + time bucket.
        """
        # ─── ENTRY-READY MODE ───
        if output_mode == "entry-ready":
            return self._automation_cycle_send_entry_ready(
                kind=kind,
                channel=channel,
                target=target,
                top=top,
                universe=universe,
                max_symbols=max_symbols,
                enrich_top=enrich_top,
                tradingview_top=tradingview_top,
                alert_limit=alert_limit,
                delivery_note=delivery_note,
            )

        # ─── OLD BATCH RENDERER (material/compact/detailed) ───
        # 1. Render canonical output (NO modification of renderer semantics)
        rendered = self.automation_cycle(
            kind=kind,
            output_mode=output_mode,
            top=top,
            universe=universe,
            max_symbols=max_symbols,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            alert_limit=alert_limit,
            delivery_note=delivery_note,
            max_live=max_live,
            max_standby=max_standby,
        )

        rendered_at = utc_now_iso()
        rendered_hash = hashlib.sha256(rendered.encode("utf-8")).hexdigest()[:16]
        rendered_len = len(rendered)

        # 2. Idempotency: skip if identical output was just sent (same kind, same hash, same 60s bucket)
        last_send = self._periodic_last_send_record()
        if last_send:
            same_kind = last_send.get("kind") == kind
            same_hash = last_send.get("renderedHash") == rendered_hash
            same_bucket = (
                last_send.get("timeBucket") ==
                (int(time.time()) // 60)
            )
            if same_kind and same_hash and same_bucket:
                return {
                    "sent": False,
                    "reason": "duplicate_suppressed",
                    "renderedHash": rendered_hash,
                    "kind": kind,
                    "outputLength": rendered_len,
                }

        # 3. NO_REPLY check
        if rendered.strip() in ("NO_REPLY", "no_reply", ""):
            self._periodic_log_delivery(
                rendered_at=rendered_at,
                rendered_hash=rendered_hash,
                rendered_len=rendered_len,
                kind=kind,
                output_mode=output_mode,
                channel=channel,
                target=target,
                sent=False,
                reason="no_reply",
                transport=None,
                error=None,
            )
            return {
                "sent": False,
                "reason": "no_reply",
                "renderedHash": rendered_hash,
                "kind": kind,
            }

        # 4. Build send command via hardened transport
        command = self.openclaw_message_send_command(
            channel=channel,
            target=target,
            message=rendered,
            refresh=False,
        )

        if not command:
            error = self.openclaw_transport_error or "transport_unavailable"
            transport_info = self.openclaw_transport_info(error=error)
            self._periodic_log_delivery(
                rendered_at=rendered_at,
                rendered_hash=rendered_hash,
                rendered_len=rendered_len,
                kind=kind,
                output_mode=output_mode,
                channel=channel,
                target=target,
                sent=False,
                reason="transport_unavailable",
                transport=transport_info,
                error=error,
            )
            return {
                "sent": False,
                "reason": "transport_unavailable",
                "error": error,
                "transport": transport_info,
                "renderedHash": rendered_hash,
                "kind": kind,
            }

        # 5. Execute send (with long-message split if needed)
        attempt_at = utc_now_iso()
        try:
            all_sent, message_ids, send_error = self._send_long_message(
                rendered, channel=channel, target=target
            )
            if all_sent:
                sent_at = utc_now_iso()
                transport_info = self.openclaw_transport_info(error=None)
                transport_info["messageIds"] = message_ids
                self._periodic_log_delivery(
                    rendered_at=rendered_at,
                    rendered_hash=rendered_hash,
                    rendered_len=rendered_len,
                    kind=kind,
                    output_mode=output_mode,
                    channel=channel,
                    target=target,
                    sent=True,
                    reason=None,
                    transport=transport_info,
                    error=None,
                    attempt_at=attempt_at,
                    sent_at=sent_at,
                )
                return {
                    "sent": True,
                    "renderedHash": rendered_hash,
                    "outputLength": rendered_len,
                    "kind": kind,
                    "outputMode": output_mode,
                    "channel": channel,
                    "target": target,
                    "transport": transport_info,
                }
            else:
                failed_at = utc_now_iso()
                transport_info = self.openclaw_transport_info(error=send_error)
                self._periodic_log_delivery(
                    rendered_at=rendered_at,
                    rendered_hash=rendered_hash,
                    rendered_len=rendered_len,
                    kind=kind,
                    output_mode=output_mode,
                    channel=channel,
                    target=target,
                    sent=False,
                    reason="send_failed",
                    transport=transport_info,
                    error=send_error,
                    attempt_at=attempt_at,
                    failed_at=failed_at,
                )
                return {
                    "sent": False,
                    "reason": "send_failed",
                    "error": send_error,
                    "transport": transport_info,
                    "renderedHash": rendered_hash,
                    "kind": kind,
                }
        except Exception as exc:
            failed_at = utc_now_iso()
            transport_info = self.openclaw_transport_info(error=str(exc))
            self._periodic_log_delivery(
                rendered_at=rendered_at,
                rendered_hash=rendered_hash,
                rendered_len=rendered_len,
                kind=kind,
                output_mode=output_mode,
                channel=channel,
                target=target,
                sent=False,
                reason="send_failed",
                transport=transport_info,
                error=str(exc),
                attempt_at=attempt_at,
                failed_at=failed_at,
            )
            return {
                "sent": False,
                "reason": "send_failed",
                "error": str(exc),
                "transport": transport_info,
                "renderedHash": rendered_hash,
                "kind": kind,
            }

    def _periodic_last_send_record(self) -> Optional[Dict[str, Any]]:
        """Load last delivery record for idempotency check."""
        if not PERIODIC_DELIVERY_LOG_PATH.exists():
            return None
        try:
            lines = PERIODIC_DELIVERY_LOG_PATH.read_text().strip().split("\n")
            if not lines:
                return None
            last = json.loads(lines[-1])
            return last if last.get("sent") else None
        except Exception:
            return None

    def _periodic_log_delivery(
        self,
        rendered_at: str,
        rendered_hash: str,
        rendered_len: int,
        kind: str,
        output_mode: str,
        channel: str,
        target: str,
        sent: bool,
        reason: Optional[str],
        transport: Optional[Dict[str, Any]],
        error: Optional[str],
        attempt_at: Optional[str] = None,
        sent_at: Optional[str] = None,
        failed_at: Optional[str] = None,
    ) -> None:
        """Append a delivery attempt record to the audit log."""
        time_bucket = int(time.time()) // 60
        record = {
            "renderedAt": rendered_at,
            "renderedHash": rendered_hash,
            "renderedLength": rendered_len,
            "kind": kind,
            "outputMode": output_mode,
            "channel": channel,
            "target": target,
            "sent": sent,
            "reason": reason,
            "transportMetadata": transport,
            "error": error,
            "timeBucket": time_bucket,
            "attemptedAt": attempt_at,
            "sentAt": sent_at,
            "failedAt": failed_at,
        }
        try:
            with open(PERIODIC_DELIVERY_LOG_PATH, "a") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception:
            pass  # logging failure must not block delivery

    def _send_long_message(self, message: str, channel: str, target: str) -> Tuple[bool, List[str], Optional[str]]:
        """
        Send a message that may exceed Telegram's 4096-char limit by splitting into chunks.
        Returns (all_sent, message_ids, last_error).
        Each chunk is prefixed with part number for traceability.
        """
        TELEGRAM_MAX = 4000  # safe margin below 4096
        if len(message) <= TELEGRAM_MAX:
            command = self.openclaw_message_send_command(
                channel=channel, target=target, message=message, refresh=False
            )
            if not command:
                return False, [], self.openclaw_transport_error or "transport_unavailable"
            try:
                result = self.run_command(command, expect_json=False)
                return True, [result.strip()], None
            except Exception as exc:
                return False, [], str(exc)

        # Split long messages at line boundaries
        chunks: List[str] = []
        current = ""
        for line in message.split("\n"):
            if len(current) + len(line) + 1 > TELEGRAM_MAX:
                if current:
                    chunks.append(current)
                current = line
            else:
                current = (current + "\n" + line) if current else line
        if current:
            chunks.append(current)

        sent_ids: List[str] = []
        last_error: Optional[str] = None
        total = len(chunks)
        for i, chunk in enumerate(chunks):
            prefix = f"[{i+1}/{total}]\n"
            prefixed = prefix + chunk
            command = self.openclaw_message_send_command(
                channel=channel, target=target, message=prefixed, refresh=False
            )
            if not command:
                last_error = self.openclaw_transport_error or "transport_unavailable"
                break
            try:
                result = self.run_command(command, expect_json=False)
                sent_ids.append(result.strip())
            except Exception as exc:
                last_error = str(exc)
                break

        return len(sent_ids) == total, sent_ids, last_error

    def discovery_radar_entry_from_row(self, row: Dict[str, Any], bucket: str) -> Dict[str, Any]:
        actionable = row.get("actionableAlert") if isinstance(row.get("actionableAlert"), dict) else {}
        entry_zone = actionable.get("entryZone") if isinstance(actionable.get("entryZone"), dict) else {}
        take_profit = actionable.get("takeProfit") if isinstance(actionable.get("takeProfit"), dict) else {}
        lifecycle = row.get("lifecycle") if isinstance(row.get("lifecycle"), dict) else {}
        readiness = row.get("readiness") if isinstance(row.get("readiness"), dict) else {}
        return {
            "bucket": bucket,
            "symbol": str(row.get("symbol") or ""),
            "side": row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0)),
            "grade": row.get("grade"),
            "bullScore": row.get("bullScore"),
            "cleanScore": row.get("cleanScore"),
            "gradeScore": row.get("gradeScore"),
            "quoteVolume": row.get("quoteVolume"),
            "priceChangePercent": row.get("priceChangePercent"),
            "lifecycleState": lifecycle.get("state"),
            "readinessState": readiness.get("state"),
            "currentStatus": row.get("currentStatus") or actionable.get("currentStatus"),
            "alertEligible": bool(row.get("alertEligible")),
            "watchlistEligible": bool(row.get("watchlistEligible")),
            "currentPrice": actionable.get("currentPrice"),
            "entryZoneLow": entry_zone.get("low"),
            "entryZoneHigh": entry_zone.get("high"),
            "stopLoss": actionable.get("stopLoss"),
            "tp1": take_profit.get("tp1"),
            "tp2": take_profit.get("tp2"),
            "tp3": take_profit.get("tp3"),
            "trigger": actionable.get("trigger"),
            "invalidation": actionable.get("invalidation"),
            "doNotChase": actionable.get("doNotChase"),
            "executionNote": actionable.get("executionNote"),
            "riskFlags": actionable.get("riskFlags") if isinstance(actionable.get("riskFlags"), list) else [],
            "setupKey": row.get("setupKey"),
            "setupVersion": row.get("setupVersion"),
            "readinessVersion": row.get("readinessVersion"),
        }

    def production_eval_regime_from_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        actionable = row.get("actionableAlert") if isinstance(row.get("actionableAlert"), dict) else {}
        execution_filter = row.get("executionFilter") if isinstance(row.get("executionFilter"), dict) else {}
        direction = row.get("direction")
        if direction not in {"long", "short"}:
            direction = str(row.get("side") or "").lower()
        if direction not in {"long", "short"}:
            direction = self.direction_label(int(row.get("bullScore", 0) or 0))
        return {
            "grade": row.get("grade"),
            "bullScore": row.get("bullScore"),
            "cleanScore": row.get("cleanScore"),
            "gradeScore": row.get("gradeScore"),
            "funding": row.get("funding"),
            "oi6hChangePct": row.get("oi6hChangePct"),
            "spreadBps": row.get("spreadBps"),
            "quoteVolume": row.get("quoteVolume"),
            "priceChangePercent": row.get("priceChangePercent"),
            "direction": direction,
            "readinessState": self.setup_readiness_state(row),
            "lifecycleState": self.setup_lifecycle_state(row),
            "currentStatus": row.get("currentStatus") or actionable.get("currentStatus"),
            "riskFlags": list((actionable.get("riskFlags") if isinstance(actionable.get("riskFlags"), list) else row.get("riskFlags") or [])),
            "executionFlags": list(execution_filter.get("riskFlags") or []),
            "doNotChase": actionable.get("doNotChase"),
        }

    def production_eval_regime_key(self, regime: Dict[str, Any]) -> str:
        regime = regime if isinstance(regime, dict) else {}
        def _num(value: Any) -> float:
            try:
                return float(value or 0.0)
            except Exception:
                return 0.0
        payload = {
            "grade": regime.get("grade") or "unknown",
            "direction": regime.get("direction") or "flat",
            "readinessState": regime.get("readinessState") or "none",
            "fundingBucket": (
                "positive" if _num(regime.get("funding")) > 0
                else "negative" if _num(regime.get("funding")) < 0
                else "flat"
            ),
            "oiBucket": (
                "expanding" if _num(regime.get("oi6hChangePct")) >= 5.0
                else "contracting" if _num(regime.get("oi6hChangePct")) <= -5.0
                else "flat"
            ),
            "spreadBucket": (
                "wide" if _num(regime.get("spreadBps")) >= 6.0
                else "tight"
            ),
            "risk": "flagged" if (regime.get("riskFlags") or regime.get("executionFlags")) else "clean",
            "doNotChase": bool(regime.get("doNotChase")),
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def production_eval_log_scan_batch(self, result: Dict[str, Any], rows: List[Dict[str, Any]]) -> None:
        if not isinstance(result, dict) or not result.get("prodMode"):
            return
        visible_symbols = set(result.get("resolvedTradable") or [])
        payload = {
            "loggedAt": utc_now_iso(),
            "type": "scan_batch",
            "source": result.get("source"),
            "prodMode": True,
            "requestedSymbols": list(result.get("requestedSymbols") or []),
            "resolvedTradable": list(result.get("resolvedTradable") or []),
            "providerEnrichmentDisabled": bool((result.get("productionPruned") or {}).get("providerEnrichmentDisabled")),
            "tradingviewDisabled": bool((result.get("productionPruned") or {}).get("tradingviewDisabled")),
            "rows": [
                {
                    "symbol": row.get("symbol"),
                    "setupKey": row.get("setupKey"),
                    "regime": self.production_eval_regime_from_row(row),
                    "alertEligible": bool(row.get("alertEligible")),
                    "watchlistEligible": bool(row.get("watchlistEligible")),
                    "visibleInProduction": str(row.get("symbol") or "") in visible_symbols,
                    "setupVersion": row.get("setupVersion"),
                    "readinessVersion": row.get("readinessVersion"),
                }
                for row in rows if isinstance(row, dict)
            ],
        }
        append_jsonl(PRODUCTION_SIGNAL_EVAL_LOG_PATH, payload)

    def production_eval_log_lifecycle_event(self, setup_key: str, monitor: Dict[str, Any], lifecycle_event: Dict[str, Any]) -> None:
        if not isinstance(monitor, dict) or not isinstance(lifecycle_event, dict):
            return
        lifecycle = monitor.get("lifecycle") if isinstance(monitor.get("lifecycle"), dict) else {}
        actionable = monitor.get("actionableAlert") if isinstance(monitor.get("actionableAlert"), dict) else {}
        regime = self.production_eval_regime_from_row(monitor)
        payload = {
            "loggedAt": utc_now_iso(),
            "type": "lifecycle_event",
            "setupKey": setup_key,
            "symbol": monitor.get("symbol"),
            "side": monitor.get("side"),
            "grade": monitor.get("grade"),
            "setupVersion": monitor.get("setupVersion"),
            "readinessVersion": monitor.get("readinessVersion"),
            "milestone": lifecycle_event.get("milestone"),
            "reason": lifecycle_event.get("reason"),
            "price": lifecycle_event.get("price"),
            "entryReferencePrice": lifecycle.get("entryReferencePrice"),
            "maxFavorableBps": lifecycle.get("maxFavorableBps"),
            "maxAdverseBps": lifecycle.get("maxAdverseBps"),
            "startedAt": lifecycle.get("startedAt"),
            "closedAt": lifecycle.get("closedAt"),
            "currentStatus": actionable.get("currentStatus"),
            "riskFlags": list(actionable.get("riskFlags") or []),
            "regime": regime,
            "regimeKey": self.production_eval_regime_key(regime),
        }
        append_jsonl(PRODUCTION_SIGNAL_EVAL_LOG_PATH, payload)

    def production_eval_log_funnel_by_regime(self, state: Dict[str, Any], *, source: str) -> None:
        state = state if isinstance(state, dict) else {}
        monitors = state.get("monitors", {}) if isinstance(state.get("monitors"), dict) else {}
        grouped: Dict[str, Dict[str, Any]] = {}
        for setup_key, monitor in monitors.items():
            if not isinstance(monitor, dict):
                continue
            evaluation = monitor.get("evaluation") if isinstance(monitor.get("evaluation"), dict) else {}
            regime = evaluation.get("lastEvaluationRegime") if isinstance(evaluation.get("lastEvaluationRegime"), dict) else self.production_eval_regime_from_row(monitor)
            regime_key = self.production_eval_regime_key(regime)
            bucket = grouped.setdefault(regime_key, {
                "regime": regime,
                "phaseCounts": {},
                "outcomeCounts": {},
                "monitors": 0,
                "confirmed": 0,
                "notified": 0,
                "openLifecycle": 0,
            })
            bucket["monitors"] += 1
            phase = str(evaluation.get("lastEvaluationPhase") or "unknown")
            outcome = str(evaluation.get("lastEvaluationOutcome") or "unknown")
            bucket["phaseCounts"][phase] = int(bucket["phaseCounts"].get(phase, 0) or 0) + 1
            bucket["outcomeCounts"][outcome] = int(bucket["outcomeCounts"].get(outcome, 0) or 0) + 1
            if self.runtime_state(monitor) == "confirmed":
                bucket["confirmed"] += 1
            if self.runtime_state(monitor) == "notified":
                bucket["notified"] += 1
            if self.live_trigger_lifecycle_open(monitor):
                bucket["openLifecycle"] += 1

        payload = {
            "loggedAt": utc_now_iso(),
            "type": "funnel_by_regime",
            "source": source,
            "rows": [
                {
                    "regimeKey": regime_key,
                    **bucket,
                }
                for regime_key, bucket in sorted(grouped.items())
            ],
        }
        signature = hashlib.sha256(
            json.dumps(payload["rows"], sort_keys=True, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        ).hexdigest()[:16]
        if state.get("lastProductionEvalFunnelSignature") == signature:
            return
        state["lastProductionEvalFunnelSignature"] = signature
        state["lastProductionEvalFunnelAt"] = payload["loggedAt"]
        append_jsonl(PRODUCTION_SIGNAL_EVAL_LOG_PATH, payload)

    def _build_discovery_radar_snapshot(
        self,
        *,
        mode: str,
        scan_result: Dict[str, Any],
        top: int = 5,
        universe: int = 24,
        enrich_top: int = 2,
        tradingview_top: int = 2,
        alert_limit: int = 3,
        max_entries: int = 12,
        sink_latest_path: Path,
        sink_log_path: Path,
        updates_canonical_state: bool,
    ) -> Dict[str, Any]:
        state_status = scan_result.get("stateStatus") if isinstance(scan_result.get("stateStatus"), dict) else {}
        candidates: List[Dict[str, Any]] = []
        seen_symbols: set[str] = set()
        for bucket, key in (("long", "topLongs"), ("short", "topShorts"), ("watch", "topWatch")):
            rows = scan_result.get(key) if isinstance(scan_result.get(key), list) else []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                symbol = str(row.get("symbol") or "")
                if not symbol or symbol in seen_symbols:
                    continue
                seen_symbols.add(symbol)
                candidates.append(self.discovery_radar_entry_from_row(row, bucket=bucket))
                if len(candidates) >= max_entries:
                    break
            if len(candidates) >= max_entries:
                break
        signature_payload = [
            {
                "symbol": entry.get("symbol"),
                "side": entry.get("side"),
                "grade": entry.get("grade"),
                "lifecycleState": entry.get("lifecycleState"),
                "readinessState": entry.get("readinessState"),
                "setupVersion": entry.get("setupVersion"),
                "readinessVersion": entry.get("readinessVersion"),
            }
            for entry in candidates
        ]
        snapshot_hash = hashlib.sha256(json.dumps(signature_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]
        latest = {
            "generatedAt": utc_now_iso(),
            "policy": {
                "mainTelegram": "C2_only",
                "c3": "silent_validation_preflight",
                "c4": "discovery_feeder_not_speaker",
                "secondarySurface": "internal_radar_log_store",
            },
            "sink": {
                "latestPath": str(sink_latest_path),
                "logPath": str(sink_log_path),
            },
            "cycle": {
                "kind": "discovery",
                "mode": mode,
                "speaker": False,
                "mutating": updates_canonical_state,
                "downstreamImpactPossible": updates_canonical_state,
                "updatesCanonicalState": updates_canonical_state,
                "writesRadarStore": True,
                "snapshotHash": snapshot_hash,
            },
            "scan": {
                "universeRequested": universe,
                "topRequested": top,
                "enrichTop": enrich_top,
                "tvTop": tradingview_top,
                "alertLimit": alert_limit,
                "universeSize": scan_result.get("universeSize"),
                "generatedAt": scan_result.get("generatedAt"),
                "alertEligible": scan_result.get("alertEligible", []),
                "watchlistEligible": scan_result.get("watchlistEligible", []),
                "pendingAlertCount": len(scan_result.get("pendingAlerts", []) if isinstance(scan_result.get("pendingAlerts"), list) else []),
                "stateSync": scan_result.get("stateSync"),
                "stateSummary": {
                    "updatedAt": state_status.get("updatedAt"),
                    "lastScanAt": state_status.get("lastScanAt"),
                    "setupsTracked": state_status.get("setupsTracked"),
                    "activeSetups": state_status.get("activeSetups"),
                    "pendingAlerts": state_status.get("pendingAlerts"),
                    "sentAlerts": state_status.get("sentAlerts"),
                    "stateCounts": state_status.get("stateCounts"),
                    "readinessCounts": state_status.get("readinessCounts"),
                    "recentTransitionCount": len(state_status.get("recentTransitions", []) if isinstance(state_status.get("recentTransitions"), list) else []),
                },
            },
            "candidates": candidates,
        }
        save_json_file(sink_latest_path, latest)
        append_jsonl(
            sink_log_path,
            {
                "generatedAt": latest.get("generatedAt"),
                "mode": mode,
                "mutating": updates_canonical_state,
                "downstreamImpactPossible": updates_canonical_state,
                "snapshotHash": snapshot_hash,
                "candidateCount": len(candidates),
                "symbols": [entry.get("symbol") for entry in candidates],
                "alertEligible": scan_result.get("alertEligible", []),
                "watchlistEligible": scan_result.get("watchlistEligible", []),
            },
        )
        return latest

    def discovery_radar_preview(
        self,
        top: int = 5,
        universe: int = 24,
        enrich_top: int = 2,
        tradingview_top: int = 2,
        alert_limit: int = 3,
        max_entries: int = 12,
        prod_mode: bool = False,
    ) -> Dict[str, Any]:
        # prod_mode=False (default): research/preview path includes news keyword scoring
        # and full feedback influence for exploratory analysis.
        scan_result = self.scan(
            top=top,
            universe_size=universe,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            persist_state=False,
            mark_alerts_sent=False,
            alert_limit=alert_limit,
            delivery_note=None,
            prod_mode=prod_mode,
        )
        return self._build_discovery_radar_snapshot(
            mode="preview",
            scan_result=scan_result,
            top=top,
            universe=universe,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            alert_limit=alert_limit,
            max_entries=max_entries,
            sink_latest_path=DISCOVERY_RADAR_PREVIEW_STATE_PATH,
            sink_log_path=DISCOVERY_RADAR_PREVIEW_LOG_PATH,
            updates_canonical_state=False,
        )

    def discovery_feed_state(
        self,
        top: int = 5,
        universe: int = 24,
        enrich_top: int = 2,
        tradingview_top: int = 2,
        alert_limit: int = 3,
        max_entries: int = 12,
        prod_mode: bool = True,
    ) -> Dict[str, Any]:
        # prod_mode=True: production discovery feeder is Binance-native and deterministic.
        # News keyword scoring and low-sample feedback influence are disabled.
        scan_result = self.scan(
            top=top,
            universe_size=universe,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            persist_state=True,
            mark_alerts_sent=False,
            alert_limit=alert_limit,
            delivery_note="discovery_feed_state",
            prod_mode=prod_mode,
        )
        return self._build_discovery_radar_snapshot(
            mode="feeder",
            scan_result=scan_result,
            top=top,
            universe=universe,
            enrich_top=enrich_top,
            tradingview_top=tradingview_top,
            alert_limit=alert_limit,
            max_entries=max_entries,
            sink_latest_path=DISCOVERY_RADAR_STATE_PATH,
            sink_log_path=DISCOVERY_RADAR_LOG_PATH,
            updates_canonical_state=True,
        )

    def discovery_radar_cycle(self, *args, **kwargs) -> Dict[str, Any]:
        raise ValueError(
            "BLOCKED: discovery-radar-cycle is deprecated because it is not operator-safe. "
            "It previously mixed preview behavior with production state mutation. "
            "Use discovery-radar-preview for non-mutating discovery preview. "
            "Use discovery-feed-state only when you intentionally want to mutate production canonical state."
        )

    def discovery_radar_status(self) -> Dict[str, Any]:
        return load_json_file(
            DISCOVERY_RADAR_STATE_PATH,
            {
                "policy": {
                    "mainTelegram": "C2_only",
                    "c3": "silent_validation_preflight",
                    "c4": "discovery_feeder_not_speaker",
                },
                "sink": {
                    "latestPath": str(DISCOVERY_RADAR_STATE_PATH),
                    "logPath": str(DISCOVERY_RADAR_LOG_PATH),
                },
                "status": "empty",
            },
        )


    def cron_list_json(self) -> Dict[str, Any]:
        try:
            return self.run_command(["openclaw", "cron", "list", "--json", "--all"], expect_json=True)
        except Exception:
            return {"jobs": [], "total": 0}

    def cron_status_json(self) -> Dict[str, Any]:
        try:
            return self.run_command(["openclaw", "cron", "status"], expect_json=True)
        except Exception:
            return {}

    def cron_store_jobs(self) -> List[Dict[str, Any]]:
        payload = load_json_file(OPENCLAW_CRON_STORE_PATH, {"jobs": []})
        jobs = payload.get("jobs", []) if isinstance(payload, dict) else []
        return [job for job in jobs if isinstance(job, dict)]

    def cron_jobs(self) -> List[Dict[str, Any]]:
        listed = self.cron_list_json()
        jobs = listed.get("jobs", []) if isinstance(listed, dict) else []
        if jobs:
            return [job for job in jobs if isinstance(job, dict)]
        store_jobs = self.cron_store_jobs()
        status = self.cron_status_json()
        if store_jobs and int(status.get("jobs", 0) or 0) == len(store_jobs):
            return store_jobs
        return store_jobs or []

    def automation_job_specs(
        self,
        channel: str = "last",
        to: Optional[str] = None,
        account_id: Optional[str] = None,
        announce: bool = False,
        discovery_cron: str = "0 * * * *",
        watchlist_cron: str = "*/5 * * * *",
        model: Optional[str] = None,
        thinking: str = "medium",
    ) -> List[Dict[str, Any]]:
        script_path = str(Path(__file__).resolve())
        discovery_message = (
            f"Use the exec tool exactly once with command=python3 {script_path} discovery-feed-state --top 5 --universe 24 --enrich-top 2 --tv-top 2 --alert-limit 3 --max-entries 12, host=gateway, security=full, ask=off, and no shell wrapper.\n"
            "Do not prepend bash, sh, timeout, cd, pipes, redirection, or extra quoting. Do not use host=node. Do not ask for approval. Wait for the full command result. If stdout is exactly NO_REPLY, reply exactly NO_REPLY. Otherwise reply with stdout exactly and nothing else."
        )
        watchlist_message = (
            f"Use the exec tool exactly once with command=python3 {script_path} automation-cycle --kind watchlist --output-mode material --top 5 --max-symbols 12 --enrich-top 2 --tv-top 2 --alert-limit 3 --delivery-note cron_actionable_alert --max-live 3 --max-standby 2, host=gateway, security=full, ask=off, and no shell wrapper.\n"
            "Do not prepend bash, sh, timeout, cd, pipes, redirection, or extra quoting. Do not use host=node. Do not ask for approval. Wait for the full command result. If stdout is exactly NO_REPLY, reply exactly NO_REPLY. Otherwise reply with stdout exactly and nothing else."
        )
        base = {
            "session": "isolated",
            "tools": "exec,read",
            "light_context": True,
            "expect_final": True,
            "thinking": thinking,
            "timeout_seconds": 240,
            "announce": announce,
            "channel": channel,
            "to": to,
            "account_id": account_id,
            "model": model,
        }
        return [
            {
                **base,
                "key": "discovery",
                "name": "Crypto Futures Discovery Scan",
                "description": "Hourly silent discovery feeder. Explicitly mutates canonical state, writes internal radar/log sink, and never speaks on main Telegram.",
                "cron": discovery_cron,
                "message": discovery_message,
            },
            {
                **base,
                "key": "watchlist",
                "name": "Crypto Futures Watchlist Refresh",
                "description": "5-minute active-watchlist refresh with pending-alert review.",
                "cron": watchlist_cron,
                "message": watchlist_message,
            },
        ]

    def automation_install(
        self,
        enabled: bool = False,
        announce: bool = False,
        channel: str = "last",
        to: Optional[str] = None,
        account_id: Optional[str] = None,
        discovery_cron: str = "0 * * * *",
        watchlist_cron: str = "*/5 * * * *",
        model: Optional[str] = None,
        thinking: str = "medium",
    ) -> Dict[str, Any]:
        if announce:
            raise ValueError("automation-install --announce is blocked. Main Telegram is reserved for C2 only. Keep discovery/watchlist automation no-deliver.")
        jobs = self.cron_jobs()
        by_name = {job.get("name"): job for job in jobs if isinstance(job, dict) and job.get("name")}
        specs = self.automation_job_specs(channel=channel, to=to, account_id=account_id, announce=announce, discovery_cron=discovery_cron, watchlist_cron=watchlist_cron, model=model, thinking=thinking)
        results = []

        for spec in specs:
            existing = by_name.get(spec["name"])
            if existing:
                cmd = [
                    "openclaw", "cron", "edit", str(existing.get("id")),
                    "--name", spec["name"],
                    "--description", spec["description"],
                    "--cron", spec["cron"],
                    "--session", spec["session"],
                    "--message", spec["message"],
                    "--tools", spec["tools"],
                    "--thinking", spec["thinking"],
                    "--timeout-seconds", str(spec["timeout_seconds"]),
                ]
                if spec.get("model"):
                    cmd.extend(["--model", spec["model"]])
                cmd.append("--light-context")
                cmd.append("--expect-final")
                if announce:
                    cmd.extend(["--announce", "--channel", channel])
                    if to:
                        cmd.extend(["--to", to])
                    if account_id:
                        cmd.extend(["--account", account_id])
                else:
                    cmd.append("--no-deliver")
                cmd.append("--enable" if enabled else "--disable")
                self.run_command(cmd)
                job_id = str(existing.get("id"))
                action = "updated"
            else:
                cmd = [
                    "openclaw", "cron", "add",
                    "--json",
                    "--name", spec["name"],
                    "--description", spec["description"],
                    "--cron", spec["cron"],
                    "--session", spec["session"],
                    "--message", spec["message"],
                    "--tools", spec["tools"],
                    "--thinking", spec["thinking"],
                    "--timeout-seconds", str(spec["timeout_seconds"]),
                    "--light-context",
                    "--expect-final",
                ]
                if spec.get("model"):
                    cmd.extend(["--model", spec["model"]])
                if not enabled:
                    cmd.append("--disabled")
                if announce:
                    cmd.extend(["--announce", "--channel", channel])
                    if to:
                        cmd.extend(["--to", to])
                    if account_id:
                        cmd.extend(["--account", account_id])
                else:
                    cmd.append("--no-deliver")
                added = self.run_command(cmd, expect_json=True)
                job_id = str((((added.get("job") or {}) if isinstance(added, dict) else {}).get("id")) or "")
                action = "created"
            results.append({
                "key": spec["key"],
                "name": spec["name"],
                "jobId": job_id,
                "action": action,
                "enabled": enabled,
                "announce": announce,
                "channel": channel if announce else None,
                "to": to if announce else None,
            })

        state = self.load_automation_state()
        state["lastInstallAt"] = datetime.now(timezone.utc).isoformat()
        state["jobs"] = {item["key"]: item for item in results}
        state["mode"] = {"enabled": enabled, "announce": announce, "channel": channel if announce else None, "to": to if announce else None, "accountId": account_id if announce else None, "model": model, "thinking": thinking}
        self.save_automation_state(state)
        return {
            "saved": True,
            "path": str(AUTOMATION_STATE_PATH),
            "jobs": results,
            "status": self.automation_status(),
        }

    def automation_status(self) -> Dict[str, Any]:
        jobs = self.cron_jobs()
        target_names = {"Crypto Futures Discovery Scan", "Crypto Futures Watchlist Refresh"}
        relevant = [job for job in jobs if isinstance(job, dict) and job.get("name") in target_names]
        automation_state = self.load_automation_state()
        pending = self.pending_alerts(limit=10)
        return {
            "path": str(AUTOMATION_STATE_PATH),
            "installed": len(relevant) >= 2,
            "jobCount": len(relevant),
            "jobs": relevant,
            "cronStatus": self.cron_status_json(),
            "cronStorePath": str(OPENCLAW_CRON_STORE_PATH),
            "savedState": automation_state,
            "pendingAlerts": pending.get("pending", []),
            "pendingAlertCount": pending.get("pendingCount", 0),
            "stateStatus": self.setup_state_status(),
        }

    def scan(self, top: int = 5, universe_size: int = 20, enrich_top: int = 2, tradingview_top: int = 0, persist_state: bool = False, mark_alerts_sent: bool = False, alert_limit: int = 10, delivery_note: Optional[str] = None, prod_mode: bool = False) -> Dict[str, Any]:
        universe = self.binance_universe(top=universe_size)
        scored = []
        for row in universe:
            symbol = row["symbol"]
            try:
                # prod_mode: skip news keyword scoring to keep production path deterministic
                score = self._score_symbol(symbol, include_auth_providers=False, prod_mode=prod_mode)
                score["quoteVolume"] = row["quoteVolume"]
                score["priceChangePercent"] = row["priceChangePercent"]
                self.apply_grade_to_row(score)
                scored.append(score)
            except Exception as exc:
                scored.append({"symbol": symbol, "error": str(exc)})
        tradable = [row for row in scored if "error" not in row]
        massive_grouped_daily = None
        effective_enrich_top = 0 if prod_mode else enrich_top
        effective_tradingview_top = 0 if prod_mode else tradingview_top
        if effective_enrich_top > 0:
            try:
                massive_grouped_daily = self.massive_grouped_daily()
            except Exception:
                massive_grouped_daily = None
            candidates = sorted(tradable, key=self.scan_sort_key, reverse=True)
            selected_symbols = []
            for row in candidates:
                if row["symbol"] in selected_symbols:
                    continue
                selected_symbols.append(row["symbol"])
                if len(selected_symbols) >= effective_enrich_top:
                    break
            for row in tradable:
                if row["symbol"] not in selected_symbols:
                    continue
                enrichment = self.provider_enrichment(row["symbol"], massive_grouped_daily=massive_grouped_daily)
                self.apply_provider_enrichment_to_row(row, enrichment)
        if effective_tradingview_top > 0:
            candidates = sorted(tradable, key=self.scan_sort_key, reverse=True)
            selected_symbols = []
            for row in candidates:
                if row["symbol"] in selected_symbols:
                    continue
                selected_symbols.append(row["symbol"])
                if len(selected_symbols) >= effective_tradingview_top:
                    break
            for row in tradable:
                if row["symbol"] not in selected_symbols:
                    continue
                confirmation = self.tradingview_confirmation(row["symbol"], bull_score=int(row.get("bullScore", 0)))
                self.apply_tradingview_enrichment_to_row(row, confirmation)
        alert_state_map = (self.load_alert_state().get("setups") or {})
        for row in tradable:
            self.apply_grade_to_row(row)
            # prod_mode: freeze feedback influence until minimum sample threshold is met
            self._apply_feedback_v2_to_row(row, prod_mode=prod_mode)
            self.apply_actionable_setup_plan(row)
            direction = row.get("direction") or self.direction_label(int(row.get("bullScore", 0) or 0))
            alert_record = alert_state_map.get(self.setup_state_key(row["symbol"], direction)) if direction in {"long", "short"} else None
            self.apply_setup_state_preview_to_row(row, alert_record=alert_record)
        output_tradable = tradable
        pruned_lifecycle_rows: List[str] = []
        if prod_mode:
            output_tradable = []
            for row in tradable:
                lifecycle_state = self.setup_lifecycle_state(row)
                if lifecycle_state in {"invalidated", "stale"}:
                    pruned_lifecycle_rows.append(str(row.get("symbol") or ""))
                    continue
                output_tradable.append(row)

        longs = sorted([r for r in output_tradable if r["bullScore"] >= 2 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]
        shorts = sorted([r for r in output_tradable if r["bullScore"] <= -2 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]
        watch = sorted([r for r in output_tradable if -1 <= r["bullScore"] <= 1 and r.get("grade") != "NO_TRADE"], key=self.scan_sort_key, reverse=True)[:top]
        result = {
            "generatedAt": int(time.time()),
            "prodMode": prod_mode,
            "universeSize": len(tradable),
            "resolvedTradable": [row["symbol"] for row in output_tradable],
            "resolvedTradableAll": [row["symbol"] for row in tradable],
            "massiveGroupedDailyDate": massive_grouped_daily.get("date") if isinstance(massive_grouped_daily, dict) else None,
            "executionFeedbackPath": str(EXECUTION_FEEDBACK_STATE_PATH),
            "productionSignalEvalLogPath": str(PRODUCTION_SIGNAL_EVAL_LOG_PATH),
            "watchlistStatePath": str(WATCHLIST_STATE_PATH),
            "alertStatePath": str(ALERT_STATE_PATH),
            "authProvidersAppliedTo": [row["symbol"] for row in tradable if "providerOverlay" in row],
            "tradingviewAppliedTo": [row["symbol"] for row in tradable if "tradingviewOverlay" in row],
            "productionPruned": {
                "providerEnrichmentDisabled": bool(prod_mode),
                "tradingviewDisabled": bool(prod_mode),
                "requestedEnrichTop": enrich_top,
                "requestedTradingviewTop": tradingview_top,
                "effectiveEnrichTop": effective_enrich_top,
                "effectiveTradingviewTop": effective_tradingview_top,
                "lifecycleHiddenSymbols": pruned_lifecycle_rows,
            },
            "tradingviewStatus": self.tradingview_status(),
            "alertEligible": [] if mark_alerts_sent else [row["symbol"] for row in output_tradable if row.get("alertEligible")],
            "watchlistEligible": [] if mark_alerts_sent else [row["symbol"] for row in output_tradable if row.get("watchlistEligible")],
            "topLongs": longs,
            "topShorts": shorts,
            "topWatch": watch,
        }
        self.production_eval_log_scan_batch(result, tradable)
        if persist_state:
            result["stateSync"] = self.sync_setup_state(tradable, source="scan")
            result["pendingAlerts"] = self.pending_alerts(limit=alert_limit, mark_sent=mark_alerts_sent, notes=delivery_note)
            result["stateStatus"] = self.setup_state_status()
        return result


def dump(data: Any) -> None:
    print(json.dumps(data, indent=2, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description="Crypto futures data connector with public and optional authenticated provider support.")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("endpoint-map", help="Print the final endpoint map")

    snapshot_parser = sub.add_parser("snapshot", help="Fetch a full public snapshot for one symbol")
    snapshot_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")
    snapshot_parser.add_argument("--with-tradingview", action="store_true", help="Add TradingView confirmation layer when available")

    scan_parser = sub.add_parser("scan", help="Run a quick public market scan")
    scan_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    scan_parser.add_argument("--universe", type=int, default=20, help="How many liquid names to score")
    scan_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get Massive and LunarCrush enrichment")
    scan_parser.add_argument("--tv-top", type=int, default=0, help="How many top candidates get TradingView confirmation overlay")
    scan_parser.add_argument("--persist-state", action="store_true", help="Persist Phase 2 watchlist and alert state from this scan")
    scan_parser.add_argument("--mark-alerts-sent", action="store_true", help="Mark pending alerts as sent after returning actionable payloads")
    scan_parser.add_argument("--alert-limit", type=int, default=10, help="How many actionable pending alerts to return")
    scan_parser.add_argument("--delivery-note", default=None, help="Optional note to store when pending alerts are marked sent")
    scan_parser.add_argument("--prod-mode", action="store_true", default=False, help="Enable production mode: skip news keyword scoring and freeze low-sample feedback influence in production paths")

    ws_plan_parser = sub.add_parser("ws-plan", help="Print the WebSocket subscription plan for one symbol")
    ws_plan_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")

    event_parser = sub.add_parser("event-layer", help="Fetch token unlock, announcement, and macro context for one symbol")
    event_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")

    sub.add_parser("provider-status", help="Show configured provider keys and rate-limit state")

    cmc_parser = sub.add_parser("cmc-quotes", help="Fetch latest CoinMarketCap quotes for one or more symbols")
    cmc_parser.add_argument("symbols", nargs="+", help="Spot symbols, for example BTC ETH SOL")

    nansen_parser = sub.add_parser("nansen-holdings", help="Fetch Nansen smart-money holdings")
    nansen_parser.add_argument("--chains", nargs="+", default=["ethereum"], help="Chains, for example ethereum solana")
    nansen_parser.add_argument("--token-symbols", nargs="*", default=None, help="Optional token symbols filter")
    nansen_parser.add_argument("--page", type=int, default=1, help="Page number")
    nansen_parser.add_argument("--per-page", type=int, default=10, help="Rows per page")

    provider_enrichment_parser = sub.add_parser("provider-enrichment", help="Fetch Massive and LunarCrush enrichment for one symbol")
    provider_enrichment_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")

    feedback_status_parser = sub.add_parser("feedback-status", help="Show persisted execution feedback state")
    feedback_status_parser.add_argument("symbol", nargs="?", default=None, help="Optional symbol, for example LINKUSDT")

    feedback_log_parser = sub.add_parser("feedback-log", help="Record execution feedback for one symbol")
    feedback_log_parser.add_argument("symbol", help="Perpetual futures symbol, for example LINKUSDT")
    feedback_log_parser.add_argument("--side", required=True, choices=["long", "short"], help="Trade side")
    feedback_log_parser.add_argument("--outcome", required=True, choices=["stop_loss", "take_profit", "runner", "no_fill", "break_even", "invalidated"], help="Observed trade outcome")
    feedback_log_parser.add_argument("--entry-quality", default="ok", choices=["clean", "ok", "poor"], help="How clean the entry execution was")
    feedback_log_parser.add_argument("--notes", default=None, help="Optional short note about the outcome")

    liquidation_status_parser = sub.add_parser("liquidation-status", help="Show persisted liquidation intelligence state")
    liquidation_status_parser.add_argument("symbol", nargs="?", default=None, help="Optional symbol, for example DOGEUSDT")

    liquidation_capture_parser = sub.add_parser("liquidation-capture", help="Capture Binance forceOrder liquidation events into local state")
    liquidation_capture_parser.add_argument("symbols", nargs="+", help="One or more perpetual futures symbols, for example BTCUSDT ETHUSDT DOGEUSDT")
    liquidation_capture_parser.add_argument("--seconds", type=int, default=20, help="How long to listen to forceOrder streams")
    liquidation_capture_parser.add_argument("--max-events", type=int, default=200, help="Maximum liquidation events to store from this capture")
    liquidation_capture_parser.add_argument("--min-notional", type=float, default=50000, help="Minimum liquidation notional in USD-equivalent to keep")
    liquidation_capture_parser.add_argument("--no-persist", action="store_true", help="Do not write capture results to local liquidation state")

    state_status_parser = sub.add_parser("state-status", help="Show Phase 2 setup state machine status")
    state_status_parser.add_argument("symbol", nargs="?", default=None, help="Optional symbol, for example XRPUSDT")

    migration_preview_parser = sub.add_parser("state-migration-preview", help="Preview canonical normalization of persisted state without writing")
    migration_preview_parser.add_argument("--scope", default="all", choices=["watchlist", "alerts", "runtime", "all"], help="Which persisted state to preview")
    migration_preview_parser.add_argument("--limit", type=int, default=3, help="How many before/after examples to show per scope")

    state_sync_parser = sub.add_parser("state-sync", help="Run a scan and persist Phase 2 setup state")
    state_sync_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    state_sync_parser.add_argument("--universe", type=int, default=20, help="How many liquid names to score")
    state_sync_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get Massive and LunarCrush enrichment")
    state_sync_parser.add_argument("--tv-top", type=int, default=0, help="How many top candidates get TradingView confirmation overlay")
    state_sync_parser.add_argument("--mark-alerts-sent", action="store_true", help="Mark pending alerts as sent after returning actionable payloads")
    state_sync_parser.add_argument("--alert-limit", type=int, default=10, help="How many actionable pending alerts to return")
    state_sync_parser.add_argument("--delivery-note", default=None, help="Optional note to store when pending alerts are marked sent")

    watchlist_refresh_parser = sub.add_parser("watchlist-refresh", help="Refresh only active Phase 2 watchlist setups")
    watchlist_refresh_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    watchlist_refresh_parser.add_argument("--max-symbols", type=int, default=12, help="How many active watchlist symbols to refresh")
    watchlist_refresh_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get Massive and LunarCrush enrichment")
    watchlist_refresh_parser.add_argument("--tv-top", type=int, default=0, help="How many top candidates get TradingView confirmation overlay")
    watchlist_refresh_parser.add_argument("--persist-state", action="store_true", help="Persist refreshed lifecycle state")
    watchlist_refresh_parser.add_argument("--mark-alerts-sent", action="store_true", help="Mark pending alerts as sent after returning actionable payloads")
    watchlist_refresh_parser.add_argument("--alert-limit", type=int, default=10, help="How many actionable pending alerts to return")
    watchlist_refresh_parser.add_argument("--delivery-note", default=None, help="Optional note to store when pending alerts are marked sent")
    watchlist_refresh_parser.add_argument("--no-prod-mode", action="store_true", default=False, help="Disable production mode (use research mode with news keyword scoring and full feedback influence)")

    pending_alerts_parser = sub.add_parser("pending-alerts", help="Show pending alert-eligible setups from Phase 2 state")
    pending_alerts_parser.add_argument("--limit", type=int, default=10, help="How many pending alerts to show")

    notification_render_parser = sub.add_parser("notification-render", help="Render deterministic notification text from canonical state")
    notification_render_parser.add_argument("--mode", default="compact", choices=["compact", "material", "detailed"], help="Notification output mode")
    notification_render_parser.add_argument("--kind", default="watchlist", choices=["watchlist", "discovery"], help="Cycle kind label for the renderer")
    notification_render_parser.add_argument("--max-live", type=int, default=3, help="Maximum LIVE pairs to render in snapshot modes")
    notification_render_parser.add_argument("--max-standby", type=int, default=5, help="Maximum STANDBY pairs to render in snapshot modes")
    notification_render_parser.add_argument("--persist-render-state", action="store_true", help="Persist renderer state after rendering")

    discovery_radar_preview_parser = sub.add_parser("discovery-radar-preview", help="Run non-mutating discovery preview and write preview-only radar/log output")
    discovery_radar_preview_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    discovery_radar_preview_parser.add_argument("--universe", type=int, default=24, help="How many liquid names to score")
    discovery_radar_preview_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get enrichment")
    discovery_radar_preview_parser.add_argument("--tv-top", type=int, default=2, help="How many top candidates get TradingView confirmation")
    discovery_radar_preview_parser.add_argument("--alert-limit", type=int, default=3, help="How many pending alerts to include in the preview summary")
    discovery_radar_preview_parser.add_argument("--max-entries", type=int, default=12, help="Maximum preview candidates stored in the latest snapshot")
    discovery_radar_preview_parser.add_argument("--prod-mode", action="store_true", default=False, help="Enable production mode for preview: skip news keyword scoring and freeze low-sample feedback influence")

    discovery_feed_state_parser = sub.add_parser("discovery-feed-state", help="Run explicit silent discovery feeder, mutate canonical state, and write operational radar/log sink")
    discovery_feed_state_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    discovery_feed_state_parser.add_argument("--universe", type=int, default=24, help="How many liquid names to score")
    discovery_feed_state_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get enrichment")
    discovery_feed_state_parser.add_argument("--tv-top", type=int, default=2, help="How many top candidates get TradingView confirmation")
    discovery_feed_state_parser.add_argument("--alert-limit", type=int, default=3, help="How many pending alerts to include in the sink summary")
    discovery_feed_state_parser.add_argument("--max-entries", type=int, default=12, help="Maximum radar candidates stored in the latest snapshot")
    discovery_feed_state_parser.add_argument("--no-prod-mode", action="store_true", default=False, help="Disable production mode (use research mode with news keyword scoring and full feedback influence)")

    discovery_radar_cycle_parser = sub.add_parser("discovery-radar-cycle", help="Deprecated legacy discovery command. Blocked because it mixed preview behavior with production state mutation")
    discovery_radar_cycle_parser.add_argument("--top", type=int, default=5, help="Deprecated")
    discovery_radar_cycle_parser.add_argument("--universe", type=int, default=24, help="Deprecated")
    discovery_radar_cycle_parser.add_argument("--enrich-top", type=int, default=2, help="Deprecated")
    discovery_radar_cycle_parser.add_argument("--tv-top", type=int, default=2, help="Deprecated")
    discovery_radar_cycle_parser.add_argument("--alert-limit", type=int, default=3, help="Deprecated")
    discovery_radar_cycle_parser.add_argument("--max-entries", type=int, default=12, help="Deprecated")

    sub.add_parser("discovery-radar-status", help="Show latest discovery radar sink snapshot")

    entry_ready_preview_parser = sub.add_parser("entry-ready-preview", help="Preview Telegram execution cards for ENTRY_READY_ONLY policy without sending")
    entry_ready_preview_parser.add_argument("--limit", type=int, default=3, help="How many real READY_IN_ZONE/LIVE_STRICT cards to render")
    entry_ready_preview_parser.add_argument("--json", action="store_true", help="Return structured preview payload instead of plain text cards")
    entry_ready_preview_parser.add_argument("--no-simulated-live", action="store_true", help="Do not append a simulated LIVE_STRICT preview card")
    entry_ready_preview_parser.add_argument("--no-simulated-cancel", action="store_true", help="Do not append a simulated CANCEL preview card")

    entry_ready_dry_run_parser = sub.add_parser("entry-ready-dry-run", help="Run dry-run delivery gate for ENTRY_READY_ONLY without sending")
    entry_ready_dry_run_parser.add_argument("--limit", type=int, default=10, help="How many eligible events to evaluate")
    entry_ready_dry_run_parser.add_argument("--reset-state", action="store_true", help="Reset dry-run ledger before evaluating this cycle")

    automation_cycle_parser = sub.add_parser("automation-cycle", help="Run a deterministic automation cycle and render final notification text")
    automation_cycle_parser.add_argument("--kind", default="watchlist", choices=["watchlist", "discovery"], help="Cycle kind")
    automation_cycle_parser.add_argument("--output-mode", default="material", choices=["compact", "material", "detailed"], help="Notification output mode")
    automation_cycle_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    automation_cycle_parser.add_argument("--universe", type=int, default=24, help="How many liquid names to score for discovery")
    automation_cycle_parser.add_argument("--max-symbols", type=int, default=12, help="How many active watchlist symbols to refresh")
    automation_cycle_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get enrichment")
    automation_cycle_parser.add_argument("--tv-top", type=int, default=2, help="How many top candidates get TradingView confirmation")
    automation_cycle_parser.add_argument("--alert-limit", type=int, default=3, help="How many actionable alerts to mark sent per cycle")
    automation_cycle_parser.add_argument("--delivery-note", default=None, help="Optional note to store when marking alerts sent")
    automation_cycle_parser.add_argument("--max-live", type=int, default=3, help="Maximum LIVE pairs to render in snapshot modes")
    automation_cycle_parser.add_argument("--max-standby", type=int, default=2, help="Maximum STANDBY pairs to render in snapshot modes")

    automation_cycle_send_parser = sub.add_parser("automation-cycle-send", help="Render and directly send notification to Telegram without agent reformatting")
    automation_cycle_send_parser.add_argument("--kind", default="watchlist", choices=["watchlist", "discovery"], help="Cycle kind")
    automation_cycle_send_parser.add_argument("--output-mode", default="material", choices=["compact", "material", "detailed", "entry-ready"], help="Notification output mode")
    automation_cycle_send_parser.add_argument("--top", type=int, default=5, help="How many names per bucket")
    automation_cycle_send_parser.add_argument("--universe", type=int, default=24, help="How many liquid names to score for discovery")
    automation_cycle_send_parser.add_argument("--max-symbols", type=int, default=12, help="How many active watchlist symbols to refresh")
    automation_cycle_send_parser.add_argument("--enrich-top", type=int, default=2, help="How many top candidates get enrichment")
    automation_cycle_send_parser.add_argument("--tv-top", type=int, default=2, help="How many top candidates get TradingView confirmation")
    automation_cycle_send_parser.add_argument("--alert-limit", type=int, default=3, help="How many actionable alerts to mark sent per cycle")
    automation_cycle_send_parser.add_argument("--delivery-note", default=None, help="Optional note to store when marking alerts sent")
    automation_cycle_send_parser.add_argument("--max-live", type=int, default=3, help="Maximum LIVE pairs to render in snapshot modes")
    automation_cycle_send_parser.add_argument("--max-standby", type=int, default=2, help="Maximum STANDBY pairs to render in snapshot modes")
    automation_cycle_send_parser.add_argument("--channel", default="telegram", help="Delivery channel")
    automation_cycle_send_parser.add_argument("--target", default="851120836", help="Delivery target")

    automation_status_parser = sub.add_parser("automation-status", help="Show Phase 3 cron automation status")

    forensic_replay_parser = sub.add_parser("forensic-replay", help="Replay one setup timeline from persisted runtime + entry-ready delivery state")
    forensic_replay_parser.add_argument("--setup-key", required=True, help="Setup key, for example XRPUSDT:long")
    forensic_replay_parser.add_argument("--from", dest="from_iso", default=None, help="Inclusive ISO timestamp lower bound")
    forensic_replay_parser.add_argument("--to", dest="to_iso", default=None, help="Inclusive ISO timestamp upper bound")

    live_trigger_status_parser = sub.add_parser("live-trigger-status", help="Show live trigger engine status")

    live_trigger_start_parser = sub.add_parser("live-trigger-start", help="Start the live trigger engine daemon")
    live_trigger_start_parser.add_argument("--channel", default="telegram", help="Delivery channel for trigger alerts")
    live_trigger_start_parser.add_argument("--target", required=True, help="Delivery target, for example Telegram chat id")
    live_trigger_start_parser.add_argument("--trigger-timeframe", default="5m", help="Trigger timeframe, default 5m")
    live_trigger_start_parser.add_argument("--context-timeframe", default="15m", help="Context timeframe, default 15m")
    live_trigger_start_parser.add_argument("--poll-seconds", type=int, default=LIVE_TRIGGER_DEFAULT_POLL_SECONDS, help="How often to resync monitored setups")
    live_trigger_start_parser.add_argument("--cooldown-seconds", type=int, default=LIVE_TRIGGER_DEFAULT_COOLDOWN_SECONDS, help="Cooldown after a trigger alert is sent")

    sub.add_parser("live-trigger-stop", help="Stop the live trigger engine daemon")

    watchdog_parser = sub.add_parser("watchdog-check", help="Run watchdog health check (or use --daemon for persistent mode)")
    watchdog_parser.add_argument("--daemon", action="store_true", help="Run as persistent daemon")

    live_trigger_run_parser = sub.add_parser("live-trigger-run", help="Run the live trigger engine in foreground")
    live_trigger_run_parser.add_argument("--channel", default="telegram", help="Delivery channel for trigger alerts")
    live_trigger_run_parser.add_argument("--target", required=True, help="Delivery target, for example Telegram chat id")
    live_trigger_run_parser.add_argument("--trigger-timeframe", default="5m", help="Trigger timeframe, default 5m")
    live_trigger_run_parser.add_argument("--context-timeframe", default="15m", help="Context timeframe, default 15m")
    live_trigger_run_parser.add_argument("--poll-seconds", type=int, default=LIVE_TRIGGER_DEFAULT_POLL_SECONDS, help="How often to resync monitored setups")
    live_trigger_run_parser.add_argument("--cooldown-seconds", type=int, default=LIVE_TRIGGER_DEFAULT_COOLDOWN_SECONDS, help="Cooldown after a trigger alert is sent")

    automation_install_parser = sub.add_parser("automation-install", help="Install or update Phase 3 cron jobs")
    automation_install_parser.add_argument("--enabled", action="store_true", help="Enable the installed cron jobs immediately")
    automation_install_parser.add_argument("--announce", action="store_true", help="Deprecated and blocked by C2-only main Telegram policy")
    automation_install_parser.add_argument("--channel", default="last", help="Delivery channel, default last")
    automation_install_parser.add_argument("--to", default=None, help="Optional explicit delivery target")
    automation_install_parser.add_argument("--account-id", default=None, help="Optional multi-account delivery id")
    automation_install_parser.add_argument("--discovery-cron", default="0 * * * *", help="Discovery cron expression")
    automation_install_parser.add_argument("--watchlist-cron", default="*/5 * * * *", help="Watchlist cron expression")
    automation_install_parser.add_argument("--model", default=None, help="Optional model override for cron jobs")
    automation_install_parser.add_argument("--thinking", default="medium", choices=["off", "minimal", "low", "medium", "high", "xhigh"], help="Thinking level for cron jobs")

    alert_sent_parser = sub.add_parser("alert-mark-sent", help="Mark one setup alert as sent")
    alert_sent_parser.add_argument("symbol", help="Perpetual futures symbol, for example XRPUSDT")
    alert_sent_parser.add_argument("--side", required=True, choices=["long", "short"], help="Setup side")
    alert_sent_parser.add_argument("--version", default=None, help="Optional setup version override")
    alert_sent_parser.add_argument("--notes", default=None, help="Optional delivery note")

    sub.add_parser("tv-status", help="Show TradingView adapter availability and discovery status")

    tv_ta_parser = sub.add_parser("tv-ta", help="Fetch TradingView technical analysis for one symbol")
    tv_ta_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")
    tv_ta_parser.add_argument("--exchange", default="binance", help="TradingView exchange, default binance")
    tv_ta_parser.add_argument("--timeframe", default="15m", help="TradingView timeframe, default 15m")

    tv_mtf_parser = sub.add_parser("tv-mtf", help="Fetch TradingView multi-timeframe alignment for one symbol")
    tv_mtf_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")
    tv_mtf_parser.add_argument("--exchange", default="binance", help="TradingView exchange, default binance")

    tv_volume_parser = sub.add_parser("tv-volume", help="Fetch TradingView volume confirmation for one symbol")
    tv_volume_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")
    tv_volume_parser.add_argument("--exchange", default="binance", help="TradingView exchange, default binance")
    tv_volume_parser.add_argument("--timeframe", default="15m", help="TradingView timeframe, default 15m")

    tv_bollinger_parser = sub.add_parser("tv-bollinger", help="Run TradingView Bollinger squeeze scan")
    tv_bollinger_parser.add_argument("--exchange", default="binance", help="TradingView exchange, default binance")
    tv_bollinger_parser.add_argument("--timeframe", default="4h", help="TradingView timeframe, default 4h")
    tv_bollinger_parser.add_argument("--bbw-threshold", type=float, default=0.04, help="Maximum Bollinger Band width")
    tv_bollinger_parser.add_argument("--limit", type=int, default=20, help="How many names to return")

    tv_confirm_parser = sub.add_parser("tv-confirm", help="Fetch TradingView finalist confirmation overlay for one symbol")
    tv_confirm_parser.add_argument("symbol", help="Perpetual futures symbol, for example HYPEUSDT")
    tv_confirm_parser.add_argument("--exchange", default="binance", help="TradingView exchange, default binance")
    tv_confirm_parser.add_argument("--timeframe", default="15m", help="TradingView timeframe, default 15m")
    tv_confirm_parser.add_argument("--bull-score", type=int, default=0, help="Optional current futures bull score for overlay calibration")

    fb_engine_status_parser = sub.add_parser("feedback-engine-status", help="Show Phase 7 feedback engine v2 status")
    fb_engine_status_parser.add_argument("--symbol", default=None, help="Optional symbol for specific learner status")
    fb_engine_status_parser.add_argument("--side", default=None, choices=["long", "short"], help="Side for specific learner status")
    sub.add_parser("feedback-engine-ingest", help="Auto-learn from state machine transitions")

    args = parser.parse_args()
    client = PublicMarketData()

    if args.command == "endpoint-map":
        dump(ENDPOINT_MAP)
        return 0
    if args.command == "provider-status":
        dump(client.provider_status())
        return 0
    if args.command == "snapshot":
        dump(client.snapshot(args.symbol.upper(), include_tradingview=args.with_tradingview))
        return 0
    if args.command == "scan":
        dump(client.scan(top=args.top, universe_size=args.universe, enrich_top=args.enrich_top, tradingview_top=args.tv_top, persist_state=args.persist_state, mark_alerts_sent=args.mark_alerts_sent, alert_limit=args.alert_limit, delivery_note=args.delivery_note, prod_mode=args.prod_mode))
        return 0
    if args.command == "ws-plan":
        symbol = args.symbol.upper()
        dump({
            "symbol": symbol,
            "binance": client.binance_ws_plan(symbol),
            "bybit": client.bybit_ws_plan(symbol),
        })
        return 0
    if args.command == "event-layer":
        symbol = args.symbol.upper()
        payload = {
            "symbol": symbol,
            "tokenUnlock": client.token_unlock_context(symbol),
            "exchangeAnnouncements": client.exchange_announcements(symbol),
            "macro": client.macro_context(),
        }
        all_items = payload["tokenUnlock"].get("results", []) + payload["exchangeAnnouncements"].get("binance", []) + payload["exchangeAnnouncements"].get("bybit", []) + payload["macro"].get("headlines", [])
        payload["headlineFlags"] = client.headline_flags(all_items)
        dump(payload)
        return 0
    if args.command == "cmc-quotes":
        dump(client.cmc_quotes_latest([symbol.upper() for symbol in args.symbols]))
        return 0
    if args.command == "nansen-holdings":
        dump(client.nansen_smart_money_holdings(chains=args.chains, token_symbols=args.token_symbols, page=args.page, per_page=args.per_page))
        return 0
    if args.command == "provider-enrichment":
        symbol = args.symbol.upper()
        enrichment = client.provider_enrichment(symbol)
        dump({
            "symbol": symbol,
            "enrichment": enrichment,
            "overlay": client.provider_overlay(symbol, 0, enrichment),
        })
        return 0
    if args.command == "feedback-status":
        dump(client.execution_feedback_status(args.symbol.upper() if args.symbol else None))
        return 0
    if args.command == "feedback-log":
        dump(client.record_execution_feedback(args.symbol.upper(), side=args.side, outcome=args.outcome, entry_quality=args.entry_quality, notes=args.notes))
        return 0
    if args.command == "liquidation-status":
        dump(client.liquidation_state_status(args.symbol.upper() if args.symbol else None))
        return 0
    if args.command == "liquidation-capture":
        dump(client.liquidation_capture([symbol.upper() for symbol in args.symbols], seconds=args.seconds, max_events=args.max_events, min_notional=args.min_notional, persist=not args.no_persist))
        return 0
    if args.command == "state-status":
        dump(client.setup_state_status(args.symbol.upper() if args.symbol else None))
        return 0
    if args.command == "state-migration-preview":
        dump(client.state_migration_preview(scope=args.scope, limit=args.limit))
        return 0
    if args.command == "state-sync":
        dump(client.scan(top=args.top, universe_size=args.universe, enrich_top=args.enrich_top, tradingview_top=args.tv_top, persist_state=True, mark_alerts_sent=args.mark_alerts_sent, alert_limit=args.alert_limit, delivery_note=args.delivery_note))
        return 0
    if args.command == "watchlist-refresh":
        prod_mode = not args.no_prod_mode  # default True; --no-prod-mode disables
        dump(client.watchlist_refresh(top=args.top, max_symbols=args.max_symbols, enrich_top=args.enrich_top, tradingview_top=args.tv_top, persist_state=args.persist_state, mark_alerts_sent=args.mark_alerts_sent, alert_limit=args.alert_limit, delivery_note=args.delivery_note, prod_mode=prod_mode))
        return 0
    if args.command == "pending-alerts":
        dump(client.pending_alerts(limit=args.limit))
        return 0
    if args.command == "notification-render":
        print(client.notification_render(mode=args.mode, cycle_kind=args.kind, max_live=args.max_live, max_standby=args.max_standby, persist_render_state=args.persist_render_state))
        return 0
    if args.command == "discovery-radar-preview":
        dump(client.discovery_radar_preview(top=args.top, universe=args.universe, enrich_top=args.enrich_top, tradingview_top=args.tv_top, alert_limit=args.alert_limit, max_entries=args.max_entries, prod_mode=args.prod_mode))
        return 0
    if args.command == "discovery-feed-state":
        prod_mode = not args.no_prod_mode  # default True; --no-prod-mode disables
        dump(client.discovery_feed_state(top=args.top, universe=args.universe, enrich_top=args.enrich_top, tradingview_top=args.tv_top, alert_limit=args.alert_limit, max_entries=args.max_entries, prod_mode=prod_mode))
        return 0
    if args.command == "discovery-radar-cycle":
        dump(client.discovery_radar_cycle(top=args.top, universe=args.universe, enrich_top=args.enrich_top, tradingview_top=args.tv_top, alert_limit=args.alert_limit, max_entries=args.max_entries))
        return 0
    if args.command == "discovery-radar-status":
        dump(client.discovery_radar_status())
        return 0
    if args.command == "entry-ready-preview":
        preview = client.entry_ready_preview(
            limit=args.limit,
            include_simulated_live=not args.no_simulated_live,
            include_simulated_cancel=not args.no_simulated_cancel,
        )
        if args.json:
            dump(preview)
        else:
            print(client.entry_ready_preview_text(
                limit=args.limit,
                include_simulated_live=not args.no_simulated_live,
                include_simulated_cancel=not args.no_simulated_cancel,
            ))
        return 0
    if args.command == "entry-ready-dry-run":
        dump(client.entry_ready_dry_run_cycle(limit=args.limit, persist=True, reset_state=args.reset_state))
        return 0
    if args.command == "automation-cycle":
        print(client.automation_cycle(kind=args.kind, output_mode=args.output_mode, top=args.top, universe=args.universe, max_symbols=args.max_symbols, enrich_top=args.enrich_top, tradingview_top=args.tv_top, alert_limit=args.alert_limit, delivery_note=args.delivery_note, max_live=args.max_live, max_standby=args.max_standby))
        return 0
    if args.command == "automation-cycle-send":
        result = client.automation_cycle_send(
            kind=args.kind,
            output_mode=args.output_mode,
            channel=args.channel,
            target=args.target,
            top=args.top,
            universe=args.universe,
            max_symbols=args.max_symbols,
            enrich_top=args.enrich_top,
            tradingview_top=args.tv_top,
            alert_limit=args.alert_limit,
            delivery_note=args.delivery_note,
            max_live=args.max_live,
            max_standby=args.max_standby,
        )
        dump(result)
        return 0
    if args.command == "alert-mark-sent":
        dump(client.mark_alert_sent(args.symbol.upper(), side=args.side, setup_version=args.version, notes=args.notes))
        return 0
    if args.command == "live-trigger-status":
        dump(client.live_trigger_engine_status())
        return 0
    if args.command == "live-trigger-start":
        dump(client.live_trigger_start(channel=args.channel, target=args.target, trigger_timeframe=args.trigger_timeframe, context_timeframe=args.context_timeframe, poll_seconds=args.poll_seconds, cooldown_seconds=args.cooldown_seconds))
        return 0
    if args.command == "live-trigger-stop":
        dump(client.live_trigger_stop())
        return 0
    if args.command == "watchdog-check":
        import subprocess, sys
        wd = str(Path(__file__).resolve().parent / "live_trigger_watchdog.py")
        if args.daemon:
            subprocess.Popen([sys.executable, wd, "--daemon"], start_new_session=True)
            print("Watchdog daemon started.")
        else:
            result = subprocess.run([sys.executable, wd], capture_output=True, text=True)
            print(result.stdout)
            return result.returncode
        return 0
    if args.command == "live-trigger-run":
        asyncio.run(client.live_trigger_run(channel=args.channel, target=args.target, trigger_timeframe=args.trigger_timeframe, context_timeframe=args.context_timeframe, poll_seconds=args.poll_seconds, cooldown_seconds=args.cooldown_seconds))
        return 0
    if args.command == "automation-status":
        dump(client.automation_status())
        return 0
    if args.command == "forensic-replay":
        dump(client.forensic_replay(setup_key=args.setup_key, from_iso=args.from_iso, to_iso=args.to_iso))
        return 0
    if args.command == "automation-install":
        dump(client.automation_install(enabled=args.enabled, announce=args.announce, channel=args.channel, to=args.to, account_id=args.account_id, discovery_cron=args.discovery_cron, watchlist_cron=args.watchlist_cron, model=args.model, thinking=args.thinking))
        return 0
    if args.command == "tv-status":
        dump(client.tradingview_status())
        return 0
    if args.command == "tv-ta":
        status = client.tradingview_status()
        if not status.get("available"):
            dump(status)
            return 0
        dump(client.tradingview.coin_analysis(args.symbol.upper(), exchange=args.exchange, timeframe=args.timeframe))
        return 0
    if args.command == "tv-mtf":
        status = client.tradingview_status()
        if not status.get("available"):
            dump(status)
            return 0
        dump(client.tradingview.multi_timeframe_analysis(args.symbol.upper(), exchange=args.exchange))
        return 0
    if args.command == "tv-volume":
        status = client.tradingview_status()
        if not status.get("available"):
            dump(status)
            return 0
        dump(client.tradingview.volume_confirmation_analysis(args.symbol.upper(), exchange=args.exchange, timeframe=args.timeframe))
        return 0
    if args.command == "tv-bollinger":
        status = client.tradingview_status()
        if not status.get("available"):
            dump(status)
            return 0
        dump(client.tradingview.bollinger_scan(exchange=args.exchange, timeframe=args.timeframe, bbw_threshold=args.bbw_threshold, limit=args.limit))
        return 0
    if args.command == "tv-confirm":
        dump(client.tradingview_confirmation(args.symbol.upper(), bull_score=args.bull_score, exchange=args.exchange, timeframe=args.timeframe))
        return 0
    if args.command == "feedback-engine-status":
        dump(client.feedback_engine_v2_status(symbol=args.symbol, side=args.side))
        return 0
    if args.command == "feedback-engine-ingest":
        dump(client.feedback_engine_v2_ingest())
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
