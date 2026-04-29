"""
Tests for reliability/health cleanup lane:
1. automation-status: cron_list_json exists and works
2. live-trigger-status: recovery-aware health classification
3. _live_trigger_derive_status correctness
4. lastError/lastErrorAt/lastRecoveredAt fields preserved as evidence
"""
import sys
import json
import subprocess
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch, MagicMock

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")
import public_market_data as pmd
import live_trigger_watchdog as ltw
from public_market_data import PublicMarketData, utc_now_iso, parse_iso_timestamp


class AutomationStatusCronListTests(unittest.TestCase):
    """Test that automation_status path has working cron_list_json."""

    def setUp(self):
        self.p = PublicMarketData()
        self.p.openclaw_node_path = "/abs/test/node"
        self.p.openclaw_script_path = "/abs/test/openclaw.mjs"
        self.p.openclaw_transport_source = "test"
        self.p.openclaw_transport_error = None

    def test_cron_list_json_method_exists(self):
        """cron_list_json is a defined method on PublicMarketData."""
        self.assertTrue(hasattr(self.p, "cron_list_json"))
        self.assertTrue(callable(getattr(self.p, "cron_list_json")))

    def test_cron_list_json_calls_openclaw_cron_list_json(self):
        """cron_list_json calls the correct openclaw CLI command."""
        calls = {}
        def fake_run(args, expect_json=False):
            calls["args"] = args
            return {"jobs": [], "total": 0}
        self.p.run_command = fake_run
        result = self.p.cron_list_json()
        self.assertEqual(calls["args"][:4], ["openclaw", "cron", "list", "--json"])
        self.assertIn("--all", calls["args"])
        self.assertEqual(result, {"jobs": [], "total": 0})

    def test_cron_list_json_falls_back_to_empty_on_error(self):
        """cron_list_json returns empty jobs list on exception."""
        def fake_run(args, expect_json=False):
            raise RuntimeError("connection refused")
        self.p.run_command = fake_run
        result = self.p.cron_list_json()
        self.assertEqual(result, {"jobs": [], "total": 0})

    def test_cron_jobs_uses_cron_list_json_first(self):
        """cron_jobs returns list from cron_list_json when available."""
        def fake_run(args, expect_json=False):
            if "status" in args:
                return {"jobs": 2}
            if "list" in args:
                return {"jobs": [{"id": "1", "name": "Crypto Futures Discovery Scan"},
                                {"id": "2", "name": "Crypto Futures Watchlist Refresh"}]}
            return {}
        self.p.run_command = fake_run
        jobs = self.p.cron_jobs()
        self.assertEqual(len(jobs), 2)
        self.assertEqual(jobs[0]["name"], "Crypto Futures Discovery Scan")

    def test_cron_jobs_falls_back_to_store_when_list_returns_empty(self):
        """cron_jobs falls back to store when cron list returns no jobs."""
        with patch.object(self.p, "cron_store_jobs", return_value=[{"id": "99", "name": "Stored Job"}]):
            def fake_run(args, expect_json=False):
                if "status" in args:
                    return {"jobs": 1}
                if "list" in args:
                    return {"jobs": []}
                return {}
            self.p.run_command = fake_run
            jobs = self.p.cron_jobs()
            self.assertEqual(len(jobs), 1)
            self.assertEqual(jobs[0]["name"], "Stored Job")


class LiveTriggerDeriveStatusTests(unittest.TestCase):
    """Test _live_trigger_derive_status computes health from current signals."""

    def setUp(self):
        self.p = PublicMarketData()

    def derive(self, pids, **overrides):
        state = {
            "status": "running",
            "monitors": {
                "BTCUSDT:long": {
                    "active": True,
                    "symbol": "BTCUSDT",
                    "side": "long",
                    "runtime": {"state": "armed"},
                }
            },
            "lastHeartbeatAt": None,
            "lastWsMessageAt": None,
            "lastErrorAt": None,
            "lastRecoveredAt": None,
            "recoveryState": "none",
            "config": {"pollSeconds": 15},
        }
        state.update(overrides)
        return self.p._live_trigger_derive_status(state, pids)

    def test_down_when_no_process(self):
        self.assertEqual(self.derive([]), "down")

    def test_healthy_when_all_signals_fresh(self):
        now = utc_now_iso()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=now,
                lastWsMessageAt=now,
            ),
            "healthy",
        )

    def test_degraded_when_heartbeat_stale(self):
        now = utc_now_iso()
        stale_heartbeat = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=stale_heartbeat,
                lastWsMessageAt=now,
            ),
            "degraded",
        )

    def test_degraded_when_ws_stale(self):
        now = utc_now_iso()
        stale_ws = (datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=now,
                lastWsMessageAt=stale_ws,
            ),
            "degraded",
        )

    def test_healthy_with_old_lastError_but_fresh_signals_and_recovery(self):
        """Historical error + fresh signals + recent recovery = healthy."""
        now = utc_now_iso()
        old_error = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        recent_recovery = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=now,
                lastWsMessageAt=now,
                lastError=old_error,
                lastErrorAt=old_error,
                lastRecoveredAt=recent_recovery,
                recoveryState="recovered",
            ),
            "healthy",
        )

    def test_degraded_when_no_recovery_and_error_recent(self):
        """Recent error without recovery record = degraded."""
        recent_error = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=utc_now_iso(),
                lastWsMessageAt=utc_now_iso(),
                lastError="WebSocket keepalive timeout",
                lastErrorAt=recent_error,
                lastRecoveredAt=None,
                recoveryState="none",
            ),
            "degraded",
        )

    def test_healthy_after_stale_recovery_window_expires_but_signals_fresh(self):
        """Stale recovery but signals fresh → still healthy (current signals win)."""
        now = utc_now_iso()
        old_error = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
        stale_recovery = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=now,
                lastWsMessageAt=now,
                lastError="old ws error",
                lastErrorAt=old_error,
                lastRecoveredAt=stale_recovery,
                recoveryState="recovered",
            ),
            "healthy",
        )

    def test_degraded_when_no_pids(self):
        # empty list → down (no running process at all)
        self.assertEqual(self.derive([]), "down")

    def test_healthy_with_config_poll_seconds_60(self):
        now = utc_now_iso()
        self.assertEqual(
            self.derive(
                [12345],
                lastHeartbeatAt=now,
                lastWsMessageAt=now,
                config={"pollSeconds": 60},
            ),
            "healthy",
        )

    def test_ws_stale_threshold_uses_config_poll_seconds(self):
        """WS stale threshold is pollSeconds*4 with min 60s."""
        # pollSeconds=10 → threshold=60s (min)
        state = {
            "monitors": {
                "BTCUSDT:long": {
                    "active": True,
                    "symbol": "BTCUSDT",
                    "runtime": {"state": "armed"},
                }
            },
            "config": {"pollSeconds": 10},
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": (datetime.now(timezone.utc) - timedelta(seconds=80)).isoformat(),
            "lastErrorAt": None,
            "lastRecoveredAt": None,
            "recoveryState": "none",
        }
        self.assertEqual(self.p._live_trigger_derive_status(state, [12345]), "degraded")

        # pollSeconds=120 → threshold=480s
        state["config"] = {"pollSeconds": 120}
        state["lastWsMessageAt"] = (datetime.now(timezone.utc) - timedelta(seconds=400)).isoformat()
        self.assertEqual(self.p._live_trigger_derive_status(state, [12345]), "healthy")

    def test_missing_heartbeat_after_grace_window_is_down(self):
        started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        self.assertEqual(
            self.derive([12345], startedAt=started, lastHeartbeatAt=None, lastWsMessageAt=utc_now_iso()),
            "down",
        )

    def test_missing_ws_after_grace_window_is_degraded(self):
        started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        self.assertEqual(
            self.derive([12345], startedAt=started, lastHeartbeatAt=utc_now_iso(), lastWsMessageAt=None),
            "degraded",
        )

    def test_missing_ws_without_expected_symbols_stays_healthy(self):
        started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        self.assertEqual(
            self.derive([12345], startedAt=started, lastHeartbeatAt=utc_now_iso(), lastWsMessageAt=None, monitors={}),
            "healthy",
        )

    def test_active_error_recovery_state_stays_degraded(self):
        now = utc_now_iso()
        self.assertEqual(
            self.derive([12345], lastHeartbeatAt=now, lastWsMessageAt=now, recoveryState="active_error"),
            "degraded",
        )


class LiveTriggerEngineStatusRecoveryFieldsTests(unittest.TestCase):
    """Test live_trigger_engine_status includes all health fields."""

    def setUp(self):
        self.p = PublicMarketData()

    def test_status_output_includes_lastErrorAt(self):
        state = {
            "monitors": {}, "recent": [], "metrics": {},
            "status": "running", "startedAt": "2026-04-23T10:00:00+00:00",
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": utc_now_iso(),
            "lastError": "old ws error",
            "lastErrorAt": "2026-04-23T09:50:00+00:00",
            "lastRecoveredAt": "2026-04-23T09:51:00+00:00",
            "recoveryState": "recovered",
            "config": {"pollSeconds": 15},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertIn("lastErrorAt", status)
        self.assertEqual(status["lastErrorAt"], "2026-04-23T09:50:00+00:00")

    def test_status_output_includes_lastRecoveredAt(self):
        state = {
            "monitors": {}, "recent": [], "metrics": {},
            "status": "running",
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": utc_now_iso(),
            "lastError": "ws timeout",
            "lastErrorAt": "2026-04-23T09:50:00+00:00",
            "lastRecoveredAt": "2026-04-23T09:51:00+00:00",
            "recoveryState": "recovered",
            "config": {},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertIn("lastRecoveredAt", status)
        self.assertEqual(status["lastRecoveredAt"], "2026-04-23T09:51:00+00:00")

    def test_status_output_includes_recoveryState(self):
        state = {
            "monitors": {}, "recent": [], "metrics": {},
            "status": "running",
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": utc_now_iso(),
            "lastError": "ws error",
            "lastErrorAt": "2026-04-23T09:50:00+00:00",
            "lastRecoveredAt": None,
            "recoveryState": "active_error",
            "config": {},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertIn("recoveryState", status)
        self.assertEqual(status["recoveryState"], "active_error")

    def test_status_output_includes_derivedStatus(self):
        state = {
            "monitors": {}, "recent": [], "metrics": {},
            "status": "running",
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": utc_now_iso(),
            "lastError": "old error",
            "lastErrorAt": "2026-04-23T09:50:00+00:00",
            "lastRecoveredAt": "2026-04-23T09:51:00+00:00",
            "recoveryState": "recovered",
            "config": {"pollSeconds": 15},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertIn("derivedStatus", status)
        self.assertEqual(status["derivedStatus"], "healthy")
        self.assertEqual(status["status"], "healthy")
        self.assertEqual(status["persistedStatus"], "running")

    def test_status_field_equals_derived_status_in_degraded_path(self):
        state = {
            "monitors": {"BTCUSDT:long": {"active": True, "symbol": "BTCUSDT", "runtime": {"state": "armed"}}},
            "recent": [], "metrics": {},
            "status": "running",
            "startedAt": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
            "lastError": "old error",
            "lastErrorAt": None,
            "lastRecoveredAt": None,
            "recoveryState": "none",
            "config": {"pollSeconds": 15},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertEqual(status["derivedStatus"], "degraded")
        self.assertEqual(status["status"], "degraded")
        self.assertEqual(status["persistedStatus"], "running")

    def test_status_output_includes_liveTrust(self):
        state = {
            "monitors": {"BTCUSDT:long": {"active": True, "symbol": "BTCUSDT", "runtime": {"state": "armed"}}}, "recent": [], "metrics": {},
            "status": "running",
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
            "lastError": "old error",
            "lastErrorAt": None,
            "lastRecoveredAt": None,
            "recoveryState": "none",
            "config": {"pollSeconds": 15},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        self.assertIn("liveTrust", status)
        self.assertFalse(status["liveTrust"]["trustLive"])
        self.assertEqual(status["liveTrust"]["reason"], "ws_stale")


class LiveTriggerWsStarvationBreakerTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.rows = [{"setupKey": "BTCUSDT:long", "symbol": "BTCUSDT"}]

    def test_repeated_ws_timeouts_with_expected_symbols_raise_starvation(self):
        with self.assertRaisesRegex(RuntimeError, "ws_message_gap:61s"):
            self.p.live_trigger_raise_on_starved_ws(
                self.rows,
                last_ws_recv_monotonic=100.0,
                poll_seconds=15,
                now_monotonic=161.0,
            )

    def test_zero_symbol_idle_loop_does_not_raise_starvation(self):
        self.p.live_trigger_raise_on_starved_ws(
            [],
            last_ws_recv_monotonic=100.0,
            poll_seconds=15,
            now_monotonic=1000.0,
        )

    def test_message_receipt_resets_starvation_timer(self):
        self.p.live_trigger_raise_on_starved_ws(
            self.rows,
            last_ws_recv_monotonic=200.0,
            poll_seconds=15,
            now_monotonic=259.0,
        )
        with self.assertRaisesRegex(RuntimeError, "ws_message_gap:61s"):
            self.p.live_trigger_raise_on_starved_ws(
                self.rows,
                last_ws_recv_monotonic=200.0,
                poll_seconds=15,
                now_monotonic=261.0,
            )

    def test_starvation_helper_uses_monotonic_default_source(self):
        with patch.object(pmd.time, "monotonic", return_value=261.0), \
             patch.object(pmd.time, "time", return_value=200.0):
            with self.assertRaisesRegex(RuntimeError, "ws_message_gap:61s"):
                self.p.live_trigger_raise_on_starved_ws(
                    self.rows,
                    last_ws_recv_monotonic=200.0,
                    poll_seconds=15,
                )


class WatchdogWsHealthTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime.now(timezone.utc)
        self.base_state = {
            "status": "degraded",
            "startedAt": (self.now - timedelta(hours=1)).isoformat(),
            "lastHeartbeatAt": self.now.isoformat(),
            "lastWsMessageAt": self.now.isoformat(),
            "config": {"pollSeconds": 15},
            "monitors": {
                "BTCUSDT:long": {
                    "active": True,
                    "symbol": "BTCUSDT",
                    "runtime": {"state": "armed"},
                }
            },
        }

    def _run_watchdog(self, state, watchdog_state=None):
        events = []
        saved_states = []
        watchdog_state = dict(watchdog_state or {})

        def fake_load_watchdog_state():
            return dict(watchdog_state)

        def fake_save_watchdog_state(payload):
            watchdog_state.clear()
            watchdog_state.update(payload)
            saved_states.append(dict(payload))

        with patch.object(ltw, "read_engine_pid", return_value=12345), \
             patch.object(ltw, "find_engine_pids", return_value=[12345]), \
             patch.object(ltw, "is_process_alive", return_value=True), \
             patch.object(ltw, "load_state", return_value=state), \
             patch.object(ltw, "load_watchdog_state", side_effect=fake_load_watchdog_state), \
             patch.object(ltw, "save_watchdog_state", side_effect=fake_save_watchdog_state), \
             patch.object(ltw, "stop_engine", side_effect=lambda pid: events.append(("stop", pid))), \
             patch.object(ltw, "start_engine", side_effect=lambda reason="manual": events.append(("start", reason)) or True), \
             patch.object(ltw, "log", side_effect=lambda msg: events.append(("log", msg))):
            result = ltw.check_and_restart()
        return result, events, watchdog_state, saved_states

    def test_watchdog_does_not_restart_when_heartbeat_fresh_and_ws_fresh(self):
        result, events, watchdog_state, _ = self._run_watchdog(dict(self.base_state))
        self.assertTrue(result)
        self.assertFalse(any(kind == "stop" for kind, _ in events))
        self.assertFalse(any(kind == "start" for kind, _ in events))
        self.assertEqual(watchdog_state.get("wsStaleObservedAt"), None)

    def test_watchdog_restarts_when_ws_stale_persists_beyond_window(self):
        state = dict(self.base_state)
        state["lastWsMessageAt"] = (self.now - timedelta(minutes=5)).isoformat()
        watchdog_state = {"wsStaleObservedAt": (self.now - timedelta(seconds=180)).isoformat()}
        result, events, _, _ = self._run_watchdog(state, watchdog_state)
        self.assertTrue(result)
        self.assertIn(("stop", 12345), events)
        self.assertIn(("start", "ws_stale"), events)

    def test_watchdog_skips_restart_on_ws_stale_when_expected_symbols_zero(self):
        state = dict(self.base_state)
        state["monitors"] = {}
        state["lastWsMessageAt"] = (self.now - timedelta(minutes=5)).isoformat()
        result, events, watchdog_state, _ = self._run_watchdog(state)
        self.assertTrue(result)
        self.assertFalse(any(kind == "stop" for kind, _ in events))
        self.assertFalse(any(kind == "start" for kind, _ in events))
        self.assertEqual(watchdog_state.get("wsStaleObservedAt"), None)

    def test_watchdog_log_uses_degraded_wording_not_healthy(self):
        state = dict(self.base_state)
        state["lastWsMessageAt"] = (self.now - timedelta(minutes=5)).isoformat()
        result, events, _, _ = self._run_watchdog(state)
        self.assertTrue(result)
        logs = [message for kind, message in events if kind == "log"]
        self.assertTrue(any("Engine alive but degraded" in message for message in logs))
        self.assertFalse(any("Engine alive and healthy" in message for message in logs))


class FallbackPollRecoveryTests(unittest.TestCase):
    """Test that fallback poll success transitions recovery state."""

    def setUp(self):
        self.p = PublicMarketData()
        self.p.save_live_trigger_state = lambda state: None
        self.p.live_trigger_tracking_rows = lambda state, targets: []
        self.p.live_trigger_targets = lambda **kwargs: []

    def test_fallback_poll_returns_true_when_no_targets(self):
        state = {"monitors": {}, "metrics": {}}
        result = self._call_fallback_poll(state, targets=[], trigger_timeframe="5m",
                                           context_timeframe="15m", poll_seconds=15, cooldown_seconds=1800)
        self.assertTrue(result)

    def _call_fallback_poll(self, state, targets, trigger_timeframe, context_timeframe, poll_seconds, cooldown_seconds):
        return self.p._live_trigger_fallback_poll(state, targets, trigger_timeframe, context_timeframe, poll_seconds, cooldown_seconds)


class LastErrorEvidencePreservationTests(unittest.TestCase):
    """Test that lastError survives as evidence even after recovery."""

    def setUp(self):
        self.p = PublicMarketData()

    def test_lastError_preserved_after_recovery_in_derived_healthy(self):
        """When derivedStatus=healthy, lastError still appears as historical evidence."""
        now = utc_now_iso()
        state = {
            "monitors": {}, "recent": [], "metrics": {},
            "status": "running",
            "lastHeartbeatAt": now,
            "lastWsMessageAt": now,
            "lastError": "WebSocket 1011 keepalive ping timeout",
            "lastErrorAt": (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat(),
            "lastRecoveredAt": (datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(),
            "recoveryState": "recovered",
            "config": {"pollSeconds": 15},
        }
        with patch.object(self.p, "load_live_trigger_state", return_value=state):
            status = self.p.live_trigger_engine_status()
        # lastError must still be present as evidence, not cleared
        self.assertEqual(status["lastError"], "WebSocket 1011 keepalive ping timeout")
        self.assertEqual(status["derivedStatus"], "healthy")


class RuntimeAliasCleanupTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_save_live_trigger_state_strips_runtime_alias_fields(self):
        captured = {}

        def fake_save(path, payload):
            captured["payload"] = payload

        state = {
            "monitors": {
                "TESTUSDT:long": {
                    "setupKey": "TESTUSDT:long",
                    "symbol": "TESTUSDT",
                    "side": "long",
                    "state": "invalidated",
                    "inactiveReason": "TARGET_REMOVED",
                    "lifecycleState": "actionable",
                    "readinessState": "retest_ready",
                    "runtime": {
                        "state": "inactive",
                        "reasonCode": "TARGET_REMOVED",
                        "eventType": "INVALIDATION",
                        "changedAt": utc_now_iso(),
                    },
                    "lifecycle": {
                        "state": "invalidated",
                        "changedAt": utc_now_iso(),
                    },
                    "readiness": {
                        "state": "pullback_standby",
                        "changedAt": utc_now_iso(),
                        "readinessVersion": "rv-1",
                    },
                }
            }
        }
        with patch.object(pmd, "save_json_file", side_effect=fake_save):
            self.p.save_live_trigger_state(state)

        monitor = captured["payload"]["monitors"]["TESTUSDT:long"]
        self.assertNotIn("state", monitor)
        self.assertNotIn("inactiveReason", monitor)
        self.assertNotIn("lifecycleState", monitor)
        self.assertNotIn("readinessState", monitor)
        self.assertEqual(monitor["lifecycle"]["state"], "invalidated")
        self.assertEqual(monitor["readiness"]["state"], "pullback_standby")

    def test_monitor_readiness_state_prefers_nested_then_falls_back_to_alias(self):
        self.assertEqual(
            self.p.monitor_readiness_state({"readiness": {"state": "pullback_standby"}, "readinessState": "retest_ready"}),
            "pullback_standby",
        )
        self.assertEqual(
            self.p.monitor_readiness_state({"readinessState": "retest_ready"}),
            "retest_ready",
        )

    def test_cleanup_script_rewrites_fixture_and_creates_backup(self):
        script_path = "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts/cleanup_live_monitor_aliases.py"
        with TemporaryDirectory() as td:
            state_path = Path(td) / "live_trigger_engine.json"
            backup_path = Path(td) / "live_trigger_engine.backup.json"
            state_path.write_text(json.dumps({
                "monitors": {
                    "TESTUSDT:long": {
                        "setupKey": "TESTUSDT:long",
                        "symbol": "TESTUSDT",
                        "side": "long",
                        "runtime": {"state": "inactive", "reasonCode": "TARGET_REMOVED", "eventType": "INVALIDATION", "changedAt": utc_now_iso()},
                        "lifecycleState": "actionable",
                        "readinessState": "retest_ready",
                        "readinessVersion": "rv-1"
                    }
                }
            }))

            completed = subprocess.run(
                ["python3", script_path, "--state-path", str(state_path), "--backup-path", str(backup_path)],
                check=True,
                capture_output=True,
                text=True,
            )
            result = json.loads(completed.stdout)
            self.assertEqual(result["monitorCount"], 1)
            self.assertTrue(backup_path.exists())

            payload = json.loads(state_path.read_text())
            monitor = payload["monitors"]["TESTUSDT:long"]
            self.assertNotIn("lifecycleState", monitor)
            self.assertNotIn("readinessState", monitor)
            self.assertEqual(monitor["lifecycle"]["state"], "actionable")
            self.assertEqual(monitor["readiness"]["state"], "retest_ready")


class LiveTriggerStartupStateTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_prepare_running_state_resets_ws_and_error_timestamps(self):
        previous = {
            "status": "degraded",
            "startedAt": "2026-04-23T10:00:00+00:00",
            "lastHeartbeatAt": "2026-04-23T10:01:00+00:00",
            "lastWsMessageAt": "2026-04-23T10:02:00+00:00",
            "lastError": "no close frame received or sent",
            "lastErrorAt": "2026-04-23T10:03:00+00:00",
            "lastRecoveredAt": "2026-04-23T10:04:00+00:00",
            "recoveryState": "recovered",
            "metrics": {"wsSessionsStarted": 10},
        }
        prepared = self.p.live_trigger_prepare_running_state(
            previous,
            channel="telegram",
            target="851120836",
            trigger_timeframe="5m",
            context_timeframe="15m",
            poll_seconds=15,
            cooldown_seconds=1800,
        )
        self.assertEqual(prepared["status"], "running")
        self.assertIsNone(prepared["lastHeartbeatAt"])
        self.assertIsNone(prepared["lastWsMessageAt"])
        self.assertIsNone(prepared["lastErrorAt"])
        self.assertIsNone(prepared["lastRecoveredAt"])
        self.assertEqual(prepared["recoveryState"], "none")
        self.assertEqual(prepared["lastError"], "no close frame received or sent")
        self.assertEqual(prepared["metrics"], {"wsSessionsStarted": 10})


class LiveTriggerTrustGateTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_live_trigger_live_trust_reports_ws_stale_reason(self):
        state = {
            "status": "running",
            "startedAt": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat(),
            "monitors": {"BTCUSDT:long": {"active": True, "symbol": "BTCUSDT", "runtime": {"state": "armed"}}},
            "lastHeartbeatAt": utc_now_iso(),
            "lastWsMessageAt": (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat(),
            "lastErrorAt": None,
            "lastRecoveredAt": None,
            "recoveryState": "none",
            "config": {"pollSeconds": 15},
        }
        trust = self.p.live_trigger_live_trust(state, [12345])
        self.assertFalse(trust["trustLive"])
        self.assertEqual(trust["derivedStatus"], "degraded")
        self.assertEqual(trust["reason"], "ws_stale")


if __name__ == "__main__":
    unittest.main()
