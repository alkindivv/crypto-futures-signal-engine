import os
import sys
import unittest
import subprocess
from copy import deepcopy
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")
from public_market_data import PublicMarketData, utc_now_iso


class RuntimeConfirmObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.p.save_live_trigger_state = lambda state: None
        self.p.openclaw_node_path = "/abs/test/node"
        self.p.openclaw_node_source = "test:node"
        self.p.openclaw_script_path = "/abs/test/openclaw.mjs"
        self.p.openclaw_script_source = "test:script"
        self.p.openclaw_transport_source = "test:script | test:node"
        self.p.openclaw_transport_checked_at = "2026-04-16T14:00:00+00:00"
        self.p.openclaw_transport_error = None

    def base_state(self, monitor=None):
        row = monitor or self.base_monitor()
        return {"monitors": {row["setupKey"]: row}, "recent": [], "metrics": {}}

    def base_monitor(self):
        return {
            "setupKey": "TESTUSDT:long",
            "symbol": "TESTUSDT",
            "side": "long",
            "grade": "A",
            "setupVersion": "v1",
            "readinessVersion": "r1",
            "runtime": {"state": "zone_touched", "changedAt": "2026-04-16T00:00:00+00:00"},
            "actionableAlert": {
                "entryStyle": "long on reclaim",
                "entryZone": {"low": 10.0, "high": 12.0},
                "stopLoss": 9.0,
                "takeProfit": {"tp1": 13.0, "tp2": 14.0, "tp3": 15.0},
                "trigger": "Eksekusi hanya jika hold area entry tetap kuat dan reclaim terjaga",
                "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced."},
            },
            "lastQualityGate": {"ok": True, "contextVerdict": "15m context masih bullish/di atas mid-zone", "deliveryMode": "ws"},
        }

    def confirm_candle(self):
        return {
            "openTime": 1,
            "closeTime": 2,
            "open": 11.4,
            "high": 12.4,
            "low": 10.4,
            "close": 12.2,
            "closed": True,
        }

    def structure_fail_candle(self):
        return {
            "openTime": 1,
            "closeTime": 2,
            "open": 11.5,
            "high": 12.2,
            "low": 10.7,
            "close": 11.0,
            "closed": True,
        }

    def context_ok(self):
        return {
            "openTime": 10,
            "closeTime": 20,
            "open": 11.6,
            "high": 12.5,
            "low": 11.2,
            "close": 12.3,
            "closed": True,
        }

    def context_fail(self):
        return {
            "openTime": 10,
            "closeTime": 20,
            "open": 11.6,
            "high": 11.8,
            "low": 9.5,
            "close": 9.8,
            "closed": True,
        }

    def test_confirm_success_delivery_success_explicit_node_and_script(self):
        monitor = self.base_monitor()
        state = self.base_state(monitor)
        call_args = {}
        evaluated = self.p.evaluate_live_trigger_candle(monitor, self.confirm_candle(), self.context_ok(), "15m")
        self.assertTrue(evaluated["confirmed"])
        self.p.live_trigger_transition(state, monitor["setupKey"], "confirmed", {
            "triggeredAt": "2026-04-16T14:00:00+00:00",
            "lastTriggeredPrice": evaluated.get("price"),
            "lastTriggerCandle": evaluated.get("candle"),
            "lastContextCandle": evaluated.get("contextCandle"),
            "lastQualityGate": monitor["lastQualityGate"],
            "contextVerdict": evaluated.get("contextVerdict"),
            "triggerReason": evaluated.get("reason"),
        }, reason_code="TRIGGER_CONFIRMED", event_type="TRIGGER_CANDLE_CLOSE", actor="test")
        self.p.live_trigger_record_evaluation(monitor, phase="trigger_eval", outcome="confirmed", reason=evaluated.get("reason"), trigger_candle=evaluated.get("candle"), context_outcome=evaluated.get("contextOutcome"))
        self.p.live_trigger_record_evaluation(monitor, phase="delivery")

        def fake_run(args, expect_json=False):
            call_args["args"] = args
            return {"ok": True}

        self.p.run_command = fake_run
        delivered = self.p.send_live_trigger_notification(state, monitor, channel="telegram", target="1", trigger_candle=evaluated.get("candle"), trigger_timeframe="5m", context_timeframe="15m", delivery_mode="ws")
        self.assertTrue(delivered["sent"])
        self.assertEqual(call_args["args"][:4], ["/abs/test/node", "/abs/test/openclaw.mjs", "message", "send"])
        self.assertEqual(monitor["notificationStatus"], "sent")
        self.assertIsNotNone(monitor.get("notificationSentAt"))
        transport = monitor.get("lastDeliveryTransport") or {}
        self.assertEqual(transport.get("nodeBinaryPath"), "/abs/test/node")
        self.assertEqual(transport.get("scriptPath"), "/abs/test/openclaw.mjs")
        self.assertEqual(transport.get("resolverSource"), "test:script | test:node")
        self.assertEqual((((monitor.get("deliveryLedger") or {}).get(delivered["deliveryKey"])) or {}).get("status"), "sent")
        self.assertEqual((((state.get("recent") or [])[-1]).get("toState")), "sent")
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "confirmed")
        self.assertEqual(monitor["evaluation"]["lastEvaluationPhase"], "delivery")
        self.assertIsNotNone(monitor.get("triggeredAt"))

    def test_confirm_success_delivery_fail_transport_unavailable(self):
        monitor = self.base_monitor()
        state = self.base_state(monitor)
        evaluated = self.p.evaluate_live_trigger_candle(monitor, self.confirm_candle(), self.context_ok(), "15m")
        self.assertTrue(evaluated["confirmed"])
        self.p.live_trigger_transition(state, monitor["setupKey"], "confirmed", {
            "triggeredAt": "2026-04-16T14:00:00+00:00",
            "lastTriggeredPrice": evaluated.get("price"),
            "lastTriggerCandle": evaluated.get("candle"),
            "lastContextCandle": evaluated.get("contextCandle"),
            "lastQualityGate": monitor["lastQualityGate"],
            "contextVerdict": evaluated.get("contextVerdict"),
            "triggerReason": evaluated.get("reason"),
        }, reason_code="TRIGGER_CONFIRMED", event_type="TRIGGER_CANDLE_CLOSE", actor="test")
        self.p.live_trigger_record_evaluation(monitor, phase="trigger_eval", outcome="confirmed", reason=evaluated.get("reason"), trigger_candle=evaluated.get("candle"), context_outcome=evaluated.get("contextOutcome"))
        self.p.live_trigger_record_evaluation(monitor, phase="delivery")
        self.p.openclaw_node_path = None
        self.p.openclaw_transport_error = "node binary not found for openclaw transport"
        self.p.resolve_openclaw_transport = lambda refresh=False: None
        delivered = self.p.send_live_trigger_notification(state, monitor, channel="telegram", target="1", trigger_candle=evaluated.get("candle"), trigger_timeframe="5m", context_timeframe="15m", delivery_mode="ws")
        self.assertFalse(delivered["sent"])
        self.assertEqual(delivered["reason"], "transport_unavailable")
        self.assertEqual(monitor["notificationStatus"], "failed")
        self.assertEqual(monitor["notificationError"], "node binary not found for openclaw transport")
        self.assertEqual((((monitor.get("deliveryLedger") or {}).get(delivered["deliveryKey"])) or {}).get("status"), "failed")
        self.assertEqual((((state.get("recent") or [])[-1]).get("toState")), "failed")
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "confirmed")
        self.assertEqual(monitor["evaluation"]["lastEvaluationPhase"], "delivery")
        self.assertIsNotNone(monitor.get("triggeredAt"))
        self.assertEqual(self.p.runtime_state(monitor), "confirmed")

    def test_confirm_success_delivery_send_failure_preserves_triggered_at_and_transport_error(self):
        monitor = self.base_monitor()
        state = self.base_state(monitor)
        evaluated = self.p.evaluate_live_trigger_candle(monitor, self.confirm_candle(), self.context_ok(), "15m")
        self.assertTrue(evaluated["confirmed"])
        self.p.live_trigger_transition(state, monitor["setupKey"], "confirmed", {
            "triggeredAt": "2026-04-16T14:00:00+00:00",
            "lastTriggeredPrice": evaluated.get("price"),
            "lastTriggerCandle": evaluated.get("candle"),
            "lastContextCandle": evaluated.get("contextCandle"),
            "lastQualityGate": monitor["lastQualityGate"],
            "contextVerdict": evaluated.get("contextVerdict"),
            "triggerReason": evaluated.get("reason"),
        }, reason_code="TRIGGER_CONFIRMED", event_type="TRIGGER_CANDLE_CLOSE", actor="test")
        self.p.live_trigger_record_evaluation(monitor, phase="trigger_eval", outcome="confirmed", reason=evaluated.get("reason"), trigger_candle=evaluated.get("candle"), context_outcome=evaluated.get("contextOutcome"))
        self.p.live_trigger_record_evaluation(monitor, phase="delivery")

        def fake_run(args, expect_json=False):
            raise RuntimeError("simulated transport failure")

        self.p.run_command = fake_run
        delivered = self.p.send_live_trigger_notification(state, monitor, channel="telegram", target="1", trigger_candle=evaluated.get("candle"), trigger_timeframe="5m", context_timeframe="15m", delivery_mode="ws")
        self.assertFalse(delivered["sent"])
        self.assertEqual(delivered["reason"], "send_failed")
        self.assertEqual(monitor["notificationStatus"], "failed")
        self.assertEqual(monitor["notificationError"], "simulated transport failure")
        self.assertIsNotNone(monitor.get("triggeredAt"))
        transport = monitor.get("lastDeliveryTransport") or {}
        self.assertEqual(transport.get("nodeBinaryPath"), "/abs/test/node")
        self.assertEqual(transport.get("scriptPath"), "/abs/test/openclaw.mjs")
        self.assertEqual(transport.get("resolverSource"), "test:script | test:node")
        self.assertEqual(transport.get("error"), "simulated transport failure")
        self.assertEqual(((((monitor.get("deliveryLedger") or {}).get(delivered["deliveryKey"])) or {}).get("transport")) or {}, transport)

    def test_resolve_openclaw_node_path_success(self):
        with TemporaryDirectory() as td:
            node_path = os.path.join(td, "node")
            with open(node_path, "w", encoding="utf-8") as handle:
                handle.write("#!/bin/sh\nexit 0\n")
            os.chmod(node_path, 0o755)
            with patch.object(self.p, "openclaw_node_candidates", return_value=[{"source": "unit:node", "path": node_path}]):
                resolved = self.p.resolve_openclaw_node_binary(refresh=True)
        self.assertEqual(resolved, node_path)
        self.assertEqual(self.p.openclaw_node_path, node_path)
        self.assertEqual(self.p.openclaw_node_source, "unit:node")
        self.assertIn("unit:node", self.p.openclaw_transport_source)
        self.assertIsNone(self.p.openclaw_transport_error)

    def test_resolve_openclaw_script_path_success(self):
        self.p.openclaw_node_path = None
        self.p.openclaw_node_source = None
        with TemporaryDirectory() as td:
            script_path = os.path.join(td, "openclaw.mjs")
            with open(script_path, "w", encoding="utf-8") as handle:
                handle.write("#!/usr/bin/env node\nconsole.log('ok')\n")
            os.chmod(script_path, 0o755)
            with patch.object(self.p, "openclaw_script_candidates", return_value=[{"source": "unit:script", "path": script_path}]):
                resolved = self.p.resolve_openclaw_script_path(refresh=True)
        self.assertEqual(resolved, script_path)
        self.assertEqual(self.p.openclaw_script_path, script_path)
        self.assertEqual(self.p.openclaw_script_source, "unit:script")
        self.assertEqual(self.p.openclaw_transport_source, "unit:script")
        self.assertIsNone(self.p.openclaw_transport_error)

    def test_resolve_openclaw_node_path_fail(self):
        with patch.object(self.p, "openclaw_node_candidates", return_value=[{"source": "unit:node", "path": "/no/such/node"}]):
            resolved = self.p.resolve_openclaw_node_binary(refresh=True)
        self.assertIsNone(resolved)
        self.assertIsNone(self.p.openclaw_node_path)
        self.assertIsNone(self.p.openclaw_node_source)
        self.assertIn("node binary not found for openclaw transport", self.p.openclaw_transport_error)

    def test_resolve_openclaw_script_path_fail(self):
        with patch.object(self.p, "openclaw_script_candidates", return_value=[{"source": "unit:script", "path": "/no/such/openclaw.mjs"}]):
            resolved = self.p.resolve_openclaw_script_path(refresh=True)
        self.assertIsNone(resolved)
        self.assertIsNone(self.p.openclaw_script_path)
        self.assertIsNone(self.p.openclaw_script_source)
        self.assertIn("openclaw script not found", self.p.openclaw_transport_error)

    def test_run_command_expect_json_surfaces_raw_output(self):
        completed = subprocess.CompletedProcess(args=["dummy"], returncode=0, stdout="✅ Sent via Telegram. Message ID: 123\n", stderr="")
        with patch("public_market_data.subprocess.run", return_value=completed):
            with self.assertRaises(RuntimeError) as ctx:
                self.p.run_command(["dummy"], expect_json=True)
        self.assertIn("command returned non-JSON output", str(ctx.exception))
        self.assertIn("Sent via Telegram", str(ctx.exception))

    def test_final_confirmation_create_sets_all_fields(self):
        now = utc_now_iso()
        monitor = {
            "setupKey": "BTCUSDT:long",
            "setupVersion": "abc123",
            "readinessVersion": "def456",
            "triggerReason": "entry_reclaim",
            "lastTriggeredPrice": 50000.0,
            "evaluation": {
                "lastTriggerCandidateCandle": {"close": 50100.0, "closeTime": 1234567890000}
            },
            "lastDeliveryTransport": {"launchMode": "explicit_node_script"},
        }
        self.p.final_confirmation_create(monitor)
        fc = monitor["finalConfirmation"]
        self.assertEqual(fc["state"], "active")
        self.assertEqual(fc["setupVersion"], "abc123")
        self.assertEqual(fc["readinessVersion"], "def456")
        self.assertEqual(fc["triggerReason"], "entry_reclaim")
        self.assertEqual(fc["entryPriceEstimate"], 50100.0)
        self.assertEqual(fc["sourceRuntimeState"], "confirmed")
        self.assertEqual(fc["deliveryStatus"], "pending")
        self.assertEqual(fc["deliveryStatusAt"], fc["confirmedAt"])
        self.assertIsNone(fc["deliveryError"])
        self.assertIsNone(fc["notificationSentAt"])
        self.assertEqual(fc["transportMetadata"], {"launchMode": "explicit_node_script"})
        self.assertIsNone(fc["expiresAt"])
        self.assertEqual(fc["expired"], False)
        self.assertIsNone(fc["resetReason"])
        self.assertIsNone(fc["resetAt"])

    def test_final_confirmation_update_delivery_sent(self):
        monitor = {"finalConfirmation": {
            "state": "active", "deliveryStatus": "pending",
            "setupVersion": "abc", "readinessVersion": "def"
        }}
        self.p.final_confirmation_update_delivery(
            monitor, "sent",
            notification_sent_at="2026-04-16T12:00:00+00:00",
            transport={"launchMode": "explicit_node_script"},
        )
        fc = monitor["finalConfirmation"]
        self.assertEqual(fc["deliveryStatus"], "sent")
        self.assertEqual(fc["notificationSentAt"], "2026-04-16T12:00:00+00:00")
        self.assertEqual(fc["deliveryStatusAt"], "2026-04-16T12:00:00+00:00")
        self.assertEqual(fc["transportMetadata"], {"launchMode": "explicit_node_script"})

    def test_final_confirmation_update_delivery_failed(self):
        monitor = {"finalConfirmation": {
            "state": "active", "deliveryStatus": "pending",
            "setupVersion": "abc", "readinessVersion": "def"
        }}
        self.p.final_confirmation_update_delivery(monitor, "failed", error="node not found")
        fc = monitor["finalConfirmation"]
        self.assertEqual(fc["deliveryStatus"], "failed")
        self.assertIsNotNone(fc["deliveryError"])

    def test_final_confirmation_update_skips_inactive(self):
        monitor = {"finalConfirmation": {"state": "reset", "deliveryStatus": "pending"}}
        self.p.final_confirmation_update_delivery(monitor, "sent")
        self.assertEqual(monitor["finalConfirmation"]["deliveryStatus"], "pending")

    def test_final_confirmation_reset_marks_reset_state(self):
        monitor = {"finalConfirmation": {
            "state": "active", "deliveryStatus": "pending",
            "setupVersion": "abc", "readinessVersion": "def"
        }}
        with patch("public_market_data.utc_now_iso", return_value="2026-04-22T15:14:19+00:00"):
            self.p.final_confirmation_reset(monitor, "setup_version_changed")
        fc = monitor["finalConfirmation"]
        self.assertEqual(fc["state"], "reset")
        self.assertEqual(fc["resetReason"], "setup_version_changed")
        self.assertEqual(fc["resetAt"], "2026-04-22T15:14:19+00:00")
        # calling reset again is idempotent
        self.p.final_confirmation_reset(monitor, "invalidated")
        self.assertEqual(fc["resetReason"], "setup_version_changed")
        self.assertEqual(fc["resetAt"], "2026-04-22T15:14:19+00:00")

    def test_final_confirmation_version_change_resets(self):
        monitor = {
            "finalConfirmation": {
                "state": "active",
                "setupVersion": "abc", "readinessVersion": "def",
                "deliveryStatus": "sent",
            },
            "setupVersion": "xyz",
            "readinessVersion": "def",
        }
        changed = self.p._final_confirmation_reset_on_version_change(monitor, "setup_version_changed")
        self.assertTrue(changed)
        self.assertEqual(monitor["finalConfirmation"]["state"], "reset")
        self.assertEqual(monitor["finalConfirmation"]["resetReason"], "setup_version_changed")
        self.assertIsNotNone(monitor["finalConfirmation"].get("resetAt"))

    def test_final_confirmation_version_unchanged_no_reset(self):
        monitor = {
            "finalConfirmation": {
                "state": "active",
                "setupVersion": "abc", "readinessVersion": "def",
            },
            "setupVersion": "abc",
            "readinessVersion": "def",
        }
        changed = self.p._final_confirmation_reset_on_version_change(monitor, "setup_version_changed")
        self.assertFalse(changed)
        self.assertEqual(monitor["finalConfirmation"]["state"], "active")

    def test_final_confirmation_maybe_create_creates_when_none(self):
        monitor = {"setupVersion": "v1", "readinessVersion": "r1"}
        self.p.final_confirmation_maybe_create_or_keep(monitor)
        self.assertEqual(monitor["finalConfirmation"]["state"], "active")
        self.assertEqual(monitor["finalConfirmation"]["setupVersion"], "v1")

    def test_final_confirmation_maybe_create_skips_when_active(self):
        monitor = {
            "setupVersion": "v1", "readinessVersion": "r1",
            "finalConfirmation": {"state": "active", "setupVersion": "v1", "readinessVersion": "r1",
                                  "confirmedAt": "2026-04-16T10:00:00+00:00",
                                  "triggerReason": "original", "entryPriceEstimate": 100.0,
                                  "sourceRuntimeState": "confirmed",
                                  "deliveryStatus": "pending", "deliveryStatusAt": "2026-04-16T10:00:00+00:00",
                                  "deliveryError": None, "notificationSentAt": None,
                                  "transportMetadata": {}, "expiresAt": None, "expired": False, "resetReason": None}
        }
        self.p.final_confirmation_maybe_create_or_keep(monitor)
        self.assertEqual(monitor["finalConfirmation"]["triggerReason"], "original")  # not overwritten

    def test_final_confirmation_maybe_create_creates_when_reset(self):
        monitor = {
            "setupVersion": "v2", "readinessVersion": "r2",
            "finalConfirmation": {"state": "reset", "resetReason": "old_reason"}
        }
        self.p.final_confirmation_maybe_create_or_keep(monitor)
        self.assertEqual(monitor["finalConfirmation"]["state"], "active")
        self.assertEqual(monitor["finalConfirmation"]["setupVersion"], "v2")

    def test_zone_touched_structure_fail(self):
        monitor = self.base_monitor()
        result = self.p.evaluate_live_trigger_candle(monitor, self.structure_fail_candle(), self.context_ok(), "15m")
        self.assertFalse(result["confirmed"])
        self.assertEqual(result["outcome"], "structure_fail")
        self.assertIn("close tidak bullish", result["reason"])

    def test_zone_touched_context_fail(self):
        monitor = self.base_monitor()
        result = self.p.evaluate_live_trigger_candle(monitor, self.confirm_candle(), self.context_fail(), "15m")
        self.assertFalse(result["confirmed"])
        self.assertEqual(result["outcome"], "context_fail")
        self.assertIn("context", result["reason"])

    def test_zone_touched_quality_gate_blocked(self):
        monitor = self.base_monitor()
        blocked = {"ok": False, "hardBlockReasons": ["depth terlalu tipis untuk trigger execution"], "contextVerdict": "15m context masih bullish/di atas mid-zone"}
        monitor["lastQualityGate"] = blocked
        monitor["lastBlockedReason"] = "; ".join(blocked["hardBlockReasons"])
        self.p.live_trigger_record_evaluation(monitor, phase="quality_gate", outcome="quality_gate_blocked", reason=monitor["lastBlockedReason"], context_outcome={"ok": False, "reason": blocked["contextVerdict"]})
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "quality_gate_blocked")
        self.assertEqual(monitor["lastBlockedReason"], "depth terlalu tipis untuk trigger execution")

    def test_invalidation_after_confirm(self):
        monitor = self.base_monitor()
        state = self.base_state(monitor)
        monitor["triggeredAt"] = "2026-04-16T14:00:00+00:00"
        monitor["triggerReason"] = "confirmed"
        monitor["notificationStatus"] = "failed"
        monitor["runtime"] = {"state": "confirmed"}
        self.p.live_trigger_record_evaluation(monitor, phase="trigger_eval", outcome="confirmed", reason="confirmed")
        changed = self.p.apply_invalidation(state, monitor["setupKey"], "CONTEXT_BROKEN", actor="test")
        self.assertTrue(changed)
        self.assertIsNone(monitor.get("triggeredAt"))
        self.assertIsNone(monitor.get("notificationStatus"))
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "confirmed")
        self.assertEqual(monitor["evaluation"]["lastResetReason"], "CONTEXT_BROKEN")

    def test_setup_version_change_after_confirm(self):
        prev = self.base_monitor()
        prev["triggeredAt"] = "2026-04-16T14:00:00+00:00"
        prev["triggerReason"] = "confirmed"
        prev["notificationStatus"] = "sent"
        prev["runtime"] = {"state": "confirmed"}
        self.p.live_trigger_record_evaluation(prev, phase="trigger_eval", outcome="confirmed", reason="confirmed")
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}
        targets = [{
            "setupKey": prev["setupKey"],
            "symbol": prev["symbol"],
            "side": prev["side"],
            "setupVersion": "v2",
            "readinessVersion": "r1",
            "grade": "A",
            "riskFlags": [],
            "managementGuidance": [],
            "actionableAlert": deepcopy(prev["actionableAlert"]),
            "state": "actionable",
            "lifecycleState": "actionable",
            "currentStatus": "ready_on_retest",
            "readinessState": "retest_ready",
            "monitorLane": "primary",
        }]
        self.p.reconcile_live_trigger_targets(state, targets, cooldown_seconds=1800)
        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "inactive")
        self.assertIsNone(monitor.get("triggeredAt"))
        self.assertIsNone(monitor.get("notificationStatus"))
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "confirmed")
        self.assertEqual(monitor["evaluation"]["lastResetReason"], "SETUP_CHANGED_INACTIVE")


if __name__ == "__main__":
    unittest.main()
