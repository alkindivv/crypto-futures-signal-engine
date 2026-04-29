import sys
import unittest
from copy import deepcopy

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData
from telegram_execution_cards import TelegramExecutionCardFormatter


class ConfirmedLanePreservationTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.p.save_live_trigger_state = lambda state: None

    def confirmed_monitor(self):
        return {
            "setupKey": "XRPUSDT:long",
            "symbol": "XRPUSDT",
            "side": "long",
            "grade": "A+",
            "setupVersion": "sv-confirmed",
            "readinessVersion": "rv-confirmed",
            "runtime": {"state": "confirmed", "changedAt": "2026-04-22T11:55:01+00:00"},
            "triggeredAt": "2026-04-22T11:55:01+00:00",
            "notificationStatus": "sent",
            "actionableAlert": {
                "currentPrice": 1.4536,
                "entryStyle": "long on reclaim",
                "entryZone": {"low": 1.4490, "high": 1.4535},
                "stopLoss": 1.4450,
                "takeProfit": {"tp1": 1.4594, "tp2": 1.4629, "tp3": 1.4688},
                "trigger": "hold zone",
                "invalidation": "lose zone",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "moderate crowding"},
                "currentStatus": "ready_on_retest",
            },
            "finalConfirmation": {
                "state": "active",
                "setupVersion": "sv-confirmed",
                "readinessVersion": "rv-confirmed",
                "confirmedAt": "2026-04-22T11:55:01+00:00",
                "triggerReason": "confirmed trigger",
                "entryPriceEstimate": 1.4536,
                "sourceRuntimeState": "confirmed",
                "deliveryStatus": "sent",
                "deliveryStatusAt": "2026-04-22T11:55:21+00:00",
                "deliveryError": None,
                "notificationSentAt": "2026-04-22T11:55:21+00:00",
                "transportMetadata": {},
                "expiresAt": None,
                "expired": False,
                "resetReason": None,
                "resetAt": None,
            },
            "evaluation": {"lastEvaluationOutcome": "confirmed"},
            "lifecycleState": "notified",
            "readinessState": "retest_ready",
            "currentStatus": "ready_on_retest",
        }

    def actionable_setup(self, setup_version="sv-confirmed", readiness_version="rv-new"):
        return {
            "setupKey": "XRPUSDT:long",
            "symbol": "XRPUSDT",
            "side": "long",
            "grade": "A+",
            "cleanScore": 99.0,
            "gradeScore": 99.0,
            "setupVersion": setup_version,
            "readinessVersion": readiness_version,
            "lifecycle": {"state": "notified", "changedAt": "2026-04-22T12:27:00+00:00"},
            "readiness": {"state": "retest_ready", "changedAt": "2026-04-22T12:27:00+00:00"},
            "actionableAlert": {
                "currentPrice": 1.4527,
                "entryStyle": "long on reclaim",
                "entryZone": {"low": 1.4492, "high": 1.4537},
                "stopLoss": 1.4450,
                "takeProfit": {"tp1": 1.4594, "tp2": 1.4629, "tp3": 1.4688},
                "trigger": "hold zone",
                "invalidation": "lose zone",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "moderate crowding"},
                "currentStatus": "ready_on_retest",
            },
            "riskFlags": [],
        }

    def reconcile_target(self, setup_version, readiness_version):
        return [{
            "setupKey": "XRPUSDT:long",
            "symbol": "XRPUSDT",
            "side": "long",
            "setupVersion": setup_version,
            "readinessVersion": readiness_version,
            "grade": "A+",
            "riskFlags": [],
            "managementGuidance": [],
            "actionableAlert": deepcopy(self.confirmed_monitor()["actionableAlert"]),
            "state": "notified",
            "lifecycleState": "notified",
            "currentStatus": "ready_on_retest",
            "readinessState": "retest_ready",
            "monitorLane": "primary",
        }]

    def standby_target(self, readiness_version="rv-standby", setup_version="sv-confirmed"):
        target = self.reconcile_target(setup_version, readiness_version)[0]
        target["currentStatus"] = "wait_pullback"
        target["readinessState"] = "pullback_standby"
        return [target]

    def inactive_monitor(self, *, top_readiness=None, nested_readiness=None, runtime_state="inactive"):
        monitor = self.confirmed_monitor()
        monitor["runtime"] = {"state": runtime_state, "changedAt": "2026-04-22T12:00:00+00:00"}
        monitor["finalConfirmation"]["state"] = "reset"
        monitor["finalConfirmation"]["resetReason"] = "test_fixture"
        monitor["finalConfirmation"]["resetAt"] = "2026-04-22T12:00:00+00:00"
        if top_readiness is None:
            monitor.pop("readinessState", None)
        else:
            monitor["readinessState"] = top_readiness
        if nested_readiness is None:
            monitor.pop("readiness", None)
        else:
            monitor["readiness"] = {"state": nested_readiness, "changedAt": "2026-04-22T12:00:00+00:00"}
        return monitor

    def recent_reason_codes(self, state):
        return [entry.get("reasonCode") for entry in state.get("recent", [])]

    def test_confirmed_readiness_drift_only_preserves_committed_runtime(self):
        prev = self.confirmed_monitor()
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.reconcile_target("sv-confirmed", "rv-drifted"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "confirmed")
        self.assertEqual(monitor["setupVersion"], "sv-confirmed")
        self.assertEqual(monitor["readinessVersion"], "rv-drifted")
        self.assertEqual(monitor["finalConfirmation"]["state"], "active")
        self.assertEqual(monitor["finalConfirmation"]["setupVersion"], "sv-confirmed")
        self.assertEqual(monitor["finalConfirmation"]["readinessVersion"], "rv-confirmed")

    def test_confirmed_setup_version_drift_resets_to_inactive_new_identity(self):
        prev = self.confirmed_monitor()
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.reconcile_target("sv-new", "rv-new"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "inactive")
        self.assertEqual(monitor["finalConfirmation"]["state"], "reset")
        self.assertEqual(monitor["finalConfirmation"]["resetReason"], "setup_version_changed")
        self.assertEqual(monitor["setupVersion"], "sv-new")
        self.assertEqual(monitor["readinessVersion"], "rv-new")

    def test_confirmed_lane_stays_live_after_readiness_drift_only(self):
        setup = self.actionable_setup(setup_version="sv-confirmed", readiness_version="rv-drifted")
        monitor = self.confirmed_monitor()
        monitor["readinessVersion"] = "rv-drifted"

        entry = self.p.notification_entry_from_setup("XRPUSDT:long", setup, {}, monitor=monitor)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["lane"], "LIVE")
        self.assertEqual(entry["setupVersion"], "sv-confirmed")
        self.assertEqual(entry["readinessVersion"], "rv-confirmed")
        self.assertEqual(entry["observedReadinessVersion"], "rv-drifted")

    def test_confirmed_lane_uses_final_confirmation_identity_when_not_actionable(self):
        setup = self.actionable_setup(setup_version="sv-watchlist-new", readiness_version="rv-watchlist-new")
        setup["readiness"] = {"state": "pullback_standby", "changedAt": "2026-04-22T12:27:00+00:00"}
        monitor = self.confirmed_monitor()

        entry = self.p.notification_entry_from_setup("XRPUSDT:long", setup, {}, monitor=monitor)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["lane"], "CONFIRMED")
        self.assertEqual(entry["setupVersion"], "sv-confirmed")
        self.assertEqual(entry["readinessVersion"], "rv-confirmed")
        self.assertEqual(entry["observedSetupVersion"], "sv-watchlist-new")

    def test_live_lane_demotes_to_confirmed_when_live_trust_blocked(self):
        setup = self.actionable_setup(setup_version="sv-confirmed", readiness_version="rv-drifted")
        monitor = self.confirmed_monitor()

        entry = self.p.notification_entry_from_setup(
            "XRPUSDT:long",
            setup,
            {},
            monitor=monitor,
            live_trust={"trustLive": False, "derivedStatus": "degraded", "reason": "ws_stale"},
        )
        self.assertIsNotNone(entry)
        self.assertEqual(entry["lane"], "CONFIRMED")
        self.assertTrue(entry["liveTrustBlocked"])
        self.assertFalse(entry["liveTrustAllowed"])
        self.assertEqual(entry["liveTrustReason"], "ws_stale")

    def test_soft_trigger_reset_context_broken_still_valid(self):
        monitor = self.confirmed_monitor()
        self.p._final_confirmation_reset_on_soft_trigger(monitor, "context_broken")
        self.assertEqual(monitor["finalConfirmation"]["state"], "reset")
        self.assertEqual(monitor["finalConfirmation"]["resetReason"], "context_broken")
        self.assertIsNotNone(monitor["finalConfirmation"]["resetAt"])

    def test_zone_reclaimed_reset_still_valid(self):
        monitor = self.confirmed_monitor()
        state = {"monitors": {monitor["setupKey"]: monitor}, "recent": [], "metrics": {}}
        changed = self.p.apply_invalidation(state, monitor["setupKey"], "ZONE_RECLAIMED", actor="test")
        self.assertTrue(changed)
        self.assertEqual(self.p.runtime_state(monitor), "inactive")
        self.assertEqual(monitor["finalConfirmation"]["state"], "reset")
        self.assertEqual(monitor["finalConfirmation"]["resetReason"], "ZONE_RECLAIMED")
        self.assertIsNotNone(monitor["finalConfirmation"]["resetAt"])

    def test_live_strict_card_metadata_prefers_final_confirmation_identity(self):
        setup = self.actionable_setup(setup_version="sv-watchlist-new", readiness_version="rv-watchlist-new")
        monitor = self.confirmed_monitor()
        event = self.p.entry_ready_build_event("XRPUSDT:long", setup, monitor, event_type="LIVE_STRICT")
        self.assertEqual(event.metadata["setupVersion"], "sv-confirmed")
        self.assertEqual(event.metadata["readinessVersion"], "rv-confirmed")

    def test_ready_card_identity_uses_latest_setup_identity(self):
        setup = self.actionable_setup(setup_version="sv-watchlist-new", readiness_version="rv-watchlist-new")
        monitor = self.confirmed_monitor()
        monitor["finalConfirmation"]["state"] = "reset"
        event = self.p.entry_ready_build_event("XRPUSDT:long", setup, monitor, event_type="READY_IN_ZONE")
        self.assertEqual(event.metadata["setupVersion"], "sv-watchlist-new")
        self.assertEqual(event.metadata["readinessVersion"], "rv-watchlist-new")

    def test_entry_ready_collect_events_emits_valid_ready_in_zone_after_setup_rollover(self):
        setup = self.actionable_setup(setup_version="sv-new-valid-ready", readiness_version="rv-new-valid-ready")
        setup["setupKey"] = "TESTUSDT:long"
        setup["symbol"] = "TESTUSDT"
        setup["actionableAlert"]["currentPrice"] = 1.4520
        setup["actionableAlert"]["entryZone"] = {"low": 1.4500, "high": 1.4550}
        setup["actionableAlert"]["stopLoss"] = 1.4440
        setup["actionableAlert"]["takeProfit"] = {"tp1": 1.4600, "tp2": 1.4650, "tp3": 1.4720}

        monitor = {
            **self.confirmed_monitor(),
            "setupKey": "TESTUSDT:long",
            "symbol": "TESTUSDT",
            "setupVersion": "sv-new-valid-ready",
            "readinessVersion": "rv-new-valid-ready",
            "runtime": {"state": "inactive", "changedAt": "2026-04-22T17:19:00+00:00"},
            "actionableAlert": deepcopy(setup["actionableAlert"]),
        }
        monitor["finalConfirmation"].update({
            "state": "reset",
            "setupVersion": "sv-old-confirmed",
            "readinessVersion": "rv-old-confirmed",
            "resetReason": "setup_version_changed",
            "resetAt": "2026-04-22T17:18:30+00:00",
        })

        self.p.load_watchlist_state = lambda: {"setups": {"TESTUSDT:long": setup}, "updatedAt": "2026-04-22T17:20:00+00:00"}
        self.p.load_alert_state = lambda: {"setups": {"TESTUSDT:long": {"delivery": {"state": "idle"}}}}
        self.p.load_live_trigger_state = lambda: {"monitors": {"TESTUSDT:long": monitor}}

        entry = self.p.notification_entry_from_setup("TESTUSDT:long", setup, {"delivery": {"state": "idle"}}, monitor=monitor)
        self.assertIsNotNone(entry)
        self.assertEqual(entry["lane"], "READY")

        events = self.p.entry_ready_collect_events(limit=10)
        self.assertEqual(len(events), 1)
        event = events[0]
        self.assertEqual(event.event_type, "READY_IN_ZONE")
        self.assertEqual(event.setup_key, "TESTUSDT:long")
        self.assertEqual(event.metadata["setupVersion"], "sv-new-valid-ready")
        self.assertEqual(event.metadata["readinessVersion"], "rv-new-valid-ready")

        card, _fallbacks = TelegramExecutionCardFormatter().render(event)
        self.assertIn("ENTRY READY — LIMIT", card)

    def test_entry_ready_collect_events_suppresses_live_strict_when_live_trust_blocked(self):
        setup = self.actionable_setup(setup_version="sv-confirmed", readiness_version="rv-confirmed")
        monitor = self.confirmed_monitor()

        self.p.load_watchlist_state = lambda: {"setups": {"XRPUSDT:long": setup}, "updatedAt": "2026-04-24T00:00:00+00:00"}
        self.p.load_alert_state = lambda: {"setups": {"XRPUSDT:long": {"delivery": {"state": "idle"}}}}
        self.p.load_live_trigger_state = lambda: {"monitors": {"XRPUSDT:long": monitor}}
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {"trustLive": False, "derivedStatus": "degraded", "reason": "ws_stale"}

        events = self.p.entry_ready_collect_events(limit=10)
        self.assertEqual(events, [])

    def test_inactive_top_level_retest_ready_demotes_to_standby_without_target_rearmed(self):
        prev = self.inactive_monitor(top_readiness="retest_ready", nested_readiness=None)
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-top"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "inactive")
        self.assertEqual(self.p.setup_readiness_state(monitor), "pullback_standby")
        self.assertEqual(monitor.get("readinessState"), "pullback_standby")
        self.assertEqual(monitor.get("lifecycleState"), "notified")
        self.assertIn("SCAN_DEMOTE_STANDBY", self.recent_reason_codes(state))
        self.assertNotIn("TARGET_REARMED", self.recent_reason_codes(state))
        readiness_entries = [entry for entry in state.get("recent", []) if entry.get("reasonCode") == "SCAN_DEMOTE_STANDBY"]
        self.assertTrue(readiness_entries)
        self.assertEqual(readiness_entries[-1].get("payload", {}).get("prevReadiness"), "retest_ready")
        self.assertEqual(readiness_entries[-1].get("payload", {}).get("currReadiness"), "pullback_standby")

    def test_inactive_nested_only_retest_ready_demotes_to_standby(self):
        prev = self.inactive_monitor(top_readiness=None, nested_readiness="retest_ready")
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-nested"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "inactive")
        self.assertEqual(self.p.setup_readiness_state(monitor), "pullback_standby")
        self.assertIn("SCAN_DEMOTE_STANDBY", self.recent_reason_codes(state))
        self.assertNotIn("TARGET_REARMED", self.recent_reason_codes(state))

    def test_inactive_nested_standby_without_top_alias_rearms_normally(self):
        prev = self.inactive_monitor(top_readiness=None, nested_readiness="pullback_standby")
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-existing"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "armed")
        self.assertIn("TARGET_REARMED", self.recent_reason_codes(state))
        self.assertNotIn("SCAN_DEMOTE_STANDBY", self.recent_reason_codes(state))

    def test_hard_invalidated_monitor_without_readiness_rearms_normally(self):
        prev = self.inactive_monitor(top_readiness=None, nested_readiness="retest_ready")
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}
        self.p.apply_invalidation(state, prev["setupKey"], "CONTEXT_BROKEN", actor="test")
        state["recent"] = []

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-after-hard"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(self.p.runtime_state(monitor), "armed")
        self.assertEqual(self.p.monitor_readiness_state(monitor), "pullback_standby")
        self.assertIn("TARGET_REARMED", self.recent_reason_codes(state))
        self.assertNotIn("SCAN_DEMOTE_STANDBY", self.recent_reason_codes(state))

    def test_new_monitor_with_standby_target_still_arms_normally(self):
        state = {"monitors": {}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-new"), cooldown_seconds=1800)

        monitor = state["monitors"]["XRPUSDT:long"]
        self.assertEqual(self.p.runtime_state(monitor), "armed")
        self.assertIn("TARGET_REARMED", self.recent_reason_codes(state))
        self.assertNotIn("SCAN_DEMOTE_STANDBY", self.recent_reason_codes(state))

    def test_soft_demotion_preserves_top_level_lifecycle_state_alias(self):
        prev = self.inactive_monitor(top_readiness="retest_ready", nested_readiness=None)
        state = {"monitors": {prev["setupKey"]: prev}, "recent": [], "metrics": {}}

        self.p.reconcile_live_trigger_targets(state, self.standby_target(readiness_version="rv-standby-lifecycle"), cooldown_seconds=1800)

        monitor = state["monitors"][prev["setupKey"]]
        self.assertEqual(monitor.get("lifecycleState"), "notified")
        self.assertEqual(self.p.setup_readiness_state(monitor), "pullback_standby")
        self.assertEqual(self.p.runtime_state(monitor), "inactive")

    def test_xrp_replay_2026_04_22_1855_to_1935_wib(self):
        monitor = {
            **self.confirmed_monitor(),
            "setupVersion": "ad2ae445348f",
            "readinessVersion": "5508dc6c03f0",
            "runtime": {"state": "confirmed", "changedAt": "2026-04-22T11:55:01.769743+00:00"},
            "triggeredAt": "2026-04-22T11:55:01.769743+00:00",
        }
        monitor["finalConfirmation"].update({
            "setupVersion": "ad2ae445348f",
            "readinessVersion": "5508dc6c03f0",
            "confirmedAt": "2026-04-22T11:55:01.769743+00:00",
            "notificationSentAt": "2026-04-22T11:55:21.456343+00:00",
            "deliveryStatusAt": "2026-04-22T11:55:21.456343+00:00",
        })

        setup_1856 = self.actionable_setup(setup_version="be4b6553fc7b", readiness_version="e24232aba265")
        setup_1856["lifecycle"]["changedAt"] = "2026-04-22T11:56:40.228537+00:00"
        setup_1856["readiness"]["changedAt"] = "2026-04-22T11:56:40.228537+00:00"
        entry_1856 = self.p.notification_entry_from_setup("XRPUSDT:long", setup_1856, {}, monitor=monitor)
        self.assertEqual(entry_1856["lane"], "LIVE")
        self.assertEqual(entry_1856["setupVersion"], "ad2ae445348f")
        self.assertEqual(entry_1856["readinessVersion"], "5508dc6c03f0")
        self.assertEqual(entry_1856["observedSetupVersion"], "be4b6553fc7b")
        self.assertEqual(entry_1856["observedReadinessVersion"], "e24232aba265")

        setup_1921 = self.actionable_setup(setup_version="ad2ae445348f", readiness_version="0d6c56295a26")
        setup_1921["lifecycle"]["changedAt"] = "2026-04-22T12:20:56.986190+00:00"
        setup_1921["readiness"]["changedAt"] = "2026-04-22T12:20:56.986190+00:00"
        state = {"monitors": {monitor["setupKey"]: monitor}, "recent": [], "metrics": {}}
        self.p.reconcile_live_trigger_targets(state, self.reconcile_target("ad2ae445348f", "0d6c56295a26"), cooldown_seconds=1800)
        monitor_1921 = state["monitors"][monitor["setupKey"]]
        entry_1921 = self.p.notification_entry_from_setup("XRPUSDT:long", setup_1921, {}, monitor=monitor_1921)
        self.assertEqual(self.p.runtime_state(monitor_1921), "confirmed")
        self.assertEqual(entry_1921["lane"], "LIVE")
        self.assertEqual(entry_1921["setupVersion"], "ad2ae445348f")
        self.assertEqual(entry_1921["readinessVersion"], "5508dc6c03f0")
        self.assertEqual(entry_1921["observedReadinessVersion"], "0d6c56295a26")

        setup_1927 = self.actionable_setup(setup_version="ad2ae445348f", readiness_version="431b75c94252")
        setup_1927["lifecycle"]["changedAt"] = "2026-04-22T12:26:04.526341+00:00"
        setup_1927["readiness"]["changedAt"] = "2026-04-22T12:26:04.526341+00:00"
        self.p.reconcile_live_trigger_targets(state, self.reconcile_target("ad2ae445348f", "431b75c94252"), cooldown_seconds=1800)
        monitor_1927 = state["monitors"][monitor["setupKey"]]
        entry_1927 = self.p.notification_entry_from_setup("XRPUSDT:long", setup_1927, {}, monitor=monitor_1927)
        self.assertEqual(self.p.runtime_state(monitor_1927), "confirmed")
        self.assertEqual(monitor_1927["finalConfirmation"]["state"], "active")
        self.assertEqual(entry_1927["lane"], "LIVE")
        self.assertEqual(entry_1927["setupVersion"], "ad2ae445348f")
        self.assertEqual(entry_1927["readinessVersion"], "5508dc6c03f0")
        self.assertEqual(entry_1927["observedReadinessVersion"], "431b75c94252")

        setup_1931 = self.actionable_setup(setup_version="873a7b59711f", readiness_version="abe2ca77620e")
        setup_1931["lifecycle"]["changedAt"] = "2026-04-22T12:30:58.173206+00:00"
        setup_1931["readiness"]["changedAt"] = "2026-04-22T12:30:58.173206+00:00"
        self.p.reconcile_live_trigger_targets(state, self.reconcile_target("873a7b59711f", "abe2ca77620e"), cooldown_seconds=1800)
        monitor_1931 = state["monitors"][monitor["setupKey"]]
        entry_1931 = self.p.notification_entry_from_setup("XRPUSDT:long", setup_1931, {}, monitor=monitor_1931)
        self.assertEqual(self.p.runtime_state(monitor_1931), "inactive")
        self.assertEqual(monitor_1931["finalConfirmation"]["state"], "reset")
        self.assertIsNotNone(monitor_1931["finalConfirmation"]["resetAt"])
        self.assertEqual(entry_1931["lane"], "READY")
        self.assertEqual(entry_1931["setupVersion"], "873a7b59711f")
        self.assertEqual(entry_1931["readinessVersion"], "abe2ca77620e")


if __name__ == "__main__":
    unittest.main()
