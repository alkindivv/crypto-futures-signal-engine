import copy
import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData


class EntryReadyDryRunGateTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self._state = {
            "updatedAt": None,
            "activeBySetup": {},
            "ledger": {},
            "cancelLedger": {},
            "cycles": [],
        }
        self._logs = []
        self.p.load_entry_ready_dry_run_state = lambda: copy.deepcopy(self._state)
        self.p.save_entry_ready_dry_run_state = self._save_state
        self.p.append_entry_ready_dry_run_log = lambda row: self._logs.append(copy.deepcopy(row))
        self.p.load_alert_state = lambda: {"setups": {}}
        self.watchlist = {"setups": {}}
        self.live_state = {"monitors": {}}
        self.p.load_watchlist_state = lambda: copy.deepcopy(self.watchlist)
        self.p.load_live_trigger_state = lambda: copy.deepcopy(self.live_state)

    def _save_state(self, state):
        self._state = copy.deepcopy(state)

    def ready_setup(self, setup_version="v1", readiness_version="r1",
                    entry_zone_low=10.0, entry_zone_high=11.0):
        return {
            "symbol": "TESTUSDT",
            "side": "long",
            "grade": "A",
            "cleanScore": 99.0,
            "setupVersion": setup_version,
            "readinessVersion": readiness_version,
            "lifecycle": {"state": "actionable"},
            "readiness": {"state": "retest_ready"},
            "actionableAlert": {
                "currentPrice": 10.5,
                "entryZone": {"low": entry_zone_low, "high": entry_zone_high},
                "stopLoss": 9.5,
                "takeProfit": {"tp1": 11.5, "tp2": 12.0, "tp3": 12.5},
                "trigger": "hold and reclaim",
                "invalidation": "break below stop",
                "executionNote": "Default management",
                "doNotChase": False,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced — no extreme crowding detected."},
            },
        }

    # ── Anti-spam: repeated READY suppression ──────────────────────────────────

    def test_repeat_cycle_same_context_is_suppressed(self):
        """
        Opportunity gate: once READY is delivered, subsequent cycles suppress the
        same opportunity with reason 'opportunity_already_open'.
        """
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup()}
        first = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.assertEqual(len(first["wouldSend"]), 1)
        self.assertEqual(len(first["suppressed"]), 0)

        second = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(second["wouldSend"]), 0)
        self.assertEqual(len(second["suppressed"]), 1)
        # New gate reason: opportunity is already open
        self.assertEqual(second["suppressed"][0]["reason"], "opportunity_already_open")

    def test_tp_only_change_generates_new_would_send(self):
        """
        Fingerprint-based identity: TP-only change → new fingerprint → new would-send.
        setupVersion/readinessVersion change alone (execution plan unchanged) → same
        fingerprint → suppressed by opportunity gate.
        """
        # Case 1: setupVersion/readinessVersion change but execution plan same → suppress
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup(setup_version="v1", readiness_version="r1")}
        first = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.assertEqual(len(first["wouldSend"]), 1)
        first_fingerprint = first["wouldSend"][0]["executionFingerprint"]

        # Same execution plan, different version → suppressed by opportunity gate
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup(setup_version="v2", readiness_version="r2")}
        second = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(second["wouldSend"]), 0)
        self.assertEqual(len(second["suppressed"]), 1)
        self.assertEqual(second["suppressed"][0]["reason"], "opportunity_already_open")
        second_fingerprint = second["suppressed"][0].get("executionFingerprint")
        self.assertEqual(second["suppressed"][0]["deliveryKey"], first["wouldSend"][0]["deliveryKey"])

        # Case 2: TP actually changes → new fingerprint → new would-send
        changed = self.ready_setup(setup_version="v2", readiness_version="r2")
        changed["actionableAlert"]["takeProfit"]["tp1"] = 0.0205  # different TP
        self.watchlist["setups"] = {"TESTUSDT:long": changed}
        third = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(third["wouldSend"]), 1)
        self.assertEqual(len(third["suppressed"]), 0)
        third_fingerprint = third["wouldSend"][0]["executionFingerprint"]
        self.assertNotEqual(third_fingerprint, first_fingerprint)

    # ── Anti-spam: CANCEL deduplication ───────────────────────────────────────

    def test_cancel_only_after_prior_would_send(self):
        """
        CANCEL is only delivered if there was a prior open opportunity.
        """
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup()}
        first = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.assertEqual(len(first["wouldSend"]), 1)

        invalidated = self.ready_setup()
        invalidated["lifecycle"] = {"state": "invalidated"}
        invalidated["readiness"] = {"state": None}
        self.watchlist["setups"] = {"TESTUSDT:long": invalidated}
        second = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(second["wouldSend"]), 1)
        self.assertEqual(second["wouldSend"][0]["eventType"], "CANCEL")
        self.assertEqual(self._state["activeBySetup"], {})

    def test_invalidated_without_prior_send_does_not_emit_cancel(self):
        """
        If no prior open opportunity exists, CANCEL is suppressed.
        """
        invalidated = self.ready_setup()
        invalidated["lifecycle"] = {"state": "invalidated"}
        invalidated["readiness"] = {"state": None}
        self.watchlist["setups"] = {"TESTUSDT:long": invalidated}
        cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.assertEqual(len(cycle["wouldSend"]), 0)
        self.assertEqual(cycle["collection"]["cancel"], 0)

    def test_zone_exit_can_trigger_cancel_when_previously_would_sent(self):
        """
        When a previously-sent setup exits the entry zone, CANCEL fires.
        """
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup()}
        self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup()}
        self.watchlist["setups"]["TESTUSDT:long"]["lifecycle"] = {"state": "candidate"}
        self.watchlist["setups"]["TESTUSDT:long"]["readiness"] = {"state": "pullback_standby"}
        self.live_state["monitors"] = {
            "TESTUSDT:long": {
                "setupKey": "TESTUSDT:long",
                "symbol": "TESTUSDT",
                "side": "long",
                "inactiveReason": "ZONE_RECLAIMED",
                "runtime": {"reasonCode": "ZONE_RECLAIMED"},
                "actionableAlert": self.ready_setup()["actionableAlert"],
                "setupVersion": "v1",
                "readinessVersion": "r1",
                "grade": "A",
            }
        }
        cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(cycle["wouldSend"]), 1)
        self.assertEqual(cycle["wouldSend"][0]["eventType"], "CANCEL")

    def test_cancel_renders_last_delivered_anchor_not_current_rolled_identity(self):
        """
        Dual-layer CANCEL continuity: render the setup the operator actually saw,
        while preserving the current invalidating identity in metadata.
        """
        original = self.ready_setup(setup_version="sv-A", readiness_version="rv-A", entry_zone_low=10.0, entry_zone_high=11.0)
        self.watchlist["setups"] = {"TESTUSDT:long": original}
        first = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        self.assertEqual(len(first["wouldSend"]), 1)
        self.assertEqual(first["wouldSend"][0]["setupVersion"], "sv-A")

        rolled = self.ready_setup(setup_version="sv-B", readiness_version="rv-B", entry_zone_low=8.0, entry_zone_high=9.0)
        rolled["lifecycle"] = {"state": "invalidated"}
        rolled["readiness"] = {"state": None}
        self.watchlist["setups"] = {"TESTUSDT:long": rolled}

        second = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(second["wouldSend"]), 1)
        cancel = second["wouldSend"][0]
        self.assertEqual(cancel["eventType"], "CANCEL")
        self.assertEqual(cancel["setupVersion"], "sv-A")
        self.assertEqual(cancel["cancelAnchorSetupVersion"], "sv-A")
        self.assertEqual(cancel["cancelCurrentSetupVersion"], "sv-B")
        self.assertEqual((cancel.get("entryZone") or {}).get("low"), 10.0)
        self.assertEqual((cancel.get("entryZone") or {}).get("high"), 11.0)
        self.assertEqual(cancel.get("stopLoss"), 9.5)

    def test_zone_exit_cancel_works_from_nested_runtime_reason_without_legacy_alias(self):
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup()}
        self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)
        degraded = self.ready_setup()
        degraded["lifecycle"] = {"state": "candidate"}
        degraded["readiness"] = {"state": "pullback_standby"}
        self.watchlist["setups"] = {"TESTUSDT:long": degraded}
        self.live_state["monitors"] = {
            "TESTUSDT:long": {
                "setupKey": "TESTUSDT:long",
                "symbol": "TESTUSDT",
                "side": "long",
                "runtime": {"state": "inactive", "reasonCode": "ZONE_RECLAIMED"},
                "actionableAlert": self.ready_setup()["actionableAlert"],
                "setupVersion": "v1",
                "readinessVersion": "r1",
                "grade": "A",
            }
        }
        cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(cycle["wouldSend"]), 1)
        self.assertEqual(cycle["wouldSend"][0]["eventType"], "CANCEL")
        self.assertEqual(cycle["wouldSend"][0]["cancelReasonFamily"], "ZONE_EXIT")

    # ── Reopen only on material reset ─────────────────────────────────────────

    def test_recovery_after_invalidation_with_material_change_reopens(self):
        """
        After CANCEL, gate stays cancelled. A materially different execution plan
        (different entry zone) reopens the gate and allows READY through.
        Version/string change alone is NOT a material reset.
        """
        self.watchlist["setups"] = {"TESTUSDT:long": self.ready_setup(
            setup_version="v1", readiness_version="r1",
            entry_zone_low=10.0, entry_zone_high=11.0)}
        self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=True)

        # Invalidate: triggers CANCEL
        invalidated = self.ready_setup(setup_version="v1", readiness_version="r1",
                                        entry_zone_low=10.0, entry_zone_high=11.0)
        invalidated["lifecycle"] = {"state": "invalidated"}
        invalidated["readiness"] = {"state": None}
        self.watchlist["setups"] = {"TESTUSDT:long": invalidated}
        cancel_cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(cancel_cycle["wouldSend"][0]["eventType"], "CANCEL")

        # Recover with SAME zone (no material change) → suppressed
        recovered_same = self.ready_setup(setup_version="v2", readiness_version="r2",
                                          entry_zone_low=10.0, entry_zone_high=11.0)
        self.watchlist["setups"] = {"TESTUSDT:long": recovered_same}
        suppressed_cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(suppressed_cycle["wouldSend"]), 0)
        self.assertEqual(len(suppressed_cycle["suppressed"]), 1)
        self.assertIn("cancelled_opportunity_no_material_reset", suppressed_cycle["suppressed"][0]["reason"])

        # Recover with DIFFERENT zone (material change) → READY through
        recovered_diff = self.ready_setup(setup_version="v2", readiness_version="r2",
                                          entry_zone_low=9.5, entry_zone_high=10.5)
        self.watchlist["setups"] = {"TESTUSDT:long": recovered_diff}
        recovery_cycle = self.p.entry_ready_dry_run_cycle(limit=10, persist=True, reset_state=False)
        self.assertEqual(len(recovery_cycle["wouldSend"]), 1)
        self.assertEqual(recovery_cycle["wouldSend"][0]["eventType"], "READY_IN_ZONE")


if __name__ == "__main__":
    unittest.main()
