import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData


class StateStatusObservabilityTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_state_status_separates_all_vs_renderable_readiness_counts(self):
        watchlist = {
            "updatedAt": "2026-04-22T18:00:00+00:00",
            "lastScanAt": "2026-04-22T18:00:00+00:00",
            "scanCount": 1,
            "recent": [],
            "setups": {
                "STALE-READY:long": {
                    "setupKey": "STALE-READY:long",
                    "symbol": "STALE-READY",
                    "side": "long",
                    "active": False,
                    "lifecycle": {"state": "stale", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": None, "changedAt": "2026-04-22T17:59:00+00:00"},
                    "actionableAlert": {"currentStatus": "ready_on_retest"},
                },
                "CANDIDATE-READY:long": {
                    "setupKey": "CANDIDATE-READY:long",
                    "symbol": "CANDIDATE-READY",
                    "side": "long",
                    "active": True,
                    "lifecycle": {"state": "candidate", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": "retest_ready", "changedAt": "2026-04-22T17:59:00+00:00", "readinessVersion": "rv-candidate"},
                    "actionableAlert": {"currentStatus": "ready_on_retest"},
                },
                "READY-NOW:long": {
                    "setupKey": "READY-NOW:long",
                    "symbol": "READY-NOW",
                    "side": "long",
                    "active": True,
                    "lifecycle": {"state": "actionable", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": "retest_ready", "changedAt": "2026-04-22T17:59:00+00:00", "readinessVersion": "rv-ready"},
                    "actionableAlert": {
                        "currentStatus": "ready_on_retest",
                        "currentPrice": 1.002,
                        "entryZone": {"low": 1.0, "high": 1.01},
                        "stopLoss": 0.99,
                        "takeProfit": {"tp1": 1.02, "tp2": 1.03, "tp3": 1.04},
                        "doNotChase": False,
                    },
                },
                "STALE-PULLBACK:long": {
                    "setupKey": "STALE-PULLBACK:long",
                    "symbol": "STALE-PULLBACK",
                    "side": "long",
                    "active": False,
                    "lifecycle": {"state": "stale", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": None, "changedAt": "2026-04-22T17:59:00+00:00"},
                    "actionableAlert": {"currentStatus": "wait_pullback"},
                },
                "WATCHLIST-BOUNCE:short": {
                    "setupKey": "WATCHLIST-BOUNCE:short",
                    "symbol": "WATCHLIST-BOUNCE",
                    "side": "short",
                    "active": True,
                    "lifecycle": {"state": "watchlist", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": "bounce_standby", "changedAt": "2026-04-22T17:59:00+00:00", "readinessVersion": "rv-bounce"},
                    "actionableAlert": {"currentStatus": "wait_bounce"},
                },
                "INVALID-BOUNCE:short": {
                    "setupKey": "INVALID-BOUNCE:short",
                    "symbol": "INVALID-BOUNCE",
                    "side": "short",
                    "active": False,
                    "lifecycle": {"state": "invalidated", "changedAt": "2026-04-22T17:59:00+00:00"},
                    "readiness": {"state": None, "changedAt": "2026-04-22T17:59:00+00:00"},
                    "actionableAlert": {"currentStatus": "wait_bounce"},
                },
            },
        }
        alerts = {
            "setups": {
                "STALE-READY:long": {"delivery": {"state": "idle"}},
                "CANDIDATE-READY:long": {"delivery": {"state": "idle"}},
                "READY-NOW:long": {"delivery": {"state": "idle"}},
                "STALE-PULLBACK:long": {"delivery": {"state": "idle"}},
                "WATCHLIST-BOUNCE:short": {"delivery": {"state": "idle"}},
                "INVALID-BOUNCE:short": {"delivery": {"state": "idle"}},
            }
        }
        live_state = {
            "monitors": {
                "READY-NOW:long": {"runtime": {"state": "inactive"}},
                "CANDIDATE-READY:long": {"runtime": {"state": "inactive"}},
                "WATCHLIST-BOUNCE:short": {"runtime": {"state": "inactive"}},
                "INVALID-BOUNCE:short": {"runtime": {"state": "inactive"}},
            }
        }

        self.p.load_watchlist_state = lambda: self.p._normalize_watchlist_state_payload(watchlist)
        self.p.load_alert_state = lambda: self.p._normalize_alert_state_payload(alerts)
        self.p.load_live_trigger_state = lambda: live_state

        status = self.p.setup_state_status()

        self.assertEqual(status["readinessCounts_all"]["retest_ready"], 2)
        self.assertEqual(status["readinessCounts_all"]["bounce_standby"], 1)
        self.assertNotIn("pullback_standby", status["readinessCounts_all"])

        self.assertEqual(status["readinessCounts_renderable"]["retest_ready"], 1)
        self.assertEqual(status["readinessCounts_renderable"]["bounce_standby"], 1)
        self.assertNotIn("pullback_standby", status["readinessCounts_renderable"])

        self.assertEqual(status["readinessCounts"], status["readinessCounts_renderable"])
        self.assertEqual(status["blockedReadinessCounts"]["stale"], 0)
        self.assertEqual(status["blockedReadinessCounts"]["invalidated"], 0)
        self.assertEqual(status["blockedReadinessCounts"]["candidate_only"], 1)
        self.assertEqual(status["blockedReadinessCounts"]["other_blockers"], 0)


    def test_normalization_ignores_alias_readiness_on_stale_or_invalidated_but_keeps_nested(self):
        stale_alias_only = self.p.normalize_watchlist_setup_record("STALE:long", {
            "symbol": "STALE",
            "side": "long",
            "lifecycle": {"state": "stale"},
            "actionableAlert": {"currentStatus": "ready_on_retest"},
        })["record"]
        self.assertIsNone((stale_alias_only.get("readiness") or {}).get("state"))

        invalidated_alias_only = self.p.normalize_watchlist_setup_record("INVALID:short", {
            "symbol": "INVALID",
            "side": "short",
            "lifecycle": {"state": "invalidated"},
            "actionableAlert": {"currentStatus": "wait_bounce"},
        })["record"]
        self.assertIsNone((invalidated_alias_only.get("readiness") or {}).get("state"))

        stale_with_nested = self.p.normalize_watchlist_setup_record("STALE-NESTED:long", {
            "symbol": "STALE-NESTED",
            "side": "long",
            "lifecycle": {"state": "stale"},
            "readiness": {"state": "retest_ready", "readinessVersion": "rv-stale"},
            "actionableAlert": {"currentStatus": "wait_pullback"},
        })["record"]
        self.assertEqual((stale_with_nested.get("readiness") or {}).get("state"), "retest_ready")
        self.assertEqual(stale_with_nested.get("readinessVersion"), "rv-stale")


if __name__ == "__main__":
    unittest.main()
