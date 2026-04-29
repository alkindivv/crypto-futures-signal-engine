import copy
import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

import public_market_data as pmd_module
from public_market_data import PublicMarketData


class DiscoveryFeederSplitTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.scan_calls = []
        self.saved_json = []
        self.appended_jsonl = []
        self.original_save_json_file = pmd_module.save_json_file
        self.original_append_jsonl = pmd_module.append_jsonl
        pmd_module.save_json_file = self._save_json_file
        pmd_module.append_jsonl = self._append_jsonl
        self.p.scan = self._scan

    def tearDown(self):
        pmd_module.save_json_file = self.original_save_json_file
        pmd_module.append_jsonl = self.original_append_jsonl

    def _save_json_file(self, path, payload):
        self.saved_json.append({"path": str(path), "payload": copy.deepcopy(payload)})

    def _append_jsonl(self, path, payload):
        self.appended_jsonl.append({"path": str(path), "payload": copy.deepcopy(payload)})

    def _scan(self, **kwargs):
        self.scan_calls.append(copy.deepcopy(kwargs))
        return {
            "generatedAt": 1776852237,
            "universeSize": 6,
            "alertEligible": ["SOLUSDT", "XRPUSDT"],
            "watchlistEligible": ["SOLUSDT", "XRPUSDT", "DOGEUSDT"],
            "pendingAlerts": [],
            "stateSync": {"saved": kwargs.get("persist_state", False), "activeSetups": 3},
            "stateStatus": {
                "updatedAt": "2026-04-22T10:03:57.783438+00:00",
                "lastScanAt": "2026-04-22T10:03:57.773586+00:00",
                "setupsTracked": 176,
                "activeSetups": 3,
                "pendingAlerts": 2,
                "sentAlerts": 0,
                "stateCounts": {"actionable": 2},
                "readinessCounts": {"retest_ready": 2},
                "recentTransitions": [{"symbol": "SOLUSDT"}],
            },
            "topLongs": [
                {
                    "symbol": "SOLUSDT",
                    "direction": "long",
                    "grade": "A+",
                    "bullScore": 9,
                    "cleanScore": 98.9,
                    "gradeScore": 98.9,
                    "quoteVolume": 123,
                    "priceChangePercent": 2.1,
                    "lifecycle": {"state": "actionable"},
                    "readiness": {"state": "retest_ready"},
                    "currentStatus": "ready_on_retest",
                    "alertEligible": True,
                    "watchlistEligible": True,
                    "setupKey": "SOLUSDT:long",
                    "setupVersion": "sv-sol",
                    "readinessVersion": "rv-sol",
                    "actionableAlert": {
                        "currentPrice": 88.2,
                        "entryZone": {"low": 87.8, "high": 88.4},
                        "stopLoss": 86.6,
                        "takeProfit": {"tp1": 88.9, "tp2": 89.6, "tp3": 90.4},
                        "trigger": "hold",
                        "invalidation": "breakdown",
                        "doNotChase": False,
                        "executionNote": "Default management",
                        "riskFlags": ["recent_sell_pressure"],
                    },
                },
                {
                    "symbol": "XRPUSDT",
                    "direction": "long",
                    "grade": "A",
                    "bullScore": 5,
                    "cleanScore": 95.3,
                    "gradeScore": 95.3,
                    "quoteVolume": 456,
                    "priceChangePercent": 0.5,
                    "lifecycle": {"state": "actionable"},
                    "readiness": {"state": "retest_ready"},
                    "currentStatus": "ready_on_retest",
                    "alertEligible": True,
                    "watchlistEligible": True,
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "sv-xrp",
                    "readinessVersion": "rv-xrp",
                    "actionableAlert": {
                        "currentPrice": 1.44,
                        "entryZone": {"low": 1.40, "high": 1.45},
                        "stopLoss": 1.39,
                        "takeProfit": {"tp1": 1.46, "tp2": 1.47, "tp3": 1.48},
                        "trigger": "hold",
                        "invalidation": "breakdown",
                        "doNotChase": False,
                        "executionNote": "Default management",
                        "riskFlags": ["long_crowding_risk"],
                    },
                },
            ],
            "topShorts": [],
            "topWatch": [],
        }

    def test_preview_is_non_mutating_and_writes_preview_sink(self):
        result = self.p.discovery_radar_preview(top=2, universe=6, enrich_top=0, tradingview_top=0, alert_limit=2, max_entries=6)

        self.assertEqual(len(self.scan_calls), 1)
        self.assertFalse(self.scan_calls[0]["persist_state"])
        self.assertEqual(result["cycle"]["mode"], "preview")
        self.assertFalse(result["cycle"]["mutating"])
        self.assertFalse(result["cycle"]["downstreamImpactPossible"])
        self.assertIn("discovery_radar_preview_latest.json", self.saved_json[0]["path"])
        self.assertIn("discovery_radar_preview_log.jsonl", self.appended_jsonl[0]["path"])
        self.assertEqual(self.appended_jsonl[0]["payload"]["mode"], "preview")

    def test_feeder_is_explicit_mutating_and_writes_operational_sink(self):
        result = self.p.discovery_feed_state(top=2, universe=6, enrich_top=0, tradingview_top=0, alert_limit=2, max_entries=6)

        self.assertEqual(len(self.scan_calls), 1)
        self.assertTrue(self.scan_calls[0]["persist_state"])
        self.assertEqual(self.scan_calls[0]["delivery_note"], "discovery_feed_state")
        self.assertEqual(result["cycle"]["mode"], "feeder")
        self.assertTrue(result["cycle"]["mutating"])
        self.assertTrue(result["cycle"]["downstreamImpactPossible"])
        self.assertIn("discovery_radar_latest.json", self.saved_json[0]["path"])
        self.assertIn("discovery_radar_log.jsonl", self.appended_jsonl[0]["path"])
        self.assertEqual(self.appended_jsonl[0]["payload"]["mode"], "feeder")

    def test_legacy_command_is_hard_blocked(self):
        with self.assertRaises(ValueError) as ctx:
            self.p.discovery_radar_cycle(top=2, universe=6, enrich_top=0, tradingview_top=0, alert_limit=2, max_entries=6)
        self.assertIn("BLOCKED: discovery-radar-cycle is deprecated", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
