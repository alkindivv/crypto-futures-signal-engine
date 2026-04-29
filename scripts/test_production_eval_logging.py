import json
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

import public_market_data as pmd
from public_market_data import PublicMarketData


class ProductionEvalLoggingTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_live_trigger_record_evaluation_persists_regime(self):
        monitor = {}
        regime = {"grade": "A", "direction": "long", "readinessState": "retest_ready"}
        self.p.live_trigger_record_evaluation(monitor, phase="trigger_eval", outcome="confirmed", regime=regime)
        self.assertEqual(monitor["evaluation"]["lastEvaluationRegime"], regime)
        self.assertEqual(monitor["evaluation"]["lastEvaluationOutcome"], "confirmed")

    def test_production_eval_log_lifecycle_event_includes_regime_key(self):
        monitor = {
            "symbol": "BTCUSDT",
            "side": "long",
            "grade": "A",
            "setupVersion": "setup-v1",
            "readinessVersion": "ready-v1",
            "actionableAlert": {"currentStatus": "ready_on_retest", "riskFlags": ["crowded"]},
            "lifecycle": {
                "entryReferencePrice": 100.0,
                "maxFavorableBps": 120.0,
                "maxAdverseBps": 35.0,
                "startedAt": "2026-04-24T00:00:00+00:00",
                "closedAt": "2026-04-24T01:00:00+00:00",
            },
            "readiness": {"state": "retest_ready"},
            "lifecycleState": "actionable",
        }
        lifecycle_event = {"milestone": "closed", "reason": "tp3_hit", "price": 103.0}
        captured = []
        with patch.object(pmd, "append_jsonl", side_effect=lambda path, payload: captured.append(payload)):
            self.p.production_eval_log_lifecycle_event("BTCUSDT:long", monitor, lifecycle_event)
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["type"], "lifecycle_event")
        self.assertIn("regime", captured[0])
        self.assertIn("regimeKey", captured[0])
        self.assertEqual(json.loads(captured[0]["regimeKey"])["grade"], "A")

    def test_production_eval_log_funnel_by_regime_dedupes_same_snapshot(self):
        state = {
            "monitors": {
                "BTCUSDT:long": {
                    "symbol": "BTCUSDT",
                    "side": "long",
                    "grade": "A",
                    "active": True,
                    "runtime": {"state": "confirmed"},
                    "readiness": {"state": "retest_ready"},
                    "lifecycle": {"state": "entered"},
                    "evaluation": {
                        "lastEvaluationPhase": "trigger_eval",
                        "lastEvaluationOutcome": "confirmed",
                        "lastEvaluationRegime": {
                            "grade": "A",
                            "direction": "long",
                            "readinessState": "retest_ready",
                            "funding": 0.001,
                            "oi6hChangePct": 7.5,
                            "spreadBps": 2.0,
                            "riskFlags": [],
                            "executionFlags": [],
                            "doNotChase": False,
                        },
                    },
                }
            }
        }
        captured = []
        with patch.object(pmd, "append_jsonl", side_effect=lambda path, payload: captured.append(payload)):
            self.p.production_eval_log_funnel_by_regime(state, source="ws_refresh")
            self.p.production_eval_log_funnel_by_regime(state, source="ws_refresh")
        self.assertEqual(len(captured), 1)
        self.assertEqual(captured[0]["type"], "funnel_by_regime")
        self.assertEqual(captured[0]["rows"][0]["confirmed"], 1)
        self.assertEqual(captured[0]["rows"][0]["phaseCounts"]["trigger_eval"], 1)
        self.assertEqual(captured[0]["rows"][0]["outcomeCounts"]["confirmed"], 1)


if __name__ == "__main__":
    unittest.main()
