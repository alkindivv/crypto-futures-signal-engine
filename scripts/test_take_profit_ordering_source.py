import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData


class SourceTakeProfitOrderingTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_normalize_take_profit_ladder_long(self):
        self.assertEqual(
            self.p.normalize_take_profit_ladder("long", 0.0008866, 0.0007256, 0.0007867),
            (0.0007256, 0.0007867, 0.0008866),
        )

    def test_normalize_take_profit_ladder_short(self):
        self.assertEqual(
            self.p.normalize_take_profit_ladder("short", 82.852, 83.11, 82.294),
            (83.11, 82.852, 82.294),
        )

    def test_actionable_setup_plan_long_targets_are_monotonic(self):
        row = {
            "direction": "long",
            "grade": "A",
            "disqualified": False,
            "features": {
                "15m": {"last": 10.5, "recentHigh": 14.0, "recentLow": 10.0},
                "1h": {"recentHigh": 14.0, "recentLow": 10.0},
            },
            "executionFilter": {"recentLocation": {"m15": 0.5, "h1": 0.5}, "riskFlags": []},
            "managementGuidance": [],
        }
        plan = self.p.actionable_setup_plan(row)
        tp = plan["takeProfit"]
        self.assertLessEqual(tp["tp1"], tp["tp2"])
        self.assertLessEqual(tp["tp2"], tp["tp3"])

    def test_actionable_setup_plan_short_targets_are_monotonic(self):
        row = {
            "direction": "short",
            "grade": "A",
            "disqualified": False,
            "features": {
                "15m": {"last": 10.5, "recentHigh": 11.0, "recentLow": 6.0},
                "1h": {"recentHigh": 11.0, "recentLow": 6.0},
            },
            "executionFilter": {"recentLocation": {"m15": 0.5, "h1": 0.5}, "riskFlags": []},
            "managementGuidance": [],
        }
        plan = self.p.actionable_setup_plan(row)
        tp = plan["takeProfit"]
        self.assertGreaterEqual(tp["tp1"], tp["tp2"])
        self.assertGreaterEqual(tp["tp2"], tp["tp3"])


if __name__ == "__main__":
    unittest.main()
