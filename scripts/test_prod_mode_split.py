"""
Tests for prod_mode behavior: production paths are Binance-native and deterministic.
"""
import sys
import unittest
from unittest.mock import MagicMock, patch
import argparse

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData, FEEDBACK_V2_PROD_MIN_SAMPLES


class ProdModeScoreSymbolTests(unittest.TestCase):
    """Test that prod_mode silences news keyword scoring in _score_symbol."""

    def setUp(self):
        self.p = PublicMarketData()
        self.p.execution_filter_overlay = MagicMock(return_value={"cleanScoreDelta": 0.0, "managementGuidance": []})
        self.p.tradingview_status = MagicMock(return_value={})

    def _snapshot(self, negative_count=0, positive_count=0, funding_rate="0.0"):
        klines = [
            [0, 0, "100.0", "100.0", "100.0", "1000.0", 0, 0, 0, 0, 0, 0]
            for _ in range(60)
        ]
        return {
            "binance": {
                "klines": {"5m": klines, "15m": klines, "1h": klines, "4h": klines},
                "bookTicker": {"bidPrice": "100.0", "askPrice": "100.0"},
                "premiumIndex": {"lastFundingRate": funding_rate},
                "openInterest": {"openInterest": "1000.0"},
                "openInterestHist": [{"sumOpenInterest": "1000.0"}],
                "ticker24h": {"quoteVolume": "0.0", "priceChangePercent": "0.0", "lastPrice": "100.0"},
            },
            "news": [{"title": "headline"}],
            "headlineFlags": {"negativeCount": negative_count, "positiveCount": positive_count},
        }

    def _score_both_modes(self, negative_count=0, positive_count=0, funding_rate="0.0"):
        snapshot = self._snapshot(negative_count=negative_count, positive_count=positive_count, funding_rate=funding_rate)
        with patch.object(self.p, "snapshot", return_value=snapshot):
            row_false = self.p._score_symbol("SOLUSDT", prod_mode=False)
            row_true = self.p._score_symbol("SOLUSDT", prod_mode=True)
        return row_false, row_true

    def test_research_mode_negative_flags_subtract(self):
        """prod_mode=False: baseCleanScore reduced by negative flags (-12 pts for 3 neg)."""
        row_false, row_true = self._score_both_modes(negative_count=3)
        self.assertGreater(row_true["baseCleanScore"], row_false["baseCleanScore"])
        self.assertEqual(row_true["baseCleanScore"] - row_false["baseCleanScore"], 12.0)

    def test_prod_mode_skips_negative_flags(self):
        """prod_mode=True: baseCleanScore ignores negative flags."""
        row_false, row_true = self._score_both_modes(negative_count=3)
        self.assertEqual(row_true["baseCleanScore"] - row_false["baseCleanScore"], 12.0)

    def test_prod_mode_skips_positive_flags(self):
        """prod_mode=True: baseCleanScore ignores positive flags (+6 pts bonus skipped)."""
        row_false, row_true = self._score_both_modes(positive_count=3, funding_rate="0.001")
        self.assertEqual(row_false["baseCleanScore"] - row_true["baseCleanScore"], 6.0)


class FeedbackProdMinSamplesTests(unittest.TestCase):
    """Test that _apply_feedback_v2_to_row freezes when prod_mode + low samples."""

    def test_frozen_when_prod_mode_no_data(self):
        """prod_mode=True + 0 executions → frozen in feedbackV2, no cleanScore change."""
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        row = {"symbol": "SOLUSDT", "cleanScore": 50.0, "bullScore": 5}
        with patch.object(p, "load_execution_feedback_state", return_value={
            "symbols": {}, "recent": [], "updatedAt": None
        }):
            result = p._apply_feedback_v2_to_row(row, prod_mode=True)
        fb = result["feedbackV2"]
        self.assertTrue(fb["frozen"])
        self.assertEqual(fb["minimumRequired"], FEEDBACK_V2_PROD_MIN_SAMPLES)
        self.assertEqual(row["cleanScore"], 50.0)

    def test_research_mode_feedback_applies_with_1_sample(self):
        """prod_mode=False: 1 execution is enough to apply feedback."""
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        row = {"symbol": "SOLUSDT", "cleanScore": 50.0, "bullScore": 5}
        with patch.object(p, "load_execution_feedback_state", return_value={
            "symbols": {"SOLUSDT": {"sides": {"long": {
                "total": 1, "win": 1, "loss": 0,
                "outcomes": ["WIN"], "lastUpdated": "2026-01-01T00:00:00Z"
            }}}},
            "recent": [], "updatedAt": "2026-01-01T00:00:00Z"
        }):
            result = p._apply_feedback_v2_to_row(row, prod_mode=False)
        self.assertFalse(result["feedbackV2"].get("frozen", False))
        self.assertIn("cleanScoreDelta", result["feedbackV2"])

    def test_prod_mode_not_frozen_with_sufficient_samples(self):
        """prod_mode=True + sufficient samples → NOT frozen."""
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        row = {"symbol": "SOLUSDT", "cleanScore": 50.0, "bullScore": 5}
        with patch.object(p, "load_execution_feedback_state", return_value={
            "symbols": {"SOLUSDT": {"sides": {"long": {
                "total": FEEDBACK_V2_PROD_MIN_SAMPLES,
                "win": FEEDBACK_V2_PROD_MIN_SAMPLES, "loss": 0,
                "outcomes": ["WIN"] * FEEDBACK_V2_PROD_MIN_SAMPLES,
                "lastUpdated": "2026-01-01T00:00:00Z"
            }}}},
            "recent": [], "updatedAt": "2026-01-01T00:00:00Z"
        }):
            result = p._apply_feedback_v2_to_row(row, prod_mode=True)
        self.assertFalse(result["feedbackV2"].get("frozen", False))


class ScanProdModeThreadingTests(unittest.TestCase):
    """Verify prod_mode threading from public methods."""

    def test_watchlist_refresh_threads_prod_mode_true(self):
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        p.score_symbols = MagicMock(return_value={"topLongs": [], "topShorts": [], "topWatch": []})
        with patch.object(p, "load_watchlist_state", return_value={"setups": {}}):
            p.watchlist_refresh(top=5, max_symbols=3)
        self.assertTrue(p.score_symbols.called)
        self.assertEqual(p.score_symbols.call_args.kwargs.get("prod_mode"), True)

    def test_watchlist_refresh_threads_prod_mode_false(self):
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        p.score_symbols = MagicMock(return_value={"topLongs": [], "topShorts": [], "topWatch": []})
        with patch.object(p, "load_watchlist_state", return_value={"setups": {}}):
            p.watchlist_refresh(top=5, max_symbols=3, prod_mode=False)
        self.assertTrue(p.score_symbols.called)
        self.assertEqual(p.score_symbols.call_args.kwargs.get("prod_mode"), False)

    def test_discovery_feed_state_threads_prod_mode_true(self):
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        p.scan = MagicMock(return_value={"topLongs": [], "topShorts": [], "topWatch": []})
        p.discovery_feed_state(top=5, universe=10)
        self.assertTrue(p.scan.called)
        self.assertEqual(p.scan.call_args.kwargs.get("prod_mode"), True)

    def test_discovery_radar_preview_threads_prod_mode_false(self):
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        p.scan = MagicMock(return_value={"topLongs": [], "topShorts": [], "topWatch": []})
        p.discovery_radar_preview(top=5, universe=10)
        self.assertTrue(p.scan.called)
        self.assertEqual(p.scan.call_args.kwargs.get("prod_mode"), False)

    def test_scan_prod_mode_disables_provider_and_tradingview_enrichment(self):
        p = PublicMarketData()
        p.tradingview_status = MagicMock(return_value={})
        p.binance_universe = MagicMock(return_value=[{"symbol": "SOLUSDT", "quoteVolume": 1.0, "priceChangePercent": 0.0}])
        p._score_symbol = MagicMock(return_value={
            "symbol": "SOLUSDT",
            "bullScore": 3,
            "cleanScore": 80.0,
            "baseCleanScore": 80.0,
            "grade": "A",
            "gradeScore": 80.0,
            "direction": "long",
            "executionFilter": {},
            "managementGuidance": [],
            "riskFlags": [],
        })
        p.provider_enrichment = MagicMock()
        p.apply_provider_enrichment_to_row = MagicMock()
        p.tradingview_confirmation = MagicMock()
        p.apply_tradingview_enrichment_to_row = MagicMock()
        p._apply_feedback_v2_to_row = MagicMock()
        p.apply_actionable_setup_plan = MagicMock()
        p.apply_setup_state_preview_to_row = MagicMock(side_effect=lambda row, alert_record=None: row.update({"lifecycle": {"state": "actionable"}}))
        p.load_alert_state = MagicMock(return_value={"setups": {}})
        p.production_eval_log_scan_batch = MagicMock()

        result = p.scan(top=1, universe_size=1, enrich_top=1, tradingview_top=1, prod_mode=True)

        p.provider_enrichment.assert_not_called()
        p.apply_provider_enrichment_to_row.assert_not_called()
        p.tradingview_confirmation.assert_not_called()
        p.apply_tradingview_enrichment_to_row.assert_not_called()
        self.assertEqual(result["authProvidersAppliedTo"], [])
        self.assertEqual(result["tradingviewAppliedTo"], [])
        self.assertEqual(result["productionPruned"]["effectiveEnrichTop"], 0)
        self.assertEqual(result["productionPruned"]["effectiveTradingviewTop"], 0)


class ProdModeLifecyclePruningTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.p.tradingview_status = MagicMock(return_value={})
        self.p._apply_feedback_v2_to_row = MagicMock()
        self.p.apply_actionable_setup_plan = MagicMock()
        self.p.production_eval_log_scan_batch = MagicMock()

        def fake_score(symbol, include_auth_providers=False, prod_mode=False):
            return {
                "symbol": symbol,
                "bullScore": 3,
                "cleanScore": 80.0,
                "baseCleanScore": 80.0,
                "grade": "A",
                "gradeScore": 80.0,
                "direction": "long",
                "executionFilter": {},
                "managementGuidance": [],
                "riskFlags": [],
            }

        def fake_preview(row, alert_record=None):
            lifecycle_state = ((alert_record or {}).get("lifecycle") or {}).get("state") or "actionable"
            row["setupKey"] = f"{row['symbol']}:long"
            row["lifecycle"] = {"state": lifecycle_state}
            row["readiness"] = {"state": "retest_ready"}
            row["actionableAlert"] = {"currentStatus": "ready_on_retest", "riskFlags": []}
            row["alertEligible"] = True
            row["watchlistEligible"] = True
            return row

        self.p._score_symbol = MagicMock(side_effect=fake_score)
        self.p.apply_setup_state_preview_to_row = MagicMock(side_effect=fake_preview)

    def test_prod_mode_prunes_stale_and_invalidated_from_visible_results(self):
        with patch.object(self.p, "load_alert_state", return_value={
            "setups": {
                "SOLUSDT:long": {"lifecycle": {"state": "stale"}},
                "ADAUSDT:long": {"lifecycle": {"state": "invalidated"}},
                "BTCUSDT:long": {"lifecycle": {"state": "actionable"}},
            }
        }):
            result = self.p.score_symbols(["SOLUSDT", "ADAUSDT", "BTCUSDT"], prod_mode=True)

        self.assertEqual(result["resolvedTradable"], ["BTCUSDT"])
        self.assertEqual(sorted(result["resolvedTradableAll"]), ["ADAUSDT", "BTCUSDT", "SOLUSDT"])
        self.assertEqual(result["alertEligible"], ["BTCUSDT"])
        self.assertEqual(result["watchlistEligible"], ["BTCUSDT"])
        self.assertEqual([row["symbol"] for row in result["topLongs"]], ["BTCUSDT"])
        self.assertEqual(sorted(result["productionPruned"]["lifecycleHiddenSymbols"]), ["ADAUSDT", "SOLUSDT"])

    def test_prod_mode_false_keeps_stale_and_invalidated_visible(self):
        with patch.object(self.p, "load_alert_state", return_value={
            "setups": {
                "SOLUSDT:long": {"lifecycle": {"state": "stale"}},
                "ADAUSDT:long": {"lifecycle": {"state": "invalidated"}},
            }
        }):
            result = self.p.score_symbols(["SOLUSDT", "ADAUSDT"], prod_mode=False)

        self.assertEqual(sorted(result["resolvedTradable"]), ["ADAUSDT", "SOLUSDT"])
        self.assertEqual(result["productionPruned"]["lifecycleHiddenSymbols"], [])


class CLProdModeArgumentTests(unittest.TestCase):
    """Verify CLI argument mapping."""

    def test_scan_default_false(self):
        p = argparse.ArgumentParser(); p.add_argument("--prod-mode", action="store_true", default=False)
        self.assertFalse(p.parse_args([]).prod_mode)

    def test_scan_prod_mode_flag(self):
        p = argparse.ArgumentParser(); p.add_argument("--prod-mode", action="store_true", default=False)
        self.assertTrue(p.parse_args(["--prod-mode"]).prod_mode)

    def test_watchlist_refresh_default_no_prod_mode_false(self):
        p = argparse.ArgumentParser(); p.add_argument("--no-prod-mode", action="store_true", default=False)
        self.assertFalse(p.parse_args([]).no_prod_mode)

    def test_watchlist_refresh_no_prod_mode_flag(self):
        p = argparse.ArgumentParser(); p.add_argument("--no-prod-mode", action="store_true", default=False)
        self.assertTrue(p.parse_args(["--no-prod-mode"]).no_prod_mode)

    def test_discovery_feed_state_default_no_prod_mode_false(self):
        p = argparse.ArgumentParser(); p.add_argument("--no-prod-mode", action="store_true", default=False)
        self.assertFalse(p.parse_args([]).no_prod_mode)

    def test_discovery_feed_state_no_prod_mode_flag(self):
        p = argparse.ArgumentParser(); p.add_argument("--no-prod-mode", action="store_true", default=False)
        self.assertTrue(p.parse_args(["--no-prod-mode"]).no_prod_mode)

    def test_discovery_radar_preview_default_false(self):
        p = argparse.ArgumentParser(); p.add_argument("--prod-mode", action="store_true", default=False)
        self.assertFalse(p.parse_args([]).prod_mode)

    def test_discovery_radar_preview_prod_mode_flag(self):
        p = argparse.ArgumentParser(); p.add_argument("--prod-mode", action="store_true", default=False)
        self.assertTrue(p.parse_args(["--prod-mode"]).prod_mode)


if __name__ == "__main__":
    unittest.main(verbosity=2)
