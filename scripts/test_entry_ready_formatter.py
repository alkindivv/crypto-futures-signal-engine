import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from telegram_execution_cards import EntryReadyEvent, TelegramExecutionCardFormatter
from public_market_data import PublicMarketData


class TelegramExecutionCardFormatterTests(unittest.TestCase):
    def setUp(self):
        self.fmt = TelegramExecutionCardFormatter()

    def test_ready_in_zone_render_is_clean_and_operational(self):
        event = EntryReadyEvent(
            event_type="READY_IN_ZONE",
            symbol="HYPEUSDT",
            side="short",
            grade="A",
            entry_low=41.009,
            entry_high=41.224,
            stop_loss=41.72,
            tp1=40.553,
            tp2=40.273,
            tp3=39.865,
            current_price=41.04,
            trigger="Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah",
            invalidation="Batalkan short bila 15m close kuat di atas stop atau reclaim high lokal bertahan",
            risk_flags=["thin_depth"],
            crowding_note="Top traders are balanced — no extreme crowding detected.",
        )
        card, fallbacks = self.fmt.render(event)
        self.assertIn("HYPEUSDT SHORT", card)
        self.assertIn("ENTRY READY — LIMIT", card)
        self.assertIn("Action: Place SHORT limit inside 41.009 - 41.224. Do not chase above 41.224.", card)
        self.assertIn("Price is still failing to reclaim the entry area, keeping downside continuation valid.", card)
        self.assertNotIn("setupVersion", card)
        self.assertNotIn("readinessVersion", card)
        self.assertNotIn("deliveryState", card)
        self.assertEqual(fallbacks, [])
        self.assertLess(len(card), 900)

    def test_live_strict_render_uses_confirmed_wording(self):
        event = EntryReadyEvent(
            event_type="LIVE_STRICT",
            symbol="ADAUSDT",
            side="short",
            grade="A",
            entry_low=0.24775,
            entry_high=0.24878,
            stop_loss=0.25298,
            tp1=0.24619,
            tp2=0.24423,
            tp3=0.24227,
            confirmed_at="2026-04-18T18:55:00.824999+00:00",
            trigger="Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah",
            invalidation="Batalkan short bila 15m close kuat di atas stop",
            metadata={"contextVerdict": "The 5m retest failed and the 15m context stayed bearish."},
        )
        card, fallbacks = self.fmt.render(event)
        self.assertIn("ENTRY READY — CONFIRMED", card)
        self.assertIn("Confirmed: 01:55 WIB", card)
        self.assertIn("Action: SHORT setup confirmed. Execute only near 0.24775 - 0.24878. Skip if price is already extended.", card)
        self.assertIn("The 15m context remains aligned, keeping the confirmed setup valid.", card)
        self.assertEqual(fallbacks, [])
        self.assertLess(len(card), 900)

    def test_cancel_render_is_clear(self):
        event = EntryReadyEvent(
            event_type="CANCEL",
            symbol="RIVERUSDT",
            side="short",
            grade="A",
            entry_low=4.7345,
            entry_high=5.0251,
            stop_loss=5.9541,
            tp1=4.2916,
            tp2=3.738,
            tp3=3.1844,
            cancel_reason="price left the intended execution zone",
        )
        card, fallbacks = self.fmt.render(event)
        self.assertIn("SETUP INVALID", card)
        self.assertIn("Action: Cancel any pending SHORT order. Wait for a new setup.", card)
        self.assertEqual(fallbacks, [])
        self.assertLess(len(card), 900)

    def test_tp_display_reorders_long_targets_into_logical_sequence(self):
        event = EntryReadyEvent(
            event_type="READY_IN_ZONE",
            symbol="BOMEUSDT",
            side="long",
            grade="A",
            entry_low=0.0005837,
            entry_high=0.0006295,
            stop_loss=0.0005158,
            tp1=0.0008866,
            tp2=0.0007256,
            tp3=0.0007867,
            current_price=0.0006112,
            trigger="Eksekusi hanya jika hold area entry tetap kuat dan reclaim terjaga",
            risk_flags=["thin_depth"],
        )
        card, fallbacks = self.fmt.render(event)
        self.assertIn("TP1: 0.0007256 | TP2: 0.0007867 | TP3: 0.0008866", card)
        self.assertIn("tp_reordered_for_display", fallbacks)

    def test_missing_tp3_uses_open_fallback(self):
        event = EntryReadyEvent(
            event_type="READY_IN_ZONE",
            symbol="TESTUSDT",
            side="long",
            grade="B",
            entry_low=1.0,
            entry_high=1.1,
            stop_loss=0.95,
            tp1=1.2,
            tp2=1.3,
            tp3=None,
            current_price=1.05,
        )
        card, fallbacks = self.fmt.render(event)
        self.assertIn("TP3: open", card)
        self.assertIn("tp3_missing", fallbacks)


class EntryReadyPreviewIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.setup_ready = {
            "symbol": "HYPEUSDT",
            "side": "short",
            "grade": "A",
            "cleanScore": 99.3,
            "lifecycle": {"state": "actionable"},
            "readiness": {"state": "retest_ready"},
            "actionableAlert": {
                "currentPrice": 41.04,
                "entryZone": {"low": 41.009, "high": 41.224},
                "stopLoss": 41.72,
                "takeProfit": {"tp1": 40.553, "tp2": 40.273, "tp3": 39.865},
                "trigger": "Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah",
                "invalidation": "Batalkan short bila 15m close kuat di atas stop atau reclaim high lokal bertahan",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": ["thin_depth"],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced — no extreme crowding detected."},
            },
        }
        self.monitor_live = {
            "setupKey": "ADAUSDT:short",
            "symbol": "ADAUSDT",
            "side": "short",
            "grade": "A",
            "runtime": {"state": "armed", "changedAt": "2026-04-18T18:55:00.824999+00:00"},
            "actionableAlert": {
                "currentPrice": 0.2479,
                "entryZone": {"low": 0.24775, "high": 0.24878},
                "stopLoss": 0.25298,
                "takeProfit": {"tp1": 0.24619, "tp2": 0.24423, "tp3": 0.24227},
                "trigger": "Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah",
                "invalidation": "Batalkan short bila 15m close kuat di atas stop",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced — no extreme crowding detected."},
            },
            "finalConfirmation": {"state": "active", "confirmedAt": "2026-04-18T18:55:00.824999+00:00", "triggerReason": "5m retest failed"},
            "contextVerdict": "The 5m retest failed and the 15m context stayed bearish.",
            "inactiveReason": "ZONE_RECLAIMED",
        }

    def test_preview_returns_real_and_simulated_cards(self):
        self.p.load_watchlist_state = lambda: {"setups": {"HYPEUSDT:short": self.setup_ready}}
        self.p.load_alert_state = lambda: {"setups": {}}
        self.p.load_live_trigger_state = lambda: {"monitors": {"ADAUSDT:short": self.monitor_live}}
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {"trustLive": True, "derivedStatus": "healthy", "reason": None}
        preview = self.p.entry_ready_preview(limit=3, include_simulated_live=True, include_simulated_cancel=True)
        cards = preview.get("cards", [])
        self.assertEqual(len(cards), 3)
        self.assertEqual(cards[0]["eventType"], "READY_IN_ZONE")
        self.assertEqual(cards[1]["eventType"], "LIVE_STRICT")
        self.assertEqual(cards[2]["eventType"], "CANCEL")
        self.assertTrue(all(card.get("charCount", 0) < 900 for card in cards))


class Batch1ACorrectnessTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def simulated_monitor(self, fc_state="active", runtime_state="armed"):
        return {
            "setupKey": "ADAUSDT:short",
            "symbol": "ADAUSDT",
            "side": "short",
            "grade": "A",
            "runtime": {"state": runtime_state, "changedAt": "2026-04-18T18:55:00.824999+00:00"},
            "actionableAlert": {
                "currentPrice": 0.2479,
                "entryZone": {"low": 0.24775, "high": 0.24878},
                "stopLoss": 0.25298,
                "takeProfit": {"tp1": 0.24619, "tp2": 0.24423, "tp3": 0.24227},
                "trigger": "Eksekusi hanya jika reclaim area entry gagal dan candle 5m/15m kembali lemah",
                "invalidation": "Batalkan short bila 15m close kuat di atas stop",
                "executionNote": "Default management",
                "doNotChase": False,
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced — no extreme crowding detected."},
            },
            "finalConfirmation": {
                "state": fc_state,
                "confirmedAt": "2026-04-18T18:55:00.824999+00:00",
                "triggerReason": "5m retest failed",
            },
        }

    def test_entry_ready_simulated_live_event_reset_state_returns_none(self):
        monitor = self.simulated_monitor(fc_state="reset", runtime_state="armed")
        self.p.load_watchlist_state = lambda: {"setups": {}}
        self.p.load_live_trigger_state = lambda: {"monitors": {monitor["setupKey"]: monitor}}
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {"trustLive": True, "derivedStatus": "healthy", "reason": None}
        self.assertIsNone(self.p.entry_ready_simulated_live_event())

    def test_entry_ready_simulated_live_event_inactive_runtime_returns_none(self):
        monitor = self.simulated_monitor(fc_state="active", runtime_state="inactive")
        self.p.load_watchlist_state = lambda: {"setups": {}}
        self.p.load_live_trigger_state = lambda: {"monitors": {monitor["setupKey"]: monitor}}
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {"trustLive": True, "derivedStatus": "healthy", "reason": None}
        self.assertIsNone(self.p.entry_ready_simulated_live_event())

    def test_entry_ready_simulated_live_event_healthy_emits_event(self):
        monitor = self.simulated_monitor(fc_state="active", runtime_state="armed")
        self.p.load_watchlist_state = lambda: {"setups": {}}
        self.p.load_live_trigger_state = lambda: {"monitors": {monitor["setupKey"]: monitor}}
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {"trustLive": True, "derivedStatus": "healthy", "reason": None}
        event = self.p.entry_ready_simulated_live_event()
        self.assertIsNotNone(event)
        self.assertEqual(event.event_type, "LIVE_STRICT")
        self.assertTrue(event.simulated)

    def test_normalize_risk_flag_strips_whitespace(self):
        self.assertEqual(self.p.normalize_risk_flag(" liquidation_zone"), "liquidation_zone")
        self.assertEqual(self.p.normalize_risk_flag("liquidation_zone"), "liquidation_zone")

    def test_setup_state_version_normalization_integration(self):
        base_row = {
            "symbol": "TESTUSDT",
            "direction": "long",
            "grade": "A",
            "bullScore": 6,
            "cleanScore": 96.0,
            "disqualified": False,
            "executionFilter": {"riskFlags": ["liquidation_zone"]},
        }
        whitespace_row = {
            **base_row,
            "executionFilter": {"riskFlags": [" liquidation_zone"]},
        }
        self.assertEqual(self.p.setup_state_version(base_row), self.p.setup_state_version(whitespace_row))

    def test_whitespace_variant_requires_normalization_to_match_execution_critical_flags(self):
        raw_flag = " liquidation_zone"
        self.assertNotIn(raw_flag, self.p.EXECUTION_CRITICAL_RISK_FLAGS)
        self.assertIn(self.p.normalize_risk_flag(raw_flag), self.p.EXECUTION_CRITICAL_RISK_FLAGS)


if __name__ == "__main__":
    unittest.main()
