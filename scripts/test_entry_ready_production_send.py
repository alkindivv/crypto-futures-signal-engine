import copy
import sys
import unittest

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData


class EntryReadyProductionSendTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()
        self.delivery_state = self.p.entry_ready_empty_state()
        self.watchlist = {"setups": {}}
        self.live_state = {"monitors": {}}
        self.sent_messages = []
        self.periodic_logs = []
        self.refresh_calls = []

        self.p.load_watchlist_state = lambda: copy.deepcopy(self.watchlist)
        self.p.load_live_trigger_state = lambda: copy.deepcopy(self.live_state)
        self.p.load_alert_state = lambda: {"setups": {}}
        self.p.load_entry_ready_delivery_state = lambda: copy.deepcopy(self.delivery_state)
        self.p.save_entry_ready_delivery_state = self._save_delivery_state
        self.p._periodic_log_delivery = lambda **kwargs: self.periodic_logs.append(copy.deepcopy(kwargs))
        self.p._send_long_message = self._fake_send_long_message
        self.p.watchlist_refresh = self._watchlist_refresh
        self.p.scan = self._scan_refresh
        # Mock live_trigger_live_trust so trustLive=True in tests (engine not running in test env)
        self.p.live_trigger_live_trust = lambda *args, **kwargs: {
            "trustLive": True, "blockLive": False, "derivedStatus": "healthy",
            "reason": None, "expectedSymbols": 1, "heartbeatFresh": True, "wsFresh": True,
            "heartbeatStaleSeconds": 90, "wsStaleSeconds": 60,
            "lastHeartbeatAt": None, "lastWsMessageAt": None,
            "lastErrorAt": None, "lastRecoveredAt": None, "recoveryState": "none",
        }

    def _save_delivery_state(self, state):
        self.delivery_state = copy.deepcopy(state)

    def _fake_send_long_message(self, message, channel="telegram", target="851120836"):
        self.sent_messages.append({
            "channel": channel,
            "target": target,
            "message": message,
        })
        return True, [str(len(self.sent_messages))], None

    def _watchlist_refresh(self, **kwargs):
        self.refresh_calls.append({"kind": "watchlist", **kwargs})
        return {"ok": True}

    def _scan_refresh(self, **kwargs):
        self.refresh_calls.append({"kind": "discovery", **kwargs})
        return {"ok": True}

    def ready_setup(self, symbol="TESTUSDT", side="long"):
        return {
            "symbol": symbol,
            "side": side,
            "grade": "A",
            "cleanScore": 99.0,
            "setupVersion": "sv1",
            "readinessVersion": "rv1",
            "active": True,
            "lifecycle": {"state": "actionable"},
            "readiness": {"state": "retest_ready"},
            "actionableAlert": {
                "currentPrice": 10.5 if side == "long" else 9.5,
                "entryZone": {"low": 10.0, "high": 11.0} if side == "long" else {"low": 9.0, "high": 10.0},
                "stopLoss": 9.5 if side == "long" else 10.5,
                "takeProfit": {"tp1": 11.5 if side == "long" else 8.5, "tp2": 12.0 if side == "long" else 8.0, "tp3": 12.5 if side == "long" else 7.5},
                "trigger": "hold and reclaim" if side == "long" else "retest failed",
                "invalidation": "break below stop" if side == "long" else "break above stop",
                "executionNote": "Default management",
                "doNotChase": False,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
                "crowdingSignals": {"crowdingNote": "Top traders are balanced - no extreme crowding detected."},
            },
        }

    def live_monitor(self, symbol="LIVEUSDT", side="short", state="active"):
        return {
            "setupKey": f"{symbol}:{side}",
            "symbol": symbol,
            "side": side,
            "grade": "A",
            "finalConfirmation": {
                "state": state,
                "confirmedAt": "2026-04-20T15:30:00+00:00",
                "triggerReason": "5m retest failed",
            },
            "contextVerdict": "The 15m context remains aligned.",
        }

    def test_ready_in_zone_first_send_second_suppressed(self):
        self.watchlist["setups"] = {"READYUSDT:long": self.ready_setup(symbol="READYUSDT", side="long")}

        first = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)
        second = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)

        self.assertTrue(first["sent"])
        self.assertFalse(second["sent"])
        self.assertEqual(second["reason"], "duplicate_suppressed")
        self.assertEqual(second["suppressedCount"], 1)
        self.assertEqual(len(self.sent_messages), 1)
        self.assertEqual(len(self.refresh_calls), 2)
        self.assertEqual(self.refresh_calls[0]["kind"], "watchlist")
        self.assertIn("ENTRY READY — LIMIT", self.sent_messages[0]["message"])

    def test_live_strict_first_send_second_suppressed(self):
        self.watchlist["setups"] = {"LIVEUSDT:short": self.ready_setup(symbol="LIVEUSDT", side="short")}
        self.live_state["monitors"] = {"LIVEUSDT:short": self.live_monitor(symbol="LIVEUSDT", side="short", state="active")}

        first = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)
        second = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)

        self.assertTrue(first["sent"])
        self.assertFalse(second["sent"])
        self.assertEqual(second["reason"], "duplicate_suppressed")
        self.assertEqual(second["suppressedCount"], 1)
        self.assertEqual(len(self.sent_messages), 1)
        self.assertEqual(len(self.refresh_calls), 2)
        self.assertIn("ENTRY READY — CONFIRMED", self.sent_messages[0]["message"])

    def test_failure_modes_do_not_send(self):
        outside = self.ready_setup(symbol="OUTSIDEUSDT", side="long")
        outside["actionableAlert"]["currentPrice"] = 11.2
        self.watchlist["setups"] = {"OUTSIDEUSDT:long": outside}
        result_outside = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)
        self.assertFalse(result_outside["sent"])
        self.assertEqual(result_outside["reason"], "no_reply")

        self.delivery_state = self.p.entry_ready_empty_state()
        self.sent_messages.clear()
        self.refresh_calls.clear()
        dnc = self.ready_setup(symbol="DNCUSDT", side="long")
        dnc["actionableAlert"]["doNotChase"] = True
        self.watchlist["setups"] = {"DNCUSDT:long": dnc}
        result_dnc = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)
        self.assertFalse(result_dnc["sent"])
        self.assertEqual(result_dnc["reason"], "no_reply")

        self.delivery_state = self.p.entry_ready_empty_state()
        self.sent_messages.clear()
        self.refresh_calls.clear()
        live_inactive = self.ready_setup(symbol="NOACTIVEUSDT", side="short")
        live_inactive["actionableAlert"]["currentPrice"] = 10.2
        self.watchlist["setups"] = {"NOACTIVEUSDT:short": live_inactive}
        self.live_state["monitors"] = {"NOACTIVEUSDT:short": self.live_monitor(symbol="NOACTIVEUSDT", side="short", state="pending")}
        result_live = self.p.automation_cycle_send(kind="watchlist", output_mode="entry-ready", channel="telegram", target="851120836", top=10)
        self.assertFalse(result_live["sent"])
        self.assertEqual(result_live["reason"], "no_reply")
        self.assertEqual(len(self.sent_messages), 0)


if __name__ == "__main__":
    unittest.main()
