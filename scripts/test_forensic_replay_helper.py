import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from public_market_data import PublicMarketData


class ForensicReplayHelperTests(unittest.TestCase):
    def setUp(self):
        self.p = PublicMarketData()

    def test_forensic_replay_returns_runtime_entry_ready_and_reset_events(self):
        live_state = {
            "monitors": {
                "XRPUSDT:long": {
                    "setupKey": "XRPUSDT:long",
                    "symbol": "XRPUSDT",
                    "side": "long",
                    "setupVersion": "873a7b59711f",
                    "readinessVersion": "abe2ca77620e",
                    "runtime": {"state": "inactive", "reasonCode": "SETUP_CHANGED_INACTIVE"},
                    "deliveryLedger": {
                        "trigger|confirmed|XRPUSDT:long|ad2ae445348f|1776858899999|ws": {
                            "status": "sent",
                            "preparedAt": "2026-04-22T11:55:01.769761+00:00",
                            "sentAt": "2026-04-22T11:55:21.456343+00:00",
                            "message": "confirmed",
                        }
                    },
                    "finalConfirmation": {
                        "state": "reset",
                        "setupVersion": "ad2ae445348f",
                        "readinessVersion": "5508dc6c03f0",
                        "confirmedAt": "2026-04-22T11:55:01.769743+00:00",
                        "resetReason": "setup_version_changed",
                        "resetAt": "2026-04-22T15:14:19+00:00",
                    },
                }
            }
        }
        watchlist_state = {
            "setups": {
                "XRPUSDT:long": {
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "873a7b59711f",
                    "readinessVersion": "abe2ca77620e",
                    "lifecycle": {"state": "notified"},
                    "readiness": {"state": "retest_ready"},
                }
            }
        }
        alert_state = {
            "setups": {
                "XRPUSDT:long": {
                    "delivery": {"state": "sent"}
                }
            }
        }
        entry_ready_state = {
            "ledger": {
                "308ff0b4fab94dfa": {
                    "eventType": "LIVE_STRICT",
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "ad2ae445348f",
                    "readinessVersion": "0d6c56295a26",
                    "lastAction": "sent",
                    "firstSeenAt": "2026-04-22T12:21:16.213341+00:00",
                    "lastDeliveredAt": "2026-04-22T12:21:16.213341+00:00",
                },
                "8e8c932bfb246b51": {
                    "eventType": "READY_IN_ZONE",
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "ad2ae445348f",
                    "readinessVersion": "431b75c94252",
                    "lastAction": "sent",
                    "firstSeenAt": "2026-04-22T12:27:02.583659+00:00",
                    "lastDeliveredAt": "2026-04-22T12:27:02.583659+00:00",
                },
                "3e40031a431d8fc9": {
                    "eventType": "READY_IN_ZONE",
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "873a7b59711f",
                    "readinessVersion": "abe2ca77620e",
                    "lastAction": "sent",
                    "firstSeenAt": "2026-04-22T12:31:37.091664+00:00",
                    "lastDeliveredAt": "2026-04-22T12:31:37.091664+00:00",
                },
            }
        }

        with TemporaryDirectory() as td:
            log_path = Path(td) / "periodic_delivery_log.jsonl"
            rows = [
                {
                    "renderedHash": "308ff0b4fab94dfa",
                    "renderedAt": "2026-04-22T12:20:56.986190+00:00",
                    "attemptedAt": "2026-04-22T12:21:16.215762+00:00",
                    "transportMetadata": {"messageIds": ["4245"]},
                },
                {
                    "renderedHash": "8e8c932bfb246b51",
                    "renderedAt": "2026-04-22T12:26:04.526341+00:00",
                    "attemptedAt": "2026-04-22T12:27:02.586024+00:00",
                    "transportMetadata": {"messageIds": ["4256"]},
                },
            ]
            with log_path.open("w", encoding="utf-8") as handle:
                for row in rows:
                    handle.write(json.dumps(row) + "\n")

            self.p.load_live_trigger_state = lambda: live_state
            self.p.load_watchlist_state = lambda: watchlist_state
            self.p.load_alert_state = lambda: alert_state
            self.p.load_entry_ready_delivery_state = lambda: entry_ready_state

            with patch("public_market_data.PERIODIC_DELIVERY_LOG_PATH", log_path):
                replay = self.p.forensic_replay(
                    setup_key="XRPUSDT:long",
                    from_iso="2026-04-22T11:50:00+00:00",
                    to_iso="2026-04-22T15:20:00+00:00",
                )

        self.assertEqual(replay["setupKey"], "XRPUSDT:long")
        self.assertEqual(replay["current"]["finalConfirmationResetAt"], "2026-04-22T15:14:19+00:00")
        self.assertEqual(replay["entryReadyEvents"][0]["lane"], "LIVE")
        self.assertEqual(replay["entryReadyEvents"][1]["lane"], "READY")
        self.assertEqual(replay["entryReadyEvents"][1]["fcActive"], False)
        self.assertEqual(replay["entryReadyEvents"][1]["actionableNow"], True)
        self.assertEqual(replay["entryReadyEvents"][0]["messageIds"], ["4245"])
        self.assertEqual(replay["runtimeConfirmedEvents"][0]["eventType"], "TRIGGER_CONFIRMED")
        replay_types = [row["eventType"] for row in replay["replay"]]
        self.assertIn("FINAL_CONFIRMATION_ACTIVE", replay_types)
        self.assertIn("TRIGGER_CONFIRMED", replay_types)
        self.assertIn("FINAL_CONFIRMATION_RESET", replay_types)

    def test_forensic_replay_respects_time_window(self):
        self.p.load_live_trigger_state = lambda: {
            "monitors": {
                "XRPUSDT:long": {
                    "setupKey": "XRPUSDT:long",
                    "runtime": {"state": "inactive"},
                    "finalConfirmation": {
                        "state": "reset",
                        "setupVersion": "ad2ae445348f",
                        "readinessVersion": "5508dc6c03f0",
                        "confirmedAt": "2026-04-22T11:55:01+00:00",
                        "resetAt": "2026-04-22T15:14:19+00:00",
                        "resetReason": "zone_reclaimed",
                    },
                    "deliveryLedger": {},
                }
            }
        }
        self.p.load_watchlist_state = lambda: {"setups": {"XRPUSDT:long": {"lifecycle": {"state": "notified"}, "readiness": {"state": "retest_ready"}}}}
        self.p.load_alert_state = lambda: {"setups": {"XRPUSDT:long": {"delivery": {"state": "sent"}}}}
        self.p.load_entry_ready_delivery_state = lambda: {
            "ledger": {
                "older": {
                    "eventType": "READY_IN_ZONE",
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "old",
                    "readinessVersion": "old",
                    "lastDeliveredAt": "2026-04-22T10:00:00+00:00",
                },
                "inside": {
                    "eventType": "READY_IN_ZONE",
                    "setupKey": "XRPUSDT:long",
                    "setupVersion": "new",
                    "readinessVersion": "new",
                    "lastDeliveredAt": "2026-04-22T12:27:02+00:00",
                },
            }
        }

        with TemporaryDirectory() as td:
            log_path = Path(td) / "periodic_delivery_log.jsonl"
            log_path.write_text("", encoding="utf-8")
            with patch("public_market_data.PERIODIC_DELIVERY_LOG_PATH", log_path):
                replay = self.p.forensic_replay(
                    setup_key="XRPUSDT:long",
                    from_iso="2026-04-22T12:00:00+00:00",
                    to_iso="2026-04-22T12:35:00+00:00",
                )

        self.assertEqual(len(replay["entryReadyEvents"]), 1)
        self.assertEqual(replay["entryReadyEvents"][0]["deliveryKey"], "inside")
        replay_types = [row["eventType"] for row in replay["replay"]]
        self.assertNotIn("FINAL_CONFIRMATION_RESET", replay_types)


if __name__ == "__main__":
    unittest.main()
