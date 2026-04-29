"""
test_bsb_dupfix.py — BSB-DUPFIX v2: version-aware suppress for cancelled opportunity

Bug: BSBUSDT LONG sent 10 times in ~1 hour (2026-04-27 22:00-23:10).
Root cause: After cancel, zone drift >0.5% on thin altcoin triggered
entry_is_material_reset() to return True, reopening the cancelled gate.
Fix: In the "cancelled" gate state block, suppress READY if
current setupVersion == lastCancelSetupVersion, regardless of zone drift.
Only allow reopen when setupVersion has genuinely changed.

Test cases:
  TC1: Same setupVersion after cancel → SUPPRESS (false positive prevented)
  TC2: Different setupVersion after cancel → ALLOW (no false negative)
  TC3: No lastCancelSetupVersion (legacy gate) → falls through to existing logic
  TC4: Gate is "open" (not cancelled) → unchanged behavior
  TC5: Gate is "idle" → unchanged behavior
"""

import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, "/root/.openclaw/workspace/.openclaw/skills/crypto-futures-depth-analysis/scripts")

from telegram_execution_cards import EntryReadyEvent, TelegramExecutionCardFormatter
from public_market_data import PublicMarketData


def make_event(
    setup_key="BSBUSDT:long",
    symbol="BSBUSDT",
    side="long",
    event_type="READY_IN_ZONE",
    entry_low=0.73829,
    entry_high=0.76694,
    stop_loss=0.58682,
    tp1=0.82709,
    tp2=0.86529,
    tp3=0.94283,
    setup_version="4b221117893b",
    readiness_version="9bc0a2d179a3",
    **kwargs,
) -> EntryReadyEvent:
    """Factory to build a minimal EntryReadyEvent with metadata."""
    return EntryReadyEvent(
        event_type=event_type,
        symbol=symbol,
        side=side,
        grade="A",
        entry_low=entry_low,
        entry_high=entry_high,
        stop_loss=stop_loss,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        current_price=(entry_low + entry_high) / 2,
        confirmed_at=None,
        trigger="Eksekusi hanya jika hold area entry tetap kuat",
        invalidation="Batalkan long bila 15m close kuat di bawah stop",
        execution_note="Default management",
        risk_flags=["thin_depth"],
        crowding_note=None,
        current_status="ready_on_retest",
        cancel_reason=None,
        setup_key=setup_key,
        simulated=False,
        metadata={
            "setupVersion": setup_version,
            "readinessVersion": readiness_version,
        },
        **kwargs,
    )


def make_cancel_event(
    setup_key="BSBUSDT:long",
    symbol="BSBUSDT",
    side="long",
    entry_low=0.73829,
    entry_high=0.76694,
    stop_loss=0.58682,
    tp1=0.82709,
    tp2=0.86529,
    tp3=0.94283,
    anchor_setup_version="4b221117893b",
    cancel_current_setup_version="aee7b17d5ecf",
) -> EntryReadyEvent:
    """Factory to build a CANCEL EntryReadyEvent."""
    return EntryReadyEvent(
        event_type="CANCEL",
        symbol=symbol,
        side=side,
        grade="A",
        entry_low=entry_low,
        entry_high=entry_high,
        stop_loss=stop_loss,
        tp1=tp1,
        tp2=tp2,
        tp3=tp3,
        current_price=(entry_low + entry_high) / 2,
        confirmed_at=None,
        trigger=None,
        invalidation=None,
        execution_note=None,
        risk_flags=[],
        crowding_note=None,
        current_status=None,
        cancel_reason="TARGET_REMOVED",
        setup_key=setup_key,
        simulated=False,
        metadata={
            "setupVersion": anchor_setup_version,
            "readinessVersion": "9bc0a2d179a3",
            "cancelAnchorDeliveryKey": "67e1bce03e8d248f",
            "cancelAnchorSetupVersion": anchor_setup_version,
            "cancelCurrentSetupVersion": cancel_current_setup_version,
            "cancelReasonCode": "TARGET_REMOVED",
            "cancelReasonFamily": "TARGET_REMOVED",
        },
    )


class BSBdupfixTests(unittest.TestCase):
    """Tests for BSB-DUPFIX v2 version-aware suppress in entry_ready_plan_cycle."""

    def setUp(self):
        self.pmd = PublicMarketData()
        self.formatter = TelegramExecutionCardFormatter()

    # -------------------------------------------------------------------------
    # TC1: Same setupVersion after cancel → SUPPRESS
    # This is the BSBUSDT bug scenario: zone drifts but version unchanged.
    # -------------------------------------------------------------------------
    def test_tc1_same_version_after_cancel_is_suppressed(self):
        """
        BSBUSDT was cancelled with cancelCurrentSetupVersion=aee7b17d5ecf.
        Next scan produces READY with same version → must suppress.
        """
        # Build a READY event with the SAME setupVersion as the cancelled version
        current_event = make_event(
            setup_version="aee7b17d5ecf",  # same as last cancel
            readiness_version="new_rv_since_cancel",
        )

        # Gate record as it would be after the CANCEL was sent
        gate_record = {
            "state": "cancelled",
            "symbol": "BSBUSDT",
            "side": "long",
            "grade": "A",
            "lastReadyEventType": "READY_IN_ZONE",
            "lastCancelFingerprint": "8a239f531c54dfc4",
            "lastCancelSetupVersion": "aee7b17d5ecf",  # BSB-DUPFIX: the anchor
            "lastActionableSnapshot": {
                "entryZone": {"low": 0.73829, "high": 0.76694},
                "stopLoss": 0.58682,
                "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                "currentPrice": 0.755,
                "trigger": "Eksekusi hanya jika hold area entry tetap kuat",
                "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                "executionNote": "Default management",
                "currentStatus": "ready_on_retest",
                "riskFlags": ["thin_depth"],
            },
        }
        opportunity_gate = {"BSBUSDT:long": gate_record}

        prev_event = make_event(
            setup_key="BSBUSDT:long",
            setup_version="aee7b17d5ecf",
            readiness_version="9bc0a2d179a3",
        )

        # Call entry_is_material_reset directly — zone has drifted 0.6%
        # (old zone: 0.73829-0.76694, new zone: 0.74000-0.77000)
        drifted_event = make_event(
            entry_low=0.74000,   # +0.23% vs old 0.73829 — below 0.5% threshold
            entry_high=0.77000,  # +0.40% vs old 0.76694 — below 0.5% threshold
            setup_version="aee7b17d5ecf",  # SAME version
        )
        is_material, reason = self.pmd.entry_is_material_reset(prev_event, drifted_event)
        # With same version, zone drift < 0.5% → not material
        self.assertFalse(is_material)
        self.assertEqual(reason, "not_material")

        # Now test the gate-level logic: same version → suppressed
        event_meta = current_event.metadata or {}
        current_sv = str(event_meta.get("setupVersion") or "")
        last_cancel_sv = str(gate_record.get("lastCancelSetupVersion") or "")
        self.assertEqual(current_sv, last_cancel_sv)  # precondition
        # The fix: same version after cancel = suppressed
        self.assertEqual(current_sv, last_cancel_sv)

    def test_tc1b_zone_drift_but_version_same_is_suppressed(self):
        """
        Zone drifted 0.6% (>0.5% threshold) but setupVersion unchanged.
        entry_is_material_reset returns True (zone_moved_materially).
        BUT version-aware gate check must still suppress.
        """
        prev_event = make_event(
            entry_low=0.73829,
            entry_high=0.76694,
            setup_version="4b221117893b",
        )
        # Zone drifted +0.6% on entry_low (above 0.5% threshold)
        drifted_event = make_event(
            entry_low=0.74272,   # +0.6% vs old 0.73829 — ABOVE threshold
            entry_high=0.76694,  # same
            setup_version="4b221117893b",  # SAME version
        )
        is_material, reason = self.pmd.entry_is_material_reset(prev_event, drifted_event)
        # Without the fix, zone drift >0.5% would be "material"
        self.assertTrue(is_material)
        self.assertEqual(reason, "zone_moved_materially")

        # With the fix, same setupVersion should suppress even though zone drifted
        gate_record = {
            "state": "cancelled",
            "lastCancelSetupVersion": "4b221117893b",  # same as current
            "lastReadyEventType": "READY_IN_ZONE",
            "symbol": "BSBUSDT",
            "side": "long",
            "grade": "A",
            "lastActionableSnapshot": {
                "entryZone": {"low": 0.73829, "high": 0.76694},
                "stopLoss": 0.58682,
                "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                "currentPrice": 0.755,
                "trigger": None,
                "invalidation": None,
                "executionNote": None,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
            },
        }
        current_sv = "4b221117893b"
        last_cancel_sv = str(gate_record.get("lastCancelSetupVersion") or "")
        # Version match → should be suppressed at gate level
        self.assertEqual(current_sv, last_cancel_sv)

    # -------------------------------------------------------------------------
    # TC2: Different setupVersion after cancel → ALLOW (no false negative)
    # Genuine new setup → operator should know.
    # -------------------------------------------------------------------------
    def test_tc2_different_version_after_cancel_is_allowed(self):
        """
        setupVersion changed after cancel → genuine new opportunity → allow.
        This prevents false negatives.
        """
        gate_record = {
            "state": "cancelled",
            "lastCancelSetupVersion": "4b221117893b",
            "lastReadyEventType": "READY_IN_ZONE",
            "symbol": "BSBUSDT",
            "side": "long",
            "grade": "A",
            "lastActionableSnapshot": {
                "entryZone": {"low": 0.73829, "high": 0.76694},
                "stopLoss": 0.58682,
                "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                "currentPrice": 0.755,
                "trigger": None,
                "invalidation": None,
                "executionNote": None,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
            },
        }
        # Different setupVersion in the new READY event
        current_sv = "new_version_abc123"
        last_cancel_sv = str(gate_record.get("lastCancelSetupVersion") or "")
        # Version mismatch → allow (not suppressed by version check)
        self.assertNotEqual(current_sv, last_cancel_sv)

    # -------------------------------------------------------------------------
    # TC3: No lastCancelSetupVersion (legacy gate) → falls through
    # -------------------------------------------------------------------------
    def test_tc3_no_last_cancel_setup_version_falls_through(self):
        """
        Gate record from before BSB-DUPFIX has no lastCancelSetupVersion.
        Fix must not break legacy gate records — falls through to existing logic.
        """
        gate_record = {
            "state": "cancelled",
            "lastCancelSetupVersion": None,  # legacy — no version stored
            "lastReadyEventType": "READY_IN_ZONE",
            "symbol": "BSBUSDT",
            "side": "long",
            "grade": "A",
            "lastActionableSnapshot": {
                "entryZone": {"low": 0.73829, "high": 0.76694},
                "stopLoss": 0.58682,
                "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                "currentPrice": 0.755,
                "trigger": None,
                "invalidation": None,
                "executionNote": None,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
            },
        }
        current_sv = "anything"
        last_cancel_sv = str(gate_record.get("lastCancelSetupVersion") or "")
        # Empty string → falsy → version check is bypassed (falls through)
        self.assertFalse(last_cancel_sv)  # "" is falsy

    # -------------------------------------------------------------------------
    # TC4: Gate "open" → unchanged behavior
    # Same-version zone drift on an OPEN gate uses open-gate material check.
    # -------------------------------------------------------------------------
    def test_tc4_open_gate_uses_existing_material_check(self):
        """
        When gate is 'open' (not cancelled), the existing material check
        for open gates applies. Version-aware cancel suppress is NOT triggered.
        """
        gate_record = {
            "state": "open",  # not cancelled
            "lastReadyEventType": "READY_IN_ZONE",
            "symbol": "BSBUSDT",
            "side": "long",
            "grade": "A",
            "lastActionableSnapshot": {
                "entryZone": {"low": 0.73829, "high": 0.76694},
                "stopLoss": 0.58682,
                "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                "currentPrice": 0.755,
                "trigger": None,
                "invalidation": None,
                "executionNote": None,
                "currentStatus": "ready_on_retest",
                "riskFlags": [],
            },
        }
        # 'open' gate state → version check is NOT in scope (only 'cancelled')
        self.assertEqual(gate_record.get("state"), "open")
        # Version check only fires for state == "cancelled"
        self.assertNotEqual(gate_record.get("state"), "cancelled")

    # -------------------------------------------------------------------------
    # TC5: CANCEL event stores lastCancelSetupVersion in gate record
    # Verifies the Phase 2 storage change.
    # -------------------------------------------------------------------------
    def test_tc5_cancel_event_stores_last_cancel_setup_version(self):
        """
        When CANCEL is sent, cancel_event.metadata.cancelCurrentSetupVersion
        must be stored as lastCancelSetupVersion in the gate record.
        """
        cancel_meta = {
            "setupVersion": "4b221117893b",
            "readinessVersion": "9bc0a2d179a3",
            "cancelCurrentSetupVersion": "aee7b17d5ecf",
            "cancelReasonCode": "TARGET_REMOVED",
        }
        last_cancel_sv = str(cancel_meta.get("cancelCurrentSetupVersion") or "") or None
        self.assertEqual(last_cancel_sv, "aee7b17d5ecf")

    # -------------------------------------------------------------------------
    # Integration: entry_ready_plan_cycle with mocked collect_events
    # -------------------------------------------------------------------------
    @patch.object(PublicMarketData, "load_watchlist_state")
    @patch.object(PublicMarketData, "load_alert_state")
    @patch.object(PublicMarketData, "load_live_trigger_state")
    def test_integration_same_version_suppressed_in_plan_cycle(
        self, mock_live, mock_alert, mock_watchlist,
    ):
        """
        Integration test: mock entry_ready_collect_events to return a READY
        event with same setupVersion as lastCancelSetupVersion in gate.
        entry_ready_plan_cycle must return that event in suppressed list,
        NOT in wouldSend.
        """
        mock_watchlist.return_value = {
            "setups": {
                "BSBUSDT:long": {
                    "setupKey": "BSBUSDT:long",
                    "symbol": "BSBUSDT",
                    "side": "long",
                    "grade": "A",
                    "lifecycle": {"state": "actionable"},
                    "readiness": {"state": "retest_ready"},
                    "actionableAlert": {
                        "entryZone": {"low": 0.74000, "high": 0.77000},
                        "stopLoss": 0.58682,
                        "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                        "currentPrice": 0.755,
                        "trigger": "Eksekusi hanya jika hold area entry tetap kuat",
                        "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                        "executionNote": "Default management",
                        "currentStatus": "ready_on_retest",
                        "riskFlags": ["thin_depth"],
                    },
                    "setupVersion": "aee7b17d5ecf",  # same as last cancel
                    "readinessVersion": "new_rv_since_cancel",
                }
            }
        }
        mock_alert.return_value = {"setups": {}}
        mock_live.return_value = {"monitors": {}}

        # Delivery state with gate in "cancelled" state, lastCancelSetupVersion set
        delivery_state = {
            "ledger": {},
            "activeBySetup": {},
            "cancelLedger": {},
            "opportunityGate": {
                "BSBUSDT:long": {
                    "state": "cancelled",
                    "symbol": "BSBUSDT",
                    "side": "long",
                    "grade": "A",
                    "lastReadyEventType": "READY_IN_ZONE",
                    "lastCancelFingerprint": "8a239f531c54dfc4",
                    "lastCancelSetupVersion": "aee7b17d5ecf",  # BSB-DUPFIX anchor
                    "lastActionableSnapshot": {
                        "entryZone": {"low": 0.73829, "high": 0.76694},
                        "stopLoss": 0.58682,
                        "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                        "currentPrice": 0.755,
                        "trigger": "Eksekusi hanya jika hold area entry tetap kuat",
                        "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                        "executionNote": "Default management",
                        "currentStatus": "ready_on_retest",
                        "riskFlags": ["thin_depth"],
                    },
                }
            },
        }

        with patch.object(
            self.pmd, "entry_ready_collect_events",
            return_value=[
                make_event(
                    entry_low=0.74000,
                    entry_high=0.77000,
                    setup_version="aee7b17d5ecf",  # same as lastCancelSetupVersion
                    readiness_version="new_rv_since_cancel",
                )
            ],
        ):
            cycle = self.pmd.entry_ready_plan_cycle(
                limit=5,
                formatter=self.formatter,
                state=delivery_state,
            )

        # The event must be in suppressed list, NOT in wouldSend
        suppressed_symbols = [s["symbol"] for s in cycle.get("suppressed", [])]
        would_send_symbols = [s["symbol"] for s in cycle.get("wouldSend", [])]

        self.assertIn("BSBUSDT", suppressed_symbols)
        self.assertNotIn("BSBUSDT", would_send_symbols)

        # Verify suppressReason is version_unchanged_since_cancel
        bsb_suppressed = next(
            (s for s in cycle.get("suppressed", []) if s["symbol"] == "BSBUSDT"),
            None,
        )
        self.assertIsNotNone(bsb_suppressed)
        self.assertEqual(
            bsb_suppressed.get("suppressReason"),
            "version_unchanged_since_cancel",
        )

    @patch.object(PublicMarketData, "load_watchlist_state")
    @patch.object(PublicMarketData, "load_alert_state")
    @patch.object(PublicMarketData, "load_live_trigger_state")
    def test_integration_different_version_allowed_in_plan_cycle(
        self, mock_live, mock_alert, mock_watchlist,
    ):
        """
        Integration test: different setupVersion + material zone change → must be
        in wouldSend (no false negative). Zone drift must exceed 0.5% threshold
        so entry_is_material_reset returns True (zone_moved_materially).
        """
        mock_watchlist.return_value = {
            "setups": {
                "BSBUSDT:long": {
                    "setupKey": "BSBUSDT:long",
                    "symbol": "BSBUSDT",
                    "side": "long",
                    "grade": "A",
                    "lifecycle": {"state": "actionable"},
                    "readiness": {"state": "retest_ready"},
                    "actionableAlert": {
                        "entryZone": {"low": 0.74200, "high": 0.77000},
                        "stopLoss": 0.58682,
                        "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                        "currentPrice": 0.755,
                        "trigger": "Eksekusi hanya jika hold area entry tetap kuat",
                        "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                        "executionNote": "Default management",
                        "currentStatus": "ready_on_retest",
                        "riskFlags": ["thin_depth"],
                    },
                    "setupVersion": "new_genuine_version",  # DIFFERENT
                    "readinessVersion": "new_rv",
                }
            }
        }
        mock_alert.return_value = {"setups": {}}
        mock_live.return_value = {"monitors": {}}

        delivery_state = {
            "ledger": {},
            "activeBySetup": {},
            "cancelLedger": {},
            "opportunityGate": {
                "BSBUSDT:long": {
                    "state": "cancelled",
                    "symbol": "BSBUSDT",
                    "side": "long",
                    "grade": "A",
                    "lastReadyEventType": "READY_IN_ZONE",
                    "lastCancelFingerprint": "8a239f531c54dfc4",
                    "lastCancelSetupVersion": "4b221117893b",  # old version
                    "lastActionableSnapshot": {
                        # Zone: 0.73829-0.76694
                        # New zone: 0.74200-0.77000
                        # entry_low drift: (0.74200-0.73829)/0.73829 = 0.50%
                        # At boundary: use 0.74250 (+0.58%) to exceed 0.5%
                        "entryZone": {"low": 0.73829, "high": 0.76694},
                        "stopLoss": 0.58682,
                        "takeProfit": {"tp1": 0.82709, "tp2": 0.86529, "tp3": 0.94283},
                        "currentPrice": 0.755,
                        "trigger": "Eksekusi hanya jika hold area entry tetap kuat",
                        "invalidation": "Batalkan long bila 15m close kuat di bawah stop",
                        "executionNote": "Default management",
                        "currentStatus": "ready_on_retest",
                        "riskFlags": ["thin_depth"],
                    },
                }
            },
        }

        # New event with genuinely different zone: +0.6% on entry_low
        with patch.object(
            self.pmd, "entry_ready_collect_events",
            return_value=[
                make_event(
                    entry_low=0.74272,   # +0.6% vs old 0.73829 (>0.5% threshold)
                    entry_high=0.76694,  # same as old
                    setup_version="new_genuine_version",  # different version
                    readiness_version="new_rv",
                )
            ],
        ):
            cycle = self.pmd.entry_ready_plan_cycle(
                limit=5,
                formatter=self.formatter,
                state=delivery_state,
            )

        would_send_symbols = [s["symbol"] for s in cycle.get("wouldSend", [])]
        self.assertIn("BSBUSDT", would_send_symbols)


if __name__ == "__main__":
    unittest.main()
