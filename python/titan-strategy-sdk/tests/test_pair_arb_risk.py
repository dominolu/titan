"""``risk_check`` metrics, posture mapping and the gross-imbalance invariant."""

import unittest

from pair_arb_harness import Harness, OrderStatus, Role, VenueOrderStatus, pair_arb  # noqa: E402
from strategies.pair_arb.risk import RiskLimits, compute_risk, posture_for  # noqa: E402


class TestPairArbRiskSnapshot(unittest.IsolatedAsyncioTestCase):
    async def test_clean_state_is_normal_and_opens(self):
        harness = Harness()
        snapshot = await harness.engine.risk_check(harness.ctx)
        self.assertTrue(snapshot.clean)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.NORMAL)
        self.assertEqual(snapshot.unhedged_qty, 0.0)
        self.assertEqual(snapshot.active_order_count, 0)
        self.assertEqual(snapshot.free_order_slots, harness.pair.capacity)

    async def test_unhedged_qty_and_age_drive_soft_and_hard_postures(self):
        harness = Harness(max_unhedged_qty_soft=5.0)
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        harness.hedge().status = OrderStatus.UNKNOWN

        snapshot = await harness.engine.risk_check(harness.ctx)
        self.assertEqual(snapshot.unhedged_qty, 10.0)
        self.assertEqual(snapshot.unknown_order_count, 1)
        self.assertIn("unhedged_qty_soft", snapshot.violations)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.RESTRICTED)

        harness.advance(3_000)  # beyond max_unhedged_ms_hard
        snapshot = await harness.engine.risk_check(harness.ctx)
        self.assertIn("unhedged_age_hard", snapshot.violations)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.EMERGENCY)
        self.assertIn(
            pair_arb.ReconcileReason.BROKER_OUTCOME_UNKNOWN, harness.reconcile_reasons()
        )

    async def test_gross_imbalance_never_nets_opposite_slots(self):
        harness = Harness()
        harness.orders_list().append(
            pair_arb.OrdersListItem(
                order_id=1,
                slot_id=1,
                role=Role.INITIATOR,
                symbol="AAA",
                broker="venue_a",
                side=pair_arb.Side.BUY,
                price=99.0,
                qty=10.0,
                status=OrderStatus.CANCELED,
                filled_qty=10.0,
            )
        )
        harness.slot.slot_id = 2
        harness.slot.target_initiator_qty = 10.0
        harness.slot.hedge_filled_qty = 5.0

        snapshot = compute_risk(harness.ctx, harness.engine.limits)
        self.assertEqual(snapshot.unhedged_qty, 5.0)
        self.assertEqual(snapshot.gross_imbalance_qty, 15.0, "invariant I15")

    def test_posture_mapping_is_monotonic_and_latched(self):
        clean = pair_arb.RiskSnapshot()
        self.assertEqual(posture_for(clean, pair_arb.Posture.NORMAL, False),
                         pair_arb.Posture.NORMAL)

        soft = pair_arb.RiskSnapshot(violations=["unhedged_qty_soft"])
        self.assertEqual(posture_for(soft, pair_arb.Posture.NORMAL, False),
                         pair_arb.Posture.RESTRICTED)

        hard = pair_arb.RiskSnapshot(violations=["unhedged_qty_hard"])
        self.assertEqual(posture_for(hard, pair_arb.Posture.RESTRICTED, False),
                         pair_arb.Posture.EMERGENCY)

        conflict = pair_arb.RiskSnapshot(violations=["slot_relation_conflict"])
        self.assertEqual(posture_for(conflict, pair_arb.Posture.NORMAL, False),
                         pair_arb.Posture.HALT)

        self.assertEqual(posture_for(clean, pair_arb.Posture.HALT, True),
                         pair_arb.Posture.HALT, "latch survives until manual review")

    async def test_manual_clear_releases_the_halt_latch(self):
        harness = Harness()
        harness.pair.posture = pair_arb.Posture.HALT
        harness.pair.posture_latched = True
        pair_arb.clear_halt(harness.pair)
        self.assertFalse(harness.pair.posture_latched)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.RESTRICTED)

    def test_limit_validation_rejects_inverted_limits(self):
        with self.assertRaises(ValueError):
            RiskLimits(max_unhedged_qty_soft=5.0, max_unhedged_qty_hard=1.0).validate()
        with self.assertRaises(ValueError):
            RiskLimits(max_unhedged_age_ns_soft=10, max_unhedged_age_ns_hard=1).validate()


if __name__ == "__main__":
    unittest.main()
