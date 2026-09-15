"""Connector normalization: what may reach ``on_fill``/``on_cancel`` and what must reconcile."""

import unittest

from pair_arb_harness import (  # noqa: E402
    ConnectorNormalizer,
    Harness,
    OrderStatus,
    Side,
    VenueOrderStatus,
    pair_arb,
)


class TestPairArbFillIntake(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.harness = Harness()
        self.order = await self.harness.start_slot()
        self.normalizer = ConnectorNormalizer(self.harness.ctx)

    def report(self, **overrides):
        payload = dict(
            order_id=self.order.order_id,
            ts_ns=self.harness.advance(),
            fill_qty=1.0,
            cumulative_filled_qty=self.order.filled_qty + 1.0,
            fill_price=self.order.price,
            status=VenueOrderStatus.PARTIALLY_FILLED,
            symbol=self.order.symbol,
            side=self.order.side,
            sequence=self.harness.now_ns,
        )
        payload.update(overrides)
        return pair_arb.VenueFillReport(**payload)

    async def test_confirmed_fill_is_delivered_as_an_increment(self):
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=None, cumulative_filled_qty=2.5)
        )
        self.assertEqual(decision.action, pair_arb.IntakeAction.DELIVER)
        self.assertEqual(decision.fill.fill_qty, 2.5)
        self.assertEqual(decision.fill.cumulative_filled_qty, 2.5)

    async def test_fill_for_an_unknown_order_goes_to_reconcile(self):
        report = self.report(order_id=4242)
        decision = self.normalizer.normalize_fill(report)
        self.assertEqual(decision.action, pair_arb.IntakeAction.RECONCILE)
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.UNKNOWN_ORDER)
        self.assertIn(pair_arb.ReconcileReason.UNKNOWN_ORDER, self.harness.reconcile_reasons())

    async def test_fill_for_an_archived_order_goes_to_reconcile(self):
        await self.harness.fill(self.order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        report = self.report(order_id=self.order.order_id, fill_qty=1.0,
                             cumulative_filled_qty=11.0)
        decision = self.normalizer.normalize_fill(report)
        self.assertEqual(decision.action, pair_arb.IntakeAction.RECONCILE)
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.STALE_SLOT_FILL)

    async def test_duplicate_report_is_dropped(self):
        first = self.normalizer.normalize_fill(self.report(fill_qty=1.0))
        self.assertEqual(first.action, pair_arb.IntakeAction.DELIVER)
        self.harness.ctx.pair.get_active(self.order.order_id).filled_qty = 1.0
        duplicate = self.normalizer.normalize_fill(
            pair_arb.VenueFillReport(
                order_id=self.order.order_id,
                ts_ns=self.harness.advance(),
                fill_qty=1.0,
                cumulative_filled_qty=1.0,
                status=VenueOrderStatus.PARTIALLY_FILLED,
                symbol=self.order.symbol,
                side=self.order.side,
                sequence=first.fill.sequence,
            )
        )
        self.assertEqual(duplicate.action, pair_arb.IntakeAction.DROP)

    async def test_cumulative_regression_goes_to_reconcile(self):
        self.order.filled_qty = 4.0
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=None, cumulative_filled_qty=3.0)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.CUMULATIVE_REGRESSION)

    async def test_inconsistent_cumulative_and_delta_go_to_reconcile(self):
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=5.0, cumulative_filled_qty=1.0)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.CUMULATIVE_JUMP)

    async def test_fill_beyond_the_order_quantity_goes_to_reconcile(self):
        self.order.filled_qty = 9.0
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=3.0, cumulative_filled_qty=12.0)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.FILL_EXCEEDS_ORDER)

    async def test_status_only_report_never_guesses_a_quantity(self):
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=None, cumulative_filled_qty=None)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.FILL_WITHOUT_QUANTITY)

    async def test_zero_delta_report_is_dropped_or_reconciled_without_accumulating(self):
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=0.0, cumulative_filled_qty=None)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.FILL_WITHOUT_QUANTITY)
        self.assertEqual(self.harness.slot.initiator_filled_qty, 0.0)

    async def test_out_of_order_sequence_goes_to_reconcile(self):
        first = self.normalizer.normalize_fill(self.report(fill_qty=1.0, sequence=100))
        self.assertEqual(first.action, pair_arb.IntakeAction.DELIVER)
        self.order.filled_qty = 1.0
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=1.0, cumulative_filled_qty=2.0, sequence=50)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.DUPLICATE_OR_OUT_OF_ORDER)

    async def test_filled_status_with_a_short_cumulative_goes_to_reconcile(self):
        decision = self.normalizer.normalize_fill(
            self.report(fill_qty=1.0, cumulative_filled_qty=1.0, status=VenueOrderStatus.FILLED)
        )
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.CUMULATIVE_JUMP)

    async def test_symbol_and_side_mismatch_go_to_reconcile(self):
        wrong_symbol = self.normalizer.normalize_fill(self.report(symbol="CCC"))
        self.assertEqual(wrong_symbol.reason, pair_arb.ReconcileReason.UNKNOWN_ORDER)
        wrong_side = self.normalizer.normalize_fill(self.report(side=Side.SELL))
        self.assertEqual(wrong_side.reason, pair_arb.ReconcileReason.UNKNOWN_ORDER)

    async def test_intercepted_fills_never_reach_the_slot(self):
        await self.harness.callbacks.on_venue_fill(self.report(order_id=9999, fill_qty=1.0))
        self.assertEqual(self.harness.slot.initiator_filled_qty, 0.0)
        self.assertEqual(self.order.filled_qty, 0.0)
        self.assertEqual(self.order.status, OrderStatus.WORKING)


class TestPairArbOrderEventIntake(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.harness = Harness()
        self.order = await self.harness.start_slot()
        self.normalizer = ConnectorNormalizer(self.harness.ctx)

    def report(self, **overrides):
        payload = dict(
            order_id=self.order.order_id,
            status=VenueOrderStatus.CANCELED,
            ts_ns=self.harness.advance(),
        )
        payload.update(overrides)
        return pair_arb.VenueOrderReport(**payload)

    async def test_cancel_confirmation_is_delivered(self):
        decision = self.normalizer.normalize_cancel_event(self.report())
        self.assertEqual(decision.action, pair_arb.IntakeAction.DELIVER)
        self.assertEqual(decision.cancel.kind, pair_arb.CancelEventKind.CANCELED)

    async def test_duplicate_cancel_acceptance_is_dropped(self):
        self.order.status = OrderStatus.CANCEL_REQUESTED
        decision = self.normalizer.normalize_cancel_event(
            self.report(status=VenueOrderStatus.NEW)
        )
        self.assertEqual(decision.action, pair_arb.IntakeAction.DROP)

    async def test_unknown_order_event_goes_to_reconcile(self):
        decision = self.normalizer.normalize_cancel_event(self.report(order_id=777))
        self.assertEqual(decision.action, pair_arb.IntakeAction.RECONCILE)
        self.assertEqual(decision.reason, pair_arb.ReconcileReason.UNKNOWN_ORDER)

    async def test_terminal_report_for_an_archived_order_is_dropped(self):
        await self.harness.engine.on_cancel(
            self.harness.ctx,
            pair_arb.CancelEvent(order_id=self.order.order_id,
                                 kind=pair_arb.CancelEventKind.CANCELED,
                                 ts_ns=self.harness.advance(),
                                 venue_status=VenueOrderStatus.CANCELED),
        )
        decision = self.normalizer.normalize_cancel_event(self.report())
        self.assertEqual(decision.action, pair_arb.IntakeAction.DROP)

    async def test_fill_race_report_is_marked_as_a_fill_not_a_cancel(self):
        decision = self.normalizer.normalize_cancel_event(
            self.report(status=VenueOrderStatus.FILLED)
        )
        self.assertEqual(decision.cancel.kind, pair_arb.CancelEventKind.FILLED)


class TestPairArbCreateResultRouting(unittest.IsolatedAsyncioTestCase):
    """Out-of-band create results must land on the same local order facts as awaited ones."""

    async def test_accepted_create_result_marks_the_order_working(self):
        harness = Harness()
        await harness.tick()
        order = harness.initiator()
        result = await harness.callbacks.on_create_result(
            order.order_id,
            pair_arb.OrderCommandResult(
                order_id=order.order_id,
                outcome=pair_arb.CommandOutcome.ACCEPTED,
                ts_ns=harness.advance(),
                venue_order_id=555,
            ),
        )
        self.assertTrue(result.has("accepted"))
        self.assertEqual(order.status, OrderStatus.WORKING)
        self.assertEqual(order.venue_order_id, 555)

    async def test_rejected_create_result_archives_and_clears_the_relation(self):
        harness = Harness()
        await harness.tick()
        order = harness.initiator()
        await harness.callbacks.on_create_result(
            order.order_id,
            pair_arb.OrderCommandResult(
                order_id=order.order_id,
                outcome=pair_arb.CommandOutcome.REJECTED,
                error_code=9,
                ts_ns=harness.advance(),
            ),
        )
        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertIsNone(harness.pair.get_active(order.order_id))
        self.assertEqual(harness.orders_list().get(order.order_id).status, OrderStatus.REJECTED)

    async def test_unknown_create_result_keeps_the_order_and_requests_reconcile(self):
        harness = Harness()
        await harness.tick()
        order = harness.initiator()
        await harness.callbacks.on_create_result(
            order.order_id,
            pair_arb.OrderCommandResult(
                order_id=order.order_id,
                outcome=pair_arb.CommandOutcome.TRANSPORT_ERROR,
                ts_ns=harness.advance(),
            ),
        )
        self.assertEqual(order.status, OrderStatus.UNKNOWN)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertIn(
            pair_arb.ReconcileReason.BROKER_OUTCOME_UNKNOWN, harness.reconcile_reasons()
        )

    async def test_create_result_for_an_unknown_order_goes_to_reconcile(self):
        harness = Harness()
        result = await harness.callbacks.on_create_result(
            4242,
            pair_arb.OrderCommandResult(
                order_id=4242, outcome=pair_arb.CommandOutcome.ACCEPTED, ts_ns=harness.advance()
            ),
        )
        self.assertEqual(result.reason, "unknown_order")
        self.assertIn(pair_arb.ReconcileReason.UNKNOWN_ORDER, harness.reconcile_reasons())


if __name__ == "__main__":
    unittest.main()
