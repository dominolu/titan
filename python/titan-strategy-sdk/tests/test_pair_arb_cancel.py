"""``on_cancel`` behaviour and the fill/cancel interleavings of requirements section 6.5."""

import unittest

from pair_arb_harness import Harness, OrderStatus, Role, VenueOrderStatus, pair_arb  # noqa: E402


def cancel_event(order_id, kind, ts_ns, status=VenueOrderStatus.UNKNOWN, error=0):
    return pair_arb.CancelEvent(
        order_id=order_id, kind=kind, ts_ns=ts_ns, venue_status=status, error_code=error
    )


class TestPairArbCancelRequestPhase(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_acceptance_is_not_a_cancel_confirmation(self):
        harness = Harness()
        order = await harness.start_slot()
        result = await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                         harness.advance()),
        )
        self.assertTrue(result.has("cancel_requested"))
        self.assertEqual(order.status, OrderStatus.CANCEL_REQUESTED)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertNotIn(order.order_id, harness.orders_list(), "not archived before cancel")

    async def test_cancel_result_from_the_broker_is_routed_through_on_cancel(self):
        harness = Harness()
        order = await harness.start_slot()
        request = harness.broker.create_requests[-1]
        response = pair_arb.OrderCommandResult(
            order_id=order.order_id,
            outcome=pair_arb.CommandOutcome.ACCEPTED,
            request_kind="cancel",
            ts_ns=harness.advance(),
        )
        result = await harness.callbacks.on_cancel_result(request, response)
        self.assertTrue(result.has("cancel_requested"))

    async def test_rejected_cancel_result_keeps_the_order_and_records_the_reason(self):
        harness = Harness()
        order = await harness.start_slot()
        request = harness.broker.create_requests[-1]
        response = pair_arb.OrderCommandResult(
            order_id=order.order_id,
            outcome=pair_arb.CommandOutcome.REJECTED,
            request_kind="cancel",
            error_code=77,
            ts_ns=harness.advance(),
        )
        result = await harness.callbacks.on_cancel_result(request, response)
        self.assertTrue(result.has("cancel_rejected"))
        self.assertEqual(order.status, OrderStatus.WORKING)
        self.assertEqual(order.error_code, 77)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)

    async def test_timeout_cancel_result_marks_unknown_and_requests_reconcile(self):
        harness = Harness()
        order = await harness.start_slot()
        request = harness.broker.create_requests[-1]
        response = pair_arb.OrderCommandResult(
            order_id=order.order_id,
            outcome=pair_arb.CommandOutcome.TIMEOUT,
            request_kind="cancel",
            ts_ns=harness.advance(),
        )
        await harness.callbacks.on_cancel_result(request, response)
        self.assertEqual(order.status, OrderStatus.UNKNOWN)
        self.assertIn(pair_arb.ReconcileReason.CANCEL_TIMEOUT, harness.reconcile_reasons())

    async def test_cancel_confirmation_archives_the_order_and_clears_the_relation(self):
        harness = Harness()
        order = await harness.start_slot()
        result = await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.CANCELED, harness.advance(),
                         status=VenueOrderStatus.CANCELED),
        )
        self.assertTrue(result.has("cancel_confirmed"))
        self.assertEqual(result.reason, "initiator_remaining")
        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertIsNone(harness.pair.get_active(order.order_id))
        self.assertEqual(harness.orders_list().get(order.order_id).status, OrderStatus.CANCELED)

    async def test_cancel_confirmation_only_clears_its_own_leg(self):
        harness = Harness(mode="TAKER_TAKER")
        order = await harness.start_slot()
        hedge = harness.hedge()
        self.assertIsNotNone(hedge, "taker-taker keeps both legs active at once")
        await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.CANCELED, harness.advance(),
                         status=VenueOrderStatus.CANCELED),
        )
        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertEqual(harness.slot.hedge_order_id, hedge.order_id, "other leg untouched")
        self.assertEqual(harness.orders_list().get(order.order_id).status, OrderStatus.CANCELED)

    async def test_other_terminal_states_are_not_treated_as_canceled(self):
        harness = Harness()
        order = await harness.start_slot()
        result = await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.ENDED_OTHER, harness.advance(),
                         status=VenueOrderStatus.REJECTED, error=99),
        )
        self.assertTrue(result.has("order_ended"))
        archived = harness.orders_list().get(order.order_id)
        self.assertEqual(archived.status, OrderStatus.REJECTED)
        self.assertEqual(harness.slot.initiator_order_id, 0)

        replacement = await harness.tick()
        self.assertTrue(replacement.has("initiator_submitted"),
                        "a rejected order does not close the slot")
        self.assertEqual(harness.initiator().qty, 10.0)


class TestPairArbFillCancelInterleavings(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_accepted_then_partial_fill_keeps_both_facts(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                         harness.advance()),
        )
        await harness.fill(order.order_id, 3.0)

        self.assertEqual(harness.slot.initiator_filled_qty, 3.0)
        self.assertEqual(order.status, OrderStatus.PARTIAL)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)

    async def test_partial_fill_then_cancel_accepted_keeps_the_fill(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 3.0)
        await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                         harness.advance()),
        )
        self.assertEqual(harness.slot.initiator_filled_qty, 3.0)
        self.assertEqual(order.status, OrderStatus.CANCEL_REQUESTED)
        self.assertEqual(order.status_before_cancel, OrderStatus.PARTIAL)

    async def test_partial_fill_then_cancel_confirmed_archives_with_fills(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 3.0)
        result = await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.CANCELED, harness.advance(),
                         status=VenueOrderStatus.CANCELED),
        )
        self.assertTrue(result.has("canceled_with_fills"))
        archived = harness.orders_list().get(order.order_id)
        self.assertEqual(archived.filled_qty, 3.0)
        self.assertEqual(archived.status, OrderStatus.CANCELED)
        self.assertEqual(harness.slot.initiator_filled_qty, 3.0)

    async def test_cancel_confirmed_then_late_fill_is_intercepted_by_the_connector(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.CANCELED, harness.advance(),
                         status=VenueOrderStatus.CANCELED),
        )
        outcome = await harness.fill(order.order_id, 2.0)
        self.assertEqual(outcome.entrypoint, "reconcile")
        self.assertIn(pair_arb.ReconcileReason.STALE_SLOT_FILL, harness.reconcile_reasons())
        self.assertEqual(harness.slot.initiator_filled_qty, 0.0)

    async def test_cancel_racing_a_fill_leaves_the_fill_for_on_fill(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        result = await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(order.order_id, pair_arb.CancelEventKind.FILLED, harness.advance(),
                         status=VenueOrderStatus.FILLED),
        )
        self.assertTrue(result.has("cancel_raced_fill"))
        self.assertEqual(result.reason, "awaiting_fill")
        self.assertEqual(harness.slot.initiator_filled_qty, 4.0, "on_cancel keeps quantities")
        self.assertEqual(order.filled_qty, 4.0)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id,
                         "the fill fact still has to arrive through on_fill")

    async def test_hedge_fill_and_cancel_reports_do_not_overwrite_each_other(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()
        await harness.fill(hedge.order_id, 4.0)
        await harness.engine.on_cancel(
            harness.ctx,
            cancel_event(hedge.order_id, pair_arb.CancelEventKind.REQUEST_REJECTED,
                         harness.advance(), error=12),
        )
        self.assertEqual(harness.slot.hedge_filled_qty, 4.0)
        self.assertEqual(hedge.status, OrderStatus.PARTIAL)
        self.assertEqual(hedge.error_code, 12)


class TestPairArbSlotCompletion(unittest.IsolatedAsyncioTestCase):
    async def test_completion_requires_target_fills_dust_bound_and_no_live_orders(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()
        await harness.fill(hedge.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.FILLED))

        # A leftover relation makes the slot ACTIVE again, never FILLED.
        harness.pair.attach(
            pair_arb.ActiveOrder(
                order_id=hedge.order_id,
                slot_id=harness.slot.slot_id,
                role=Role.HEDGE,
                status=OrderStatus.WORKING,
                qty=1.0,
            )
        )
        harness.slot.hedge_order_id = hedge.order_id
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ACTIVE))

    async def test_filled_slot_does_not_create_two_successors_in_one_tick(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.fill(harness.hedge().order_id, 10.0, status=VenueOrderStatus.FILLED)

        first = await harness.tick()
        second = await harness.tick()
        self.assertTrue(first.has("slot_started"))
        self.assertFalse(second.has("slot_started"))
        self.assertEqual(harness.slot.slot_id, 2)
        self.assertEqual(len(harness.broker.create_requests), 3)


if __name__ == "__main__":
    unittest.main()
