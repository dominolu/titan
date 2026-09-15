"""``on_fill`` behaviour: slot accumulation, hedge obligations, successor slots, ratio."""

import unittest

from pair_arb_harness import (  # noqa: E402
    Harness,
    OrderStatus,
    Role,
    Side,
    VenueOrderStatus,
    pair_arb,
)


class TestPairArbPartialFills(unittest.IsolatedAsyncioTestCase):
    async def test_partial_initiator_fill_only_accumulates_the_slot(self):
        harness = Harness()
        order = await harness.start_slot()

        outcome = await harness.fill(order.order_id, 4.0)
        self.assertTrue(outcome.has("fill_recorded"))
        self.assertEqual(harness.slot.initiator_filled_qty, 4.0)
        self.assertEqual(harness.slot.hedge_filled_qty, 0.0)
        self.assertEqual(order.filled_qty, 4.0)
        self.assertEqual(order.fill_count, 1)
        self.assertEqual(order.status, OrderStatus.PARTIAL)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertEqual(harness.slot.hedge_order_id, 0, "partial maker fills never hedge")
        self.assertEqual(len(harness.broker.create_requests), 1)
        self.assertEqual(harness.slot.first_imbalance_ts_ns, order.last_fill_ts_ns)

    async def test_partial_fills_accumulate_deltas_from_cumulative_reports(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 1.0)
        await harness.fill(order.order_id, 1.0)
        await harness.fill(order.order_id, 3.0)
        self.assertEqual(harness.slot.initiator_filled_qty, 5.0)
        self.assertEqual(order.filled_qty, 5.0)
        self.assertEqual(order.cumulative_filled_qty, 5.0)
        self.assertEqual(order.fill_count, 3)

    async def test_full_initiator_fill_archives_the_order_and_hedges_immediately(self):
        harness = Harness()
        order = await harness.start_slot()

        outcome = await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertTrue(outcome.has("initiator_archived"))
        self.assertTrue(outcome.has("hedge_submitted"))
        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertIsNone(harness.pair.get_active(order.order_id))
        archived = harness.orders_list().get(order.order_id)
        self.assertIsNotNone(archived)
        self.assertEqual(archived.status, OrderStatus.FILLED)
        self.assertEqual(archived.filled_qty, 10.0)
        self.assertEqual(archived.slot_id, 1)

        hedge = harness.hedge()
        self.assertIsNotNone(hedge)
        self.assertEqual(hedge.role, Role.HEDGE)
        self.assertEqual(hedge.side, Side.SELL)
        self.assertEqual(hedge.symbol, "BBB")
        self.assertEqual(hedge.broker, "venue_b")
        self.assertEqual(hedge.qty, 10.0)
        self.assertEqual(hedge.price, 100.0, "taker sell crosses the best bid")
        self.assertEqual(int(hedge.time_in_force), int(pair_arb.TimeInForce.IOC))

    async def test_hedge_quantity_uses_the_total_slot_fill_not_the_last_delta(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        await harness.fill(order.order_id, 6.0, status=VenueOrderStatus.FILLED)
        self.assertEqual(harness.hedge().qty, 10.0)

    async def test_pair_ratio_is_applied_for_fractional_hedge_sizes(self):
        harness = Harness(hedge_ratio_abs=0.63, slot_unit=10.0, max_position=10.0)
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertAlmostEqual(harness.hedge().qty, 6.3, places=9)

    async def test_dust_sized_hedge_gap_completes_the_slot_without_a_hedge_order(self):
        harness = Harness(hedge_ratio_abs=0.02, slot_unit=10.0, max_position=10.0,
                          dust_threshold=0.5)
        order = await harness.start_slot()
        outcome = await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertFalse(outcome.has("hedge_submitted"))
        self.assertEqual(harness.slot.hedge_order_id, 0)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.FILLED))


class TestPairArbHedgeLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_hedge_partial_fill_keeps_the_order_and_does_not_duplicate(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()

        await harness.fill(hedge.order_id, 4.0)
        self.assertEqual(harness.slot.hedge_filled_qty, 4.0)
        self.assertEqual(harness.slot.hedge_order_id, hedge.order_id)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ACTIVE))

        outcome = await harness.tick()
        self.assertNotIn("hedge_submitted", outcome.actions)
        self.assertEqual(len(harness.broker.create_requests), 2)

    async def test_hedge_full_fill_completes_the_slot(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()

        outcome = await harness.fill(hedge.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertTrue(outcome.has("hedge_archived"))
        self.assertTrue(outcome.has("slot_completed"))
        self.assertEqual(harness.slot.hedge_order_id, 0)
        self.assertEqual(harness.pair.active_count(), 0)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.FILLED))

    async def test_successor_slot_is_created_by_the_next_tick(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.fill(harness.hedge().order_id, 10.0, status=VenueOrderStatus.FILLED)

        result = await harness.tick()
        self.assertTrue(result.has("slot_started"))
        self.assertEqual(harness.slot.slot_id, 2)
        self.assertEqual(harness.slot.initiator_filled_qty, 0.0)
        self.assertEqual(harness.slot.hedge_filled_qty, 0.0)
        self.assertEqual(harness.initiator().qty, 10.0)
        self.assertEqual(len(harness.orders_list()), 2, "history stays in OrdersList")

    async def test_fill_after_slot_completion_still_belongs_to_its_own_slot(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.fill(harness.hedge().order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.tick()

        self.assertEqual(harness.orders_list().get(order.order_id).slot_id, 1)
        self.assertEqual(harness.pair.current_slot.slot_id, 2, "slot ids are monotonic")

    async def test_slot_state_requires_both_relations_to_be_cleared(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()
        # Hedge fills complete the quantities but the relation is still live.
        hedge.filled_qty = 10.0
        harness.slot.hedge_filled_qty = 10.0
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ACTIVE))

        harness.slot.hedge_order_id = 0
        harness.pair.detach(hedge.order_id)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.FILLED))

    async def test_taker_taker_mode_submits_both_legs_as_ioc(self):
        harness = Harness(mode="TAKER_TAKER")
        order = await harness.start_slot()
        self.assertEqual(int(order.time_in_force), int(pair_arb.TimeInForce.IOC))
        self.assertEqual(order.price, 50.2, "taker buy crosses the best ask")
        hedge = harness.hedge()
        self.assertIsNotNone(hedge, "taker-taker submits both legs in one scheduling cycle")
        self.assertEqual(int(hedge.time_in_force), int(pair_arb.TimeInForce.IOC))
        self.assertEqual(hedge.side, Side.SELL)
        self.assertEqual(hedge.price, 100.0)
        self.assertEqual(hedge.qty, 10.0)

    async def test_taker_taker_does_not_stack_a_second_hedge_on_an_active_one(self):
        harness = Harness(mode="TAKER_TAKER")
        order = await harness.start_slot()
        hedge = harness.hedge()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        self.assertEqual(harness.slot.hedge_order_id, hedge.order_id)
        self.assertEqual(len(harness.broker.create_requests), 2, "one hedge order at a time")

    async def test_taker_taker_hedges_the_gap_after_the_hedge_order_ends(self):
        harness = Harness(mode="TAKER_TAKER")
        order = await harness.start_slot()
        hedge = harness.hedge()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=hedge.order_id,
                                 kind=pair_arb.CancelEventKind.ENDED_OTHER,
                                 ts_ns=harness.advance(),
                                 venue_status=VenueOrderStatus.EXPIRED),
        )
        result = await harness.tick()
        self.assertTrue(result.has("hedge_submitted"), "invariant I11: gap is hedged")
        self.assertEqual(harness.hedge().qty, 10.0)


class TestPairArbEntrypointBoundaries(unittest.IsolatedAsyncioTestCase):
    async def test_on_tick_never_invents_or_changes_fill_quantities(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        before = (harness.slot.initiator_filled_qty, harness.slot.hedge_filled_qty,
                  order.filled_qty, order.fill_count)

        harness.set_market(right_bid=104.0, right_ask=104.2)
        await harness.tick(refresh_market=False)
        await harness.tick(refresh_market=False)
        after = (harness.slot.initiator_filled_qty, harness.slot.hedge_filled_qty,
                 order.filled_qty, order.fill_count)
        self.assertEqual(before, after, "invariant I10")

    async def test_on_cancel_never_changes_fill_quantities(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        fills = (harness.slot.initiator_filled_qty, order.filled_qty)

        result = await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(
                order_id=order.order_id,
                kind=pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                ts_ns=harness.advance(),
            ),
        )
        self.assertTrue(result.has("cancel_requested"))
        self.assertEqual(fills, (harness.slot.initiator_filled_qty, order.filled_qty))
        self.assertEqual(harness.slot.hedge_order_id, 0, "on_cancel never hedges")
        self.assertEqual(len(harness.broker.create_requests), 1, "on_cancel never creates slots")

    async def test_initiator_cancel_with_remaining_replaces_maker_and_hedges_the_gap(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)

        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=order.order_id,
                                 kind=pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                                 ts_ns=harness.advance()),
        )
        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=order.order_id,
                                 kind=pair_arb.CancelEventKind.CANCELED,
                                 ts_ns=harness.advance(),
                                 venue_status=VenueOrderStatus.CANCELED),
        )
        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertEqual(harness.orders_list().get(order.order_id).filled_qty, 4.0)

        result = await harness.tick()
        self.assertTrue(result.has("initiator_submitted"))
        replacement = harness.initiator()
        self.assertEqual(replacement.qty, 6.0, "remaining quantity is re-placed")
        self.assertNotEqual(replacement.order_id, order.order_id)
        self.assertIsNotNone(harness.hedge(), "the unhedged 4.0 is caught up as well")
        self.assertEqual(harness.hedge().qty, 4.0)

    async def test_hedge_cancel_with_gap_is_resubmitted_by_on_tick(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()

        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=hedge.order_id,
                                 kind=pair_arb.CancelEventKind.REQUEST_ACCEPTED,
                                 ts_ns=harness.advance()),
        )
        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=hedge.order_id,
                                 kind=pair_arb.CancelEventKind.CANCELED,
                                 ts_ns=harness.advance(),
                                 venue_status=VenueOrderStatus.CANCELED),
        )
        self.assertEqual(harness.slot.hedge_order_id, 0)

        result = await harness.tick()
        self.assertTrue(result.has("hedge_submitted"))
        self.assertEqual(harness.hedge().qty, 10.0)
        self.assertNotEqual(harness.hedge().order_id, hedge.order_id)

    async def test_hedge_catch_up_runs_even_in_restricted_posture(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()
        await harness.engine.on_cancel(
            harness.ctx,
            pair_arb.CancelEvent(order_id=hedge.order_id,
                                 kind=pair_arb.CancelEventKind.CANCELED,
                                 ts_ns=harness.advance(),
                                 venue_status=VenueOrderStatus.CANCELED),
        )
        # A stale maker book restricts new slots and re-quotes but must not block the hedge.
        harness.ctx.market_left.ts_ns = harness.now_ns - 5_000 * 1_000_000
        result = await harness.tick(refresh_market=False)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.RESTRICTED)
        self.assertTrue(result.has("hedge_submitted"), "RESTRICTED still allows hedging")


if __name__ == "__main__":
    unittest.main()
