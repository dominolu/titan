"""``on_tick`` behaviour: slot creation, spread/requote, cancel safety, posture gates."""

import unittest

from pair_arb_harness import (  # noqa: E402
    NS,
    Harness,
    OrderStatus,
    Role,
    Side,
    VenueOrderStatus,
    pair_arb,
)


class TestPairArbSlotCreation(unittest.IsolatedAsyncioTestCase):
    async def test_first_tick_creates_one_slot_and_a_maker_order(self):
        harness = Harness()
        outcome = await harness.tick()

        self.assertEqual(harness.slot.slot_id, 1, "slot ids increment from the previous value")
        self.assertEqual(harness.slot.target_initiator_qty, 10.0)
        self.assertEqual(harness.pair.active_count(), 1)
        order = harness.initiator()
        self.assertIsNotNone(order)
        self.assertEqual(order.side, Side.BUY)
        self.assertEqual(order.symbol, "AAA")
        self.assertEqual(order.broker, "venue_a")
        self.assertEqual(order.price, 99.0, "maker buy = taker best bid - spread")
        self.assertEqual(order.qty, 10.0)
        self.assertEqual(int(order.time_in_force), int(pair_arb.TimeInForce.GTX))
        self.assertEqual(order.status, OrderStatus.WORKING)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertEqual(harness.slot.hedge_order_id, 0)
        self.assertTrue(outcome.has("slot_started"))

    async def test_no_slot_is_created_before_start_time(self):
        harness = Harness(start_time_ns=1_000_000 * NS)
        await harness.tick()
        self.assertEqual(harness.pair.active_count(), 0)
        self.assertEqual(harness.slot.slot_id, 0)

    async def test_short_spread_sells_the_initiator_leg_at_the_taker_ask(self):
        harness = Harness(direction="SHORT_SPREAD")
        await harness.tick()
        order = harness.initiator()
        self.assertEqual(order.side, Side.SELL)
        self.assertEqual(order.price, 101.2, "maker sell = taker best ask + spread")
        self.assertEqual(harness.pair.direction.hedge_side, Side.BUY)

    async def test_slot_unit_is_clamped_by_remaining_capacity_and_drains_at_max_position(self):
        harness = Harness(max_position=15.0, slot_unit=10.0)
        order = await harness.start_slot()
        self.assertEqual(harness.slot.target_initiator_qty, 10.0, "first slice uses slot_unit")
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        await harness.fill(harness.hedge().order_id, 10.0, status=VenueOrderStatus.FILLED)

        # Second slot clamps to the remaining 5 units of max_position.
        order = await harness.start_slot()
        self.assertEqual(order.qty, 5.0)
        self.assertEqual(harness.slot.slot_id, 2)

    async def test_max_position_reached_switches_the_pair_to_draining(self):
        harness = Harness(max_position=10.0, slot_unit=10.0)
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        hedge = harness.hedge()
        await harness.fill(hedge.order_id, 10.0, status=VenueOrderStatus.FILLED)

        outcome = await harness.tick()
        self.assertTrue(outcome.has("max_position_reached"))
        self.assertEqual(int(harness.pair.status), int(pair_arb.PairStatus.DRAINING))
        self.assertEqual(harness.slot.slot_id, 1, "no successor slot beyond max_position")

    async def test_rejected_create_archives_the_order_and_retries_next_tick(self):
        harness = Harness()
        harness.broker.create_outcome = pair_arb.CommandOutcome.REJECTED
        await harness.tick()

        self.assertEqual(harness.slot.initiator_order_id, 0)
        self.assertEqual(harness.pair.active_count(), 0)
        self.assertEqual(harness.pair.reject_count, 1)
        self.assertEqual(len(harness.orders_list()), 1)
        archived = next(iter(harness.orders_list()))
        self.assertEqual(archived.status, OrderStatus.REJECTED)
        self.assertEqual(archived.error_code, 42)

        harness.broker.create_outcome = pair_arb.CommandOutcome.ACCEPTED
        result = await harness.tick()
        self.assertTrue(result.has("initiator_submitted"))
        self.assertEqual(harness.slot.initiator_order_id, harness.initiator().order_id)

    async def test_create_timeout_marks_the_order_unknown_and_stops_replacement(self):
        harness = Harness()
        harness.broker.create_outcome = pair_arb.CommandOutcome.TIMEOUT
        await harness.tick()

        order = harness.initiator()
        self.assertEqual(order.status, OrderStatus.UNKNOWN)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertIn(
            pair_arb.ReconcileReason.BROKER_OUTCOME_UNKNOWN, harness.reconcile_reasons()
        )
        await harness.tick()
        self.assertEqual(len(harness.broker.create_requests), 1, "invariant I8")


class TestPairArbSpreadChecks(unittest.IsolatedAsyncioTestCase):
    async def test_maker_is_kept_while_the_spread_stays_inside_requote_distance(self):
        harness = Harness()
        await harness.tick()
        harness.set_market(right_bid=99.8, right_ask=100.0)
        outcome = await harness.tick(refresh_market=False)
        self.assertTrue(outcome.has("maker_kept"))
        self.assertEqual(harness.pair.active_count(), 1, "no replacement maker while kept")
        self.assertEqual(len(harness.broker.cancel_requests), 0)

    async def test_maker_is_canceled_once_when_the_spread_drifts_past_the_threshold(self):
        harness = Harness()
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)
        first = await harness.tick(refresh_market=False)
        self.assertTrue(first.has("cancel_requested:requote_distance"))
        self.assertEqual(len(harness.broker.cancel_requests), 1)
        self.assertEqual(harness.initiator().status, OrderStatus.CANCEL_REQUESTED)
        self.assertEqual(harness.slot.initiator_order_id, harness.initiator().order_id)

        second = await harness.tick(refresh_market=False)
        self.assertEqual(len(harness.broker.cancel_requests), 1, "one cancel per active maker")
        self.assertEqual(second.reason, "initiator_waiting")
        self.assertEqual(second.actions[0], "initiator_waiting")

    async def test_no_replacement_maker_is_created_before_cancel_confirmation(self):
        harness = Harness()
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)
        await harness.tick(refresh_market=False)
        await harness.tick(refresh_market=False)
        self.assertEqual(harness.pair.active_count(), 1)
        self.assertEqual(len(harness.broker.create_requests), 1)

    async def test_missing_market_data_blocks_requote_actions(self):
        harness = Harness()
        await harness.tick()
        harness.ctx.market_right = pair_arb.MarketView()
        outcome = await harness.tick(refresh_market=False)
        self.assertEqual(outcome.reason, "no_reference_price")
        self.assertEqual(len(harness.broker.cancel_requests), 0)


class TestPairArbCancelSafety(unittest.IsolatedAsyncioTestCase):
    async def test_cancel_timeout_marks_unknown_and_requests_reconcile(self):
        harness = Harness()
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)
        await harness.tick(refresh_market=False)

        harness.advance(2_000)  # beyond cancel_timeout_ns
        outcome = await harness.tick(refresh_market=False)
        self.assertTrue(outcome.has("cancel_timeout"))
        self.assertEqual(harness.initiator().status, OrderStatus.UNKNOWN)
        self.assertEqual(harness.slot.initiator_order_id, harness.initiator().order_id)
        self.assertIn(pair_arb.ReconcileReason.CANCEL_TIMEOUT, harness.reconcile_reasons())
        self.assertTrue(harness.ctx.alerts)

    async def test_unknown_order_is_never_replaced(self):
        harness = Harness()
        await harness.tick()
        order = harness.initiator()
        order.status = OrderStatus.UNKNOWN

        await harness.tick()
        await harness.tick()
        self.assertEqual(len(harness.broker.create_requests), 1, "invariant I8")
        self.assertEqual(len(harness.broker.cancel_requests), 0)

    async def test_cancel_rejection_restores_the_previous_state_and_retries(self):
        harness = Harness()
        harness.broker.cancel_outcome = pair_arb.CommandOutcome.REJECTED
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)

        first = await harness.tick(refresh_market=False)
        order = harness.initiator()
        self.assertTrue(first.has("cancel_rejected:requote_distance"))
        self.assertEqual(order.status, OrderStatus.WORKING)
        self.assertEqual(order.cancel_failures, 1)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)

        harness.broker.cancel_outcome = pair_arb.CommandOutcome.ACCEPTED
        second = await harness.tick(refresh_market=False)
        self.assertTrue(second.has("cancel_requested:requote_distance"))
        self.assertEqual(order.status, OrderStatus.CANCEL_REQUESTED)

    async def test_repeated_cancel_rejections_end_in_reconcile(self):
        harness = Harness(cancel_retry_limit=1)
        harness.broker.cancel_outcome = pair_arb.CommandOutcome.REJECTED
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)

        await harness.tick(refresh_market=False)
        outcome = await harness.tick(refresh_market=False)
        self.assertTrue(outcome.has("cancel_unknown"))
        self.assertEqual(harness.initiator().status, OrderStatus.UNKNOWN)
        self.assertIn(
            pair_arb.ReconcileReason.CANCEL_STATE_CONFLICT, harness.reconcile_reasons()
        )

    async def test_broker_timeout_on_cancel_marks_unknown_without_resend(self):
        harness = Harness()
        harness.broker.cancel_outcome = pair_arb.CommandOutcome.TIMEOUT
        await harness.tick()
        harness.set_market(right_bid=98.5, right_ask=98.7)

        await harness.tick(refresh_market=False)
        self.assertEqual(harness.initiator().status, OrderStatus.UNKNOWN)
        self.assertEqual(len(harness.broker.cancel_requests), 1)
        await harness.tick(refresh_market=False)
        self.assertEqual(len(harness.broker.cancel_requests), 1, "unknown cancels are not resent")


class TestPairArbAbnormalRelations(unittest.IsolatedAsyncioTestCase):
    async def test_terminal_order_that_is_still_referenced_is_audited_not_replaced(self):
        harness = Harness()
        order = await harness.start_slot()
        order.status = OrderStatus.FILLED

        outcome = await harness.tick()
        self.assertTrue(outcome.has("slot_relation_conflict"))
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ERROR))
        self.assertIn(
            pair_arb.ReconcileReason.SLOT_ORDER_CONFLICT, harness.reconcile_reasons()
        )
        self.assertEqual(len(harness.broker.create_requests), 1, "no duplicate order")
        self.assertEqual(harness.slot.initiator_order_id, order.order_id,
                         "the relation is cleared only after it can be confirmed")
        self.assertEqual(int(harness.pair.status), int(pair_arb.PairStatus.RECONCILING))

    async def test_canceled_order_left_in_the_slot_is_audited_and_not_overwritten(self):
        harness = Harness()
        order = await harness.start_slot()
        order.status = OrderStatus.CANCELED

        outcome = await harness.tick()
        self.assertTrue(outcome.has("slot_relation_conflict"))
        self.assertEqual(len(harness.broker.create_requests), 1)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)
        self.assertEqual(int(harness.pair.status), int(pair_arb.PairStatus.RECONCILING))

    async def test_slot_reference_without_an_active_order_is_an_error(self):
        harness = Harness()
        order = await harness.start_slot()
        harness.pair.detach(order.order_id)
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ERROR))

        outcome = await harness.tick()
        self.assertTrue(outcome.has("slot_relation_conflict"))
        self.assertEqual(harness.slot.initiator_order_id, 0,
                         "with no confirmable order the relation is restored")
        self.assertEqual(int(harness.pair.status), int(pair_arb.PairStatus.RECONCILING))

    async def test_two_active_orders_for_one_role_are_detected(self):
        harness = Harness()
        order = await harness.start_slot()
        harness.pair.attach(
            pair_arb.ActiveOrder(
                order_id=order.order_id + 100,
                slot_id=harness.slot.slot_id,
                role=Role.INITIATOR,
                status=OrderStatus.WORKING,
                qty=1.0,
            )
        )
        self.assertEqual(int(harness.slot_state()), int(pair_arb.SlotState.ERROR))
        outcome = await harness.tick()
        self.assertTrue(outcome.has("slot_relation_conflict"))
        self.assertEqual(len(harness.broker.create_requests), 1)


class TestPairArbPostureGates(unittest.IsolatedAsyncioTestCase):
    async def test_restricted_posture_blocks_new_slots_until_facts_recover(self):
        harness = Harness()
        harness.set_market(ts_ns=harness.now_ns - 5_000 * NS)
        outcome = await harness.tick(refresh_market=False)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.RESTRICTED)
        self.assertEqual(harness.slot.slot_id, 0)
        self.assertTrue(outcome.empty or not outcome.has("slot_started"))

        harness.set_market(ts_ns=harness.now_ns)
        await harness.tick(refresh_market=False)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.NORMAL)
        self.assertEqual(harness.slot.slot_id, 1)

    async def test_halt_posture_only_issues_explicit_cancels(self):
        harness = Harness()
        await harness.tick()
        harness.pair.posture = pair_arb.Posture.HALT
        harness.pair.posture_latched = True

        outcome = await harness.tick()
        self.assertEqual(outcome.reason, "halt")
        self.assertTrue(outcome.has("cancel_requested:halt"))
        self.assertEqual(harness.initiator().status, OrderStatus.CANCEL_REQUESTED)
        self.assertEqual(len(harness.broker.create_requests), 1, "HALT never creates orders")

    async def test_halt_latch_survives_a_clean_risk_check(self):
        harness = Harness()
        await harness.tick()
        harness.pair.posture = pair_arb.Posture.HALT
        harness.pair.posture_latched = True
        snapshot = await harness.engine.risk_check(harness.ctx)
        self.assertTrue(snapshot.clean)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.HALT)

    async def test_capacity_exhaustion_refuses_new_orders_and_alerts(self):
        harness = Harness()
        for index in range(harness.pair.capacity):
            harness.pair.attach(
                pair_arb.ActiveOrder(
                    order_id=100 + index,
                    slot_id=100 + index,
                    role=Role.INITIATOR,
                    symbol="AAA",
                    broker="venue_a",
                    side=Side.BUY,
                    price=1.0,
                    qty=1.0,
                    status=OrderStatus.WORKING,
                )
            )
        self.assertTrue(harness.pair.capacity_exhausted())

        outcome = await harness.tick()
        self.assertEqual(harness.pair.posture, pair_arb.Posture.RESTRICTED)
        self.assertEqual(harness.slot.slot_id, 0)
        self.assertFalse(outcome.has("slot_started"))
        self.assertTrue(any("capacity" in alert for alert in harness.ctx.alerts))
        self.assertIn(
            pair_arb.ReconcileReason.CAPACITY_EXHAUSTED, harness.reconcile_reasons()
        )

    async def test_active_order_array_never_overwrites_an_existing_record(self):
        harness = Harness()
        for index in range(harness.pair.capacity):
            harness.pair.attach(
                pair_arb.ActiveOrder(
                    order_id=200 + index,
                    slot_id=200 + index,
                    role=Role.INITIATOR,
                    status=OrderStatus.WORKING,
                )
            )
        with self.assertRaises(BufferError):
            harness.pair.attach(pair_arb.ActiveOrder(order_id=999, status=OrderStatus.WORKING))
        self.assertIsNone(harness.pair.get_active(999))
        self.assertIsNotNone(harness.pair.get_active(200))


if __name__ == "__main__":
    unittest.main()
