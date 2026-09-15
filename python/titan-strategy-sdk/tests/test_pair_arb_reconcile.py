"""Reconcile, restart recovery, audit records and cross-strategy concurrency."""

import asyncio
import unittest

from pair_arb_harness import Harness, OrderStatus, VenueOrderStatus, pair_arb  # noqa: E402


def reconcile_request(reason=pair_arb.ReconcileReason.RECONNECT, order_id=0, text="test"):
    return pair_arb.ReconcileRequest(
        reason=reason, ts_ns=0, pair_id="pair-1", order_id=order_id, event_text=text
    )


class TestPairArbReconcile(unittest.IsolatedAsyncioTestCase):
    async def test_reconcile_recovers_a_missing_fill_exactly_once(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        harness.broker.publish_order(
            order.order_id, VenueOrderStatus.PARTIALLY_FILLED, qty=10.0, price=99.0,
            cumulative_filled_qty=6.0, last_fill_price=99.0,
        )

        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RESOLVED)
        self.assertEqual(order.filled_qty, 6.0)
        self.assertEqual(harness.slot.initiator_filled_qty, 6.0)
        self.assertEqual(order.status, OrderStatus.PARTIAL)
        self.assertEqual(harness.pair.status, pair_arb.PairStatus.RUNNING)
        self.assertIn("recovered_fill:1:2.0", record.actions)

        await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(order.filled_qty, 6.0, "reconcile never double counts a fill")
        self.assertEqual(harness.slot.initiator_filled_qty, 6.0)

    async def test_reconcile_of_a_terminal_venue_order_archives_and_clears_the_relation(self):
        harness = Harness()
        order = await harness.start_slot()
        harness.broker.publish_order(
            order.order_id, VenueOrderStatus.FILLED, qty=10.0, price=99.0,
            cumulative_filled_qty=10.0, last_fill_price=99.0,
        )
        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RESOLVED)
        self.assertEqual(harness.slot.initiator_order_id, 0)
        archived = harness.orders_list().get(order.order_id)
        self.assertEqual(archived.status, OrderStatus.FILLED)
        self.assertEqual(archived.filled_qty, 10.0)

        result = await harness.tick()
        self.assertTrue(result.has("hedge_submitted"), "the recovered fill is hedged next tick")
        self.assertEqual(harness.hedge().qty, 10.0)

    async def test_unresolved_orders_keep_the_pair_reconciling_and_halted(self):
        harness = Harness()
        order = await harness.start_slot()
        order.status = OrderStatus.UNKNOWN
        harness.broker.forget_order(order.order_id)

        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RETRY_REQUIRED)
        self.assertEqual(harness.pair.status, pair_arb.PairStatus.RECONCILING)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.HALT)

        await harness.tick()
        self.assertEqual(harness.slot.slot_id, 1)
        self.assertEqual(len(harness.broker.create_requests), 1, "no new orders while halted")

    async def test_venue_only_orders_require_manual_review(self):
        harness = Harness()
        order = await harness.start_slot()
        harness.broker.publish_order(
            order.order_id, VenueOrderStatus.NEW, qty=10.0, price=99.0
        )
        harness.broker.open_orders = [{"order_id": 98765, "status": int(VenueOrderStatus.NEW)}]

        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.MANUAL_REQUIRED)
        self.assertIn("venue_only_orders", record.difference)
        self.assertEqual(harness.pair.status, pair_arb.PairStatus.ERROR)
        self.assertTrue(harness.pair.posture_latched)
        self.assertEqual(harness.pair.posture, pair_arb.Posture.HALT)

    async def test_restored_active_order_keeps_its_relation(self):
        harness = Harness()
        order = await harness.start_slot()
        harness.broker.publish_order(
            order.order_id, VenueOrderStatus.PARTIALLY_FILLED, qty=10.0, price=99.0,
            cumulative_filled_qty=3.0, last_fill_price=99.0,
        )
        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RESOLVED)
        self.assertEqual(order.status, OrderStatus.PARTIAL)
        self.assertEqual(harness.slot.initiator_order_id, order.order_id)

    async def test_audit_record_captures_the_required_fields(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        harness.broker.publish_order(order.order_id, VenueOrderStatus.PARTIALLY_FILLED,
                                     qty=10.0, price=99.0)

        record = await harness.engine.reconcile(
            harness.ctx,
            reconcile_request(pair_arb.ReconcileReason.CANCEL_TIMEOUT, order.order_id, "late"),
        )
        self.assertEqual(record.reason, pair_arb.ReconcileReason.CANCEL_TIMEOUT)
        self.assertEqual(record.pair_id, "pair-1")
        self.assertEqual(record.order_id, order.order_id)
        self.assertEqual(record.slot_id, 1)
        self.assertEqual(record.event_text, "late")
        self.assertEqual(record.local_snapshot["initiator_filled_qty"], 4.0)
        self.assertIn(str(order.order_id), record.venue_snapshot)
        self.assertIn("open_orders", record.venue_snapshot)
        self.assertEqual(record.account_snapshot, {})
        self.assertTrue(record.actions)
        self.assertNotEqual(record.conclusion, 0)
        self.assertEqual(harness.audit(), [record])

    async def test_reconcile_never_resends_unknown_requests(self):
        harness = Harness()
        harness.broker.create_outcome = pair_arb.CommandOutcome.TIMEOUT
        await harness.tick()
        order = harness.initiator()
        self.assertEqual(order.status, OrderStatus.UNKNOWN)
        self.assertEqual(len(harness.broker.create_requests), 1)

        await harness.engine.reconcile(harness.ctx, reconcile_request())
        await harness.tick()
        self.assertEqual(len(harness.broker.create_requests), 1, "invariant I8")

    async def test_reconcile_resolving_an_unknown_order_unblocks_replacement(self):
        harness = Harness()
        harness.broker.create_outcome = pair_arb.CommandOutcome.TIMEOUT
        await harness.tick()
        order = harness.initiator()
        self.assertEqual(order.status, OrderStatus.UNKNOWN)

        harness.broker.publish_order(
            order.order_id, VenueOrderStatus.CANCELED, qty=10.0, price=99.0
        )
        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RESOLVED)
        self.assertEqual(harness.slot.initiator_order_id, 0)

        result = await harness.tick()
        self.assertTrue(result.has("initiator_submitted"))
        self.assertEqual(harness.initiator().qty, 10.0)


class TestPairArbRestartRecovery(unittest.IsolatedAsyncioTestCase):
    async def test_reconnect_gate_stops_new_slots_until_facts_are_restored(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 4.0)
        active = harness.initiator()
        harness.broker.publish_order(active.order_id, VenueOrderStatus.PARTIALLY_FILLED,
                                     qty=10.0, price=99.0, cumulative_filled_qty=4.0,
                                     last_fill_price=99.0)

        harness.engine.mark_not_ready(harness.ctx)
        self.assertEqual(harness.pair.status, pair_arb.PairStatus.DRAINING)
        blocked = await harness.tick()
        self.assertFalse(blocked.has("slot_started"))
        self.assertEqual(harness.slot.initiator_filled_qty, 4.0,
                         "recovery keeps confirmed fills")

        record = await harness.engine.reconcile(harness.ctx, reconcile_request())
        self.assertEqual(record.conclusion, pair_arb.AuditConclusion.RESOLVED)
        harness.engine.mark_ready(harness.ctx)
        self.assertEqual(harness.pair.status, pair_arb.PairStatus.RUNNING)

        resumed = await harness.tick()
        self.assertTrue(resumed.has("maker_kept") or resumed.has("initiator_waiting"))
        self.assertEqual(harness.slot.initiator_filled_qty, 4.0)

    async def test_restore_from_venue_rebinds_orders_to_the_current_slot(self):
        harness = Harness()
        order = await harness.start_slot()
        item = pair_arb.OrdersListItem(
            order_id=order.order_id,
            slot_id=harness.slot.slot_id,
            role=pair_arb.Role.INITIATOR,
            symbol="AAA",
            broker="venue_a",
            side=pair_arb.Side.BUY,
            price=99.0,
            qty=10.0,
            status=OrderStatus.PARTIAL,
            filled_qty=4.0,
        )
        harness.pair.detach(order.order_id)
        harness.slot.initiator_order_id = 0
        harness.engine.restore_from_venue(harness.ctx, [item])
        self.assertEqual(harness.slot.initiator_filled_qty, 4.0)
        self.assertEqual(harness.orders_list().get(order.order_id).filled_qty, 4.0)

    async def test_orders_list_keeps_one_record_per_order_id(self):
        harness = Harness()
        order = await harness.start_slot()
        await harness.fill(order.order_id, 10.0, status=VenueOrderStatus.FILLED)
        item = harness.orders_list().get(order.order_id)
        with self.assertRaises(ValueError):
            harness.orders_list().append(item)
        self.assertEqual(len(harness.orders_list()), 1)


class _GatedBroker(pair_arb.SimulatedBroker):
    """Broker that blocks the first create on a test-released event."""

    def __init__(self):
        super().__init__()
        self.gate = asyncio.Event()
        self.entered = asyncio.Event()

    async def create_order(self, request):
        self.entered.set()
        await self.gate.wait()
        return await super().create_order(request)


class TestPairArbConcurrency(unittest.IsolatedAsyncioTestCase):
    async def test_one_pair_waiting_on_the_broker_does_not_block_another_pair(self):
        blocked = Harness(broker=_GatedBroker())
        healthy = Harness(pair_id="pair-2")

        pending = asyncio.ensure_future(blocked.tick())
        await blocked.broker.entered.wait()
        self.assertFalse(pending.done())

        result = await healthy.tick()
        self.assertTrue(result.has("slot_started"))
        self.assertEqual(healthy.slot.slot_id, 1)
        self.assertEqual(len(healthy.broker.create_requests), 1)

        blocked.broker.gate.set()
        await pending
        self.assertEqual(blocked.slot.slot_id, 1)
        self.assertEqual(blocked.ctx.pair.pair_id, "pair-1")


if __name__ == "__main__":
    unittest.main()
