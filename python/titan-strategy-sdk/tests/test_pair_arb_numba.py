"""Behaviour tests for the ABI v12 Numba ``pair_arb`` strategy.

These mirror the requirements-level scenarios of ``test_pair_arb_*.py`` but drive the real
strategy object the runtime loads: ticks go in through ``on_tick``, fills and order events through
``on_filled``/``on_order``, and the emitted orders are read back from the backtest command buffer.
"""

import unittest

import numpy as np

from pair_arb_numba_harness import (  # noqa: E402
    L,
    NumbaHarness,
    default_parameters,
    pair_arb_numba,
)


def commands_of_kind(harness, kind):
    return [harness.command(i) for i in range(harness.command_count)
            if harness.command(i)["kind"] == kind]


class TestPairArbNumbaSlotLifecycle(unittest.IsolatedAsyncioTestCase):
    def test_first_tick_creates_slot_and_post_only_maker_order(self):
        harness = NumbaHarness()
        harness.tick()

        self.assertEqual(harness.slot_id, 1)
        self.assertAlmostEqual(float(harness.state[L.F_SLOT_TARGET_LOTS]), 10.0)
        self.assertEqual(harness.command_count, 1)

        order = harness.command(0)
        self.assertEqual(order["kind"], 1)
        self.assertEqual(order["side"], 1)
        self.assertEqual(order["time_in_force"], 1)  # post only
        self.assertEqual(order["order_type"], 0)
        self.assertEqual(order["asset_no"], 0)
        self.assertEqual(order["local_account_no"], 0)
        self.assertAlmostEqual(float(order["price"]), 990.0)  # (100.0 - 1.0) / 0.1 tick
        self.assertAlmostEqual(float(order["qty"]), 10.0)
        self.assertEqual(harness.initiator_order_id, int(order["order_id"]))
        self.assertEqual(harness.order_status(harness.initiator_order_id), L.ORDER_WORKING)

    def test_no_slot_before_start_time(self):
        harness = NumbaHarness(start_time_ns=1_000_000 * 1_000_000)
        harness.tick()
        self.assertEqual(harness.slot_id, 0)
        self.assertEqual(harness.command_count, 0)

    def test_short_spread_sells_the_left_leg_at_the_taker_ask(self):
        harness = NumbaHarness(direction="SHORT_SPREAD")
        harness.tick()

        order = harness.command(0)
        self.assertEqual(order["side"], -1)
        self.assertEqual(order["asset_no"], 0)
        self.assertAlmostEqual(float(order["price"]), 1012.0)  # (100.2 + 1.0) / 0.1

    def test_taker_taker_submits_both_legs_as_ioc(self):
        harness = NumbaHarness(mode="TAKER_TAKER")
        harness.tick()

        self.assertEqual(harness.command_count, 2)
        initiator, hedge = harness.command(0), harness.command(1)
        self.assertEqual(initiator["time_in_force"], 3)  # IOC
        self.assertEqual(initiator["asset_no"], 0)
        self.assertAlmostEqual(float(initiator["price"]), 502.0)  # left ask 50.2 / 0.1
        self.assertEqual(hedge["time_in_force"], 3)
        self.assertEqual(hedge["asset_no"], 1)
        self.assertEqual(hedge["side"], -1)

    def test_slot_target_is_clamped_by_max_position_and_then_drains(self):
        harness = NumbaHarness(max_position_lots=15.0)
        harness.tick()
        self.assertAlmostEqual(float(harness.state[L.F_SLOT_TARGET_LOTS]), 10.0)

        initiator = harness.initiator_order_id
        harness.fill(initiator, 10.0, status=L.VENUE_FILLED)
        harness.fill(harness.hedge_order_id, 10.0, status=L.VENUE_FILLED)
        harness.tick()

        self.assertEqual(harness.slot_id, 2)
        self.assertAlmostEqual(float(harness.state[L.F_SLOT_TARGET_LOTS]), 5.0)

        second = harness.initiator_order_id
        harness.fill(second, 5.0, status=L.VENUE_FILLED)
        harness.fill(harness.hedge_order_id, 5.0, status=L.VENUE_FILLED)
        harness.tick()
        self.assertEqual(harness.status, L.PAIR_DRAINING)
        self.assertEqual(harness.slot_id, 2)


class TestPairArbNumbaRequote(unittest.IsolatedAsyncioTestCase):
    def test_maker_is_kept_inside_the_requote_distance(self):
        harness = NumbaHarness()
        harness.tick()
        harness.set_market(right_bid=99.8, right_ask=100.0)
        harness.tick()
        self.assertEqual(len(commands_of_kind(harness, 2)), 0)
        self.assertEqual(harness.order_status(harness.initiator_order_id), L.ORDER_WORKING)

    def test_maker_is_canceled_once_when_the_spread_drifts(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.set_market(right_bid=98.5, right_ask=98.7)
        harness.tick()

        cancels = commands_of_kind(harness, 2)
        self.assertEqual(len(cancels), 1)
        self.assertEqual(cancels[0]["asset_no"], 0)
        self.assertEqual(int(cancels[0]["order_id"]), initiator)
        self.assertEqual(harness.order_status(initiator), L.ORDER_CANCEL_REQUESTED)

        harness.tick()
        self.assertEqual(len(commands_of_kind(harness, 2)), 1, "one cancel per active maker")
        self.assertEqual(harness.initiator_order_id, initiator, "no re-place before confirmation")

    def test_cancel_confirmation_clears_the_relation_and_allows_replacement(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.set_market(right_bid=98.5, right_ask=98.7)
        harness.tick()

        harness.order_event(initiator, L.VENUE_CANCELED)
        self.assertEqual(harness.initiator_order_id, 0)
        self.assertEqual(harness.order_index(initiator), -1)

        harness.tick()
        self.assertNotEqual(harness.initiator_order_id, 0)
        replacement = harness.last_new_order(asset_no=0)
        self.assertAlmostEqual(float(replacement["price"]), 975.0)  # (98.5 - 1.0) / 0.1
        self.assertAlmostEqual(float(replacement["qty"]), 10.0)

    def test_rejected_order_clears_the_relation_and_next_tick_replaces(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.order_event(initiator, L.VENUE_REJECTED)

        self.assertEqual(harness.initiator_order_id, 0)
        history = harness.history_entries()
        self.assertEqual(history[-1]["order_id"], initiator)
        self.assertEqual(history[-1]["status"], L.ORDER_REJECTED)

        harness.tick()
        self.assertNotEqual(harness.initiator_order_id, 0)
        self.assertEqual(len(commands_of_kind(harness, 1)), 2)

    def test_cancel_timeout_marks_unknown_and_blocks_replacement(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.set_market(right_bid=98.5, right_ask=98.7)
        harness.tick()

        harness.advance(2_000)  # beyond cancel_timeout_ns
        harness.tick()
        self.assertEqual(harness.order_status(initiator), L.ORDER_UNKNOWN)
        self.assertEqual(harness.posture, L.POSTURE_RESTRICTED)
        self.assertEqual(harness.initiator_order_id, initiator)

        harness.tick()
        self.assertEqual(len(commands_of_kind(harness, 1)), 1, "unknown orders are never re-sent")


class TestPairArbNumbaFills(unittest.IsolatedAsyncioTestCase):
    def test_partial_fill_accumulates_without_hedging(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 4.0)

        self.assertAlmostEqual(harness.init_filled, 4.0)
        self.assertEqual(harness.hedge_order_id, 0)
        self.assertEqual(len(commands_of_kind(harness, 1)), 1)
        self.assertEqual(harness.order_status(initiator), L.ORDER_PARTIAL)

    def test_cumulative_reports_are_deduplicated_and_deltas_applied(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 4.0)
        harness.fill(initiator, 4.0, cumulative=4.0)  # duplicate cumulative report
        self.assertAlmostEqual(harness.init_filled, 4.0)
        harness.fill(initiator, 3.0, cumulative=7.0)
        self.assertAlmostEqual(harness.init_filled, 7.0)
        harness.fill(initiator, 2.0, cumulative=6.0)  # regression is ignored
        self.assertAlmostEqual(harness.init_filled, 7.0)

    def test_full_initiator_fill_archives_and_submits_the_hedge_ioc(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 10.0, status=L.VENUE_FILLED)

        self.assertEqual(harness.initiator_order_id, 0)
        self.assertEqual(harness.order_index(initiator), -1)
        hedge = harness.hedge_order_id
        self.assertNotEqual(hedge, 0)

        hedges = [command for command in commands_of_kind(harness, 1)
                  if command["asset_no"] == 1]
        self.assertEqual(len(hedges), 1)
        self.assertEqual(hedges[0]["side"], -1)
        self.assertEqual(hedges[0]["time_in_force"], 3)
        self.assertEqual(hedges[0]["local_account_no"], 1)
        self.assertAlmostEqual(float(hedges[0]["qty"]), 10.0)
        self.assertAlmostEqual(float(hedges[0]["price"]), 1000.0)

        history = harness.history_entries()
        self.assertEqual(history[-1]["order_id"], initiator)
        self.assertEqual(history[-1]["status"], L.ORDER_FILLED)
        self.assertEqual(history[-1]["slot_id"], 1)

    def test_hedge_uses_total_slot_fill_and_the_ratio(self):
        harness = NumbaHarness(hedge_ratio_abs=0.6)
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 4.0)
        harness.fill(initiator, 6.0, status=L.VENUE_FILLED)

        hedge = [command for command in commands_of_kind(harness, 1)
                 if command["asset_no"] == 1][-1]
        self.assertAlmostEqual(float(hedge["qty"]), 6.0)

    def test_dust_sized_hedge_gap_completes_the_slot_without_a_hedge_order(self):
        harness = NumbaHarness(hedge_ratio_abs=0.02, dust_lots=0.5, max_position_lots=20.0)
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 10.0, status=L.VENUE_FILLED)

        self.assertEqual(harness.hedge_order_id, 0)
        self.assertEqual(len([c for c in commands_of_kind(harness, 1) if c["asset_no"] == 1]), 0)

        harness.tick()
        self.assertEqual(harness.slot_id, 2, "a dust-only slot still lets the next slot start")

    def test_hedge_full_fill_completes_the_slot_and_advances_on_the_next_tick(self):
        harness = NumbaHarness()
        harness.tick()
        harness.fill(harness.initiator_order_id, 10.0, status=L.VENUE_FILLED)
        hedge = harness.hedge_order_id
        harness.fill(hedge, 10.0, status=L.VENUE_FILLED)

        self.assertEqual(harness.hedge_order_id, 0)
        self.assertGreater(int(harness.state_i64[L.I_SLOT_COMPLETED_TS]), 0)
        self.assertEqual(harness.hedge_filled, 10.0)

        harness.tick()
        self.assertEqual(harness.slot_id, 2)
        self.assertEqual(harness.init_filled, 0.0)
        self.assertEqual(harness.hedge_filled, 0.0)
        self.assertEqual(len(commands_of_kind(harness, 1)), 3)

    def test_over_fill_latches_halt(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.fill(initiator, 20.0)

        self.assertEqual(harness.posture, L.POSTURE_HALT)
        self.assertEqual(int(harness.state_i64[L.I_LAST_ERROR]), 3)
        self.assertAlmostEqual(harness.init_filled, 0.0, msg="the bad fill is not accumulated")

    def test_hedge_gap_is_resubmitted_after_the_hedge_order_is_canceled(self):
        harness = NumbaHarness()
        harness.tick()
        harness.fill(harness.initiator_order_id, 10.0, status=L.VENUE_FILLED)
        hedge = harness.hedge_order_id
        harness.order_event(hedge, L.VENUE_CANCELED)
        self.assertEqual(harness.hedge_order_id, 0)

        harness.tick()
        self.assertNotEqual(harness.hedge_order_id, 0)
        self.assertNotEqual(harness.hedge_order_id, hedge)


class TestPairArbNumbaRiskAndCapacity(unittest.IsolatedAsyncioTestCase):
    def test_unhedged_quantity_escalates_the_posture(self):
        harness = NumbaHarness(max_unhedged_lots_soft=5.0, max_unhedged_lots_hard=8.0)
        harness.tick()
        harness.fill(harness.initiator_order_id, 10.0, status=L.VENUE_FILLED)
        harness.state[L.F_SLOT_HEDGE_FILLED_LOTS] = 0.0
        harness.state_i64[L.I_SLOT_HEDGE_ORDER_ID] = 0

        harness.tick()
        self.assertEqual(harness.posture, L.POSTURE_EMERGENCY)
        self.assertAlmostEqual(float(harness.state[L.F_UNHEDGED_LOTS]), 10.0)

    def test_active_order_capacity_is_never_overwritten(self):
        harness = NumbaHarness()
        for index in range(L.MAX_ACTIVE_ORDERS):
            harness.state_i64[L.i_order_field(index, L.I_ORDER_ID)] = 900 + index
            harness.state_i64[L.i_order_field(index, L.I_ORDER_STATUS)] = L.ORDER_WORKING
            harness.state_i64[L.i_order_field(index, L.I_ORDER_SLOT)] = 50
            harness.state_i64[L.i_order_field(index, L.I_ORDER_ROLE)] = L.ROLE_INITIATOR

        harness.tick()
        self.assertEqual(harness.slot_id, 0, "no slot while the active array is full")
        self.assertEqual(harness.command_count, 0)
        for index in range(L.MAX_ACTIVE_ORDERS):
            self.assertEqual(
                int(harness.state_i64[L.i_order_field(index, L.I_ORDER_ID)]), 900 + index
            )

    def test_halt_posture_only_cancels(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.state_i64[L.I_POSTURE_LATCHED] = 1

        harness.tick()
        self.assertEqual(harness.posture, L.POSTURE_HALT)
        self.assertEqual(harness.order_status(initiator), L.ORDER_CANCEL_REQUESTED)
        self.assertEqual(len(commands_of_kind(harness, 1)), 1, "HALT never creates orders")

    def test_on_stop_cancels_working_orders_and_marks_stopped(self):
        harness = NumbaHarness()
        harness.tick()
        initiator = harness.initiator_order_id
        harness.stop()

        self.assertEqual(harness.status, L.PAIR_STOPPED)
        self.assertEqual(harness.order_status(initiator), L.ORDER_CANCEL_REQUESTED)


class TestPairArbNumbaBuild(unittest.TestCase):
    def test_build_exposes_the_abi_v12_callbacks(self):
        strategy = pair_arb_numba.build(default_parameters())
        self.assertEqual(strategy.strategy_id, "pair_arb")
        for name in ("on_start", "on_tick", "on_filled", "on_order", "on_stop"):
            self.assertTrue(callable(getattr(strategy, name)), name)
        self.assertEqual(strategy.state.dtype, np.dtype(np.float64))
        self.assertEqual(strategy.state.ndim, 1)
        self.assertTrue(strategy.state.flags.c_contiguous)
        self.assertEqual(strategy.state_i64.dtype, np.dtype(np.int64))
        self.assertEqual(strategy.state_i64.ndim, 1)

    def test_invalid_parameters_are_rejected(self):
        base = default_parameters()
        with self.assertRaises(ValueError):
            pair_arb_numba.build({**base, "hedge_ratio_abs": 0.0})
        with self.assertRaises(ValueError):
            pair_arb_numba.build({**base, "direction": "SIDEWAYS"})
        with self.assertRaises(ValueError):
            pair_arb_numba.build({**base, "max_unhedged_lots_soft": 5.0,
                                  "max_unhedged_lots_hard": 1.0})
        with self.assertRaises(ValueError):
            pair_arb_numba.build({**base, "left_asset_no": 1, "right_asset_no": 1})


if __name__ == "__main__":
    unittest.main()
