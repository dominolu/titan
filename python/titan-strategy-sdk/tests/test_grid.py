"""Grid business regressions; native execution is covered by test_grid_native.py."""

from contextlib import ExitStack
from itertools import permutations
import unittest
from unittest.mock import patch

import numpy as np
from numba.core.registry import CPUDispatcher

from titan_strategy import abi_v13
from strategies.grid import strategy as grid


def row(dtype, **values):
    result = np.zeros(1, dtype=dtype)
    for name, value in values.items():
        result[0][name] = value
    return result[0]


class Context:
    def __init__(self, **parameters):
        self.state_array = grid.build(parameters).state
        self.state = self.state_array[0]
        self.now = 1_000_000_000
        self.market_row = row(abi_v13.market_dtype, best_bid_ticks=99, best_ask_ticks=101)
        self.account_row = row(abi_v13.account_dtype, state=4, account_epoch=1, account_sequence=1)
        self.position_row = row(abi_v13.position_dtype, account_sequence=1)
        self.balance_row = row(abi_v13.balance_dtype, total_units=100000, available_units=100000)
        self.active = np.zeros(0, dtype=abi_v13.active_order_dtype)
        self.fill_rows = np.zeros(0, dtype=abi_v13.fill_dtype)
        self.order_rows = np.zeros(0, dtype=abi_v13.order_event_dtype)
        self.cancel_rows = np.zeros(0, dtype=abi_v13.cancel_event_dtype)
        self.recovery_rows = None
        self.tick_rows = np.zeros(0, dtype=abi_v13.tick_dtype)
        self.submits, self.cancels = [], []
        self.next_id, self.sequence, self.stop_complete = 1, 1, False

    def account(self, _):
        return self.account_row

    def position(self, *_):
        return self.position_row

    def balance(self, *_):
        return self.balance_row

    def market(self, _):
        return self.market_row

    def active_orders(self):
        return self.active

    def ticks(self):
        return self.tick_rows

    def fills(self):
        return self.fill_rows

    def order_events(self):
        return self.order_rows

    def cancel_events(self):
        return self.cancel_rows

    def recovery_orders(self):
        return self.active if self.recovery_rows is None else self.recovery_rows

    def complete_stop(self):
        self.stop_complete = True

    def submit_order(self, account, asset, side, order_type, qty, price, tif, reduce_only=False):
        order_id = self.next_id
        self.next_id += 1
        order = row(abi_v13.active_order_dtype, order_id=order_id, account_no=account,
                    asset_no=asset, side=side, order_type=order_type, qty_lots=qty,
                    price_ticks=price, time_in_force=tif, reduce_only=reduce_only,
                    status=grid.ACCEPTED, created_ts_ns=self.now, updated_ts_ns=self.now)
        self.active = np.append(self.active, order)
        self.submits.append(order.copy())
        return order_id

    def cancel_order(self, account, asset, order_id):
        self.cancels.append(int(order_id))
        return len(self.cancels)

    def tick(self, bid=99, ask=101):
        self.market_row["best_bid_ticks"], self.market_row["best_ask_ticks"] = bid, ask
        self.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=self.now)], dtype=abi_v13.tick_dtype)
        grid.on_tick(self)

    def start(self):
        assert grid.on_start(self) == 0
        self.tick()
        return self

    def fill(self, order_id, qty, price, *, update_position=True, final=False):
        index = next(i for i, o in enumerate(self.active) if o["order_id"] == order_id)
        order = self.active[index]
        cumulative = int(order["cumulative_filled_lots"]) + qty
        order["cumulative_filled_lots"] = cumulative
        order["status"] = grid.FILLED if cumulative == order["qty_lots"] else grid.PARTIAL
        self.sequence += 1
        fill = row(abi_v13.fill_dtype, order_id=order_id, side=order["side"],
                   fill_price_ticks=price, fill_qty_lots=qty, cumulative_filled_lots=cumulative,
                   account_sequence=self.sequence, final_fill=final)
        if update_position:
            previous = int(self.position_row["qty_lots"])
            signed = qty if order["side"] == grid.BUY else -qty
            if previous == 0 or (previous > 0) == (signed > 0):
                cost = abs(previous) * int(self.position_row["average_price_ticks"]) + qty * price
                self.position_row["average_price_ticks"] = round(cost / (abs(previous) + qty))
            elif qty >= abs(previous):
                self.position_row["average_price_ticks"] = price if qty > abs(previous) else 0
            self.position_row["qty_lots"] += qty if order["side"] == grid.BUY else -qty
            self.position_row["account_sequence"] = self.sequence
        self.fill_rows = np.array([fill], dtype=abi_v13.fill_dtype)
        grid.on_fill(self)

    def terminal(self, order_id, status=grid.CANCELED, cumulative=None):
        index = next(i for i, o in enumerate(self.active) if o["order_id"] == order_id)
        order = self.active[index]
        event = row(abi_v13.order_event_dtype, order_id=order_id, status=status,
                    price_ticks=order["price_ticks"], qty_lots=order["qty_lots"],
                    cumulative_filled_lots=order["cumulative_filled_lots"] if cumulative is None else cumulative)
        self.active = np.delete(self.active, index)
        self.order_rows = np.array([event], dtype=abi_v13.order_event_dtype)
        grid.on_order(self)


class TestGrid(unittest.TestCase):
    def setUp(self):
        self.stack = ExitStack()
        self.addCleanup(self.stack.close)
        # A normal Python context exercises the same source without pretending it is a native ABI.
        for name, value in list(vars(grid).items()):
            if isinstance(value, CPUDispatcher):
                self.stack.enter_context(patch.object(grid, name, value.py_func))

    def test_neutral_quotes_both_sides_and_market_does_not_move_anchor(self):
        ctx = Context().start()
        self.assertEqual([(int(o["side"]), int(o["price_ticks"])) for o in ctx.submits], [(1, 95), (2, 105)])
        ctx.tick(101, 103)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 100)
        self.assertEqual(len(ctx.submits), 2)

    def test_partial_fill_does_not_cancel_or_reanchor(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 100)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 1)
        self.assertEqual(ctx.cancels, [])

    def test_reset_waits_for_confirmed_cancel(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        self.assertEqual(ctx.cancels, [2])
        self.assertEqual(len(ctx.submits), 2)
        self.assertEqual(ctx.state["grid"]["status"], grid.RESETTING)
        ctx.terminal(2)
        self.assertEqual(grid.ref_count(ctx.state), 0)
        ctx.now += 500_000_000
        ctx.tick(94, 96)
        self.assertEqual(len(ctx.submits), 4)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 95)

    def test_retained_orders_survive_reanchor(self):
        ctx = Context(order_policy="retain", buy_levels=2, sell_levels=2).start()
        old_sell_ids = [int(o["order_id"]) for o in ctx.submits if o["side"] == 2]
        ctx.fill(1, 2, 95, final=True)
        self.assertEqual(ctx.cancels, [])
        self.assertTrue(all(grid.find_ref(ctx.state, oid) >= 0 for oid in old_sell_ids))
        self.assertEqual(grid.ref_count(ctx.state, grid.GRID, grid.BUY), 2)

    def test_anchor_uses_last_execution_or_whole_order_average(self):
        for mode, expected in (("last_fill", 94), ("order_average", 95)):
            with self.subTest(mode=mode):
                ctx = Context(anchor_mode=mode).start()
                ctx.fill(1, 1, 96)
                ctx.fill(1, 1, 94, final=True)
                self.assertEqual(ctx.state["grid"]["anchor_ticks"], expected)

    def test_average_anchor_preserves_fractional_ticks_until_quote_rounding(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95)
        ctx.fill(1, 1, 94, final=True)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 94.5)
        self.assertEqual(grid.level_price(ctx.state["config"], 94.5, grid.BUY, 1), 89)
        self.assertEqual(grid.level_price(ctx.state["config"], 94.5, grid.SELL, 1), 100)

    def test_duplicate_fill_is_idempotent_even_after_ref_released(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95)
        grid.on_fill(ctx)
        ctx.fill(1, 1, 95, final=True)
        grid.on_fill(ctx)
        self.assertEqual(ctx.state["grid"]["fill_count"], 2)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 2)

    def test_terminal_before_fill_keeps_reservation(self):
        ctx = Context().start()
        ctx.terminal(1, grid.FILLED, cumulative=2)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 100)
        self.assertEqual(grid.ref_count(ctx.state), 2)
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 2, 2
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=1, side=1,
                                     fill_price_ticks=95, fill_qty_lots=2, cumulative_filled_lots=2,
                                     account_sequence=2, final_fill=1)], dtype=abi_v13.fill_dtype)
        grid.on_fill(ctx)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 95)

    def test_position_ahead_of_known_execution_waits_for_independent_fill(self):
        ctx = Context().start()
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 2, 4
        ctx.terminal(1, grid.FILLED, cumulative=2)
        self.assertNotEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 0)
        self.assertEqual(len(ctx.submits), 2)

    def test_old_resting_order_gets_a_fresh_terminal_fill_delivery_deadline(self):
        ctx = Context().start()
        ctx.now += 11_000_000_000
        ctx.terminal(1, grid.FILLED, cumulative=2)
        self.assertNotEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        ctx.now += 10_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_cancel_fill_race_applies_both_fills(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        ctx.fill(2, 2, 105, final=True)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 0)
        self.assertEqual(ctx.state["grid"]["grid_profit_tick_lots"], 20)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 105)
        self.assertEqual(ctx.state["grid"]["completed_pairs"], 1)

    def test_partial_cancel_accounts_fill_before_releasing_order(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95)
        ctx.terminal(1, grid.CANCELED)
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 100)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 1)

    def test_cancel_acceptance_without_terminal_order_does_not_release(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        ctx.cancel_rows = np.array([row(abi_v13.cancel_event_dtype, order_id=2, request_result=0,
                                       final_status=grid.CANCELED)], dtype=abi_v13.cancel_event_dtype)
        grid.on_cancel(ctx)
        self.assertGreaterEqual(grid.find_ref(ctx.state, 2), 0)
        self.assertEqual(len(ctx.submits), 2)

    def test_cancel_failures_back_off_and_eventually_require_reconciliation(self):
        ctx = Context(cancel_retry_limit=2).start()
        ctx.fill(1, 2, 95, final=True)
        grid.on_timer(ctx)
        self.assertEqual(len(ctx.cancels), 1)
        ctx.now += 500_000_000
        grid.on_timer(ctx)
        ctx.now += 500_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(len(ctx.cancels), 2)

    def test_long_and_short_initialize_before_post_only_grid(self):
        for direction, expected_side in (("long_bias", 1), ("short_bias", 2)):
            with self.subTest(direction=direction):
                ctx = Context(direction=direction).start()
                self.assertEqual(len(ctx.submits), 1)
                initial = ctx.submits[0]
                self.assertEqual(initial["side"], expected_side)
                self.assertEqual(initial["qty_lots"], 8)
                self.assertEqual(initial["time_in_force"], grid.IOC)
                ctx.fill(1, 8, 101 if expected_side == 1 else 99, final=True)
                self.assertTrue(ctx.state["grid"]["initialization_done"])
                self.assertEqual(len(ctx.submits), 3)
                self.assertEqual(ctx.submits[1]["time_in_force"], grid.POST_ONLY)

    def test_initial_partial_ioc_retries_only_after_terminal(self):
        ctx = Context(direction="long_bias").start()
        ctx.fill(1, 3, 101)
        self.assertEqual(len(ctx.submits), 1)
        ctx.terminal(1, grid.EXPIRED)
        ctx.now += 500_000_000
        ctx.tick()
        self.assertEqual(ctx.submits[-1]["qty_lots"], 5)
        self.assertEqual(ctx.submits[-1]["time_in_force"], grid.IOC)

    def test_initial_inventory_pnl_is_separate_from_grid_cycles(self):
        ctx = Context(direction="long_bias").start()
        ctx.fill(1, 8, 101, final=True)
        sell = next(o for o in ctx.submits if o["time_in_force"] == 4 and o["side"] == 2)
        ctx.fill(int(sell["order_id"]), 2, 106, final=True)
        self.assertEqual(ctx.state["grid"]["grid_profit_tick_lots"], 0)
        self.assertEqual(ctx.state["grid"]["inventory_profit_tick_lots"], 10)

    def test_fixed_notional_sizes_each_actual_price(self):
        ctx = Context(size_mode="fixed_notional", order_notional_tick_lots=1000).start()
        self.assertEqual([int(o["qty_lots"]) for o in ctx.submits], [10, 9])

    def test_allocated_size_uses_capital_leverage_and_grid_count(self):
        ctx = Context(size_mode="allocated", allocated_capital_tick_lots=800, leverage=2).start()
        self.assertEqual([int(o["qty_lots"]) for o in ctx.submits], [2, 1])

    def test_geometric_spacing_is_rounded_outwards(self):
        ctx = Context(spacing_mode="geometric").start()
        self.assertEqual([int(o["price_ticks"]) for o in ctx.submits], [94, 106])

    def test_pending_orders_reserve_position_capacity(self):
        ctx = Context(max_position_lots=2, buy_levels=3, sell_levels=3).start()
        self.assertEqual(len(ctx.submits), 2)

    def test_insufficient_margin_does_not_submit_or_shrink_order(self):
        ctx = Context()
        ctx.balance_row["available_units"] = 1
        ctx.start()
        self.assertEqual(ctx.submits, [])

    def test_low_margin_rejects_directional_initialization(self):
        ctx = Context(direction="long_bias")
        ctx.balance_row["available_units"] = 1
        ctx.start()
        self.assertEqual(ctx.state["grid"]["status"], grid.ERROR)
        self.assertEqual(ctx.submits, [])

    def test_stale_market_cancels_then_resumes_without_reinitializing(self):
        ctx = Context(order_policy="retain").start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(1, grid.FILLED)
        ctx.now += 5_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.PAUSED)
        for oid in [int(r["order_id"]) for r in ctx.state["refs"] if r["order_id"]]:
            ctx.terminal(oid)
        ctx.now += 500_000_000
        ctx.tick(94, 96)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 2)
        self.assertEqual(ctx.state["grid"]["status"], grid.RUNNING)
        self.assertTrue(all(o["time_in_force"] == 4 for o in ctx.submits))

    def test_position_projection_can_lag_but_disagreement_has_a_deadline(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95, update_position=False)
        self.assertNotEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        ctx.position_row["account_sequence"] = 2
        grid.on_position(ctx)
        self.assertNotEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        ctx.now += 10_000_000_000
        grid.on_position(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_account_epoch_change_and_unknown_order_fail_closed(self):
        ctx = Context().start()
        ctx.account_row["account_epoch"] = 2
        grid.on_account_state(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(len(ctx.submits), 2)

    def test_rejections_back_off_and_latch_error(self):
        ctx = Context(max_rejections=2).start()
        ctx.terminal(1, grid.REJECTED)
        self.assertEqual(len(ctx.submits), 2)
        ctx.terminal(2, grid.REJECTED)
        ctx.now += 500_000_000
        ctx.tick()
        self.assertEqual(ctx.state["grid"]["status"], grid.ERROR)
        self.assertEqual(len(ctx.submits), 2)

    def test_stop_waits_for_cancels_and_reduce_only_flatten_confirmation(self):
        ctx = Context().start()
        ctx.fill(1, 1, 95)
        grid.on_stop(ctx)
        self.assertFalse(ctx.stop_complete)
        ctx.terminal(1)
        ctx.terminal(2)
        ctx.now += 500_000_000
        grid.on_timer(ctx)
        exit_order = ctx.submits[-1]
        self.assertEqual((exit_order["side"], exit_order["qty_lots"], exit_order["reduce_only"]), (2, 1, 1))
        ctx.fill(int(exit_order["order_id"]), 1, 98, final=True, update_position=False)
        self.assertFalse(ctx.stop_complete)
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 0, ctx.sequence
        grid.on_position(ctx)
        self.assertTrue(ctx.stop_complete)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)

    def test_cancel_only_stop_preserves_inventory(self):
        ctx = Context(stop_action="cancel").start()
        ctx.fill(1, 1, 95)
        grid.on_stop(ctx)
        ctx.terminal(1)
        ctx.terminal(2)
        self.assertTrue(ctx.stop_complete)
        self.assertEqual(ctx.position_row["qty_lots"], 1)

    def test_loss_and_price_guards_stop_new_grid_orders(self):
        for parameters in ({"max_loss_tick_lots": 1}, {"stop_below_ticks": 94}):
            with self.subTest(parameters=parameters):
                ctx = Context(**parameters).start()
                ctx.fill(1, 1, 95)
                ctx.tick(89, 91)
                self.assertEqual(ctx.state["grid"]["status"], grid.STOPPING)
                self.assertEqual(len(ctx.submits), 2)

    def test_strict_start_refuses_existing_inventory_and_takeover_uses_cost(self):
        ctx = Context()
        ctx.position_row["qty_lots"] = 3
        self.assertEqual(grid.on_start(ctx), -1)
        ctx = Context(start_mode="takeover")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 3, 98
        ctx.start()
        self.assertEqual(ctx.state["grid"]["initial_cost_tick_lots"], 294)

    def test_recovery_is_command_free_and_requires_exact_intent_and_fill_match(self):
        ctx = Context().start()
        count = len(ctx.submits)
        ctx.account_row["account_epoch"] = 2
        self.assertEqual(grid.on_recover(ctx), 0)
        self.assertEqual(len(ctx.submits), count)
        self.assertEqual(ctx.cancels, [])
        self.assertEqual(ctx.state["grid"]["market_ts_ns"], 0)
        ctx.active[0]["cumulative_filled_lots"] = 1
        self.assertEqual(grid.on_recover(ctx), -1)

    def test_recovery_does_not_discard_missing_orders(self):
        ctx = Context().start()
        ctx.active = ctx.active[1:]
        self.assertEqual(grid.on_recover(ctx), -1)

    def test_invalid_configurations_are_rejected(self):
        for parameters in (
            {"lower_price_ticks": 120}, {"buy_levels": 0, "sell_levels": 0},
            {"grid_levels": 100}, {"min_order_lots": 3},
            {"size_mode": "fixed_notional", "order_notional_tick_lots": 1},
            {"stop_below_ticks": 100, "stop_above_ticks": 90},
            {"max_position_lots": grid.SAFE_INTEGER},
        ):
            with self.subTest(parameters=parameters), self.assertRaises(ValueError):
                grid.build(parameters)

    def test_upper_boundary_clips_long_exit_and_blocks_new_buys(self):
        ctx = Context(start_mode="takeover", direction="long_bias")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 1, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 118
        ctx.tick(117, 119)
        self.assertEqual([(int(o["side"]), int(o["price_ticks"]), int(o["qty_lots"])) for o in ctx.submits], [(2, 120, 1)])

    def test_lower_boundary_clips_short_exit(self):
        ctx = Context(start_mode="takeover", direction="short_bias")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = -1, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 82
        ctx.tick(81, 83)
        self.assertEqual([(int(o["side"]), int(o["price_ticks"])) for o in ctx.submits], [(1, 80)])

    def test_cancel_only_checkpoint_resumes_without_a_second_initialization(self):
        ctx = Context(stop_action="cancel").start()
        ctx.fill(1, 1, 95)
        grid.on_stop(ctx)
        ctx.terminal(1)
        ctx.terminal(2)
        self.assertEqual(grid.on_recover(ctx), 0)
        self.assertEqual(ctx.state["grid"]["stop_requested"], 0)
        ctx.now += 500_000_000
        ctx.tick()
        self.assertEqual(ctx.state["grid"]["status"], grid.RUNNING)
        self.assertTrue(all(o["time_in_force"] == grid.POST_ONLY for o in ctx.submits))

    def test_unknown_result_cannot_release_capacity_or_become_a_successful_stop(self):
        ctx = Context().start()
        ctx.active[0]["status"] = grid.UNKNOWN
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(grid.ref_count(ctx.state), 2)
        grid.on_stop(ctx)
        self.assertFalse(ctx.stop_complete)

    def test_missing_pending_acknowledgement_times_out_without_resubmitting(self):
        ctx = Context().start()
        ctx.active = ctx.active[:0]
        ctx.now += 10_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(len(ctx.submits), 2)

    def test_future_dated_ticks_do_not_make_market_ready(self):
        ctx = Context()
        grid.on_start(ctx)
        ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now + 1)], dtype=abi_v13.tick_dtype)
        grid.on_tick(ctx)
        self.assertEqual(ctx.submits, [])

    def test_grid_quotes_do_not_cross_current_book(self):
        ctx = Context(buy_levels=3).start()
        ctx.terminal(1)
        count = len(ctx.submits)
        ctx.now += 500_000_000
        ctx.tick(89, 91)
        self.assertTrue(all(o["price_ticks"] < 91 for o in ctx.submits[count:] if o["side"] == grid.BUY))

    def test_no_grid_order_is_created_outside_bounds(self):
        ctx = Context(buy_levels=16, sell_levels=16).start()
        self.assertTrue(all(80 <= o["price_ticks"] <= 120 for o in ctx.submits))
        self.assertEqual(len(set(int(o["price_ticks"]) for o in ctx.submits)), len(ctx.submits))

    def test_partial_paired_fills_count_a_complete_cycle_only_once(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        ctx.fill(2, 1, 105)
        self.assertEqual(ctx.state["grid"]["completed_pairs"], 0)
        ctx.fill(2, 1, 105, final=True)
        self.assertEqual(ctx.state["grid"]["completed_pairs"], 1)
        self.assertEqual(ctx.state["grid"]["paired_lots"], 2)

    def test_review_full_fill_cancels_counterpart_before_position_projection(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True, update_position=False)
        self.assertEqual(ctx.cancels, [2])
        self.assertEqual(len(ctx.submits), 2)

    def test_review_missing_partial_fill_has_a_bounded_delivery_wait(self):
        ctx = Context(market_stale_after_ns=100_000_000_000).start()
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.PARTIAL, 1
        grid.on_timer(ctx)
        ctx.now += 10_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(len(ctx.submits), 2)

    def test_review_missing_position_projection_cannot_wait_forever(self):
        ctx = Context(market_stale_after_ns=100_000_000_000).start()
        ctx.fill(1, 2, 95, final=True, update_position=False)
        grid.on_stop(ctx)
        ctx.terminal(2)
        ctx.now += 10_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertFalse(ctx.stop_complete)

    def test_review_regressing_terminal_cumulative_never_releases_pending_fills(self):
        ctx = Context().start()
        ctx.terminal(1, grid.CANCELED, cumulative=1)
        ctx.order_rows[0]["cumulative_filled_lots"] = 0
        grid.on_order(ctx)
        self.assertGreaterEqual(grid.find_ref(ctx.state, 1), 0)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_review_unknown_fill_is_not_hidden_by_another_orders_sequence(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=999, side=1,
                                     fill_qty_lots=1, cumulative_filled_lots=1,
                                     fill_price_ticks=95, account_sequence=1)], dtype=abi_v13.fill_dtype)
        grid.on_fill(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_review_late_execution_after_zero_fill_cancel_is_not_discarded(self):
        ctx = Context().start()
        ctx.terminal(1)
        ctx.fill(2, 2, 105, final=True)
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=1, side=1,
                                     fill_qty_lots=1, cumulative_filled_lots=1,
                                     fill_price_ticks=95, account_sequence=1)], dtype=abi_v13.fill_dtype)
        grid.on_fill(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_review_low_margin_does_not_block_reduce_only_grid_exit(self):
        ctx = Context(start_mode="takeover", direction="long_bias")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 3, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 100
        ctx.balance_row["available_units"] = 0
        ctx.tick()
        self.assertEqual(len(ctx.submits), 1)
        self.assertEqual((ctx.submits[0]["side"], ctx.submits[0]["reduce_only"]), (2, 1))

    def test_review_untradable_residual_is_detected_before_staging_new_opening_risk(self):
        ctx = Context(start_mode="takeover", direction="long_bias", min_order_lots=2)
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 1, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 100
        ctx.tick()
        self.assertEqual(g["status"], grid.ERROR)
        self.assertEqual(ctx.submits, [])

    def test_review_neutral_order_can_reduce_without_funding_a_reversal(self):
        ctx = Context(start_mode="takeover")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 1, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 100
        ctx.balance_row["available_units"] = 0
        ctx.tick()
        self.assertEqual(len(ctx.submits), 1)
        order = ctx.submits[0]
        self.assertEqual((order["side"], order["qty_lots"], order["reduce_only"]), (2, 1, 1))

    def test_review_negative_available_margin_still_allows_proven_inventory_reduction(self):
        ctx = Context(start_mode="takeover", direction="long_bias")
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 3, 100
        ctx.balance_row["available_units"] = -1
        self.assertEqual(grid.on_start(ctx), 0)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 100
        ctx.tick()
        self.assertEqual(len(ctx.submits), 1)
        self.assertTrue(ctx.submits[0]["reduce_only"])

    def test_review_retain_never_crosses_a_still_reserved_opposite_order(self):
        ctx = Context(order_policy="retain", buy_levels=2, sell_levels=2).start()
        ctx.fill(3, 2, 125, final=True)
        ctx.terminal(1)
        count = len(ctx.submits)
        ctx.now += 500_000_000
        ctx.tick(124, 126)
        old_ask = 110
        self.assertTrue(all(o["price_ticks"] < old_ask for o in ctx.submits[count:] if o["side"] == grid.BUY))

    def test_review_takeover_rejects_excess_position_before_cost_arithmetic(self):
        ctx = Context(start_mode="takeover", max_position_lots=2)
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 3, 100
        self.assertEqual(grid.on_start(ctx), -1)
        self.assertEqual(ctx.submits, [])

    def test_review_risk_stop_flattens_even_with_manual_cancel_policy(self):
        ctx = Context(stop_action="cancel", max_loss_tick_lots=1).start()
        ctx.fill(1, 1, 95)
        ctx.tick(89, 91)
        ctx.terminal(1)
        ctx.terminal(2)
        ctx.now += 500_000_000
        grid.on_timer(ctx)
        self.assertFalse(ctx.stop_complete)
        self.assertTrue(any(o["reduce_only"] for o in ctx.submits))

    def test_review_risk_stopped_checkpoint_does_not_resume_as_manual_pause(self):
        ctx = Context(stop_action="cancel", stop_below_ticks=94).start()
        ctx.tick(89, 91)
        ctx.terminal(1)
        ctx.terminal(2)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)
        self.assertEqual(grid.on_recover(ctx), 0)
        ctx.now += 500_000_000
        ctx.tick()
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)
        self.assertEqual(len(ctx.submits), 2)

    def test_review_zero_progress_ioc_attempts_are_bounded(self):
        ctx = Context(direction="long_bias", max_rejections=2).start()
        ctx.terminal(1, grid.EXPIRED)
        ctx.now += 500_000_000
        ctx.tick()
        ctx.terminal(2, grid.EXPIRED)
        self.assertEqual(ctx.state["grid"]["status"], grid.ERROR)

    def test_review_accepted_ioc_without_terminal_result_times_out(self):
        ctx = Context(direction="long_bias", market_stale_after_ns=100_000_000_000).start()
        ctx.now += 10_000_000_000
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(len(ctx.submits), 1)

    def test_history_protective_exit_survives_transient_zero_fill_iocs(self):
        ctx = Context(max_rejections=2).start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(2)
        grid.on_stop(ctx)
        for _ in range(3):
            ctx.now += 500_000_000
            ctx.tick(94, 96)
            order = ctx.submits[-1]
            self.assertTrue(order["reduce_only"])
            ctx.terminal(int(order["order_id"]), grid.EXPIRED)
            self.assertEqual(ctx.state["grid"]["status"], grid.STOPPING)
            self.assertFalse(ctx.stop_complete)
        ctx.now += 500_000_000
        ctx.tick(94, 96)
        ctx.fill(int(ctx.submits[-1]["order_id"]), 2, 93, final=True)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)
        self.assertTrue(ctx.stop_complete)

    def test_history_protective_rejections_do_not_exhaust_opening_retry_counter(self):
        ctx = Context(max_rejections=2).start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(2)
        grid.on_stop(ctx)
        for _ in range(3):
            ctx.now += 500_000_000
            ctx.tick(94, 96)
            ctx.terminal(int(ctx.submits[-1]["order_id"]), grid.REJECTED)
            self.assertEqual(ctx.state["grid"]["status"], grid.STOPPING)
        self.assertEqual(ctx.state["grid"]["expected_position_lots"], 2)
        self.assertFalse(ctx.stop_complete)

    def test_history_stop_drains_rejections_without_starting_another_grid(self):
        ctx = Context(max_rejections=1).start()
        grid.on_stop(ctx)
        ctx.terminal(1, grid.REJECTED)
        ctx.terminal(2, grid.REJECTED)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)
        self.assertTrue(ctx.stop_complete)
        self.assertEqual(len(ctx.submits), 2)

    def test_history_protective_exit_deadline_is_not_reset_by_fresh_quotes(self):
        ctx = Context(flatten_timeout_ns=2_000_000_000).start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(2)
        grid.on_stop(ctx)
        started = int(ctx.state["grid"]["flatten_started_ns"])
        ctx.now += 500_000_000
        ctx.tick(94, 96)
        ctx.terminal(int(ctx.submits[-1]["order_id"]), grid.EXPIRED)
        count = len(ctx.submits)
        ctx.now = started + 2_000_000_000
        ctx.tick(94, 96)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
        self.assertEqual(ctx.state["grid"]["last_error"], 608)
        self.assertFalse(ctx.stop_complete)
        self.assertEqual(len(ctx.submits), count)

    def test_history_recovery_preserves_protective_deadline_and_rejects_expiry(self):
        ctx = Context(flatten_timeout_ns=2_000_000_000).start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(2)
        grid.on_stop(ctx)
        started = int(ctx.state["grid"]["flatten_started_ns"])
        count = len(ctx.submits)
        ctx.account_row["account_epoch"] = 2
        self.assertEqual(grid.on_recover(ctx), 0)
        self.assertEqual(ctx.state["grid"]["flatten_started_ns"], started)
        ctx.now = started + 2_000_000_000
        self.assertEqual(grid.on_recover(ctx), -1)
        self.assertEqual(ctx.state["grid"]["last_error"], 710)
        self.assertEqual(len(ctx.submits), count)

    def test_history_recovery_does_not_accept_a_rewound_protective_clock(self):
        ctx = Context().start()
        ctx.fill(1, 2, 95, final=True)
        ctx.terminal(2)
        grid.on_stop(ctx)
        ctx.now -= 1
        self.assertEqual(grid.on_recover(ctx), -1)
        self.assertFalse(ctx.stop_complete)

    def test_review_cancel_timeout_does_not_reset_on_unrelated_order_refresh(self):
        ctx = Context(market_stale_after_ns=100_000_000_000).start()
        ctx.fill(1, 2, 95, final=True)
        ctx.active[1]["status"] = grid.CANCEL_PENDING
        ctx.now += 10_000_000_000
        ctx.active[1]["updated_ts_ns"] = ctx.now
        grid.on_timer(ctx)
        self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)

    def test_review_recovery_consumes_proven_cancellation_without_commands(self):
        ctx = Context().start()
        ctx.recovery_rows = ctx.active.copy()
        ctx.recovery_rows["status"] = grid.CANCELED
        ctx.active = ctx.active[:0]
        ctx.account_row["account_epoch"] = 2
        self.assertEqual(grid.on_recover(ctx), 0)
        self.assertEqual(grid.ref_count(ctx.state), 0)
        self.assertEqual(ctx.cancels, [])
        self.assertEqual(len(ctx.submits), 2)

    def test_review_valid_fixed_notional_minimum_is_not_rejected_by_crossed_extremes(self):
        ctx = Context(size_mode="fixed_notional", order_notional_tick_lots=1000, min_notional_tick_lots=880)
        ctx.start()
        self.assertEqual(len(ctx.submits), 2)

    def test_review_source_compatible_neutral_starts_without_short_inventory(self):
        ctx = Context(neutral_mode="inventory_only").start()
        self.assertEqual([int(o["side"]) for o in ctx.submits], [grid.BUY])

    def test_review_source_allocated_quantity_is_frozen_at_creation(self):
        ctx = Context(size_mode="allocated_fixed_qty", allocated_capital_tick_lots=4000, leverage=2).start()
        self.assertEqual([int(o["qty_lots"]) for o in ctx.submits], [10, 10])
        ctx.fill(1, 10, 95, final=True)
        ctx.terminal(2)
        ctx.now += 500_000_000
        ctx.tick(94, 96)
        self.assertTrue(all(o["qty_lots"] == 10 for o in ctx.submits))
        self.assertEqual(ctx.state["grid"]["frozen_order_qty_lots"], 10)

    def test_review_live_notional_limit_uses_current_mark_outside_grid_range(self):
        ctx = Context(start_mode="takeover", max_notional_tick_lots=1000)
        ctx.position_row["qty_lots"], ctx.position_row["average_price_ticks"] = 8, 100
        grid.on_start(ctx)
        g = ctx.state["grid"]
        g["initialized"], g["initialization_done"], g["status"], g["anchor_ticks"] = 1, 1, grid.RUNNING, 100
        ctx.tick(129, 131)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPING)
        self.assertTrue(ctx.submits[-1]["reduce_only"])

    def test_review_execution_order_position_permutations_preserve_business_facts(self):
        cases = (
            (("buy_fill", "buy_order", "sell_cancel", "position"), 2, 0),
            (("buy_fill", "buy_order", "sell_fill", "sell_order", "position"), 0, 20),
        )
        for events, final_position, profit in cases:
            for ordering in permutations(events):
                with self.subTest(ordering=ordering):
                    ctx = Context().start()
                    for event in ordering:
                        if event.endswith("fill"):
                            buy = event == "buy_fill"
                            oid, side, price = (1, grid.BUY, 95) if buy else (2, grid.SELL, 105)
                            for active in ctx.active:
                                if active["order_id"] == oid:
                                    active["status"], active["cumulative_filled_lots"] = grid.FILLED, 2
                            ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=oid, side=side,
                                                         fill_qty_lots=2, cumulative_filled_lots=2,
                                                         fill_price_ticks=price, account_sequence=2 if buy else 3,
                                                         final_fill=1)], dtype=abi_v13.fill_dtype)
                            grid.on_fill(ctx)
                        elif event == "position":
                            ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = final_position, 10
                            grid.on_position(ctx)
                        else:
                            ctx.terminal(1 if event == "buy_order" else 2,
                                         grid.CANCELED if event == "sell_cancel" else grid.FILLED,
                                         cumulative=0 if event == "sell_cancel" else 2)
                    self.assertNotIn(ctx.state["grid"]["status"], (grid.ERROR, grid.RECONCILING))
                    self.assertEqual(ctx.state["grid"]["expected_position_lots"], final_position)
                    self.assertEqual(ctx.state["grid"]["grid_profit_tick_lots"], profit)
                    self.assertEqual(ctx.state["grid"]["fill_count"], 1 if final_position else 2)
                    self.assertEqual(ctx.state["grid"]["anchor_ticks"], 95 if final_position else 105)


if __name__ == "__main__":
    unittest.main()
