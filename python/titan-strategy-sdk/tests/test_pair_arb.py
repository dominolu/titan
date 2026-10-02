import json
from pathlib import Path
import unittest
from unittest.mock import patch

import numpy as np

from titan_strategy import abi_v13
from strategies.pair_arb import strategy


def fresh_state(**overrides):
    parameters = json.loads(
        (Path(__file__).parents[3] / "strategies" / "pair_arb" / "parameters.json").read_text()
    )
    parameters.update(overrides)
    definition = strategy.build(strategy.SPEC.validate_parameters(parameters))
    return definition.state.copy()[0]


def record(dtype, **values):
    result = np.zeros(1, dtype=dtype)
    for name, value in values.items():
        result[0][name] = value
    return result[0]


class Context:
    def __init__(self, state, *, now=1_000, active=None, fills=None, orders=None, cancels=None):
        self.state = state
        self.now = now
        self._active = active if active is not None else np.zeros(0, dtype=abi_v13.active_order_dtype)
        self._fills = fills if fills is not None else np.zeros(0, dtype=abi_v13.fill_dtype)
        self._orders = orders if orders is not None else np.zeros(0, dtype=abi_v13.order_event_dtype)
        self._cancels = cancels if cancels is not None else np.zeros(0, dtype=abi_v13.cancel_event_dtype)
        self._positions = {
            (0, 0): record(abi_v13.position_dtype, account_no=0, asset_no=0, account_sequence=10),
            (1, 1): record(abi_v13.position_dtype, account_no=1, asset_no=1, account_sequence=11),
        }
        self._accounts = {
            0: record(abi_v13.account_dtype, account_no=0, account_epoch=1,
                      account_sequence=10, state=strategy.ACCOUNT_READY),
            1: record(abi_v13.account_dtype, account_no=1, account_epoch=1,
                      account_sequence=11, state=strategy.ACCOUNT_READY),
        }
        self._markets = {
            0: record(abi_v13.market_dtype, asset_no=0, best_bid_ticks=100,
                      best_ask_ticks=101, tick_size=1, lot_size=1),
            1: record(abi_v13.market_dtype, asset_no=1, best_bid_ticks=202,
                      best_ask_ticks=203, tick_size=1, lot_size=1),
        }
        self.submits = []
        self.cancels = []
        self.next_order_id = 100

    def account(self, account_no):
        return self._accounts.get(account_no, record(abi_v13.account_dtype))

    def position(self, account_no, asset_no):
        return self._positions.get((account_no, asset_no), record(abi_v13.position_dtype))

    def market(self, asset_no):
        return self._markets.get(asset_no, record(abi_v13.market_dtype))

    def active_orders(self):
        return self._active

    def fills(self):
        return self._fills

    def order_events(self):
        return self._orders

    def cancel_events(self):
        return self._cancels

    def position_events(self):
        return np.zeros(0, dtype=abi_v13.position_event_dtype)

    def balance_events(self):
        return np.zeros(0, dtype=abi_v13.balance_event_dtype)

    def account_state_events(self):
        return np.zeros(0, dtype=abi_v13.account_state_event_dtype)

    def submit_order(self, account, asset, side, order_type, qty, price, tif):
        order_id = self.next_order_id
        self.next_order_id += 1
        self.submits.append((order_id, account, asset, side, order_type, qty, price, tif))
        return order_id

    def cancel_order(self, account, asset, order_id):
        self.cancels.append((account, asset, order_id))
        return len(self.cancels)


def active_order(order_id, *, role=strategy.INITIATOR, status=strategy.ACCEPTED_STATUS,
                 qty=2, filled=0, updated=1_000):
    account = 0 if role == strategy.INITIATOR else 1
    asset = 0 if role == strategy.INITIATOR else 1
    side = strategy.BUY if role == strategy.INITIATOR else strategy.SELL
    return record(
        abi_v13.active_order_dtype, order_id=order_id, account_no=account, asset_no=asset,
        price_ticks=100 if role == strategy.INITIATOR else 202, qty_lots=qty,
        cumulative_filled_lots=filled, updated_ts_ns=updated, side=side, status=status,
    )


class TestPairArbV3StateMachine(unittest.TestCase):
    def test_private_state_contains_only_minimal_order_refs(self):
        names = set(strategy.state_dtype.names)
        self.assertEqual(names, {"pair", "slot", "order_refs"})
        self.assertEqual(set(strategy.order_ref_dtype.names), {"order_id", "slot_id", "role", "reserved"})

    def test_start_rejects_unreconciled_position(self):
        state = fresh_state()
        context = Context(state)
        context._positions[(0, 0)]["qty_lots"] = 2
        strategy.on_start.py_func(context)
        self.assertEqual(int(state["pair"]["status"]), strategy.ERROR)
        self.assertEqual(int(state["pair"]["last_error"]), 304)

    def test_both_accounts_must_be_ready(self):
        state = fresh_state()
        state["pair"]["left_account_epoch"] = 1
        state["pair"]["right_account_epoch"] = 1
        state["pair"]["ready_mask"] = strategy.READY_ALL
        context = Context(state)
        context._accounts[1]["state"] = 5
        with patch.object(strategy, "refresh_readiness", strategy.refresh_readiness.py_func):
            strategy.risk_check.py_func(context)
        self.assertEqual(int(state["pair"]["ready_mask"]) & strategy.READY_ACCOUNTS, 0)
        self.assertGreaterEqual(int(state["pair"]["posture"]), strategy.RESTRICTED)

    def test_capacity_respects_hedge_ratio(self):
        state = fresh_state(hedge_ratio_numerator=2, hedge_ratio_denominator=1,
                            max_position_lots=10, slot_lots=10)
        context = Context(state)
        self.assertEqual(strategy.position_capacity.py_func(context), 5)

    def test_partial_maker_fill_does_not_submit_hedge(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        fills = np.zeros(1, dtype=abi_v13.fill_dtype)
        fills[0]["order_id"], fills[0]["fill_qty_lots"] = 7, 1
        fills[0]["fill_price_ticks"], fills[0]["account_sequence"] = 100, 12
        context = Context(state, fills=fills)
        with (
            patch.object(strategy, "submit_role", strategy.submit_role.py_func),
            patch.object(strategy, "ensure_draining_hedge", strategy.ensure_draining_hedge.py_func),
        ):
            strategy.on_fill.py_func(context)
        self.assertEqual(int(state["slot"]["initiator_filled_lots"]), 1)
        self.assertEqual(context.submits, [])
        self.assertEqual(int(state["slot"]["initiator_order_id"]), 7)

    def test_full_maker_fill_submits_exact_hedge_gap(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        fills = np.zeros(1, dtype=abi_v13.fill_dtype)
        fills[0]["order_id"], fills[0]["fill_qty_lots"] = 7, 2
        fills[0]["fill_price_ticks"], fills[0]["account_sequence"] = 100, 12
        fills[0]["final_fill"] = 1
        context = Context(state, fills=fills)
        with (
            patch.object(strategy, "submit_role", strategy.submit_role.py_func),
            patch.object(strategy, "ensure_draining_hedge", strategy.ensure_draining_hedge.py_func),
        ):
            strategy.on_fill.py_func(context)
        self.assertEqual(len(context.submits), 1)
        self.assertEqual(context.submits[0][5], 2)
        self.assertEqual(context.submits[0][7], strategy.IOC)
        self.assertEqual(int(state["pair"]["expected_left_position_lots"]), 2)

    def test_taker_taker_stages_both_legs_in_one_callback(self):
        state = fresh_state(mode="TAKER_TAKER")
        p = state["pair"]
        p["status"], p["posture"], p["ready_mask"] = strategy.RUNNING, strategy.NORMAL, strategy.READY_ALL
        p["left_account_epoch"], p["right_account_epoch"] = 1, 1
        p["left_market_ts_ns"], p["right_market_ts_ns"] = 1_000, 1_000
        context = Context(state, now=1_000)
        with (
            patch.object(strategy, "opportunity_valid", strategy.opportunity_valid.py_func),
            patch.object(strategy, "position_capacity", strategy.position_capacity.py_func),
            patch.object(strategy, "submit_role", strategy.submit_role.py_func),
        ):
            strategy.begin_slot.py_func(context)
        self.assertEqual(len(context.submits), 2)
        self.assertEqual(context.submits[0][7], strategy.IOC)
        self.assertEqual(context.submits[1][7], strategy.IOC)

    def test_rejected_and_expired_are_not_recorded_as_canceled(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        events = np.zeros(1, dtype=abi_v13.cancel_event_dtype)
        events[0]["order_id"] = 7
        events[0]["final_status"] = strategy.EXPIRED_STATUS
        context = Context(state, cancels=events)
        with patch.object(
            strategy, "ensure_draining_hedge", strategy.ensure_draining_hedge.py_func
        ):
            strategy.on_cancel.py_func(context)
        self.assertEqual(int(state["slot"]["initiator_order_id"]), 0)
        self.assertEqual(int(state["pair"]["reject_count"]), 0)

    def test_next_slot_waits_for_position_projection(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_filled_lots"] = 2
        state["slot"]["hedge_filled_lots"] = 2
        state["slot"]["initiator_fill_sequence"] = 20
        state["slot"]["hedge_fill_sequence"] = 21
        state["pair"]["expected_left_position_lots"] = 2
        state["pair"]["expected_right_position_lots"] = -2
        context = Context(state)
        self.assertFalse(strategy.positions_caught_up.py_func(context))
        context._positions[(0, 0)]["account_sequence"] = 20
        context._positions[(1, 1)]["account_sequence"] = 21
        context._positions[(0, 0)]["qty_lots"] = 2
        context._positions[(1, 1)]["qty_lots"] = -2
        self.assertTrue(strategy.positions_caught_up.py_func(context))

    def test_restore_rejects_position_projection_mismatch(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["pair"]["expected_left_position_lots"] = 2
        state["pair"]["expected_right_position_lots"] = -2
        context = Context(state)
        context._positions[(0, 0)]["qty_lots"] = 1
        context._positions[(1, 1)]["qty_lots"] = -2
        strategy.on_start.py_func(context)
        self.assertEqual(int(state["pair"]["status"]), strategy.ERROR)
        self.assertEqual(int(state["pair"]["last_error"]), 307)

    def test_restore_rejects_order_role_identity_mismatch(self):
        state = fresh_state()
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        active = np.zeros(1, dtype=abi_v13.active_order_dtype)
        active[0] = active_order(7)
        active[0]["asset_no"] = 1
        strategy.on_start.py_func(Context(state, active=active))
        self.assertEqual(int(state["pair"]["status"]), strategy.ERROR)
        self.assertEqual(int(state["pair"]["last_error"]), 305)

    def test_draining_cancel_submits_confirmed_partial_gap(self):
        state = fresh_state()
        state["pair"]["status"] = strategy.DRAINING
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_filled_lots"] = 1
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        events = np.zeros(1, dtype=abi_v13.cancel_event_dtype)
        events[0]["order_id"] = 7
        events[0]["final_status"] = strategy.CANCELED_STATUS
        context = Context(state, cancels=events)
        with (
            patch.object(strategy, "submit_role", strategy.submit_role.py_func),
            patch.object(strategy, "ensure_draining_hedge", strategy.ensure_draining_hedge.py_func),
        ):
            strategy.on_cancel.py_func(context)
        self.assertEqual(len(context.submits), 1)
        self.assertEqual(context.submits[0][5], 1)
        self.assertEqual(context.submits[0][7], strategy.IOC)

    def test_stop_keeps_cancel_and_hedge_commands_enabled(self):
        state = fresh_state()
        state["pair"]["status"] = strategy.RUNNING
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 2
        state["slot"]["initiator_filled_lots"] = 1
        state["slot"]["initiator_order_id"] = 7
        self.assertTrue(strategy.bind_ref.py_func(state, 7, strategy.INITIATOR))
        active = np.zeros(1, dtype=abi_v13.active_order_dtype)
        active[0] = active_order(7, qty=2, filled=1)
        context = Context(state, active=active)
        with (
            patch.object(strategy, "cancel_role", strategy.cancel_role.py_func),
            patch.object(strategy, "submit_role", strategy.submit_role.py_func),
            patch.object(strategy, "ensure_draining_hedge", strategy.ensure_draining_hedge.py_func),
        ):
            strategy.on_stop.py_func(context)
        self.assertEqual(int(state["pair"]["status"]), strategy.DRAINING)
        self.assertEqual(context.cancels, [(0, 0, 7)])
        self.assertEqual(len(context.submits), 1)
        self.assertEqual(context.submits[0][5], 1)


if __name__ == "__main__":
    unittest.main()
