import json
from pathlib import Path
import unittest

import numpy as np

from titan_strategy import abi_v13
from strategies.pair_arb import strategy


def fresh_state():
    parameters = json.loads(
        (Path(__file__).parents[3] / "strategies" / "pair_arb" / "parameters.json").read_text()
    )
    return strategy.build(strategy.SPEC.validate_parameters(parameters)).state.copy()


class StartContext:
    def __init__(self, state, active, positions=None, account_state=strategy.ACCOUNT_READY):
        self.state = state
        self._active = active
        self._positions = positions or {}
        self._account_state = account_state

    def active_orders(self):
        return self._active

    def account(self, account_no):
        value = np.zeros(1, dtype=abi_v13.account_dtype)[0]
        value["account_no"] = account_no
        value["account_epoch"] = 1
        value["account_sequence"] = 10 + account_no
        value["state"] = self._account_state
        return value

    def position(self, account_no, asset_no):
        value = np.zeros(1, dtype=abi_v13.position_dtype)[0]
        value["account_no"] = account_no
        value["asset_no"] = asset_no
        value["qty_lots"] = self._positions.get((account_no, asset_no), 0)
        return value


class OrderContext:
    def __init__(self, state, events):
        self.state = state
        self.now = 100
        self._events = events

    def order_events(self):
        return self._events

class TestPairArbV13StateMachine(unittest.TestCase):
    def test_slot_requires_full_initiator_target_before_completion(self):
        state = fresh_state()
        slot = state[0]["slot"]
        slot["id"] = 1
        slot["target_lots"] = 2
        self.assertFalse(strategy.finish_slot.py_func(state[0], 100))
        self.assertEqual(int(slot["id"]), 1)

        slot["initiator_filled_lots"] = 2
        slot["hedge_filled_lots"] = 2
        self.assertTrue(strategy.finish_slot.py_func(state[0], 101))
        self.assertEqual(int(slot["id"]), 0)

    def test_restore_rejects_missing_private_order(self):
        state = fresh_state()
        order = state[0]["orders"][0]
        order["order_id"] = 99
        order["account_no"] = 0
        order["asset_no"] = 0
        order["side"] = strategy.BUY
        order["qty_lots"] = 2
        active = np.zeros(0, dtype=abi_v13.active_order_dtype)
        strategy.on_start.py_func(StartContext(state[0], active))
        self.assertEqual(int(state[0]["pair"]["status"]), strategy.FAULTED)
        self.assertEqual(int(state[0]["pair"]["last_error"]), 303)

    def test_start_rejects_nonzero_unreconciled_position(self):
        state = fresh_state()
        active = np.zeros(0, dtype=abi_v13.active_order_dtype)
        strategy.on_start.py_func(StartContext(state[0], active, {(0, 0): 2}))
        self.assertEqual(int(state[0]["pair"]["status"]), strategy.FAULTED)
        self.assertEqual(int(state[0]["pair"]["last_error"]), 308)

    def test_start_rejects_account_that_is_not_ready(self):
        state = fresh_state()
        active = np.zeros(0, dtype=abi_v13.active_order_dtype)
        strategy.on_start.py_func(StartContext(state[0], active, account_state=5))
        self.assertEqual(int(state[0]["pair"]["status"]), strategy.FAULTED)
        self.assertEqual(int(state[0]["pair"]["last_error"]), 304)

    def test_replacement_fills_accumulate_and_create_hedge_debt(self):
        state = fresh_state()[0]
        state["slot"]["id"] = 1
        state["slot"]["target_lots"] = 5
        self.assertTrue(strategy.create_obligation.py_func(state, 11, 2, 10))
        state["slot"]["initiator_filled_lots"] += 2
        self.assertTrue(strategy.create_obligation.py_func(state, 12, 3, 20))
        state["slot"]["initiator_filled_lots"] += 3
        self.assertEqual(int(state["slot"]["initiator_filled_lots"]), 5)
        self.assertEqual(strategy.hedge_debt.py_func(state), 5)

        strategy.apply_hedge_fill.py_func(state, 2, 30)
        self.assertEqual(strategy.hedge_debt.py_func(state), 3)
        strategy.apply_hedge_fill.py_func(state, 3, 40)
        self.assertEqual(strategy.hedge_debt.py_func(state), 0)

    def test_unknown_submit_result_halts_strategy(self):
        state = fresh_state()[0]
        order = state["orders"][0]
        order["order_id"] = 77
        order["account_no"] = 0
        order["asset_no"] = 0
        order["qty_lots"] = 1
        events = np.zeros(1, dtype=abi_v13.order_event_dtype)
        events[0]["order_id"] = 77
        events[0]["account_no"] = 0
        events[0]["asset_no"] = 0
        events[0]["status"] = strategy.UNKNOWN_STATUS
        strategy.on_order.py_func(OrderContext(state, events))
        self.assertEqual(int(state["pair"]["status"]), strategy.FAULTED)
        self.assertEqual(int(state["pair"]["last_error"]), 503)


if __name__ == "__main__":
    unittest.main()
