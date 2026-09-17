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
    def __init__(self, state, active):
        self.state = state
        self._active = active

    def active_orders(self):
        return self._active

    def account(self, account_no):
        value = np.zeros(1, dtype=abi_v13.account_dtype)[0]
        value["account_no"] = account_no
        value["account_epoch"] = 1
        value["state"] = strategy.ACCOUNT_READY
        return value

    def position(self, account_no, asset_no):
        value = np.zeros(1, dtype=abi_v13.position_dtype)[0]
        value["account_no"] = account_no
        value["asset_no"] = asset_no
        return value


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


if __name__ == "__main__":
    unittest.main()
