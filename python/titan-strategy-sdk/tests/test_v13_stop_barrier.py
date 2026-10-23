import unittest

from numba import njit

from titan_strategy import abi_v13
from titan_strategy.definition import Capability, StrategyDefinition, StrategySpec
from titan_strategy.state import int64, new_state, record
from titan_strategy.static_compiler import StrategyCompileError, validate_strategy_definition


@njit
def on_stop(ctx):
    ctx.complete_stop()


@njit
def on_recover(ctx):
    for order in ctx.recovery_orders():
        ctx.state["value"] += order["qty_lots"]


@njit
def mutate_recovery(ctx):
    orders = ctx.recovery_orders()
    orders[0]["status"] = 6


class TestStopBarrierContract(unittest.TestCase):
    def test_completion_requires_capability(self):
        spec = StrategySpec("stop_test", "1.0.0", 1)
        definition = StrategyDefinition(spec, new_state(record(value=int64)), {"on_stop": on_stop})
        with self.assertRaisesRegex(StrategyCompileError, "STOP_BARRIER"):
            validate_strategy_definition(definition, {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()})

    def test_declared_barrier_can_complete(self):
        spec = StrategySpec("stop_test", "1.0.0", 1, capabilities=Capability.STOP_BARRIER)
        definition = StrategyDefinition(spec, new_state(record(value=int64)), {"on_stop": on_stop})
        result = validate_strategy_definition(definition, {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()})
        self.assertIn("complete_stop", result.handlers[0].context_calls)

    def test_recovery_order_results_require_explicit_capability(self):
        definition = StrategyDefinition(StrategySpec("recover_test", "1.0.0", 1),
                                        new_state(record(value=int64)), {"on_recover": on_recover})
        with self.assertRaisesRegex(StrategyCompileError, "RECOVERY_ORDER_RESULTS"):
            validate_strategy_definition(definition, {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()})

    def test_reconciled_order_results_are_read_only(self):
        spec = StrategySpec("recover_test", "1.0.0", 1, capabilities=Capability.RECOVERY_ORDER_RESULTS)
        definition = StrategyDefinition(spec, new_state(record(value=int64)), {"on_recover": mutate_recovery})
        with self.assertRaisesRegex(StrategyCompileError, "read-only"):
            validate_strategy_definition(definition, {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()})


if __name__ == "__main__":
    unittest.main()
