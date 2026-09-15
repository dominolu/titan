"""Guards for the unified ABI layering and the strategy-facing facade.

The point of the three-file split is that a strategy never handles ABI details. These tests lock
both halves of that statement: where ABI content is allowed to live, and what a strategy sees.
"""

import ast
import ctypes
import sys
import unittest
from pathlib import Path

import numpy as np

SDK_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = Path(__file__).resolve().parents[3]
if str(SDK_ROOT) not in sys.path:
    sys.path.append(str(SDK_ROOT))

import titan_strategy  # noqa: E402
from titan_strategy import abi_v10, callbacks, context  # noqa: E402
from titan_strategy.abi_v10 import (  # noqa: E402
    ORDER_COMMAND_CANCEL,
    ORDER_COMMAND_NEW,
    ORD_TYPE_LIMIT,
    SIDE_BUY,
    SIDE_SELL,
    TIME_IN_FORCE_IOC,
    TIME_IN_FORCE_POST_ONLY,
    backtest_command_buffer_dtype,
    order_command_dtype,
    runtime_ctx_dtype,
)
from titan_strategy.callbacks import SUBMIT_OK  # noqa: E402

SDK_MODULES = {
    "abi_v10": SDK_ROOT / "titan_strategy" / "abi_v10.py",
    "callbacks": SDK_ROOT / "titan_strategy" / "callbacks.py",
    "context": SDK_ROOT / "titan_strategy" / "context.py",
    "compiler": SDK_ROOT / "titan_strategy" / "compiler.py",
    "intrinsic": SDK_ROOT / "titan_strategy" / "intrinsic.py",
}


def _relative_imports(path: Path) -> set:
    tree = ast.parse(path.read_text())
    modules = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level:
            modules.add(node.module or "")
        elif isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith("titan_strategy"):
                    modules.add(alias.name)
    return modules


class TestAbiLayering(unittest.TestCase):
    def test_abi_dtypes_are_defined_in_exactly_one_module(self):
        marker = "_dtype = np.dtype("
        for name, path in SDK_MODULES.items():
            count = path.read_text().count(marker)
            if name == "abi_v10":
                self.assertGreater(count, 0, "abi_v10.py must own the dtype definitions")
            else:
                self.assertEqual(count, 0, f"{name}.py must not define ABI dtypes")

    def test_imports_only_point_downwards(self):
        allowed = {
            "intrinsic": set(),
            "abi_v10": {"intrinsic"},
            "callbacks": {"abi_v10", "intrinsic"},
            "context": {"abi_v10", "callbacks", "intrinsic"},
            "compiler": {"abi_v10", "callbacks", "context"},
        }
        for name, path in SDK_MODULES.items():
            imports = {item.split(".")[-1] for item in _relative_imports(path)}
            imports.discard(name)
            unexpected = imports - allowed[name]
            self.assertFalse(
                unexpected,
                f"{name}.py imports {sorted(unexpected)}; allowed: {sorted(allowed[name])}",
            )

    def test_abi_version_has_a_single_python_definition(self):
        self.assertEqual(abi_v10.ABI_VERSION, 12)
        self.assertNotIn("ABI_VERSION = ", SDK_MODULES["compiler"].read_text())
        self.assertEqual(titan_strategy.compile_strategy.__module__, "titan_strategy.compiler")

    def test_strategy_packages_do_not_handle_abi_internals(self):
        forbidden = (
            "titan_strategy.abi_v10",
            "titan_strategy.callbacks",
            "titan_strategy.intrinsic",
            "titan_strategy.compiler",
            "carray(",
            "address_as_void_pointer(",
            "np.dtype(",
        )
        for path in sorted((REPO_ROOT / "strategies").glob("*/strategy.py")):
            source = path.read_text()
            for marker in forbidden:
                self.assertNotIn(
                    marker,
                    source,
                    f"{path.name} must express intent through the Strategy facade, "
                    f"not through ABI internals ({marker})",
                )


class TestCompatibilitySurface(unittest.TestCase):
    """Names that already had callers before the split keep working from ``context``."""

    def test_context_reexports_the_historical_import_surface(self):
        from titan_strategy.context import (  # noqa: F401
            Strategy,
            backtest_command_buffer_dtype as ctx_command_buffer,
            callback_bridge,
            fill_dtype as ctx_fill,
            order_command_dtype as ctx_command,
            order_event_dtype as ctx_order_event,
            position_event_dtype as ctx_position_event,
            runtime_ctx_dtype as ctx_runtime,
            validate_handler,
            validate_runtime_descriptor,
        )

        self.assertIs(ctx_runtime, runtime_ctx_dtype)
        self.assertIs(ctx_command, order_command_dtype)
        self.assertIs(ctx_command_buffer, backtest_command_buffer_dtype)
        self.assertIs(validate_runtime_descriptor, abi_v10.validate_runtime_descriptor)
        self.assertIs(validate_handler, callbacks.validate_handler)
        self.assertIs(Strategy, context.Strategy)
        self.assertIsNotNone(callback_bridge)
        self.assertIs(ctx_fill, abi_v10.fill_dtype)
        self.assertIs(ctx_order_event, abi_v10.order_event_dtype)
        self.assertIs(ctx_position_event, abi_v10.position_event_dtype)

    def test_constants_are_exported_from_both_layers(self):
        self.assertEqual(abi_v10.EVENT_ERROR, 8)
        self.assertEqual(abi_v10.EVENT_STOP, 9)
        self.assertEqual(context.EVENT_ERROR, abi_v10.EVENT_ERROR)
        self.assertEqual(abi_v10.SIDE_BUY, 1)
        self.assertEqual(abi_v10.SIDE_SELL, -1)
        self.assertEqual(abi_v10.TIME_IN_FORCE_POST_ONLY, 1)
        self.assertEqual(abi_v10.TIME_IN_FORCE_IOC, 3)


class _FacadeDriver:
    """Compiles a strategy that only ever touches the facade."""

    def __init__(self):
        self.state = np.zeros(8, dtype=np.float64)
        self.state_i64 = np.zeros(8, dtype=np.int64)
        self.markets = np.zeros(2, dtype=abi_v10.market_state_dtype)
        self.positions = np.zeros(2, dtype=np.float64)
        self.commands = np.zeros(8, dtype=order_command_dtype)
        self.command_buffer = np.zeros(1, dtype=backtest_command_buffer_dtype)
        self.command_buffer[0]["commands_ptr"] = self.commands.ctypes.data
        self.command_buffer[0]["num_commands"] = 0
        self.command_buffer[0]["command_capacity"] = self.commands.size
        self.ctx = np.zeros(1, dtype=runtime_ctx_dtype)
        self.ctx[0]["state_f64_ptr"] = self.state.ctypes.data
        self.ctx[0]["state_f64_len"] = self.state.size
        self.ctx[0]["state_i64_ptr"] = self.state_i64.ctypes.data
        self.ctx[0]["state_i64_len"] = self.state_i64.size
        self.ctx[0]["markets_ptr"] = self.markets.ctypes.data
        self.ctx[0]["num_markets"] = self.markets.size
        self.ctx[0]["positions_ptr"] = self.positions.ctypes.data
        self.ctx[0]["num_positions"] = self.positions.size
        self.ctx[0]["backtest_commands"] = self.command_buffer.ctypes.data
        self.ctx[0]["event_kind"] = abi_v10.EVENT_TICK
        self.ctx[0]["now"] = 42


def _build_driver():
    from numba import njit

    from titan_strategy.context import Strategy

    @njit
    def drive(ctx_arr):
        strategy = Strategy(ctx_arr)
        strategy.state[0] = strategy.now * 1.0
        strategy.state_i64[0] = 7
        strategy.state[1] = strategy.best_bid(0)
        maker = strategy.submit_maker_order(0, 1, 100.0, 2.0, SIDE_BUY, 0)
        taker = strategy.submit_taker_sell(1, 2, 50.0, 1.5, 1)
        canceled = strategy.cancel_order(2, 1, 1)
        strategy.stop()
        return maker + taker + canceled

    return drive


class TestStrategyFacade(unittest.TestCase):
    def test_facade_encodes_orders_without_abi_handling(self):
        harness = _FacadeDriver()
        harness.markets[0]["best_bid"] = 99.0
        harness.markets[0]["best_ask"] = 99.5

        drive = _build_driver()
        result = drive(harness.ctx)

        self.assertEqual(result, SUBMIT_OK * 3)
        self.assertEqual(harness.ctx[0]["stop_requested"], 1)
        self.assertEqual(harness.state[0], 42.0)
        self.assertEqual(harness.state_i64[0], 7)
        self.assertEqual(harness.state[1], 99.0)

        self.assertEqual(harness.command_buffer[0]["num_commands"], 3)
        maker, taker, cancel = harness.commands[:3]
        self.assertEqual(maker["kind"], ORDER_COMMAND_NEW)
        self.assertEqual(maker["side"], SIDE_BUY)
        self.assertEqual(maker["time_in_force"], TIME_IN_FORCE_POST_ONLY)
        self.assertEqual(maker["order_type"], ORD_TYPE_LIMIT)
        self.assertEqual(maker["asset_no"], 0)
        self.assertEqual(maker["order_id"], 1)
        self.assertEqual(maker["price"], 100.0)
        self.assertEqual(maker["qty"], 2.0)
        self.assertEqual(maker["local_account_no"], 0)

        self.assertEqual(taker["kind"], ORDER_COMMAND_NEW)
        self.assertEqual(taker["side"], SIDE_SELL)
        self.assertEqual(taker["time_in_force"], TIME_IN_FORCE_IOC)
        self.assertEqual(taker["asset_no"], 1)
        self.assertEqual(taker["local_account_no"], 1)

        self.assertEqual(cancel["kind"], ORDER_COMMAND_CANCEL)
        self.assertEqual(cancel["asset_no"], 1)
        self.assertEqual(cancel["order_id"], 2)
        self.assertEqual(cancel["local_account_no"], 1)

    def test_callback_bridge_hands_the_handler_a_facade_only(self):
        from numba import njit

        from titan_strategy.context import Strategy, callback_bridge

        harness = _FacadeDriver()
        harness.markets[0]["best_ask"] = 12.5

        @njit
        def handler(strategy):
            strategy.state[2] = strategy.best_ask(0)
            strategy.submit_maker_bid(0, 5, 10.0, 1.0)

        bridge = callback_bridge(handler)
        self.assertEqual(bridge(ctypes.c_void_p(harness.ctx.ctypes.data)), 0)
        self.assertEqual(harness.state[2], 12.5)
        self.assertEqual(harness.command_buffer[0]["num_commands"], 1)
        self.assertEqual(harness.commands[0]["side"], SIDE_BUY)
        self.assertEqual(harness.commands[0]["order_id"], 5)


if __name__ == "__main__":
    unittest.main()
