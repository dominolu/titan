"""Execute the Rust-built shared library through the actual V13 ABI."""

import ctypes
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

import numpy as np

from titan_strategy import abi_v13
from titan_strategy.state import canonical_state_schema, state_schema_hash, validate_state_dtype
from strategies.grid import strategy as grid
from test_grid import Context, row


class TestGridNative(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(__file__).parents[3]
        output = Path(cls.temporary.name) / "grid"
        binary = os.environ.get("TITAN_TEST_BINARY", str(cls.root / "target/debug/titan"))
        result = subprocess.run([
            binary, "strategy", "compile", "--strategy", str(cls.root / "strategies/grid/strategy.py"),
            "--parameters", str(cls.root / "strategies/grid/parameters.json"),
            "--target", "x86_64-unknown-linux-gnu", "--cpu-baseline", "x86-64-v2",
            "--artifact-format", "pair", "--output", str(output), "--json",
        ], capture_output=True, text=True, timeout=360)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)
        paths = json.loads(result.stdout)["paths"]
        cls.manifest_path = next(path for path in paths if path.endswith(".manifest.cbor"))
        cls.binary = binary
        cls.library = ctypes.CDLL(next(path for path in paths if path.endswith(".so")))
        layout = validate_state_dtype(grid.state_dtype, max_state_bytes=65536, max_alignment=8)
        cls.schema_hash = state_schema_hash(canonical_state_schema(layout, schema_version=grid.SPEC.state_schema_version))

    def invoke(self, name, ctx, *, expected=0, context_overrides=None):
        native = np.zeros(1, dtype=abi_v13.runtime_context_dtype)
        n = native[0]
        n["struct_size"], n["abi_version"] = native.dtype.itemsize, 13
        n["now_ns"] = ctx.now
        n["event_kind"] = {"on_start": 0, "on_tick": 1, "on_fill": 4, "on_order": 5,
                           "on_cancel": 6, "on_position": 7, "on_timer": 10,
                           "on_stop": 11, "on_recover": 12}[name]
        n["state_ptr"], n["state_len"] = ctx.state_array.ctypes.data, ctx.state_array.nbytes
        n["state_alignment"], n["state_schema_version"] = grid.state_dtype.alignment, grid.SPEC.state_schema_version
        n["state_schema_hash"] = list(self.schema_hash)
        buffers = {}
        for field, value, dtype in (
            ("markets", ctx.market_row, abi_v13.market_dtype),
            ("positions", ctx.position_row, abi_v13.position_dtype),
            ("balances", ctx.balance_row, abi_v13.balance_dtype),
            ("accounts", ctx.account_row, abi_v13.account_dtype),
        ):
            buffers[field] = np.array([value], dtype=dtype)
        buffers.update(active_orders=ctx.active, ticks=ctx.tick_rows, fills=ctx.fill_rows,
                       order_events=ctx.order_rows, cancel_events=ctx.cancel_rows)
        for field, values in buffers.items():
            n[field + "_ptr"], n[field + "_len"] = values.ctypes.data, len(values)
        recovery = ctx.recovery_orders()
        n["event_payload_ptr"], n["event_payload_len"] = recovery.ctypes.data, recovery.nbytes

        @ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
        def submit(_, request, result):
            values = ctypes.string_at(request, abi_v13.submit_order_request_dtype.itemsize)
            order = np.frombuffer(values, dtype=abi_v13.submit_order_request_dtype)[0]
            result[0] = ctx.submit_order(int(order["account_no"]), int(order["asset_no"]),
                                         int(order["side"]), int(order["order_type"]),
                                         int(order["qty_lots"]), int(order["price_ticks"]),
                                         int(order["time_in_force"]), bool(order["reduce_only"]))
            return 0

        @ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
        def cancel(_, request, result):
            values = ctypes.string_at(request, abi_v13.cancel_order_request_dtype.itemsize)
            order = np.frombuffer(values, dtype=abi_v13.cancel_order_request_dtype)[0]
            result[0] = ctx.cancel_order(int(order["account_no"]), int(order["asset_no"]), int(order["order_id"]))
            return 0

        n["submit_order"] = ctypes.cast(submit, ctypes.c_void_p).value
        n["cancel_order"] = ctypes.cast(cancel, ctypes.c_void_p).value
        for field, value in (context_overrides or {}).items():
            n[field] = value
        callback = getattr(self.library, "titan_strategy_" + name)
        callback.argtypes, callback.restype = [ctypes.c_void_p], ctypes.c_int32
        result = callback(native.ctypes.data)
        self.assertEqual(result, expected, (name, int(ctx.state["grid"]["last_error"])))
        return int(n["flags"])

    def test_native_callbacks_quote_fill_cancel_stop_and_recover(self):
        ctx = Context()
        self.invoke("on_start", ctx)
        ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now)], dtype=abi_v13.tick_dtype)
        self.invoke("on_tick", ctx)
        self.assertEqual([int(o["price_ticks"]) for o in ctx.submits], [95, 105])
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.FILLED, 2
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 2, 2
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=1, side=1, fill_qty_lots=2,
                                     cumulative_filled_lots=2, fill_price_ticks=95,
                                     account_sequence=2, final_fill=1)], dtype=abi_v13.fill_dtype)
        self.invoke("on_fill", ctx)
        self.assertEqual(ctx.cancels, [2])
        self.assertEqual(ctx.state["grid"]["anchor_ticks"], 95)
        # Replay of a completed execution is harmless in native code as well.
        self.invoke("on_fill", ctx)
        self.assertEqual(ctx.state["grid"]["fill_count"], 1)
        ctx.active = ctx.active[:0]
        ctx.order_rows = np.array([row(abi_v13.order_event_dtype, order_id=2, status=grid.CANCELED,
                                      price_ticks=105, qty_lots=2)], dtype=abi_v13.order_event_dtype)
        self.invoke("on_order", ctx)
        self.assertEqual(self.invoke("on_stop", ctx), 0)
        ctx.now += 500_000_000
        self.invoke("on_timer", ctx)
        self.assertEqual(ctx.submits[-1]["reduce_only"], 1)
        exit_id = int(ctx.submits[-1]["order_id"])
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.FILLED, 2
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 0, 3
        ctx.fill_rows[0] = row(abi_v13.fill_dtype, order_id=exit_id, side=2, fill_qty_lots=2,
                               cumulative_filled_lots=2, fill_price_ticks=98, account_sequence=3, final_fill=1)
        self.assertEqual(self.invoke("on_fill", ctx) & 1, 1)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)
        ctx.active = ctx.active[:0]
        ctx.account_row["account_epoch"] = 2
        before = len(ctx.submits)
        self.invoke("on_recover", ctx)
        self.assertEqual(len(ctx.submits), before)

    def test_cli_trace_drains_and_rejects_incomplete_stop(self):
        root = Path(self.temporary.name)
        trace = json.loads((self.root / "fixtures/grid_v13_trace.json").read_text())
        for complete in (True, False):
            directory = root / ("complete" if complete else "incomplete")
            directory.mkdir()
            data = dict(trace)
            if not complete:
                data["events"] = trace["events"][:2]
            (directory / "trace.json").write_text(json.dumps(data))
            config = directory / "backtest.toml"
            config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 64\nallow_unsigned_artifact = true\n')
            environment = dict(os.environ, TITAN_HOME=str(directory / "home"))
            result = subprocess.run([self.binary, "run", self.manifest_path, "-e", "backtest",
                                     "-m", "tick", "-c", str(config), "--json"],
                                    env=environment, capture_output=True, text=True, timeout=30)
            if complete:
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                reports = list((directory / "home").rglob("result.json"))
                self.assertEqual(len(reports), 1)
                report = json.loads(reports[0].read_text())
                self.assertEqual(report["status"], "COMPLETED")
                self.assertEqual(report["submit_count"], 3)
                self.assertEqual(report["cancel_count"], 1)
            else:
                self.assertNotEqual(result.returncode, 0, result.stdout + result.stderr)
                logs = "\n".join(path.read_text() for path in (directory / "home").rglob("*.log"))
                self.assertIn("trace ended before strategy stop completed", logs + result.stderr)

    def test_native_recovery_uses_reconciled_terminal_results(self):
        ctx = Context()
        self.invoke("on_start", ctx)
        ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now)], dtype=abi_v13.tick_dtype)
        self.invoke("on_tick", ctx)
        ctx.recovery_rows = ctx.active.copy()
        ctx.recovery_rows["status"] = grid.CANCELED
        ctx.active = ctx.active[:0]
        ctx.account_row["account_epoch"] = 2
        self.invoke("on_recover", ctx)
        self.assertEqual(len(ctx.submits), 2)
        self.assertEqual(ctx.cancels, [])
        self.assertTrue(all(r["order_id"] == 0 for r in ctx.state["refs"]))

    def test_native_fill_cancels_before_position_and_zero_margin_allows_reduction(self):
        ctx = Context()
        self.invoke("on_start", ctx)
        ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now)], dtype=abi_v13.tick_dtype)
        self.invoke("on_tick", ctx)
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.FILLED, 2
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=1, side=1, fill_qty_lots=2,
                                     cumulative_filled_lots=2, fill_price_ticks=95,
                                     account_sequence=2, final_fill=1)], dtype=abi_v13.fill_dtype)
        self.invoke("on_fill", ctx)
        self.assertEqual(ctx.cancels, [2])
        self.assertEqual(len(ctx.submits), 2)
        ctx.active = ctx.active[:0]
        ctx.order_rows = np.array([row(abi_v13.order_event_dtype, order_id=2, status=grid.CANCELED,
                                      price_ticks=105, qty_lots=2)], dtype=abi_v13.order_event_dtype)
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 2, 2
        self.invoke("on_order", ctx)
        ctx.balance_row["available_units"] = 0
        ctx.now += 500_000_000
        ctx.market_row["best_bid_ticks"], ctx.market_row["best_ask_ticks"] = 94, 96
        ctx.tick_rows[0]["receive_ts_ns"] = ctx.now
        self.invoke("on_tick", ctx)
        self.assertEqual(ctx.submits[-1]["side"], grid.SELL)
        self.assertEqual(ctx.submits[-1]["reduce_only"], 1)

    def test_native_recovery_payload_rejects_invalid_length_or_callback_kind(self):
        for invalid in ({"event_payload_len": 1}, {"event_kind": 1}):
            with self.subTest(invalid=invalid):
                ctx = Context()
                self.invoke("on_start", ctx)
                ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now)], dtype=abi_v13.tick_dtype)
                self.invoke("on_tick", ctx)
                self.invoke("on_recover", ctx, expected=-1, context_overrides=invalid)
                self.assertEqual(ctx.state["grid"]["status"], grid.RECONCILING)
                self.assertEqual(len(ctx.submits), 2)
                self.assertEqual(ctx.cancels, [])

    def test_native_protective_exit_retries_beyond_opening_limit_then_confirms_flat(self):
        ctx = Context()
        self.invoke("on_start", ctx)
        ctx.tick_rows = np.array([row(abi_v13.tick_dtype, receive_ts_ns=ctx.now)], dtype=abi_v13.tick_dtype)
        self.invoke("on_tick", ctx)
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.FILLED, 2
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 2, 2
        ctx.fill_rows = np.array([row(abi_v13.fill_dtype, order_id=1, side=1, fill_qty_lots=2,
                                     cumulative_filled_lots=2, fill_price_ticks=95,
                                     account_sequence=2, final_fill=1)], dtype=abi_v13.fill_dtype)
        self.invoke("on_fill", ctx)
        ctx.active = ctx.active[:0]
        ctx.order_rows = np.array([row(abi_v13.order_event_dtype, order_id=2, status=grid.CANCELED,
                                      price_ticks=105, qty_lots=2)], dtype=abi_v13.order_event_dtype)
        self.invoke("on_order", ctx)
        self.invoke("on_stop", ctx)
        for _ in range(6):
            ctx.now += 500_000_000
            ctx.tick_rows[0]["receive_ts_ns"] = ctx.now
            self.invoke("on_tick", ctx)
            order = ctx.submits[-1]
            self.assertTrue(order["reduce_only"])
            ctx.active = ctx.active[:0]
            ctx.order_rows[0] = row(abi_v13.order_event_dtype, order_id=order["order_id"], status=grid.EXPIRED,
                                    price_ticks=order["price_ticks"], qty_lots=order["qty_lots"])
            self.assertEqual(self.invoke("on_order", ctx) & 1, 0)
            self.assertEqual(ctx.state["grid"]["status"], grid.STOPPING)
        ctx.now += 500_000_000
        ctx.tick_rows[0]["receive_ts_ns"] = ctx.now
        self.invoke("on_tick", ctx)
        exit_id = int(ctx.submits[-1]["order_id"])
        ctx.active[0]["status"], ctx.active[0]["cumulative_filled_lots"] = grid.FILLED, 2
        ctx.position_row["qty_lots"], ctx.position_row["account_sequence"] = 0, 3
        ctx.fill_rows[0] = row(abi_v13.fill_dtype, order_id=exit_id, side=2, fill_qty_lots=2,
                               cumulative_filled_lots=2, fill_price_ticks=98, account_sequence=3, final_fill=1)
        self.assertEqual(self.invoke("on_fill", ctx) & 1, 1)
        self.assertEqual(ctx.state["grid"]["status"], grid.STOPPED)


if __name__ == "__main__":
    unittest.main()
