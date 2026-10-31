"""Cross-exchange execution invariants, native V13 callbacks and CLI traces."""

import ctypes
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest
from contextlib import ExitStack
from itertools import permutations
from unittest.mock import patch

import numpy as np
from numba.core.registry import CPUDispatcher

from titan_strategy import abi_v13
from titan_strategy.definition import Capability
from titan_strategy.state import canonical_state_schema, state_schema_hash, validate_state_dtype
from strategies import cross_exchange_mm as mm


def row(dtype, **values):
    result = np.zeros(1, dtype=dtype)
    for name, value in values.items():
        result[0][name] = value
    return result[0]


class Context:
    def __init__(self, **parameters):
        self.state_array = mm.build(dict(warmup_ns=1_000_000_000, **parameters)).state
        self.state = self.state_array[0]
        self.now, self.is_paused, self.stop_complete = 1_000_000_000, False, False
        self.markets = np.array([row(abi_v13.market_dtype, asset_no=i,
            best_bid_ticks=9990, best_ask_ticks=10010, best_bid_qty_lots=1000,
            best_ask_qty_lots=1000) for i in range(2)], dtype=abi_v13.market_dtype)
        self.positions = np.array([
            row(abi_v13.position_dtype, account_no=0, asset_no=1, account_sequence=1),
            row(abi_v13.position_dtype, account_no=1, asset_no=0, account_sequence=1)],
            dtype=abi_v13.position_dtype)
        self.accounts = np.array([row(abi_v13.account_dtype, account_no=i,
            account_epoch=1, state=4, account_sequence=1) for i in range(2)], dtype=abi_v13.account_dtype)
        self.active = np.zeros(0, dtype=abi_v13.active_order_dtype)
        self.ticks_rows = np.zeros(0, dtype=abi_v13.tick_dtype)
        self.fills_rows = np.zeros(0, dtype=abi_v13.fill_dtype)
        self.orders_rows = np.zeros(0, dtype=abi_v13.order_event_dtype)
        self.cancel_rows = np.zeros(0, dtype=abi_v13.cancel_event_dtype)
        self.outcomes = np.zeros(0, dtype=abi_v13.active_order_dtype)
        self.submits, self.cancels, self.sequence = [], [], 1
        self.reject_submit, self.reject_cancel = False, False
        self.invoke = lambda name: getattr(mm, name)(self)

    def account(self, account): return self.accounts[account]
    def position(self, account, asset):
        return next(p for p in self.positions if p["account_no"] == account and p["asset_no"] == asset)
    def market(self, asset): return self.markets[asset]
    def active_orders(self): return self.active
    def ticks(self): return self.ticks_rows
    def fills(self): return self.fills_rows
    def order_events(self): return self.orders_rows
    def cancel_events(self): return self.cancel_rows
    def recovery_orders(self): return self.outcomes
    def complete_stop(self): self.stop_complete = True

    @property
    def hedges(self): return [o for o in self.submits if o["time_in_force"] == mm.IOC]

    def submit_order(self, account, asset, side, order_type, qty, price, tif, reduce_only=False):
        if self.reject_submit:
            return 0
        oid = len(self.submits) + 1
        order = row(abi_v13.active_order_dtype, order_id=oid, account_no=account, asset_no=asset,
            side=side, order_type=order_type, qty_lots=qty, price_ticks=price,
            time_in_force=tif, reduce_only=reduce_only, status=mm.ACCEPTED,
            created_ts_ns=self.now, updated_ts_ns=self.now)
        self.submits.append(order.copy())
        self.active = np.append(self.active, order)
        return oid

    def cancel_order(self, _account, _asset, oid):
        self.cancels.append(int(oid))
        return 0 if self.reject_cancel else len(self.cancels)

    def tick(self, assets=(0, 1)):
        self.ticks_rows = np.array([row(abi_v13.tick_dtype, asset_no=a,
            receive_ts_ns=self.now) for a in assets], dtype=abi_v13.tick_dtype)
        return self.invoke("on_tick")

    def start(self):
        assert self.invoke("on_start") == 0
        self.tick()
        self.now += 1_000_000_000
        self.tick()
        return self

    def terminal(self, oid, status=mm.CANCELED, cumulative=None):
        order = next(o for o in self.submits if o["order_id"] == oid)
        ref = next((r for r in self.state["refs"] if r["order_id"] == oid), None)
        if cumulative is None:
            cumulative = int(ref["applied_lots"]) if ref is not None else 0
        self.active = self.active[self.active["order_id"] != oid]
        self.sequence += 1
        self.orders_rows = np.array([row(abi_v13.order_event_dtype, order_id=oid,
            account_no=order["account_no"], asset_no=order["asset_no"], price_ticks=order["price_ticks"],
            qty_lots=order["qty_lots"], status=status, cumulative_filled_lots=cumulative,
            account_sequence=self.sequence)], dtype=abi_v13.order_event_dtype)
        return self.invoke("on_order")

    def fill(self, oid=1, qty=5, price=None, cumulative=None, position_first=True):
        order = next(o for o in self.submits if o["order_id"] == oid)
        ref = next(r for r in self.state["refs"] if r["order_id"] == oid)
        cumulative = int(ref["applied_lots"]) + qty if cumulative is None else cumulative
        price = int(order["price_ticks"]) if price is None else price
        self.sequence += 1
        if position_first:
            self.position(order["account_no"], order["asset_no"])["qty_lots"] += qty if order["side"] == mm.BUY else -qty
        for o in self.active:
            if o["order_id"] == oid:
                o["cumulative_filled_lots"] = cumulative
                o["status"] = mm.FILLED if cumulative == order["qty_lots"] else mm.PARTIAL
        if cumulative == order["qty_lots"]:
            self.active = self.active[self.active["order_id"] != oid]
        self.fills_rows = np.array([row(abi_v13.fill_dtype, order_id=oid,
            account_no=order["account_no"], asset_no=order["asset_no"], side=order["side"],
            fill_price_ticks=price, fill_qty_lots=qty, cumulative_filled_lots=cumulative,
            account_sequence=self.sequence, final_fill=cumulative == order["qty_lots"])], dtype=abi_v13.fill_dtype)
        return self.invoke("on_fill")

    def clear_makers(self):
        ids = [int(r["order_id"]) for r in self.state["refs"][:2] if r["order_id"]]
        for oid in ids:
            self.terminal(oid)


class TestCrossExchangeMM(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        for name, value in list(vars(mm).items()):
            if isinstance(value, CPUDispatcher):
                stack.enter_context(patch.object(mm, name, value.py_func))

    def test_name_defaults_and_no_unsafe_pause_capability(self):
        definition = mm.build({})
        self.assertEqual(definition.spec.strategy_type, "cross_exchange_mm")
        self.assertEqual(definition.spec.recovery_state_schema_version, 2)
        self.assertFalse(definition.spec.capabilities & Capability.PAUSE_SAFE)

    def test_invalid_conversion_bindings_and_risk_budgets(self):
        for params in ({"traded_asset_no": 0}, {"hedge_account_no": 0}, {"order_qty_lots": 11},
                {"hedge_lots_per_maker_lot": 0.5}, {"hedge_lots_per_maker_lot": 2},
                {"hedge_min_order_lots": 2}, {"max_unhedged_lots": 4}, {"skew_bps": 11}):
            with self.subTest(params=params), self.assertRaises(ValueError):
                mm.build(params)

    def test_warmup_postonly_and_hedge_cost_price_bounds(self):
        ctx = Context()
        self.assertEqual(ctx.invoke("on_start"), 0)
        ctx.tick()
        self.assertFalse(ctx.submits)
        ctx.now += 1_000_000_000
        ctx.tick()
        self.assertEqual([(int(o["side"]), int(o["price_ticks"])) for o in ctx.submits], [(1, 9980), (2, 10022)])
        self.assertTrue(all(o["time_in_force"] == mm.POST_ONLY for o in ctx.submits))

    def test_quote_costs_and_top_size_are_enforced(self):
        base = Context().start()
        costly = Context(hedge_fee_bps=10, min_profit_bps=5).start()
        self.assertLess(costly.submits[0]["price_ticks"], base.submits[0]["price_ticks"])
        self.assertGreater(costly.submits[1]["price_ticks"], base.submits[1]["price_ticks"])
        ctx = Context()
        ctx.markets[0]["best_bid_qty_lots"] = 4
        ctx.start()
        self.assertEqual([o["side"] for o in ctx.submits], [mm.SELL])

    def test_unprofitable_resting_quote_is_pulled_inside_rate_limit(self):
        ctx = Context().start()
        ctx.now += 1
        ctx.markets[0]["best_bid_ticks"] -= 1
        ctx.markets[0]["best_ask_ticks"] -= 1
        ctx.tick((0,))
        self.assertIn(1, ctx.cancels)
        self.assertEqual(len(ctx.submits), 2)

    def test_start_requires_both_accounts_flat_ready_and_clean(self):
        for fault in ("maker_position", "hedge_position", "account", "order"):
            with self.subTest(fault=fault):
                ctx = Context()
                if fault == "maker_position": ctx.positions[0]["qty_lots"] = 1
                if fault == "hedge_position": ctx.positions[1]["qty_lots"] = -1
                if fault == "account": ctx.accounts[1]["state"] = 1
                if fault == "order": ctx.submit_order(1, 0, 1, 1, 1, 10000, mm.IOC)
                self.assertLess(ctx.invoke("on_start"), 0)

    def test_buy_partial_fill_immediately_hedges_before_position_delivery(self):
        ctx = Context().start()
        ctx.fill(qty=2, position_first=False)
        self.assertEqual(len(ctx.hedges), 1)
        hedge = ctx.hedges[0]
        self.assertEqual((hedge["account_no"], hedge["asset_no"], hedge["side"], hedge["qty_lots"]), (1, 0, mm.SELL, 2))
        self.assertEqual(hedge["price_ticks"], 9986)
        self.assertEqual(hedge["reduce_only"], 0)
        self.assertEqual(set(ctx.cancels), {1, 2})

    def test_sell_fill_hedges_buy_and_integer_lot_conversion(self):
        ctx = Context(hedge_lots_per_maker_lot=10, max_hedge_position_lots=100, max_unhedged_lots=100).start()
        ctx.fill(oid=2, qty=1)
        self.assertEqual((ctx.hedges[0]["side"], ctx.hedges[0]["qty_lots"]), (mm.BUY, 10))
        self.assertEqual(ctx.hedges[0]["price_ticks"], 10015)

    def test_fills_and_replays_never_double_hedge(self):
        ctx = Context().start()
        ctx.fill(qty=2)
        ctx.invoke("on_fill")
        ctx.fill(qty=3)
        self.assertEqual(len(ctx.hedges), 1)
        self.assertEqual(ctx.state["mm"]["expected_position_lots"], 5)
        ctx.fill(oid=3, qty=2)
        ctx.invoke("on_fill")
        self.assertEqual(len(ctx.hedges), 1)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual([int(o["qty_lots"]) for o in ctx.hedges], [2, 3])

    def test_partial_hedge_waits_for_terminal_before_residual_retry(self):
        ctx = Context().start()
        ctx.fill()
        ctx.fill(oid=3, qty=2)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(len(ctx.hedges), 1)
        ctx.terminal(3, cumulative=2)
        self.assertEqual(len(ctx.hedges), 1)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(ctx.hedges[-1]["qty_lots"], 3)

    def test_terminal_before_native_fill_never_fabricates_hedge_progress(self):
        ctx = Context().start()
        ctx.fill()
        ctx.terminal(3, cumulative=2)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(ctx.state["mm"]["hedge_position_lots"], 0)
        self.assertEqual(len(ctx.hedges), 1)
        ctx.fill(oid=3, qty=2)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(ctx.hedges[-1]["qty_lots"], 3)

    def test_hedge_fill_terminal_position_permutations(self):
        for ordering in permutations(("fill", "order", "position")):
            with self.subTest(ordering=ordering):
                ctx = Context().start()
                ctx.fill()
                for action in ordering:
                    if action == "fill": ctx.fill(oid=3, qty=2, position_first=False)
                    if action == "order": ctx.terminal(3, cumulative=2)
                    if action == "position":
                        ctx.positions[1]["qty_lots"] = -2
                        ctx.invoke("on_position")
                self.assertEqual(ctx.state["mm"]["error"], 0)
                self.assertEqual(ctx.state["mm"]["hedge_position_lots"], -2)
                self.assertEqual(ctx.state["refs"][mm.HEDGE]["order_id"], 0)

    def test_cancel_ack_does_not_release_hedge_reservation(self):
        ctx = Context().start()
        ctx.fill(qty=2)
        ctx.fill(oid=2, qty=2)
        self.assertIn(3, ctx.cancels)
        ctx.cancel_rows = np.array([row(abi_v13.cancel_event_dtype, account_no=1, asset_no=0,
            order_id=3, request_result=1, final_status=mm.CANCELED)], dtype=abi_v13.cancel_event_dtype)
        ctx.invoke("on_cancel")
        self.assertEqual(ctx.state["refs"][mm.HEDGE]["order_id"], 3)
        self.assertEqual(len(ctx.hedges), 1)

    def test_cancel_fill_race_creates_corrective_hedge_only_after_known_fill(self):
        ctx = Context().start()
        ctx.fill(qty=2)
        ctx.fill(oid=2, qty=2)
        self.assertEqual(mm.debt(ctx.state), 0)
        ctx.fill(oid=3, qty=2)
        self.assertEqual(mm.debt(ctx.state), -2)
        self.assertEqual(len(ctx.hedges), 1)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual((ctx.hedges[-1]["side"], ctx.hedges[-1]["qty_lots"]), (mm.BUY, 2))

    def test_known_hedge_rejection_retries_but_keeps_original_deadline(self):
        ctx = Context().start()
        ctx.fill()
        started = int(ctx.state["mm"]["debt_started_ns"])
        ctx.terminal(3, status=mm.REJECTED)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(len(ctx.hedges), 2)
        self.assertEqual(ctx.state["mm"]["debt_started_ns"], started)
        self.assertEqual(ctx.hedges[-1]["qty_lots"], 5)

    def test_retries_do_not_relax_anchored_price_protection(self):
        ctx = Context().start()
        ctx.fill()
        first = int(ctx.hedges[0]["price_ticks"])
        ctx.terminal(3, status=mm.REJECTED)
        ctx.now += 600_000_000
        ctx.markets[0]["best_bid_ticks"], ctx.markets[0]["best_ask_ticks"] = 9700, 9720
        ctx.tick()
        self.assertGreaterEqual(ctx.hedges[-1]["price_ticks"], first)

    def test_unknown_or_missing_hedge_is_never_resubmitted(self):
        for fault in ("unknown", "missing", "pending"):
            with self.subTest(fault=fault):
                ctx = Context().start()
                ctx.fill()
                if fault == "missing": ctx.active = ctx.active[ctx.active["order_id"] != 3]
                else: ctx.active[-1]["status"] = mm.UNKNOWN if fault == "unknown" else mm.PENDING
                ctx.invoke("on_timer")
                ctx.now += 10_000_000_000
                ctx.tick()
                self.assertEqual(len(ctx.hedges), 1)
                self.assertEqual(ctx.state["refs"][mm.HEDGE]["order_id"], 3)
                self.assertNotEqual(ctx.state["mm"]["error"], 0)

    def test_bounded_attempts_fail_with_debt_preserved(self):
        ctx = Context(max_hedge_attempts=2).start()
        ctx.fill()
        ctx.terminal(3, status=mm.REJECTED)
        ctx.now += 600_000_000
        ctx.tick()
        ctx.terminal(4, status=mm.REJECTED)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(len(ctx.hedges), 2)
        self.assertEqual(mm.debt(ctx.state), 5)
        self.assertEqual(ctx.state["mm"]["error"], 202)

    def test_stale_hedge_market_does_not_stop_exposure_clock(self):
        ctx = Context().start()
        ctx.now += 6_000_000_000
        ctx.fill()
        self.assertEqual(len(ctx.hedges), 0)
        ctx.clear_makers()  # Isolate the hedge deadline from a missing maker-cancel confirmation.
        ctx.now += 10_000_000_000
        ctx.invoke("on_timer")
        self.assertEqual(ctx.state["mm"]["error"], 201)
        self.assertEqual(mm.debt(ctx.state), 5)

    def test_maker_stale_does_not_block_known_hedging(self):
        ctx = Context().start()
        ctx.now += 11_000_000_000
        ctx.tick((0,))
        ctx.fill()
        self.assertEqual(len(ctx.hedges), 1)

    def test_hedge_account_discontinuity_or_submit_failure_latches(self):
        for fault in ("epoch", "unready", "submit"):
            with self.subTest(fault=fault):
                ctx = Context().start()
                if fault == "epoch": ctx.accounts[1]["account_epoch"] = 2
                if fault == "unready": ctx.accounts[1]["state"] = 1
                if fault == "submit": ctx.reject_submit = True
                ctx.fill()
                self.assertNotEqual(ctx.state["mm"]["error"], 0)
                self.assertEqual(len(ctx.hedges), 0)
                self.assertEqual(mm.debt(ctx.state), 5)

    def test_loss_stops_maker_but_finishes_known_hedge(self):
        ctx = Context(max_gross_loss_tick_lots=1).start()
        ctx.fill(price=11000)
        self.assertEqual(ctx.state["mm"]["loss_latched"], 1)
        self.assertEqual(len(ctx.hedges), 1)
        self.assertEqual(set(ctx.cancels), {2})

    def test_matched_inventory_is_not_flat_and_pnl_uses_both_legs(self):
        ctx = Context().start()
        maker_price = int(ctx.submits[0]["price_ticks"])
        ctx.fill()
        ctx.fill(oid=3, qty=5, price=9990)
        self.assertEqual(mm.debt(ctx.state), 0)
        self.assertEqual(tuple(ctx.positions["qty_lots"]), (5, -5))
        self.assertEqual(ctx.state["mm"]["gross_pnl_tick_lots"], (9990 - maker_price) * 5)

    def test_price_only_loss_during_rewarm_still_trips(self):
        ctx = Context().start()
        ctx.fill()
        ctx.fill(oid=3, qty=5)
        ctx.clear_makers()
        mm.reset_prices(ctx.state)
        ctx.now += 1
        ctx.markets[1]["best_bid_ticks"], ctx.markets[1]["best_ask_ticks"] = 9490, 9510
        ctx.tick()
        self.assertEqual(ctx.state["mm"]["loss_latched"], 1)
        self.assertEqual(len(ctx.submits), 3)

    def test_stop_drains_known_debt_and_waits_for_both_positions(self):
        ctx = Context().start()
        ctx.fill()
        ctx.invoke("on_stop")
        ctx.clear_makers()
        self.assertFalse(ctx.stop_complete)
        ctx.fill(oid=3, qty=5, position_first=False)
        self.assertFalse(ctx.stop_complete)
        ctx.positions[1]["qty_lots"] = -5
        ctx.invoke("on_position")
        self.assertTrue(ctx.stop_complete)
        self.assertEqual(tuple(ctx.positions["qty_lots"]), (5, -5))

    def test_stop_still_retries_proven_rejection(self):
        ctx = Context().start()
        ctx.fill()
        ctx.invoke("on_stop")
        ctx.clear_makers()
        ctx.terminal(3, status=mm.REJECTED)
        ctx.now += 600_000_000
        ctx.tick()
        self.assertEqual(len(ctx.hedges), 2)
        self.assertFalse(ctx.stop_complete)
        ctx.fill(oid=4, qty=5)
        self.assertTrue(ctx.stop_complete)

    def test_failed_stop_cannot_report_completion(self):
        ctx = Context().start()
        ctx.fill()
        ctx.invoke("on_stop")
        ctx.clear_makers()
        ctx.now += 10_000_000_000
        ctx.tick()
        self.assertFalse(ctx.stop_complete)
        self.assertNotEqual(ctx.state["mm"]["error"], 0)

    def test_recover_hedge_proofs_keep_debt_deadline_and_emit_no_commands(self):
        ctx = Context().start()
        ctx.fill()
        ctx.clear_makers()
        original = int(ctx.state["mm"]["debt_started_ns"])
        self.assertLess(ctx.invoke("on_recover"), 0)
        ctx.outcomes = ctx.active.copy()
        ctx.accounts["account_epoch"] = 2
        self.assertEqual(ctx.invoke("on_recover"), 0)
        self.assertEqual(ctx.state["mm"]["debt_started_ns"], original)
        self.assertEqual(ctx.state["mm"]["reference_ts_ns"], 0)
        self.assertEqual(len(ctx.hedges), 1)
        ctx.now += 10_000_000_000
        self.assertLess(ctx.invoke("on_recover"), 0)

    def test_recovery_rejects_unconsumed_fills_and_unknown_active(self):
        ctx = Context().start()
        ctx.fill()
        ctx.clear_makers()
        ctx.outcomes = ctx.active.copy()
        ctx.outcomes[0]["cumulative_filled_lots"] = 1
        self.assertLess(ctx.invoke("on_recover"), 0)
        ctx.outcomes[0]["cumulative_filled_lots"] = 0
        ctx.active[0]["status"] = mm.UNKNOWN
        self.assertLess(ctx.invoke("on_recover"), 0)

    def test_tick_ratio_and_hedge_pnl_conversion(self):
        ctx = Context(reference_tick_ratio=0.1, hedge_lots_per_maker_lot=10,
            max_hedge_position_lots=100, max_unhedged_lots=100).start()
        ctx.fill(qty=1)
        ctx.fill(oid=3, qty=10)
        expected = (ctx.state["mm"]["cash_tick_lots"] + 10000
            + (ctx.state["mm"]["hedge_cash_tick_lots"] - 100000) * 0.1 / 10)
        self.assertAlmostEqual(ctx.state["mm"]["gross_pnl_tick_lots"], expected)


class TestCrossExchangeMMNative(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temporary.cleanup)
        cls.root = Path(__file__).parents[3]
        cls.binary = os.environ.get("TITAN_TEST_BINARY", str(cls.root / "target/debug/titan"))
        cls.source = cls.root / "strategies/cross_exchange_mm.py"
        output = Path(cls.temporary.name) / "cross_exchange_mm"
        result = subprocess.run([cls.binary, "strategy", "compile", "--strategy", str(cls.source),
            "--parameters", "/dev/stdin", "--artifact-format", "pair", "--output", str(output), "--json"],
            input="{}", capture_output=True, text=True, timeout=360)
        if result.returncode:
            raise AssertionError(result.stdout + result.stderr)
        paths = json.loads(result.stdout)["paths"]
        cls.library = ctypes.CDLL(next(p for p in paths if p.endswith(".so")))
        cls.manifest = next(p for p in paths if p.endswith(".manifest.cbor"))
        layout = validate_state_dtype(mm.state_dtype, max_state_bytes=65536, max_alignment=8)
        cls.schema_hash = state_schema_hash(canonical_state_schema(layout, schema_version=2))

    def invoke(self, name, ctx):
        native = np.zeros(1, dtype=abi_v13.runtime_context_dtype)
        n = native[0]
        n["struct_size"], n["abi_version"], n["now_ns"] = native.dtype.itemsize, 13, ctx.now
        n["event_kind"] = {"on_start": 0, "on_tick": 1, "on_fill": 4, "on_order": 5,
            "on_cancel": 6, "on_position": 7, "on_account_state": 9, "on_timer": 10,
            "on_stop": 11, "on_recover": 12}[name]
        n["flags"] = 2 if ctx.is_paused else 0
        n["state_ptr"], n["state_len"] = ctx.state_array.ctypes.data, ctx.state_array.nbytes
        n["state_alignment"], n["state_schema_version"] = mm.state_dtype.alignment, 2
        n["state_schema_hash"] = list(self.schema_hash)
        buffers = {"markets": ctx.markets, "positions": ctx.positions, "accounts": ctx.accounts,
            "active_orders": ctx.active, "ticks": ctx.ticks_rows, "fills": ctx.fills_rows,
            "order_events": ctx.orders_rows, "cancel_events": ctx.cancel_rows}
        for key, values in buffers.items():
            n[key + "_ptr"], n[key + "_len"] = values.ctypes.data, len(values)
        n["event_payload_ptr"], n["event_payload_len"] = ctx.outcomes.ctypes.data, ctx.outcomes.nbytes

        @ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
        def submit(_, request, output):
            o = np.frombuffer(ctypes.string_at(request, abi_v13.submit_order_request_dtype.itemsize),
                dtype=abi_v13.submit_order_request_dtype)[0]
            output[0] = ctx.submit_order(int(o["account_no"]), int(o["asset_no"]), int(o["side"]),
                int(o["order_type"]), int(o["qty_lots"]), int(o["price_ticks"]),
                int(o["time_in_force"]), bool(o["reduce_only"]))
            return 0

        @ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
        def cancel(_, request, output):
            o = np.frombuffer(ctypes.string_at(request, abi_v13.cancel_order_request_dtype.itemsize),
                dtype=abi_v13.cancel_order_request_dtype)[0]
            output[0] = ctx.cancel_order(int(o["account_no"]), int(o["asset_no"]), int(o["order_id"]))
            return 0

        n["submit_order"], n["cancel_order"] = ctypes.cast(submit, ctypes.c_void_p).value, ctypes.cast(cancel, ctypes.c_void_p).value
        callback = getattr(self.library, "titan_strategy_" + name)
        callback.argtypes, callback.restype = [ctypes.c_void_p], ctypes.c_int32
        result = callback(native.ctypes.data)
        ctx.stop_complete |= bool(n["flags"] & 1)
        return result

    def context(self, **parameters):
        ctx = Context(**parameters)
        ctx.invoke = lambda name: self.invoke(name, ctx)
        return ctx

    def test_native_incremental_fills_reserve_one_hedge(self):
        ctx = self.context().start()
        self.assertEqual(ctx.fill(qty=2, position_first=False), 0)
        self.assertEqual(len(ctx.hedges), 1)
        self.assertEqual(ctx.hedges[0]["qty_lots"], 2)
        self.assertEqual(ctx.invoke("on_fill"), 0)
        self.assertEqual(len(ctx.hedges), 1)

    def test_native_partial_terminal_race_residual_and_stop(self):
        ctx = self.context().start()
        ctx.fill()
        ctx.invoke("on_stop")
        ctx.clear_makers()
        self.assertEqual(ctx.terminal(3, cumulative=2), 0)
        self.assertEqual(ctx.fill(oid=3, qty=2), 0)
        ctx.now += 600_000_000
        self.assertEqual(ctx.tick(), 0)
        self.assertEqual(ctx.hedges[-1]["qty_lots"], 3)
        ctx.fill(oid=4, qty=3, position_first=False)
        self.assertFalse(ctx.stop_complete)
        ctx.positions[1]["qty_lots"] = -5
        self.assertEqual(ctx.invoke("on_position"), 0)
        self.assertTrue(ctx.stop_complete)

    def test_native_unknown_hedge_freezes_and_preserves_identity(self):
        ctx = self.context().start()
        ctx.fill()
        ctx.active[-1]["status"] = mm.UNKNOWN
        self.assertEqual(ctx.invoke("on_timer"), 0)
        self.assertNotEqual(ctx.state["mm"]["error"], 0)
        self.assertEqual(len(ctx.hedges), 1)
        self.assertEqual(ctx.state["refs"][mm.HEDGE]["order_id"], 3)

    def test_native_recovery_does_not_reset_deadline_or_submit(self):
        ctx = self.context().start()
        ctx.fill()
        ctx.clear_makers()
        started = int(ctx.state["mm"]["debt_started_ns"])
        ctx.outcomes = ctx.active.copy()
        ctx.accounts["account_epoch"] = 2
        self.assertEqual(ctx.invoke("on_recover"), 0)
        self.assertEqual(ctx.state["mm"]["debt_started_ns"], started)
        self.assertEqual(len(ctx.hedges), 1)

    def test_cli_trace_hedges_and_rejects_incomplete_stop(self):
        initial = {"now_ns": 1_000_000_000,
            "markets": [{"asset_no": i, "best_bid_ticks": 9990, "best_ask_ticks": 10010,
                         "best_bid_qty_lots": 1000, "best_ask_qty_lots": 1000} for i in range(2)],
            "accounts": [{"account_no": i, "account_epoch": 1, "account_sequence": 1, "state": 4} for i in range(2)],
            "positions": [{"account_no": 0, "asset_no": 1}, {"account_no": 1, "asset_no": 0}]}
        events = [{"kind": "tick", "now_ns": t * 1_000_000_000,
            "ticks": [{"asset_no": i, "receive_ts_ns": t * 1_000_000_000, "price_ticks": 10000} for i in range(2)]}
            for t in range(1, 62)]
        positions = [{"account_no": 0, "asset_no": 1, "qty_lots": 5}, {"account_no": 1, "asset_no": 0}]
        ask = {"order_id": 2, "account_no": 0, "asset_no": 1, "side": mm.SELL, "order_type": 1,
            "time_in_force": 4, "status": mm.ACCEPTED, "price_ticks": 10022, "qty_lots": 5,
            "created_ts_ns": 61_000_000_000, "updated_ts_ns": 61_000_000_000}
        hedge = {"order_id": 3, "account_no": 1, "asset_no": 0, "side": mm.SELL, "order_type": 1,
            "time_in_force": 2, "status": mm.ACCEPTED, "price_ticks": 9986, "qty_lots": 5,
            "created_ts_ns": 61_100_000_000, "updated_ts_ns": 61_100_000_000}
        events.extend([
            {"kind": "fill", "now_ns": 61_100_000_000,
             "state": dict(initial, now_ns=61_100_000_000, positions=positions, active_orders=[ask]),
             "fills": [{"order_id": 1, "account_no": 0, "asset_no": 1, "side": mm.BUY,
                "fill_price_ticks": 9980, "fill_qty_lots": 5, "cumulative_filled_lots": 5,
                "account_sequence": 2, "final_fill": 1}]},
            {"kind": "stop", "now_ns": 61_200_000_000,
             "state": dict(initial, now_ns=61_200_000_000, positions=positions, active_orders=[ask, hedge])},
            {"kind": "order", "now_ns": 61_300_000_000,
             "state": dict(initial, now_ns=61_300_000_000, positions=positions, active_orders=[hedge]),
             "orders": [{"order_id": 2, "account_no": 0, "asset_no": 1,
                "price_ticks": 10022, "qty_lots": 5, "status": mm.CANCELED}]},
            {"kind": "fill", "now_ns": 61_400_000_000,
             "state": dict(initial, now_ns=61_400_000_000, positions=[positions[0],
                {"account_no": 1, "asset_no": 0, "qty_lots": -5}]),
             "fills": [{"order_id": 3, "account_no": 1, "asset_no": 0, "side": mm.SELL,
                "fill_price_ticks": 9990, "fill_qty_lots": 5, "cumulative_filled_lots": 5,
                "account_sequence": 3, "final_fill": 1}]},
        ])
        for complete in (False, True):
            directory = Path(self.temporary.name) / ("complete" if complete else "incomplete")
            directory.mkdir()
            (directory / "trace.json").write_text(json.dumps({"schema_version": 1,
                "initial": initial, "events": events if complete else events[:-1]}))
            config = directory / "backtest.toml"
            config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 64\nallow_unsigned_strategy = true\n')
            result = subprocess.run([self.binary, "run", self.manifest, "-e", "backtest", "-m", "tick",
                "-c", str(config), "--json"], capture_output=True, text=True,
                env=dict(os.environ, TITAN_HOME=str(directory / "home")), timeout=30)
            if complete:
                self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
                reports = list((directory / "home").rglob("result.json"))
                self.assertEqual(len(reports), 1)
                report = json.loads(reports[0].read_text())
                self.assertEqual(report["status"], "COMPLETED")
                self.assertEqual((report["submit_count"], report["cancel_count"]), (3, 1))
            else:
                self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
