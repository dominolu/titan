"""Real native callbacks: reset semantics, projection boundaries and state budget."""
from pathlib import Path
import ctypes
import json
import tempfile
import unittest

import numpy as np

from titan_strategy import abi_v13
from titan_strategy.context_v13 import _DEPTH_BATCH_DTYPE
from titan_strategy.definition import Capability, EventSubscription, StrategySpec
from titan_strategy.types import EventKind, EventQos
from test_v13_static_compiler import _Request, _compile_with_rust


class TestHostDepthContract(unittest.TestCase):
    def test_projection_requires_reliable_depth(self):
        with self.assertRaisesRegex(ValueError, "reliable ordered"):
            StrategySpec("depth_test", "1.0.0", 1,
                         subscriptions=(EventSubscription(EventKind.DEPTH, "on_depth", 1, EventQos.LATEST),),
                         capabilities=Capability.MARKET_DATA | Capability.HOST_DEPTH_100)


class TestOrderflowNativeDepth(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory()
        root = Path(__file__).parents[3]
        cls.result = _compile_with_rust(_Request(
            root / "strategies/orderflow_events/strategy.py",
            json.loads((root / "strategies/orderflow_events/parameters.json").read_text()),
            "x86_64-unknown-linux-gnu", "x86-64-v2", "pair",
            Path(cls.temporary.name) / "orderflow",
            {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()},
        ))
        cls.library = ctypes.CDLL(str(cls.result.compiled_strategy.paths[0]))

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def setUp(self):
        result = self.result.strategy
        self.state = np.frombuffer(result.initial_state, dtype=result.state_layout.dtype).copy()
        self.context = np.zeros(1, dtype=abi_v13.runtime_context_dtype)
        row = self.context[0]
        row["struct_size"] = self.context.dtype.itemsize
        row["abi_version"] = 13
        row["state_ptr"] = self.state.ctypes.data
        row["state_len"] = self.state.nbytes
        row["state_alignment"] = result.state_layout.alignment
        row["state_schema_version"] = 2
        row["state_schema_hash"] = list(result.schema_hash)
        row["now_ns"] = 1_000_000_000
        self.info = np.zeros(1, dtype=_DEPTH_BATCH_DTYPE)
        self.info[0]["version"] = 1
        self.info[0]["valid"] = 1
        self.info[0]["reset"] = 1
        self.info[0]["stream_epoch"] = 1
        self.items = np.zeros(200, dtype=abi_v13.depth_dtype)
        for i in range(100):
            for side, offset, price in [(1, 0, 1000-i), (2, 100, 1001+i)]:
                item = self.items[offset+i]
                item["side"], item["price_ticks"], item["qty_lots"] = side, price, 10
                item["action"], item["is_snapshot"] = 1, 1
                item["exchange_ts_ns"] = row["now_ns"]
                item["receive_ts_ns"] = row["now_ns"]
        row["depth_ptr"], row["depth_len"] = self.items.ctypes.data, len(self.items)
        row["event_payload_ptr"], row["event_payload_len"] = self.info.ctypes.data, self.info.nbytes

    def invoke(self, name, kind):
        callback = getattr(self.library, "titan_strategy_" + name)
        callback.argtypes, callback.restype = [ctypes.c_void_p], ctypes.c_int32
        self.context[0]["event_kind"] = kind
        self.context[0]["last_error_code"] = 0
        self.assertEqual(callback(self.context.ctypes.data), 0)

    def test_state_budget_and_snapshot_never_generate_flow(self):
        self.assertLessEqual(self.state.nbytes, 64_000)
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["book"]["ready"], 1)
        self.assertEqual(self.state[0]["book"]["depth5_bid"], 50)
        self.assertEqual(self.state[0]["book"]["depth5_ask"], 50)
        self.assertEqual(self.state[0]["flow"]["prints_seen"], 0)
        self.assertEqual(self.state[0]["flow"]["touch_n"], 0)
        self.invoke("on_timer", 10)
        self.assertEqual(self.state[0]["ev"]["events_total"], 0)
        self.assertEqual(self.state[0]["obs"]["book_ready"], 0)

    def test_quantity_reduction_then_resync_clears_windows_and_ghosts(self):
        self.invoke("on_depth", 3)
        self.info[0]["reset"] = 0
        self.items[0]["qty_lots"] = 5
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["flow"]["prints_seen"], 1)
        self.info[0]["reset"], self.info[0]["stream_epoch"] = 1, 2
        self.items["price_ticks"] += 200
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["flow"]["prints_seen"], 1)
        self.assertEqual(self.state[0]["flow"]["touch_n"], 0)
        self.assertEqual(self.state[0]["ring"]["bkt_sell"].sum(), 0)
        self.assertNotIn(1000, self.state[0]["book"]["bid_price"])
        self.assertEqual(self.state[0]["book"]["epoch"], 2)

    def test_invalidation_is_visible_even_with_zero_items(self):
        self.invoke("on_depth", 3)
        self.info[0]["valid"] = 0
        self.context[0]["depth_len"] = 0
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["book"]["ready"], 0)
        self.assertEqual(self.state[0]["obs"]["book_ready"], 0)
        ticks = np.zeros(2, dtype=abi_v13.tick_dtype)
        ticks["price_ticks"], ticks["qty_lots"] = 900, 10
        ticks["side"], ticks["receive_ts_ns"] = [1, 2], 1_000_000_000
        self.context[0]["ticks_ptr"], self.context[0]["ticks_len"] = ticks.ctypes.data, 2
        self.invoke("on_tick", 1)
        self.assertEqual(self.state[0]["book"]["ready"], 0)
        self.assertEqual(self.state[0]["book"]["last_update_ns"], 0)

    def test_leaving_top100_is_not_counted_as_a_removal(self):
        self.invoke("on_depth", 3)
        self.info[0]["reset"] = 0
        self.items[:100]["price_ticks"] += 1
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["flow"]["prints_seen"], 0)
        self.assertEqual(self.state[0]["flow"]["touch_n"], 0)

    def _depth_time(self, now_ns):
        self.context[0]["now_ns"] = now_ns
        self.items["exchange_ts_ns"] = now_ns
        self.items["receive_ts_ns"] = now_ns

    def test_deleted_price_refill_reaches_native_e04_detector(self):
        start = 901_000_000_000
        self._depth_time(start)
        self.invoke("on_depth", 3)
        # The detector is exercised after a previously warmed interval.
        self.state[0]["book"]["warm_since_ns"] = 0
        self.state[0]["cfg"]["max_spread_bp"] = 20
        self.info[0]["reset"] = 0
        self.items["is_snapshot"] = 0
        for cycle in range(3):
            self._depth_time(start + (2 * cycle + 1) * 10_000_000)
            self.items[:100]["price_ticks"] -= 1
            self.invoke("on_depth", 3)
            self._depth_time(start + (2 * cycle + 2) * 10_000_000)
            self.items[:100]["price_ticks"] += 1
            self.invoke("on_depth", 3)
        flow = self.state[0]["flow"]
        slot = next(i for i, price in enumerate(flow["ice_price"]) if price == 1000)
        self.assertEqual(flow["ice_fills"][slot], 3)
        self.assertEqual(flow["ice_refill_ms"][slot], 10)
        self.invoke("on_timer", 10)
        self.assertEqual(self.state[0]["ev"]["count"][4], 1)
        self.assertEqual(self.state[0]["obs"]["e04_count"], 1)

    def test_deleted_price_outside_previous_coverage_does_not_count_as_refill(self):
        self.items[100:]["price_ticks"] += 1000
        self.invoke("on_depth", 3)
        self.info[0]["reset"] = 0
        self._depth_time(1_010_000_000)
        self.items[:100]["price_ticks"] -= 1
        self.invoke("on_depth", 3)
        # Better bids move the deleted price outside the observed Top100.
        # Existing bids exiting below the window must not consume tracker slots.
        self._depth_time(1_020_000_000)
        self.items[:100]["price_ticks"] += 200
        self.invoke("on_depth", 3)
        self._depth_time(1_030_000_000)
        self.items[99]["price_ticks"] = 1000
        self.invoke("on_depth", 3)
        flow = self.state[0]["flow"]
        slot = next(i for i, price in enumerate(flow["ice_price"]) if price == 1000)
        self.assertEqual(flow["ice_refill_ms"][slot], 0)

    def test_resnapshot_drops_deleted_price_evidence(self):
        self.invoke("on_depth", 3)
        self.info[0]["reset"] = 0
        self.items[:100]["price_ticks"] -= 1
        self._depth_time(1_010_000_000)
        self.invoke("on_depth", 3)
        self.info[0]["reset"] = 1
        self.info[0]["stream_epoch"] = 2
        self.items[:100]["price_ticks"] += 1
        self._depth_time(1_020_000_000)
        self.invoke("on_depth", 3)
        self.assertEqual(self.state[0]["flow"]["ice_refill_ms"].sum(), 0)
        self.assertEqual(self.state[0]["flow"]["touch_n"], 0)
