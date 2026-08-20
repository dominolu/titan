"""Tests for the two-level (market → instrument) event bot.

* Mock-bot tests validate the two-level context structure, global-frame dispatch and
  bar-boundary logic without market data or the Rust fill.
* The real-backtest test validates the Rust fill end-to-end: per-asset BBO/trades/bar
  snapshots, global on_tick cadence and on_bar aggregation.
"""

import unittest

import numba
import numpy as np
from numba import carray, float64, int64, njit, uint64
from numba.core.types import voidptr
from numba.experimental import jitclass

from hftbacktest.eventbot import (
    instrument_ctx_dtype,
    run_event_bot,
)
from hftbacktest.intrinsic import address_as_void_pointer
from hftbacktest.types import (
    BUY_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
)


event_dtype = np.dtype(
    [
        ("ev", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("px", "f8"),
        ("qty", "f8"),
        ("order_id", "u8"),
        ("ival", "i8"),
        ("fval", "f8"),
    ],
    align=True,
)


def build_mock_bot(n_frames, n_assets=1):
    """Mock bot with the same jitclass surface as the real bots."""

    spec = [
        ("frame", int64),
        ("t", int64),
        ("n_frames", int64),
        ("n_assets", int64),
        ("ptr", voidptr),
    ]

    @jitclass(spec)
    class MockBot:
        def __init__(self, n_frames, n_assets):
            self.frame = 0
            self.t = 0
            self.n_frames = n_frames
            self.n_assets = n_assets
            self.ptr = address_as_void_pointer(0)

        @property
        def num_assets(self):
            return self.n_assets

        def elapse(self, duration):
            self.t += duration
            self.frame += 1
            return 0 if self.frame < self.n_frames else 1

        @property
        def current_timestamp(self):
            return self.t

        def last_trades(self, asset_no):
            return np.empty(0, dtype=event_dtype)

        def clear_last_trades(self, asset_no):
            pass

        def orders(self, asset_no):
            raise NotImplementedError

        def submit_buy_order(self, asset_no, order_id, price, qty, tif, ty, wait):
            raise NotImplementedError

        def submit_sell_order(self, asset_no, order_id, price, qty, tif, ty, wait):
            raise NotImplementedError

        def cancel(self, asset_no, order_id, wait):
            raise NotImplementedError

        def clear_inactive_orders(self, asset_no):
            pass

        def wait_order_response(self, asset_no, order_id, timeout):
            return 0

        def depth(self, asset_no):
            raise NotImplementedError

        def position(self, asset_no):
            return 0.0

    return MockBot(n_frames, n_assets)


class TestEventBot(unittest.TestCase):
    def test_two_level_structure_and_global_dispatch(self):
        """Single market, two instruments: structure exposes market → instruments and
        on_tick is called once per frame."""
        bot = build_mock_bot(n_frames=3, n_assets=2)

        # Mock fill: writes synthetic snapshots (no trades) into the instrument element.
        @njit
        def mock_fill(_ptr, _kind, asset_no, addr):
            c = carray(address_as_void_pointer(addr), 1, instrument_ctx_dtype)[0]
            c["bid"] = 100.0 + asset_no
            c["ask"] = 101.0 + asset_no
            c["mid"] = 100.5 + asset_no
            c["position"] = 0.0

        state_global = np.zeros(64, dtype=np.float64)

        @njit
        def on_tick(s):
            s.global_state[0] += 1
            # 两级访问：market 0 的两个品种
            m = s.instruments(0)
            s.global_state[1] += m[0]["mid"] + m[1]["mid"]
            # 全局状态
            s.global_state[0] += 1

        @njit
        def on_bar(s):
            s.global_state[3] += 1

        ok = run_event_bot(
            bot,
            on_tick,
            on_bar,
            markets=[[0, 1]],
            symbol_ids=[10, 11],
            frame_interval=500_000_000,
            bar_interval=1_000_000_000,
            fill=mock_fill,
            state_global=state_global,
        )
        self.assertTrue(ok)
        self.assertEqual(state_global[0], 6)  # 每帧 +2（帧内两处自增）
        self.assertEqual(state_global[1], 3 * (100.5 + 101.5))
        self.assertEqual(state_global[3], 1)  # 3 帧 0.5s 跨过 1s bar 边界

    def test_multi_market_structure(self):
        """Two markets: instruments must be addressed per market."""
        bot = build_mock_bot(n_frames=2, n_assets=2)

        @njit
        def mock_fill(_ptr, _kind, asset_no, addr):
            c = carray(address_as_void_pointer(addr), 1, instrument_ctx_dtype)[0]
            c["bid"] = 100.0 + asset_no
            c["ask"] = 101.0 + asset_no
            c["mid"] = 100.5 + asset_no

        state_global = np.zeros(64, dtype=np.float64)

        @njit
        def on_tick(s):
            # 跨所价差：market0 品种0 与 market1 品种0
            s.global_state[0] += 1
            s.global_state[1] += s.instruments(0)[0]["mid"]
            s.global_state[2] += s.instruments(1)[0]["mid"]
            s.global_state[3] = s.n_markets()

        run_event_bot(
            bot,
            on_tick,
            markets=[[0], [1]],
            symbol_ids=[1, 1],  # 同一品种跨所
            frame_interval=500_000_000,
            bar_interval=1_000_000_000,
            fill=mock_fill,
            state_global=state_global,
        )
        self.assertEqual(state_global[3], 2)
        self.assertEqual(state_global[1], 2 * 100.5)
        self.assertEqual(state_global[2], 2 * 101.5)

    def test_on_tick_requires_njit_handler(self):
        bot = build_mock_bot(2)
        with self.assertRaises(TypeError):
            run_event_bot(bot, lambda s: None)


class TestEventBotRealBacktest(unittest.TestCase):
    """End-to-end validation against the real Rust backtest engine and Rust fill."""

    def _make_asset(self, snap_bids, snap_asks, trades):
        n = len(snap_bids) + len(snap_asks) + len(trades)
        ev = np.zeros(n, dtype=event_dtype)
        k = 0
        for px, qty in snap_bids:
            ev[k] = (DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT,
                     1_000_000_000, 1_000_000_000, px, qty, 0, 0, 0)
            k += 1
        for px, qty in snap_asks:
            ev[k] = (DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT,
                     1_000_000_000, 1_000_000_000, px, qty, 0, 0, 0)
            k += 1
        for t, px, qty, side_buy in trades:
            f = TRADE_EVENT | (BUY_EVENT if side_buy else SELL_EVENT) | EXCH_EVENT | LOCAL_EVENT
            ev[k] = (f, t, t, px, qty, 0, 0, 0)
            k += 1
        return ev

    def test_two_market_two_asset(self):
        from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest

        btc = self._make_asset(
            [(100, 1), (99, 2)],
            [(101, 1), (102, 2)],
            [
                (1_100_000_000, 100, 1, False),
                (1_200_000_000, 101, 2, True),
                (1_400_000_000, 100.5, 1, False),
                (1_700_000_000, 102, 1, True),
            ],
        )
        eth = self._make_asset(
            [(10, 5), (9, 5)],
            [(11, 5), (12, 5)],
            [
                (1_150_000_000, 10, 5, False),
                (1_250_000_000, 10.5, 3, True),
                (1_450_000_000, 11, 2, False),
            ],
        )

        def asset(d):
            return (
                BacktestAsset()
                .linear_asset(1.0)
                .data(d)
                .no_partial_fill_exchange()
                .constant_order_latency(0, 0)
                .power_prob_queue_model3(3.0)
                .tick_size(0.1)
                .lot_size(0.001)
                .roi_lb(0.0)
                .roi_ub(1.0)
                .last_trades_capacity(16)
            )

        hbt = HashMapMarketDepthBacktest([asset(btc), asset(eth)])
        state_global = np.zeros(64)
        per_instr = np.zeros((2, 64))
        market_states = np.zeros((2, 64))

        @njit
        def on_tick(s):
            s.global_state[0] += 1
            s.global_state[1] += s.instruments(0)[0]["mid"]
            s.global_state[2] += s.instruments(1)[0]["mid"]
            s.global_state[3] += s.trades(0, 0).shape[0] if s.trades(0, 0).shape[0] > 0 else 0
            s.global_state[4] += s.trades(1, 0).shape[0] if s.trades(1, 0).shape[0] > 0 else 0
            s.instrument_state(0, 0)[0] += 1
            s.instrument_state(1, 0)[0] += 1

        @njit
        def on_bar(s):
            s.global_state[5] += 1
            if s.global_state[6] < 0.5:
                c = s.instruments(0)[0]
                s.global_state[6] = 1.0
                s.global_state[7] = c["bar_o"]
                s.global_state[8] = c["bar_c"]
                s.global_state[9] = c["bar_v"]

        ok = run_event_bot(
            hbt,
            on_tick,
            on_bar,
            markets=[[0], [1]],
            symbol_ids=[1, 2],
            frame_interval=100_000_000,
            bar_interval=500_000_000,
            state_global=state_global,
            instrument_states=per_instr,
            market_states=market_states,
        )

        self.assertTrue(ok)
        self.assertEqual(state_global[0], 7)
        self.assertEqual(state_global[1], 7 * 100.5)  # BTC mid
        self.assertEqual(state_global[2], 7 * 10.5)   # ETH mid
        self.assertEqual(state_global[3], 4)          # BTC 4 笔成交
        self.assertEqual(state_global[4], 3)          # ETH 3 笔成交
        self.assertGreaterEqual(state_global[5], 1)   # 至少一根 bar
        self.assertEqual(state_global[7], 100.0)      # 首根 bar open
        self.assertEqual(state_global[9], 4.0)        # 首根 bar 成交量
        self.assertGreater(per_instr[0][0], 0)
        self.assertGreater(per_instr[1][0], 0)


if __name__ == "__main__":
    unittest.main()
