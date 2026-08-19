"""Unit tests for the on_tick / on_bar event-bot adapter.

These tests validate the Numba callback mechanism with a mock jitclass bot, so they run
without any market data. They cover:

* frame-mode on_tick: one call per frame, all trades forwarded, empty frames still tick
* trade-bar aggregation: OHLCV accumulated from the frame's trades
* end-of-data handling: the final frame (which elapse consumes while returning
  EndOfData) is still delivered
"""

import unittest

import numba
import numpy as np
from numba import njit, int64
from numba.experimental import jitclass

from hftbacktest.eventbot import run_event_bot


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


def build_mock_bot():
    """A mock bot with the same jitclass surface as the real backtest/live bots:
    elapse() -> int64, current_timestamp property, last_trades(0), clear_last_trades(0).
    """

    spec = [
        ("all_trades", numba.from_dtype(event_dtype)[:]),
        ("counts", int64[:]),
        ("frame", int64),
        ("t", int64),
    ]

    @jitclass(spec)
    class MockBot:
        def __init__(self, trades, counts):
            self.all_trades = trades
            self.counts = counts
            self.frame = 0
            self.t = 0

        def elapse(self, duration):
            self.t += duration
            self.frame += 1
            # Mirrors the real engine: EndOfData is returned in the same call that
            # consumed the final frame's events.
            return 0 if self.frame < self.counts.shape[0] else 1

        @property
        def current_timestamp(self):
            return self.t

        def last_trades(self, asset_no):
            off = int(self.counts[: self.frame - 1].sum()) if self.frame > 1 else 0
            n = self.counts[self.frame - 1]
            return self.all_trades[off : off + n]

        def clear_last_trades(self, asset_no):
            pass

    # 3 frames: 2 trades @0.5s, empty @1.0s, 3 trades @1.5s
    trades = np.zeros(5, dtype=event_dtype)
    frame_ts = [500_000_000, 500_000_000, 1_500_000_000, 1_500_000_000, 1_500_000_000]
    for i, (px, qty) in enumerate([(100, 1), (101, 2), (102, 3), (99, 1), (103, 4)]):
        trades[i]["px"] = px
        trades[i]["qty"] = qty
        trades[i]["exch_ts"] = frame_ts[i]
        trades[i]["local_ts"] = frame_ts[i]
    counts = np.array([2, 0, 3], dtype=np.int64)
    return MockBot(trades, counts)


class TestEventBot(unittest.TestCase):
    def test_frame_mode_and_bar_aggregation(self):
        bot = build_mock_bot()
        stats = np.zeros(6)  # tick_calls, tick_n_sum, first_px_sum, bar_calls, bar_open, bar_vol

        @njit
        def on_tick(ts, trades, n, ctx):
            ctx[0] += 1
            ctx[1] += n
            if n > 0:
                ctx[2] += trades[0].px

        @njit
        def on_bar(open_ts, o, h, l, c, v, ctx):
            ctx[3] += 1
            ctx[4] = o
            ctx[5] = v

        ok = run_event_bot(
            bot,
            on_tick,
            on_bar,
            ctx=stats,
            frame_interval=500_000_000,
            bar_interval=1_000_000_000,
        )

        self.assertTrue(ok)
        # One frame callback per frame (3 frames), including the empty one.
        self.assertEqual(stats[0], 3)
        # All 5 trades forwarded.
        self.assertEqual(stats[1], 5)
        # First trade of each non-empty frame: 100 + 102.
        self.assertEqual(stats[2], 202)
        # One trade bar closed (open=100, volume=1+2+3+1+4).
        self.assertEqual(stats[3], 1)
        self.assertEqual(stats[4], 100)
        self.assertEqual(stats[5], 11)

    def test_on_tick_requires_njit_handler(self):
        bot = build_mock_bot()
        with self.assertRaises(TypeError):
            run_event_bot(bot, lambda ts, trades, n, ctx: None)

    def test_on_bar_optional(self):
        bot = build_mock_bot()
        stats = np.zeros(2)

        @njit
        def on_tick(ts, trades, n, ctx):
            ctx[0] += 1
            ctx[1] += n

        run_event_bot(
            bot,
            on_tick,
            frame_interval=500_000_000,
            bar_interval=1_000_000_000,
            ctx=stats,
        )
        self.assertEqual(stats[0], 3)
        self.assertEqual(stats[1], 5)


if __name__ == "__main__":
    unittest.main()
