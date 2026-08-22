import unittest

import numpy as np
from numba import njit

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from hftbacktest.eventbot import BAR_COMPLETE, BAR_EMPTY, run_event_bot, timed_bar_dtype
from hftbacktest.types import (
    BUY_EVENT,
    DEPTH_SNAPSHOT_EVENT,
    EXCH_EVENT,
    LOCAL_EVENT,
    SELL_EVENT,
    TRADE_EVENT,
    event_dtype,
)


def make_rows():
    rows = np.zeros(5, dtype=event_dtype)
    rows[0] = (
        DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT,
        100,
        100,
        99.0,
        1.0,
        0,
        0,
        0,
    )
    rows[1] = (
        DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT,
        100,
        100,
        101.0,
        1.0,
        0,
        0,
        0,
    )
    rows[2] = (TRADE_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 150, 150, 100.0, 2.0, 0, 0, 0)
    rows[3] = (TRADE_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 250, 250, 101.0, 3.0, 0, 0, 0)
    rows[4] = (TRADE_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 350, 350, 102.0, 4.0, 0, 0, 0)
    return rows


def make_asset(rows):
    return (
        BacktestAsset()
        .linear_asset(1.0)
        .data(rows)
        .no_partial_fill_exchange()
        .constant_order_latency(0, 0)
        .power_prob_queue_model3(3.0)
        .tick_size(0.1)
        .lot_size(0.001)
        .roi_lb(0.0)
        .roi_ub(200.0)
        .last_trades_capacity(16)
    )


def make_bot():
    return HashMapMarketDepthBacktest([make_asset(make_rows())])


class TestRustOwnedEventBot(unittest.TestCase):
    def test_lifecycle_and_global_tick_batch(self):
        @njit
        def on_start(s):
            s.state[0] += 1

        @njit
        def on_tick(s):
            s.state[1] += 1
            ticks = s.ticks()
            s.state[2] += len(ticks)
            for item in ticks:
                s.state[3] += item["asset_no"]
                s.state[4] += item["event"]["qty"]

        @njit
        def on_stop(s):
            s.state[5] += 1

        state = run_event_bot(
            make_bot(),
            on_tick=on_tick,
            on_start=on_start,
            on_stop=on_stop,
            frame_interval=100,
        )

        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 4)
        self.assertEqual(state[2], 5)
        self.assertEqual(state[3], 0)
        self.assertEqual(state[4], 11)
        self.assertEqual(state[5], 1)

    def test_tick_batch_is_global_across_assets(self):
        rows0 = make_rows()
        rows1 = make_rows()
        rows1["px"] += 1_000.0

        @njit
        def on_tick(s):
            s.state[0] += 1
            s.state[1] += s.num_ticks
            for item in s.ticks():
                s.state[2] += item["asset_no"]

        state = run_event_bot(
            HashMapMarketDepthBacktest([make_asset(rows0), make_asset(rows1)]),
            on_tick=on_tick,
            frame_interval=100,
        )
        self.assertEqual(state[0], 4)
        self.assertEqual(state[1], 10)
        self.assertEqual(state[2], 5)

    def test_tick_context_exposes_rust_market_state(self):
        @njit
        def on_tick(s):
            if s.state[0] == 0:
                s.state[1] = s.num_assets
                s.state[2] = s.best_bid(0)
                s.state[3] = s.best_ask(0)
                s.state[4] = s.best_bid_qty(0)
                s.state[5] = s.best_ask_qty(0)
            s.state[0] += 1

        state = run_event_bot(make_bot(), on_tick=on_tick, frame_interval=100)
        self.assertEqual(state[1], 1)
        self.assertEqual(state[2], 99)
        self.assertEqual(state[3], 101)
        self.assertEqual(state[4], 1)
        self.assertEqual(state[5], 1)

    def test_strategy_can_stop_rust_loop(self):
        @njit
        def on_tick(s):
            s.state[0] += 1
            s.stop()

        @njit
        def on_stop(s):
            s.state[1] += 1

        state = run_event_bot(make_bot(), on_tick=on_tick, on_stop=on_stop, frame_interval=100)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 1)

    def test_callback_exception_is_fatal_and_notifies_error_then_stop(self):
        state = np.zeros(8, dtype=np.float64)

        @njit
        def on_tick(s):
            raise ValueError("boom")

        @njit
        def on_error(s):
            s.state[0] += 1
            s.state[1] = s.last_error

        @njit
        def on_stop(s):
            s.state[2] += 1

        with self.assertRaises(RuntimeError):
            run_event_bot(
                make_bot(),
                on_tick=on_tick,
                on_error=on_error,
                on_stop=on_stop,
                frame_interval=100,
                state=state,
            )
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], -1000)
        self.assertEqual(state[2], 1)

    def test_stop_callback_cannot_submit_new_order(self):
        @njit
        def on_stop(s):
            s.state[0] = s.submit_buy_order(0, 123, 100.0, 1.0, 0, 0, False)

        state = run_event_bot(make_bot(), on_stop=on_stop, frame_interval=100)
        self.assertEqual(state[0], -3)

    def test_unconnected_callbacks_fail_at_startup(self):
        @njit
        def handler(s):
            pass

        with self.assertRaises(NotImplementedError):
            run_event_bot(make_bot(), on_funding=handler)
        with self.assertRaises(NotImplementedError):
            run_event_bot(make_bot(), on_timer=handler)

    def test_unknown_bot_backend_is_rejected_before_ffi(self):
        class UnknownBot:
            ptr = 1

        with self.assertRaises(TypeError):
            run_event_bot(UnknownBot())

    def test_order_commands_and_fills_stay_in_rust_runtime(self):
        @njit
        def on_tick(s):
            if s.state_i64[0] == 0:
                # GTC limit buy. The callback only appends a POD command; Rust submits it
                # after this callback and owns all subsequent matching and fill events.
                s.state_i64[1] = s.submit_buy_order(0, 42, 200.0, 1.0, 0, 0, False)
                s.state_i64[0] = 1

        @njit
        def on_filled(s):
            fills = s.fills()
            s.state[0] += len(fills)
            for fill in fills:
                s.state[1] += fill["qty"]
                s.state[2] = fill["order_id"]
            s.state[3] = s.position(0)

        @njit
        def on_order(s):
            s.state[4] += len(s.orders())

        @njit
        def on_position(s):
            s.state[5] += 1
            s.state[6] = s.position(0)

        state_i64 = np.zeros(8, dtype=np.int64)
        state = run_event_bot(
            make_bot(),
            on_tick=on_tick,
            on_filled=on_filled,
            on_order=on_order,
            on_position=on_position,
            frame_interval=100,
            state_i64=state_i64,
        )

        self.assertEqual(state_i64[1], 0)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 1)
        self.assertEqual(state[2], 42)
        self.assertEqual(state[3], 1)
        self.assertGreaterEqual(state[4], 1)
        self.assertEqual(state[5], 1)
        self.assertEqual(state[6], 1)

    def test_multiple_partial_fills_in_one_frame_are_not_collapsed(self):
        rows = np.zeros(5, dtype=event_dtype)
        rows[0] = (
            DEPTH_SNAPSHOT_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT,
            100,
            100,
            100.0,
            1.0,
            0,
            0,
            0,
        )
        rows[1] = (
            DEPTH_SNAPSHOT_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT,
            100,
            100,
            101.0,
            10.0,
            0,
            0,
            0,
        )
        rows[2] = (TRADE_EVENT | BUY_EVENT | EXCH_EVENT | LOCAL_EVENT, 150, 150, 101.0, 1.0, 0, 0, 0)
        rows[3] = (TRADE_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 250, 250, 100.0, 2.0, 0, 0, 0)
        rows[4] = (TRADE_EVENT | SELL_EVENT | EXCH_EVENT | LOCAL_EVENT, 260, 260, 100.0, 2.0, 0, 0, 0)
        asset = (
            BacktestAsset()
            .linear_asset(1.0)
            .data(rows)
            .partial_fill_exchange()
            .constant_order_latency(0, 0)
            .risk_adverse_queue_model()
            .tick_size(0.1)
            .lot_size(1.0)
            .roi_lb(0.0)
            .roi_ub(200.0)
            .last_trades_capacity(16)
        )

        @njit
        def on_tick(s):
            if s.state_i64[0] == 0:
                s.submit_buy_order(0, 77, 100.0, 3.0, 0, 0, False)
                s.state_i64[0] = 1

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] += len(s.fills())
            for fill in s.fills():
                s.state[2] += fill["qty"]

        @njit
        def on_position(s):
            s.state[3] += 1
            s.state[4] = s.position(0)

        state = run_event_bot(
            HashMapMarketDepthBacktest([asset]),
            on_tick=on_tick,
            on_filled=on_filled,
            on_position=on_position,
            frame_interval=100,
        )
        self.assertEqual(state[0], 2)
        self.assertEqual(state[1], 2)
        self.assertEqual(state[2], 3)
        self.assertEqual(state[3], 2)
        self.assertEqual(state[4], 3)

    def test_handlers_must_be_single_argument_njit(self):
        with self.assertRaises(TypeError):
            run_event_bot(make_bot(), on_tick=lambda s: None)

        @njit
        def invalid(a, b):
            pass

        with self.assertRaises(TypeError):
            run_event_bot(make_bot(), on_tick=invalid)

    def test_materialized_bar_history_excludes_current_callback_bar(self):
        bars = np.zeros(3, dtype=timed_bar_dtype)
        for i, open_px in enumerate([10.0, 20.0, 30.0]):
            open_ts = i * 60
            bars[i]["asset_no"] = 0
            bars[i]["timeframe_ns"] = 60
            bars[i]["bar"] = (
                open_ts,
                open_ts + 60,
                open_px,
                open_px + 1,
                open_px - 1,
                open_px + 0.5,
                1.0,
                open_px,
                0.5,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_bar(s):
            call = int(s.state[0])
            history = s.open(0, 60)
            s.state[call + 1] = len(history)
            if len(history) > 0:
                s.state[call + 4] = history[-1]
            if len(history) > 1:
                s.state[7] = history[-2]
            s.state[0] += 1

        state = run_event_bot(
            on_bar=on_bar,
            data_mode="bar",
            bars=bars,
            history_capacity=8,
        )
        self.assertEqual(state[0], 3)
        self.assertEqual(list(state[1:4]), [0, 1, 2])
        self.assertEqual(state[5], 10)
        self.assertEqual(state[6], 20)
        self.assertEqual(state[7], 10)

    def test_bar_order_fills_only_at_following_open(self):
        bars = np.zeros(3, dtype=timed_bar_dtype)
        for i, open_px in enumerate([10.0, 20.0, 30.0]):
            bars[i]["asset_no"] = 0
            bars[i]["timeframe_ns"] = 60
            bars[i]["bar"] = (
                i * 60,
                (i + 1) * 60,
                open_px,
                open_px + 100.0,  # must never fill an order from the same on_bar
                open_px - 9.0,
                open_px + 0.5,
                1.0,
                open_px,
                0.5,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_bar(s):
            s.state[0] += 1
            if s.state[0] == 1:
                s.submit_buy_order(0, 7, 1_000.0, 2.0, 0, 0, False)

        @njit
        def on_filled(s):
            fill = s.fills()[0]
            s.state[1] += 1
            s.state[2] = fill["price"]
            s.state[3] = s.now
            s.state[4] = s.position(0)
            s.state[5] = len(s.open(0, 60))
            s.state[6] = s.open(0, 60)[-1]

        state = run_event_bot(
            on_bar=on_bar,
            on_filled=on_filled,
            data_mode="bar",
            bars=bars,
            history_capacity=8,
        )
        self.assertEqual(state[0], 3)
        self.assertEqual(state[1], 1)
        self.assertEqual(state[2], 20)
        self.assertEqual(state[3], 60)
        self.assertEqual(state[4], 2)
        self.assertEqual(state[5], 1)
        self.assertEqual(state[6], 10)

    def test_bar_batch_groups_assets_by_close_and_timeframe(self):
        bars = np.zeros(4, dtype=timed_bar_dtype)
        row = 0
        for period in range(2):
            for asset_no in range(2):
                open_px = 10.0 + period * 10.0 + asset_no
                bars[row]["asset_no"] = asset_no
                bars[row]["timeframe_ns"] = 60
                bars[row]["bar"] = (
                    period * 60,
                    (period + 1) * 60,
                    open_px,
                    open_px + 1,
                    open_px - 1,
                    open_px + 0.5,
                    1.0,
                    open_px,
                    0.5,
                    1,
                    BAR_COMPLETE,
                )
                row += 1

        @njit
        def on_bar(s):
            s.state[0] += 1
            s.state[1] += s.num_bars
            for item in s.bars():
                s.state[2] += item["asset_no"]

        state = run_event_bot(on_bar=on_bar, data_mode="bar", bars=bars)
        self.assertEqual(state[0], 2)
        self.assertEqual(state[1], 4)
        self.assertEqual(state[2], 2)

    def test_multitimeframe_bar_orders_do_not_time_travel(self):
        bars = np.zeros(3, dtype=timed_bar_dtype)
        specs = [(60, 240, 300), (300, 0, 300), (60, 300, 360)]
        for row, (timeframe, open_ts, close_ts) in enumerate(specs):
            bars[row]["asset_no"] = 0
            bars[row]["timeframe_ns"] = timeframe
            bars[row]["bar"] = (
                open_ts,
                close_ts,
                10.0,
                11.0,
                9.0,
                10.5,
                1.0,
                10.0,
                0.5,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_bar(s):
            if s.bar_timeframe == 60 and s.bar_close_ts == 300:
                s.submit_buy_order(0, 99, 100.0, 1.0, 0, 0, False)

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] = s.now

        state = run_event_bot(data_mode="bar", bars=bars, on_bar=on_bar, on_filled=on_filled)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 300)

    def test_empty_bar_never_executes_market_order(self):
        bars = np.zeros(3, dtype=timed_bar_dtype)
        for row in range(3):
            flags = BAR_COMPLETE | (BAR_EMPTY if row == 1 else 0)
            price = np.nan if row == 1 else 10.0 + row
            bars[row]["asset_no"] = 0
            bars[row]["timeframe_ns"] = 60
            bars[row]["bar"] = (
                row * 60,
                (row + 1) * 60,
                price,
                price,
                price,
                price,
                0.0 if row == 1 else 1.0,
                0.0,
                0.0,
                0,
                flags,
            )

        @njit
        def on_bar(s):
            if s.bar_close_ts == 60:
                s.submit_buy_order(0, 5, 0.0, 1.0, 0, 1, False)

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] = s.now
            s.state[2] = s.fills()[0]["price"]

        state = run_event_bot(data_mode="bar", bars=bars, on_bar=on_bar, on_filled=on_filled)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 120)
        self.assertEqual(state[2], 12)

    def test_unconnected_hybrid_fails_explicitly(self):
        with self.assertRaises(NotImplementedError):
            run_event_bot(make_bot(), data_mode="hybrid")

    def test_tick_batch_overflow_fails_instead_of_growing_unbounded(self):
        with self.assertRaises(RuntimeError):
            run_event_bot(make_bot(), frame_interval=1_000, max_tick_batch=1)


if __name__ == "__main__":
    unittest.main()
