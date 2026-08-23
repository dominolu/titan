import unittest

import numpy as np
from numba import njit

from hftbacktest import BacktestAsset, HashMapMarketDepthBacktest
from hftbacktest.eventbot import (
    BAR_COMPLETE,
    BAR_EMPTY,
    run_event_bot,
    timed_bar_dtype,
    timer_dtype,
    funding_dtype,
)
from hftbacktest.strategies import create_dual_ma_strategy
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
    def test_funding_updates_at_settlement_and_delivers_typed_callback_later(self):
        bars = np.zeros(3, dtype=timed_bar_dtype)
        for index in range(3):
            bars[index]["asset_no"] = 0
            bars[index]["timeframe_ns"] = 60
            bars[index]["bar"] = (
                index * 60,
                (index + 1) * 60,
                100,
                100,
                100,
                100,
                10,
                1000,
                0,
                1,
                BAR_COMPLETE,
            )
        funding = np.zeros(1, dtype=funding_dtype)
        funding[0] = (1, 0, 0, 1, 0, 80, 100, 120, 130, 0.001, 100, 0, 0)

        @njit
        def on_bar(s):
            if s.state_i64[0] == 0:
                s.submit_buy_order(0, 1, 100.0, 1.0, 0, 1, False)
                s.state_i64[0] = 1

        @njit
        def on_funding(s):
            event = s.funding()
            s.state[0] = event["amount"]
            s.state[1] = event["position_qty"]
            s.state_i64[1] = s.now

        state = np.zeros(3)
        state_i64 = np.zeros(3, dtype=np.int64)
        run_event_bot(
            data_mode="bar",
            bars=bars,
            funding=funding,
            on_bar=on_bar,
            on_funding=on_funding,
            state=state,
            state_i64=state_i64,
        )
        self.assertAlmostEqual(state[0], -0.1)
        self.assertEqual(state[1], 1.0)
        self.assertEqual(state_i64[1], 130)

    def test_timer_advances_after_bar_data_and_exposes_typed_payload(self):
        bars = np.zeros(1, dtype=timed_bar_dtype)
        bars[0]["asset_no"] = 0
        bars[0]["timeframe_ns"] = 60
        bars[0]["bar"] = (0, 60, 1, 1, 1, 1, 1, 1, 0, 1, BAR_COMPLETE)
        timers = np.zeros(1, dtype=timer_dtype)
        timers[0] = (100, 7, 9)

        @njit
        def on_timer(s):
            timer = s.timer()
            s.state_i64[0] = s.now
            s.state_i64[1] = timer["owner_id"]
            s.state_i64[2] = timer["timer_id"]

        state_i64 = np.zeros(3, dtype=np.int64)
        run_event_bot(
            data_mode="bar",
            bars=bars,
            timers=timers,
            on_timer=on_timer,
            state_i64=state_i64,
        )
        self.assertEqual(tuple(state_i64), (100, 7, 9))

    def test_tick_timer_advances_after_tick_data_end(self):
        timers = np.zeros(1, dtype=timer_dtype)
        timers[0] = (500, 12, 13)

        @njit
        def on_timer(s):
            timer = s.timer()
            s.state_i64[0] = s.now
            s.state_i64[1] = timer["owner_id"]
            s.state_i64[2] = timer["timer_id"]

        state_i64 = np.zeros(3, dtype=np.int64)
        run_event_bot(
            make_bot(),
            timers=timers,
            on_timer=on_timer,
            state_i64=state_i64,
        )
        self.assertEqual(tuple(state_i64), (500, 12, 13))

    def test_tick_timer_without_market_event_submits_through_normal_transport(self):
        timers = np.zeros(1, dtype=timer_dtype)
        timers[0] = (125, 21, 22)

        @njit
        def on_timer(s):
            s.state_i64[0] = s.now
            s.submit_buy_order(0, 90, 200.0, 1.0, 0, 0, False)

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state_i64[1] = s.now

        state_i64 = np.zeros(2, dtype=np.int64)
        state = run_event_bot(
            make_bot(),
            timers=timers,
            on_timer=on_timer,
            on_filled=on_filled,
            state_i64=state_i64,
            frame_interval=100,
        )
        self.assertEqual(state[0], 1.0)
        self.assertEqual(tuple(state_i64), (125, 125))

    def test_tick_funding_settles_exchange_then_projects_after_report_latency(self):
        funding = np.zeros(1, dtype=funding_dtype)
        funding[0] = (2, 0, 0, 1, 0, 160, 180, 200, 220, 0.001, 100.0, 0.0, 0.0)

        @njit
        def on_tick(s):
            if s.state_i64[0] == 0 and s.now == 100:
                s.submit_buy_order(0, 91, 200.0, 1.0, 0, 0, False)
                s.state_i64[0] = 1

        @njit
        def on_funding(s):
            event = s.funding()
            s.state[0] = event["amount"]
            s.state[1] = event["position_qty"]
            s.state_i64[1] = s.now

        @njit
        def on_filled(s):
            s.state[2] += 1
            s.state_i64[2] = s.now

        state = np.zeros(3)
        state_i64 = np.zeros(3, dtype=np.int64)
        run_event_bot(
            make_bot(),
            on_tick=on_tick,
            funding=funding,
            on_funding=on_funding,
            on_filled=on_filled,
            state=state,
            state_i64=state_i64,
            frame_interval=100,
        )
        self.assertAlmostEqual(state[0], -0.1)
        self.assertEqual(state[1], 1.0)
        self.assertEqual(state[2], 1.0)
        self.assertEqual(state_i64[1], 220)

    def test_bar_matching_mode_is_explicit_and_volume_limited(self):
        bars = np.zeros(2, dtype=timed_bar_dtype)
        bars[0]["asset_no"] = 0
        bars[0]["timeframe_ns"] = 60
        bars[0]["bar"] = (0, 60, 100, 101, 99, 100, 2, 0, 0, 1, BAR_COMPLETE)
        bars[1]["asset_no"] = 0
        bars[1]["timeframe_ns"] = 60
        bars[1]["bar"] = (60, 120, 100, 101, 90, 100, 2, 0, 0, 1, BAR_COMPLETE)

        @njit
        def on_bar(s):
            if s.state_i64[0] == 0:
                s.submit_buy_order(0, 92, 95.0, 2.0, 0, 0, False)
                s.state_i64[0] = 1

        @njit
        def on_filled(s):
            s.state[0] += s.fills()[0]["qty"]

        touch = run_event_bot(
            data_mode="bar",
            bars=bars,
            on_bar=on_bar,
            on_filled=on_filled,
            bar_matching="touch",
            volume_participation=0.5,
        )
        conservative = run_event_bot(
            data_mode="bar",
            bars=bars,
            on_bar=on_bar,
            on_filled=on_filled,
            bar_matching="conservative_ohlc",
            volume_participation=0.5,
        )
        self.assertEqual(touch[0], 1.0)
        self.assertEqual(conservative[0], 0.0)

    def test_dual_ma_strategy_trades_only_on_crosses(self):
        closes = [3.0, 2.0, 1.0, 2.0, 3.0, 2.0, 1.0, 1.0]
        bars = np.zeros(len(closes), dtype=timed_bar_dtype)
        for index, close in enumerate(closes):
            bars[index]["asset_no"] = 0
            bars[index]["timeframe_ns"] = 60
            bars[index]["bar"] = (
                index * 60,
                (index + 1) * 60,
                close,
                close,
                close,
                close,
                1.0,
                close,
                0.0,
                1,
                BAR_COMPLETE,
            )

        strategy = create_dual_ma_strategy(
            closes=np.asarray(closes),
            short_period=2,
            long_period=3,
            timeframe_ns=60,
            quantity=1.0,
        )
        state = run_event_bot(
            data_mode="bar",
            bars=bars,
            history_capacity=4,
            on_bar=strategy.on_bar,
            on_filled=strategy.on_filled,
            on_stop=strategy.on_stop,
            state=strategy.state,
            state_i64=strategy.state_i64,
        )

        self.assertEqual(state[2], 1)
        self.assertEqual(state[3], 1)
        self.assertEqual(state[4], 2)
        self.assertEqual(state[6], 0)
        self.assertEqual(strategy.state_i64[2], 1)
        self.assertEqual(strategy.state_i64[3], 1)
        self.assertEqual(strategy.state_i64[1], 0)
        self.assertEqual(strategy.state_i64[4], len(closes))
        self.assertAlmostEqual(strategy.short_ma[-1], 1.0)
        self.assertAlmostEqual(strategy.long_ma[-1], 4.0 / 3.0)

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

    def test_stop_market_triggers_on_completed_range_and_fills_next_open(self):
        bars = np.zeros(4, dtype=timed_bar_dtype)
        highs = [101.0, 106.0, 104.0, 104.0]
        opens = [100.0, 101.0, 103.0, 104.0]
        for row in range(4):
            bars[row]["asset_no"] = 0
            bars[row]["timeframe_ns"] = 60
            bars[row]["bar"] = (
                row * 60,
                (row + 1) * 60,
                opens[row],
                highs[row],
                99.0,
                opens[row],
                10.0,
                0.0,
                0.0,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_bar(s):
            if s.bar_close_ts == 60:
                s.submit_stop_buy_order(0, 81, 105.0, 0.0, 1.0, 3, False, False)

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] = s.fills()[0]["price"]
            s.state_i64[0] = s.now

        state_i64 = np.zeros(2, dtype=np.int64)
        state = run_event_bot(
            data_mode="bar",
            bars=bars,
            on_bar=on_bar,
            on_filled=on_filled,
            state_i64=state_i64,
        )
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 103.0)
        self.assertEqual(state_i64[0], 120)

    def test_tick_stop_market_uses_tick_trigger_and_tick_matcher(self):
        @njit
        def on_start(s):
            s.submit_stop_buy_order(0, 83, 101.0, 0.0, 1.0, 3, False, False)

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] = s.fills()[0]["price"]

        state = run_event_bot(make_bot(), on_start=on_start, on_filled=on_filled)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 101.0)

    def test_gtd_expires_at_deadline_without_waiting_for_market_data(self):
        bars = np.zeros(2, dtype=timed_bar_dtype)
        for row in range(2):
            bars[row]["asset_no"] = 0
            bars[row]["timeframe_ns"] = 60
            bars[row]["bar"] = (
                row * 120,
                row * 120 + 60,
                100.0,
                101.0,
                99.0,
                100.0,
                10.0,
                0.0,
                0.0,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_bar(s):
            if s.bar_close_ts == 60:
                s.submit_buy_order(0, 82, 50.0, 1.0, 0, 0, False, False, 90)

        @njit
        def on_order(s):
            if s.now == 90:
                s.state[0] += 1
                s.state_i64[0] = s.now

        state_i64 = np.zeros(2, dtype=np.int64)
        state = run_event_bot(
            data_mode="bar",
            bars=bars,
            on_bar=on_bar,
            on_order=on_order,
            state_i64=state_i64,
        )
        self.assertEqual(state[0], 1)
        self.assertEqual(state_i64[0], 90)

    def test_hybrid_merges_bar_signals_but_executes_only_on_tick_backend(self):
        bars = np.zeros(2, dtype=timed_bar_dtype)
        for row in range(2):
            bars[row]["asset_no"] = 0
            bars[row]["timeframe_ns"] = 100
            bars[row]["bar"] = (
                100 + row * 100,
                200 + row * 100,
                90.0,
                200.0,
                1.0,
                90.0,
                1000.0,
                0.0,
                0.0,
                1,
                BAR_COMPLETE,
            )

        @njit
        def on_tick(s):
            s.state_i64[0] += 1

        @njit
        def on_bar(s):
            s.state_i64[1] += 1
            if s.bar_close_ts == 200:
                s.submit_buy_order(0, 90, 0.0, 1.0, 3, 1, False)
            else:
                s.state[2] = s.open(0, 100)[-1]

        @njit
        def on_filled(s):
            s.state[0] += 1
            s.state[1] = s.fills()[0]["price"]

        state_i64 = np.zeros(3, dtype=np.int64)
        state = run_event_bot(
            make_bot(),
            data_mode="hybrid",
            bars=bars,
            frame_interval=100,
            on_tick=on_tick,
            on_bar=on_bar,
            on_filled=on_filled,
            state_i64=state_i64,
        )
        self.assertGreater(state_i64[0], 0)
        self.assertEqual(state_i64[1], 2)
        self.assertEqual(state[0], 1)
        self.assertEqual(state[1], 101.0)
        self.assertEqual(state[2], 90.0)

    def test_tick_batch_overflow_fails_instead_of_growing_unbounded(self):
        with self.assertRaises(RuntimeError):
            run_event_bot(make_bot(), frame_interval=1_000, max_tick_batch=1)


if __name__ == "__main__":
    unittest.main()
