"""Long-only dual moving-average strategy for the Rust-owned Bar runtime."""

from dataclasses import dataclass

import numpy as np
from numba import njit
from ta_numba.trend import sma_numba


@dataclass(frozen=True, slots=True)
class DualMaStrategy:
    """Cold-path bundle passed directly to :func:`run_event_bot`.

    ``state`` layout: short MA, long MA, golden crosses, death crosses,
    cumulative filled quantity, last fill price.

    ``state_i64`` layout: next order ID, submission errors, buy submissions,
    sell submissions, current Bar cursor.
    """

    on_bar: object
    on_filled: object
    on_stop: object
    state: np.ndarray
    state_i64: np.ndarray
    short_period: int
    long_period: int
    timeframe_ns: int
    asset_no: int
    quantity: float
    short_ma: np.ndarray
    long_ma: np.ndarray


def init(closes, short_period, long_period):
    """Precomputes aligned SMA arrays once during backtest initialization."""

    closes = np.ascontiguousarray(closes, dtype=np.float64)
    if closes.ndim != 1:
        raise ValueError("closes must be a one-dimensional array")
    if len(closes) == 0:
        raise ValueError("closes cannot be empty")
    if not np.all(np.isfinite(closes)):
        raise ValueError("closes must contain only finite values")
    short_ma = sma_numba(closes, short_period, short_period)
    long_ma = sma_numba(closes, long_period, long_period)
    return (
        np.ascontiguousarray(short_ma, dtype=np.float64),
        np.ascontiguousarray(long_ma, dtype=np.float64),
    )


def create_dual_ma_strategy(
    *,
    closes,
    short_period=20,
    long_period=50,
    timeframe_ns=60_000_000_000,
    asset_no=0,
    quantity=1.0,
):
    """Creates a precomputed Numba ``on_bar(s)`` backtest strategy.

    ``init`` calculates the complete aligned SMA arrays once. ``on_bar`` advances a
    monotonic cursor and can read only the previous/current entries used by its signal.
    A golden cross targets ``quantity`` long; a death cross closes an existing long
    position. Orders use conservative NextOpen market execution.
    """

    short_period = int(short_period)
    long_period = int(long_period)
    timeframe_ns = int(timeframe_ns)
    asset_no = int(asset_no)
    quantity = float(quantity)
    if short_period <= 0:
        raise ValueError("short_period must be positive")
    if long_period <= short_period:
        raise ValueError("long_period must be greater than short_period")
    if timeframe_ns <= 0:
        raise ValueError("timeframe_ns must be positive")
    if asset_no < 0:
        raise ValueError("asset_no cannot be negative")
    if not np.isfinite(quantity) or quantity <= 0:
        raise ValueError("quantity must be finite and positive")
    short_ma, long_ma = init(closes, short_period, long_period)

    @njit
    def on_bar(s):
        if s.bar_timeframe != timeframe_ns:
            return

        current_close = np.nan
        for item in s.bars():
            if item["asset_no"] == asset_no:
                current_close = item["bar"]["close"]
                break
        if not np.isfinite(current_close):
            return

        bar_index = s.state_i64[4]
        if bar_index >= len(short_ma):
            s.state_i64[1] += 1
            s.stop()
            return
        s.state_i64[4] = bar_index + 1

        if bar_index < long_period:
            return
        previous_short = short_ma[bar_index - 1]
        current_short = short_ma[bar_index]
        previous_long = long_ma[bar_index - 1]
        current_long = long_ma[bar_index]
        if not np.isfinite(current_short) or not np.isfinite(current_long):
            s.state_i64[1] += 1
            return
        s.state[0] = current_short
        s.state[1] = current_long

        golden_cross = previous_short <= previous_long and current_short > current_long
        death_cross = previous_short >= previous_long and current_short < current_long
        if golden_cross:
            s.state[2] += 1.0
            position = s.position(asset_no)
            buy_qty = quantity - position
            if buy_qty > 0.0:
                order_id = s.state_i64[0]
                s.state_i64[0] += 1
                result = s.submit_buy_order(
                    asset_no, order_id, current_close, buy_qty, 0, 1, False
                )
                if result == 0:
                    s.state_i64[2] += 1
                else:
                    s.state_i64[1] += 1
        elif death_cross:
            s.state[3] += 1.0
            position = s.position(asset_no)
            if position > 0.0:
                order_id = s.state_i64[0]
                s.state_i64[0] += 1
                result = s.submit_sell_order(
                    asset_no, order_id, current_close, position, 0, 1, False
                )
                if result == 0:
                    s.state_i64[3] += 1
                else:
                    s.state_i64[1] += 1

    @njit
    def on_filled(s):
        for fill in s.fills():
            if fill["asset_no"] == asset_no:
                s.state[4] += fill["qty"]
                s.state[5] = fill["price"]

    @njit
    def on_stop(s):
        s.state[6] = s.position(asset_no)

    state = np.zeros(8, dtype=np.float64)
    state_i64 = np.zeros(8, dtype=np.int64)
    state_i64[0] = 1
    return DualMaStrategy(
        on_bar=on_bar,
        on_filled=on_filled,
        on_stop=on_stop,
        state=state,
        state_i64=state_i64,
        short_period=short_period,
        long_period=long_period,
        timeframe_ns=timeframe_ns,
        asset_no=asset_no,
        quantity=quantity,
        short_ma=short_ma,
        long_ma=long_ma,
    )
