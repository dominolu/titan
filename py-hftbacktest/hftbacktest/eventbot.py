"""Event-driven bot adapter: on_tick (frame mode) / on_bar, all native Numba callbacks.

The framework's native model is a pull-based ``while hbt.elapse(...)`` loop, which is the
fastest way to drive a strategy (the loop body is a Numba-compiled function calling the
jitclass bot via ctypes into the Rust core — no Python interpreter in the hot path).

This module adds an optional callback layer on top of the same loop, keeping it at the same
performance tier:

* ``on_tick`` — called once per *frame* (default 1 ms) with the frame's trades as a
  preallocated structured array. The handler is a ``@njit`` function, so dispatch is a
  native Numba-to-Numba call.
* ``on_bar`` — trade-bar aggregation (OHLCV); called natively when a bar closes.

Constraints (by design, to stay at HFT speed):

* Handlers must be ``@njit`` (nopython-compatible) functions; no Python closures.
* The trades array passed to ``on_tick`` is reused across frames — copy out anything you
  need to keep.
* Mutable strategy state lives in ``ctx`` (an extra argument passed to every callback).
  Pass any writable Numba object — a preallocated array or a ``@jitclass`` instance.
* Single asset (``asset_no = 0``) for now.
* Bars are trade bars: empty intervals produce no bar.
"""

import numpy as np
from numba import njit, uint64

from .types import event_dtype

# Max trades forwarded to on_tick in one callback. A frame with more trades is split into
# multiple on_tick calls so no trade is dropped.
MAX_FRAME_TRADES = 4096


@njit
def _noop_bar(_open_ts, _open, _high, _low, _close, _volume, _ctx):
    return


@njit
def _run_frame_loop(hbt, on_tick, on_bar, ctx, frame_interval, bar_interval):
    """Native event loop: drives ``hbt.elapse(frame_interval)``, aggregates each frame's
    trades, invokes ``on_tick(ts, trades, n)`` once per frame (or per chunk for very active
    frames), and invokes ``on_bar(open_ts, open, high, low, close, volume)`` when a
    trade bar closes.

    Returns True when the data ends (backtest) or the loop is otherwise terminated.
    """
    asset_no = 0
    # Trade-bar accumulator: open_ts, open, high, low, close, volume.
    bar = np.zeros(6, dtype=np.float64)
    bar_initialized = False
    # Reused across frames: on_tick must copy what it keeps.
    frame_trades = np.empty(MAX_FRAME_TRADES, dtype=event_dtype)

    while True:
        r = hbt.elapse(frame_interval)
        trades = hbt.last_trades(asset_no)
        n = len(trades)
        # 注意：elapse 在消费完最后一个事件的同时返回 EndOfData（r != 0），
        # 此时当前时间戳尚未推进，但最后一帧的成交已在缓冲里。用最后一笔成交的
        # 交易所时间作为本帧时间，避免最后一帧丢失或 bar 边界错位。
        ts = hbt.current_timestamp
        if n > 0:
            ts = trades[n - 1].exch_ts

        # Aggregate the whole frame into the trade bar, forwarding trades to on_tick in
        # chunks so nothing is dropped.
        off = 0
        while off < n:
            chunk = n - off
            if chunk > MAX_FRAME_TRADES:
                chunk = MAX_FRAME_TRADES
            for i in range(chunk):
                frame_trades[i] = trades[off + i]
                trade = trades[off + i]
                px = trade.px
                qty = trade.qty
                if not bar_initialized:
                    bar[0] = ts
                    bar[1] = px
                    bar[2] = px
                    bar[3] = px
                    bar[4] = px
                    bar[5] = 0.0
                    bar_initialized = True
                else:
                    if px > bar[2]:
                        bar[2] = px
                    if px < bar[3]:
                        bar[3] = px
                    bar[4] = px
                bar[5] += qty
            on_tick(ts, frame_trades, chunk, ctx)
            off += chunk

        if n == 0:
            # Time-driven frame: still notify on_tick (with an empty frame) so strategies
            # keep their cadence even when there is no trade activity.
            on_tick(ts, frame_trades, 0, ctx)

        if bar_initialized and ts - bar[0] >= bar_interval:
            on_bar(bar[0], bar[1], bar[2], bar[3], bar[4], bar[5], ctx)
            bar_initialized = False

        hbt.clear_last_trades(asset_no)
        if r != 0:
            break

    return True


def run_event_bot(
    hbt,
    on_tick,
    on_bar=None,
    ctx=0,
    frame_interval=1_000_000,
    bar_interval=1_000_000_000,
):
    """Runs a backtest or live bot with callback style.

    Args:
        hbt: The bot instance (``HashMapMarketDepthBacktest``,
            ``ROIVectorMarketDepthBacktest``, or a live bot).
        on_tick: ``@njit`` handler ``on_tick(ts, trades, n, ctx)``, called once per frame
            (default 1 ms). ``trades`` is a preallocated ``event_dtype`` array reused
            across calls; ``n`` is the number of trades in this frame/chunk.
        on_bar: Optional ``@njit`` handler ``on_bar(open_ts, open, high, low, close,
            volume, ctx)``, called natively when a trade bar closes.
        ctx: Mutable strategy state forwarded to every callback. Any writable Numba
            object (preallocated array or ``@jitclass`` instance).
        frame_interval: Frame duration in nanoseconds (default 1 ms).
        bar_interval: Trade-bar duration in nanoseconds (default 1 s).

    Returns:
        True when the loop reaches the end of data / terminates normally.
    """
    from numba.core.dispatcher import Dispatcher

    if not isinstance(on_tick, Dispatcher):
        raise TypeError("on_tick must be a @njit function")
    if on_bar is not None and not isinstance(on_bar, Dispatcher):
        raise TypeError("on_bar must be a @njit function")
    on_bar = on_bar if on_bar is not None else _noop_bar

    return _run_frame_loop(
        hbt,
        on_tick,
        on_bar,
        ctx,
        uint64(frame_interval),
        uint64(bar_interval),
    )
