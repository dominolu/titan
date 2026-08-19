"""Event-driven bot adapter: on_tick (frame mode) / on_bar, all native Numba callbacks.

The framework's native model is a pull-based ``while hbt.elapse(...)`` loop, which is the
fastest way to drive a strategy (the loop body is a Numba-compiled function calling the
jitclass bot via ctypes into the Rust core — no Python interpreter in the hot path).

This module adds an optional callback layer on top of the same loop, keeping it at the same
performance tier:

* ``on_tick`` — called once per *frame* (default 1 ms). The handler is a ``@njit``
  function ``on_tick(ctx)`` receiving the strategy's global context; dispatch is a
  native Numba-to-Numba call.
* ``on_bar`` — trade-bar aggregation (OHLCV); ``on_bar(ctx)`` is called natively when a
  bar closes.

Constraints (by design, to stay at HFT speed):

* Handlers must be ``@njit`` (nopython-compatible) functions; no Python closures.
* Every strategy owns a **global context**: a ``@jitclass`` instance that persists for the
  whole run and is passed to every callback. It carries the strategy's own state fields
  plus the frame data the framework writes each frame.
* Compose the context from :data:`FRAME_CTX_FIELDS` plus your own fields; the framework
  fills ``ts``/``n``/``trades``/``bar_open_ts``/``o``/``h``/``l``/``c``/``v`` every frame.
* The context's ``trades`` array is reused across frames — copy out anything you need to
  keep.
* Single asset (``asset_no = 0``) for now.
* Bars are trade bars: empty intervals produce no bar.
"""

import numba
import numpy as np
from numba import float64, int64, njit, uint64
from numba.experimental import jitclass

from .types import event_dtype

# Max trades forwarded to on_tick in one callback. A frame with more trades is split into
# multiple on_tick calls so no trade is dropped.
MAX_FRAME_TRADES = 4096


@njit
def _noop_bar(_ctx):
    return


# Framework-managed fields of the strategy context. Compose your own context by
# extending this spec with your strategy fields:
#
#   @jitclass(FRAME_CTX_FIELDS + [("my_pos", float64), ("orders", int64[:])])
#   class MyCtx:
#       def __init__(self, orders):
#           init_frame_fields(self)
#           self.my_pos = 0.0
#           self.orders = orders
#
# The engine overwrites ``ctx.trades`` with its own preallocated buffer at run start.
FRAME_CTX_FIELDS = [
    ("ts", int64),
    ("n", int64),
    ("trades", numba.from_dtype(event_dtype)[:]),
    ("bar_open_ts", int64),
    ("o", float64),
    ("h", float64),
    ("l", float64),
    ("c", float64),
    ("v", float64),
]


@njit
def init_frame_fields(ctx):
    """Initializes the framework-managed fields of a strategy context."""
    ctx.ts = 0
    ctx.n = 0
    ctx.trades = np.empty(1, dtype=event_dtype)
    ctx.bar_open_ts = 0
    ctx.o = 0.0
    ctx.h = 0.0
    ctx.l = 0.0
    ctx.c = 0.0
    ctx.v = 0.0


@jitclass(FRAME_CTX_FIELDS + [("state", float64[:])])
class EventBotContext:
    """Default strategy context: frame/bar data + a generic ``state`` array.

    Fields:
        ts: Current frame timestamp (nanoseconds; the last trade's exchange timestamp
            when the frame has trades).
        n: Number of trades in this frame/chunk.
        trades: Preallocated ``event_dtype`` array; only ``trades[0..n)`` is valid.
            Reused across frames — copy out what you need to keep.
        bar_open_ts, o, h, l, c, v: The accumulating trade bar (OHLCV). When
            ``on_bar`` fires, these hold the closed bar.
        state: Preallocated ``float64`` array for strategy state; persists across
            frames and callbacks.
    """

    def __init__(self, state, trades):
        init_frame_fields(self)
        self.trades = trades
        self.state = state


@njit
def _run_frame_loop(hbt, on_tick, on_bar, ctx, frame_interval, bar_interval):
    """Native event loop: drives ``hbt.elapse(frame_interval)``, aggregates each frame's
    trades into ``ctx``, invokes ``on_tick(ctx)`` once per frame (or per chunk for very
    active frames), and invokes ``on_bar(ctx)`` when a trade bar closes.

    Returns True when the data ends (backtest) or the loop is otherwise terminated.
    """
    asset_no = 0
    bar_initialized = False
    # Reused across frames: the engine owns the buffer, the context only references it.
    ctx.trades = np.empty(MAX_FRAME_TRADES, dtype=event_dtype)

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
        ctx.ts = ts

        # Aggregate the whole frame into the trade bar, forwarding trades to on_tick in
        # chunks so nothing is dropped.
        off = 0
        while off < n:
            chunk = n - off
            if chunk > MAX_FRAME_TRADES:
                chunk = MAX_FRAME_TRADES
            for i in range(chunk):
                ctx.trades[i] = trades[off + i]
                trade = trades[off + i]
                px = trade.px
                qty = trade.qty
                if not bar_initialized:
                    ctx.bar_open_ts = ts
                    ctx.o = px
                    ctx.h = px
                    ctx.l = px
                    ctx.c = px
                    ctx.v = 0.0
                    bar_initialized = True
                else:
                    if px > ctx.h:
                        ctx.h = px
                    if px < ctx.l:
                        ctx.l = px
                    ctx.c = px
                ctx.v += qty
            ctx.n = chunk
            on_tick(ctx)
            off += chunk

        if n == 0:
            # Time-driven frame: still notify on_tick (with an empty frame) so strategies
            # keep their cadence even when there is no trade activity.
            ctx.n = 0
            on_tick(ctx)

        if bar_initialized and ts - ctx.bar_open_ts >= bar_interval:
            on_bar(ctx)
            bar_initialized = False

        hbt.clear_last_trades(asset_no)
        if r != 0:
            break

    return True


def run_event_bot(
    hbt,
    on_tick,
    on_bar=None,
    ctx=None,
    state=None,
    frame_interval=1_000_000,
    bar_interval=1_000_000_000,
):
    """Runs a backtest or live bot with callback style.

    Args:
        hbt: The bot instance (``HashMapMarketDepthBacktest``,
            ``ROIVectorMarketDepthBacktest``, or a live bot).
        on_tick: ``@njit`` handler ``on_tick(ctx)``, called once per frame (default
            1 ms). Read the frame from the context: ``ctx.ts``, ``ctx.n``,
            ``ctx.trades[0..n)``.
        on_bar: Optional ``@njit`` handler ``on_bar(ctx)``, called natively when a
            trade bar closes. Read ``ctx.bar_open_ts``, ``ctx.o/h/l/c/v``.
        ctx: The strategy's global context (any ``@jitclass`` instance composed from
            :data:`FRAME_CTX_FIELDS` plus your own state fields). It persists for the
            whole run and is passed to every callback. Defaults to an
            :class:`EventBotContext` (frame fields + a generic ``state`` array).
        state: Preallocated ``float64`` array for strategy state (persists across
            callbacks, exposed as ``ctx.state``). Only used when ``ctx`` is not given;
            defaults to a 64-element array.
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
    if ctx is None:
        if state is None:
            state = np.zeros(64)
        ctx = EventBotContext(state, np.empty(1, dtype=event_dtype))

    return _run_frame_loop(
        hbt,
        on_tick,
        on_bar,
        ctx,
        uint64(frame_interval),
        uint64(bar_interval),
    )
