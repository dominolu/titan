"""Event-driven bot adapter with a Rust-defined two-level strategy context.

The context is a Rust ``#[repr(C)]`` structure (see ``py-hftbacktest/src/ctx.rs``) laid out
as **market → instrument**:

* ``StrategyCtx``: global frame clock, market array pointer, global strategy state.
* ``MarketCtx``: one venue (exchange/connector), its instruments and venue state.
* ``InstrumentCtx``: per-instrument frame snapshot (BBO, trades view, frame aggregates,
  trade bar) and per-instrument strategy state.

Dispatch is **global only**: ``on_tick(s)`` fires once per frame, ``on_bar(s)`` once per
global bar boundary; ``s`` is a ``Strategy`` object combining the bot (order operations)
with the two-level context (market data + state). This keeps a single calling convention
for single-market/single-instrument, single-market/multi-instrument and multi-market
strategies — the two-level structure makes cross-market vs same-market explicit in code.

Every frame the loop calls a Rust fill function (``fill_strategy_ctx``) once per asset to
refresh the snapshot fields directly from the bot — zero-copy, no Python in the hot path.
"""

from ctypes import CDLL, POINTER, byref, c_int32, c_size_t, c_uint64, c_void_p

import numba
import numpy as np
from numba import carray, float64, int32, int64, njit, uint64
from numba.experimental import jitclass

from . import _hftbacktest
from .intrinsic import address_as_void_pointer
from .types import event_dtype

STATE_SIZE = 64

# Bot kinds for fill_strategy_ctx dispatch (mirrors py-hftbacktest/src/ctx.rs).
BOT_KIND_HASHMAP_BT = 0
BOT_KIND_ROIVEC_BT = 1
BOT_KIND_HASHMAP_LIVE = 2
BOT_KIND_ROIVEC_LIVE = 3

# ---------------------------------------------------------------------------
# Rust-defined layouts, mirrored as structured dtypes (aligned like repr(C)).
# ---------------------------------------------------------------------------

instrument_ctx_dtype = np.dtype(
    [
        ("frame_ts", "i8"),
        ("exch_ts", "i8"),
        ("n", "i8"),
        ("trades_ptr", "u8"),
        ("last_px", "f8"),
        ("last_qty", "f8"),
        ("bid", "f8"),
        ("bid_qty", "f8"),
        ("ask", "f8"),
        ("ask_qty", "f8"),
        ("mid", "f8"),
        ("spread", "f8"),
        ("frame_volume", "f8"),
        ("frame_buy_vol", "f8"),
        ("frame_sell_vol", "f8"),
        ("frame_vwap", "f8"),
        ("bar_open_ts", "i8"),
        ("bar_o", "f8"),
        ("bar_h", "f8"),
        ("bar_l", "f8"),
        ("bar_c", "f8"),
        ("bar_v", "f8"),
        ("bar_inited", "i8"),
        ("position", "f8"),
        ("symbol_id", "i8"),
        ("tick_size", "f8"),
        ("lot_size", "f8"),
        ("state_ptr", "u8"),
    ],
    align=True,
)

market_ctx_dtype = np.dtype(
    [
        ("market_id", "i8"),
        ("n_instruments", "i8"),
        ("instruments_ptr", "u8"),
        ("market_state_ptr", "u8"),
    ],
    align=True,
)

strategy_ctx_dtype = np.dtype(
    [
        ("frame_ts", "i8"),
        ("n_markets", "i8"),
        ("markets_ptr", "u8"),
        ("next_bar_ts", "i8"),
        ("state_global_ptr", "u8"),
    ],
    align=True,
)


_lib = CDLL(_hftbacktest.__file__)
_fill_strategy_ctx = _lib.fill_strategy_ctx
_fill_strategy_ctx.restype = None
_fill_strategy_ctx.argtypes = [c_void_p, c_int32, c_uint64, c_uint64]

_strategy_ctx_layout = _lib.strategy_ctx_layout
_strategy_ctx_layout.restype = None
_strategy_ctx_layout.argtypes = [
    POINTER(c_size_t),
    POINTER(c_size_t),
    POINTER(c_size_t),
]


def _check_layout():
    """Verifies the Python dtype mirrors exactly match the Rust structs."""
    instr = c_size_t()
    market = c_size_t()
    strategy = c_size_t()
    _strategy_ctx_layout(byref(instr), byref(market), byref(strategy))
    assert instr.value == instrument_ctx_dtype.itemsize, (
        f"InstrumentCtx layout mismatch: Rust={instr.value} Python={instrument_ctx_dtype.itemsize}"
    )
    assert market.value == market_ctx_dtype.itemsize, (
        f"MarketCtx layout mismatch: Rust={market.value} Python={market_ctx_dtype.itemsize}"
    )
    assert strategy.value == strategy_ctx_dtype.itemsize, (
        f"StrategyCtx layout mismatch: Rust={strategy.value} Python={strategy_ctx_dtype.itemsize}"
    )


_check_layout()


@njit
def _noop_bar(_s):
    return


def _bot_kind(hbt):
    name = type(hbt).__name__
    live = "Live" in name
    roivec = "ROIVector" in name
    if roivec:
        return BOT_KIND_ROIVEC_LIVE if live else BOT_KIND_ROIVEC_BT
    return BOT_KIND_HASHMAP_LIVE if live else BOT_KIND_HASHMAP_BT


_strategy_classes = {}


def _make_strategy_class(hbt):
    """Builds (and caches) the Strategy jitclass for a concrete bot type."""
    key = id(type(hbt))
    if key in _strategy_classes:
        return _strategy_classes[key]

    bot_type = numba.typeof(hbt)

    @jitclass(
        [
            ("hbt", bot_type),
            ("ctx_arr", numba.from_dtype(strategy_ctx_dtype)[:]),
            ("markets_arr", numba.from_dtype(market_ctx_dtype)[:]),
            ("instr_flat", numba.from_dtype(instrument_ctx_dtype)[:]),
            ("asset_addrs", uint64[:]),
            ("n_assets", int64),
        ]
    )
    class Strategy:
        def __init__(self, hbt, ctx_arr, markets_arr, instr_flat, asset_addrs, n_assets):
            self.hbt = hbt
            self.ctx_arr = ctx_arr
            self.markets_arr = markets_arr
            self.instr_flat = instr_flat
            self.asset_addrs = asset_addrs
            self.n_assets = n_assets

        @property
        def frame_ts(self):
            return self.ctx_arr[0]["frame_ts"]

        @property
        def global_state(self):
            return carray(
                address_as_void_pointer(self.ctx_arr[0]["state_global_ptr"]),
                STATE_SIZE,
                float64,
            )

        def market_state(self, m):
            return carray(
                address_as_void_pointer(self.markets_arr[m]["market_state_ptr"]),
                STATE_SIZE,
                float64,
            )

        def instrument_state(self, m, i):
            return carray(
                address_as_void_pointer(self.instrument(m, i)["state_ptr"]),
                STATE_SIZE,
                float64,
            )

        @property
        def num_assets(self):
            return self.n_assets

        def n_markets(self):
            return self.ctx_arr[0]["n_markets"]

        def n_instruments(self, m):
            return self.markets_arr[m]["n_instruments"]

        def instruments(self, m):
            """Contiguous view of market m's instruments."""
            return carray(
                address_as_void_pointer(self.markets_arr[m]["instruments_ptr"]),
                self.markets_arr[m]["n_instruments"],
                instrument_ctx_dtype,
            )

        def instrument(self, m, i):
            return self.instruments(m)[i]

        def trades(self, m, i):
            """Zero-copy view of the current frame's trades for instrument (m, i)."""
            c = self.instrument(m, i)
            return carray(address_as_void_pointer(c["trades_ptr"]), c["n"], event_dtype)

        # ---- 订单/账户操作（委托 hbt） ----
        def submit_buy_order(self, asset_no, order_id, price, qty, time_in_force, order_type, wait):
            return self.hbt.submit_buy_order(
                asset_no, order_id, price, qty, time_in_force, order_type, wait
            )

        def submit_sell_order(self, asset_no, order_id, price, qty, time_in_force, order_type, wait):
            return self.hbt.submit_sell_order(
                asset_no, order_id, price, qty, time_in_force, order_type, wait
            )

        def cancel(self, asset_no, order_id, wait):
            return self.hbt.cancel(asset_no, order_id, wait)

        def orders(self, asset_no):
            return self.hbt.orders(asset_no)

        def clear_inactive_orders(self, asset_no):
            self.hbt.clear_inactive_orders(asset_no)

        def wait_order_response(self, asset_no, order_id, timeout):
            return self.hbt.wait_order_response(asset_no, order_id, timeout)

        def depth(self, asset_no):
            return self.hbt.depth(asset_no)

        def position(self, asset_no):
            return self.hbt.position(asset_no)

    _strategy_classes[key] = Strategy
    return Strategy


@njit
def _run_global_loop(s, on_tick, on_bar, fill, frame_interval, bar_interval, kind):
    """Global-frame event loop.

    Each frame:
      1. elapse the whole bot (all assets advance on the same clock),
      2. refresh every instrument snapshot via the Rust fill function,
      3. call ``on_tick(s)`` once,
      4. close global bars and call ``on_bar(s)`` when the boundary is crossed,
      5. clear the per-asset trade buffers.
    """
    hbt = s.hbt
    n = s.n_assets
    while True:
        r = hbt.elapse(frame_interval)

        # 1) Rust 填充每个资产的快照（零拷贝，含 trades_ptr 视图与 bar 累计）
        for a in range(n):
            fill(hbt.ptr, int32(kind), uint64(a), s.asset_addrs[a])

        ts = hbt.current_timestamp
        s.ctx_arr[0]["frame_ts"] = ts

        # 2) on_tick：全局帧，一帧一次
        on_tick(s)

        # 3) on_bar：按全局帧时钟对齐的 bar 边界
        if s.ctx_arr[0]["next_bar_ts"] == 0:
            s.ctx_arr[0]["next_bar_ts"] = ts + bar_interval
        if ts >= s.ctx_arr[0]["next_bar_ts"]:
            on_bar(s)
            for m in range(s.n_markets()):
                insts = s.instruments(m)
                for i in range(s.n_instruments(m)):
                    insts[i]["bar_open_ts"] = 0
                    insts[i]["bar_o"] = 0.0
                    insts[i]["bar_h"] = 0.0
                    insts[i]["bar_l"] = 0.0
                    insts[i]["bar_c"] = 0.0
                    insts[i]["bar_v"] = 0.0
                    insts[i]["bar_inited"] = 0
            while s.ctx_arr[0]["next_bar_ts"] <= ts:
                s.ctx_arr[0]["next_bar_ts"] += bar_interval

        # 4) 清理成交缓冲
        for a in range(n):
            hbt.clear_last_trades(a)

        if r != 0:
            break
    return True


def run_event_bot(
    hbt,
    on_tick,
    on_bar=None,
    markets=None,
    symbol_ids=None,
    frame_interval=1_000_000,
    bar_interval=1_000_000_000,
    fill=None,
    state_global=None,
    market_states=None,
    instrument_states=None,
):
    """Runs a backtest or live bot with global-frame callbacks.

    Args:
        hbt: The bot instance (backtest or live).
        on_tick: ``@njit`` handler ``on_tick(s)`` called once per frame.
        on_bar: Optional ``@njit`` handler ``on_bar(s)`` called at global bar boundaries.
        markets: Grouping of assets into venues, e.g. ``[[0, 1], [2]]`` means market 0
            holds assets 0 and 1, market 1 holds asset 2. Defaults to a single market
            with all assets. Must partition all assets exactly once.
        symbol_ids: Optional per-asset symbol ids (defaults to asset numbers).
        frame_interval: Frame duration in nanoseconds (default 1 ms).
        bar_interval: Global bar duration in nanoseconds (default 1 s).
        fill: Snapshot fill function ``fill(hbt_ptr, kind, asset_no, ctx_addr)``.
            Defaults to the Rust ``fill_strategy_ctx``; injectable for tests.
        state_global: Optional ``float64[64]`` global strategy state (persists across
            runs, exposed as ``s.global_state``).
        market_states: Optional ``float64[n_markets, 64]`` per-market state.
        instrument_states: Optional ``float64[n_assets, 64]`` per-instrument state.

    Returns:
        True when the loop terminates normally (end of data / end of run).
    """
    from numba.core.dispatcher import Dispatcher

    if not isinstance(on_tick, Dispatcher):
        raise TypeError("on_tick must be a @njit function")
    if on_bar is not None and not isinstance(on_bar, Dispatcher):
        raise TypeError("on_bar must be a @njit function")
    on_bar = on_bar if on_bar is not None else _noop_bar

    n_assets = int(hbt.num_assets)
    if markets is None:
        markets = [list(range(n_assets))]
    flat = [a for m in markets for a in m]
    if sorted(flat) != list(range(n_assets)):
        raise ValueError("markets must partition all assets 0..n_assets-1 exactly once")
    if symbol_ids is None:
        symbol_ids = list(range(n_assets))
    if len(symbol_ids) != n_assets:
        raise ValueError("symbol_ids must have one id per asset")

    kind = _bot_kind(hbt)
    n_markets = len(markets)

    # ---- 分配两级 ctx（布局与 Rust 结构一致）----
    ctx_arr = np.zeros(1, dtype=strategy_ctx_dtype)
    markets_arr = np.zeros(n_markets, dtype=market_ctx_dtype)
    instr_flat = np.zeros(n_assets, dtype=instrument_ctx_dtype)

    market_offsets = []
    asset_to_flat = {}
    flat_order = []
    for assets in markets:
        market_offsets.append(len(flat_order))
        for a in assets:
            asset_to_flat[a] = len(flat_order)
            flat_order.append(a)

    base = instr_flat.ctypes.data
    itemsize = instrument_ctx_dtype.itemsize
    asset_addrs = np.zeros(n_assets, dtype=np.uint64)
    if state_global is None:
        state_global = np.zeros(STATE_SIZE)
    if market_states is None:
        market_states = np.zeros((n_markets, STATE_SIZE))
    if instrument_states is None:
        instrument_states = np.zeros((n_assets, STATE_SIZE))
    if state_global.shape != (STATE_SIZE,):
        raise ValueError("state_global must have shape (64,)")
    if market_states.shape != (n_markets, STATE_SIZE):
        raise ValueError("market_states must have shape (n_markets, 64)")
    if instrument_states.shape != (n_assets, STATE_SIZE):
        raise ValueError("instrument_states must have shape (n_assets, 64)")
    for a in range(n_assets):
        fi = asset_to_flat[a]
        asset_addrs[a] = base + fi * itemsize
        instr_flat[fi]["symbol_id"] = symbol_ids[a]
        instr_flat[fi]["state_ptr"] = instrument_states[a].ctypes.data
    for m in range(n_markets):
        markets_arr[m]["market_id"] = m
        markets_arr[m]["n_instruments"] = len(markets[m])
        markets_arr[m]["instruments_ptr"] = base + market_offsets[m] * itemsize
        markets_arr[m]["market_state_ptr"] = market_states[m].ctypes.data
    ctx_arr[0]["n_markets"] = n_markets
    ctx_arr[0]["markets_ptr"] = markets_arr.ctypes.data
    ctx_arr[0]["state_global_ptr"] = state_global.ctypes.data

    Strategy = _make_strategy_class(hbt)
    s = Strategy(hbt, ctx_arr, markets_arr, instr_flat, asset_addrs, n_assets)
    return _run_global_loop(
        s,
        on_tick,
        on_bar,
        fill if fill is not None else _fill_strategy_ctx,
        uint64(frame_interval),
        uint64(bar_interval),
        kind,
    )
