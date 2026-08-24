"""Rust-owned event runtime with single-argument Numba callbacks.

Rust owns the event queue, clock, market state and matching. Python performs cold-path
configuration only. Each user handler is ``@njit`` and has exactly one argument ``s``;
a Numba ``cfunc`` bridge lets Rust invoke it without returning to the Python interpreter.

The implementation supports the Rust-owned global TickBatch loop for backtests and live
bots, event-jump materialized Bar backtests, and deterministic Bar-signal/Tick-execution Hybrid.
"""

import inspect
from ctypes import CDLL, POINTER, c_double, c_int64, c_size_t, c_uint32, c_uint64, c_void_p

import numba
import numpy as np
from numba import carray, cfunc, float64, int64, njit, types
from numba.experimental import jitclass

from . import _hftbacktest
from .intrinsic import address_as_void_pointer
from .types import event_dtype

ABI_VERSION = 8
EVENT_SLOT_COUNT = 32

EVENT_START = 0
EVENT_ORDER = 1
EVENT_FILLED = 2
EVENT_POSITION = 3
EVENT_FUNDING = 4
EVENT_BAR = 5
EVENT_TICK = 6
EVENT_TIMER = 7
EVENT_ERROR = 8
EVENT_STOP = 9

BAR_COMPLETE = 1 << 0
BAR_EMPTY = 1 << 1
BAR_SYNTHETIC = 1 << 2
BAR_NATIVE = 1 << 3
BAR_PARTIAL = 1 << 4


bar_dtype = np.dtype(
    [
        ("open_ts", "i8"),
        ("close_ts", "i8"),
        ("open", "f8"),
        ("high", "f8"),
        ("low", "f8"),
        ("close", "f8"),
        ("volume", "f8"),
        ("quote_volume", "f8"),
        ("buy_volume", "f8"),
        ("trade_count", "u8"),
        ("flags", "u8"),
    ],
    align=True,
)

# Event is repr(C, align(64)); TickItem therefore places it at offset 64.
tick_item_dtype = np.dtype(
    {
        "names": ["asset_no", "event"],
        "formats": ["u8", event_dtype],
        "offsets": [0, 64],
        "itemsize": 128,
    },
    align=True,
)

bar_item_dtype = np.dtype(
    [("asset_no", "u8"), ("bar", bar_dtype)],
    align=True,
)

timed_bar_dtype = np.dtype(
    [("asset_no", "u8"), ("timeframe_ns", "i8"), ("bar", bar_dtype)],
    align=True,
)

timer_dtype = np.dtype(
    [("deadline_ts", "i8"), ("owner_id", "u8"), ("timer_id", "u8")], align=True
)

funding_dtype = np.dtype(
    [
        ("event_id", "u8"),
        ("asset_no", "u4"),
        ("venue_no", "u4"),
        ("instrument_id", "u4"),
        ("currency", "u4"),
        ("price_source", "u4"),
        ("position_snapshot", "u1"),
        ("formula", "u1"),
        ("rounding_mode", "u1"),
        ("boundary", "u1"),
        ("publication_ts", "i8"),
        ("effective_ts", "i8"),
        ("settlement_ts", "i8"),
        ("delivery_ts", "i8"),
        ("rate", "f8"),
        ("mark_price", "f8"),
        ("position_qty", "f8"),
        ("amount", "f8"),
        ("rounding_increment", "f8"),
    ],
    align=True,
)

bar_history_view_dtype = np.dtype(
    [
        ("asset_no", "u8"),
        ("timeframe_ns", "i8"),
        ("bars_ptr", "u8"),
        ("capacity", "u8"),
        ("len", "u8"),
        ("next", "u8"),
    ],
    align=True,
)

fill_dtype = np.dtype(
    [
        ("asset_no", "u8"),
        ("order_id", "u8"),
        ("venue_order_id", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("sequence", "u8"),
        ("price", "f8"),
        ("qty", "f8"),
        ("venue_no", "u4"),
        ("instrument_id", "u4"),
        ("reason", "u4"),
        ("side", "i1"),
        ("maker", "u1"),
        ("_reserved", "u1", (2,)),
    ],
    align=True,
)

order_event_dtype = np.dtype(
    [
        ("asset_no", "u8"),
        ("order_id", "u8"),
        ("venue_order_id", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("sequence", "u8"),
        ("price", "f8"),
        ("qty", "f8"),
        ("exec_price", "f8"),
        ("exec_qty", "f8"),
        ("venue_no", "u4"),
        ("instrument_id", "u4"),
        ("reason", "u4"),
        ("side", "i1"),
        ("status", "u1"),
        ("request", "u1"),
        ("maker", "u1"),
        ("_reserved", "u1", (4,)),
    ],
    align=True,
)

market_state_dtype = np.dtype(
    [
        ("best_bid", "f8"),
        ("best_ask", "f8"),
        ("best_bid_qty", "f8"),
        ("best_ask_qty", "f8"),
        ("tick_size", "f8"),
        ("lot_size", "f8"),
    ],
    align=True,
)

order_command_dtype = np.dtype(
    [
        ("kind", "u1"),
        ("side", "i1"),
        ("time_in_force", "u1"),
        ("order_type", "u1"),
        ("_reserved", "u1", (4,)),
        ("asset_no", "u8"),
        ("order_id", "u8"),
        ("price", "f8"),
        ("qty", "f8"),
        ("trigger_price", "f8"),
        ("gtd_expiry_ts", "i8"),
    ],
    align=True,
)

_ctx_fields = [
    ("abi_version", "u4"),
    ("struct_size", "u4"),
    ("event_kind", "u4"),
    ("stop_requested", "u4"),
    ("now", "i8"),
    ("generation", "u8"),
    ("user_data", "u8"),
    ("bot_ptr", "u8"),
    ("ticks_ptr", "u8"),
    ("num_ticks", "u8"),
    ("bars_ptr", "u8"),
    ("num_bars", "u8"),
    ("bar_timeframe_ns", "i8"),
    ("bar_close_ts", "i8"),
    ("fills_ptr", "u8"),
    ("num_fills", "u8"),
    ("orders_ptr", "u8"),
    ("num_orders", "u8"),
    ("histories_ptr", "u8"),
    ("num_histories", "u8"),
    ("payload_ptr", "u8"),
    ("payload_len", "u8"),
    ("state_f64_ptr", "u8"),
    ("state_f64_len", "u8"),
    ("state_i64_ptr", "u8"),
    ("state_i64_len", "u8"),
    ("commands_ptr", "u8"),
    ("num_commands", "u8"),
    ("command_capacity", "u8"),
    ("positions_ptr", "u8"),
    ("num_positions", "u8"),
    ("markets_ptr", "u8"),
    ("num_markets", "u8"),
    ("last_error", "i8"),
]
runtime_ctx_dtype = np.dtype(_ctx_fields, align=True)


_lib = CDLL(_hftbacktest.__file__)
_runtime_layout = _lib.strategy_runtime_layout
_runtime_layout.restype = None
_runtime_layout.argtypes = [POINTER(c_size_t), POINTER(c_size_t)]


def _check_layout():
    if np.dtype(np.uintp).itemsize != 8:
        raise RuntimeError("the strategy runtime currently requires a 64-bit process")
    sizes = (c_size_t * 10)()
    offsets = (c_size_t * 40)()
    _runtime_layout(sizes, offsets)
    expected_sizes = [
        runtime_ctx_dtype.itemsize,
        tick_item_dtype.itemsize,
        bar_dtype.itemsize,
        bar_item_dtype.itemsize,
        fill_dtype.itemsize,
        timed_bar_dtype.itemsize,
        bar_history_view_dtype.itemsize,
        order_command_dtype.itemsize,
        order_event_dtype.itemsize,
        market_state_dtype.itemsize,
    ]
    if list(sizes) != expected_sizes:
        raise RuntimeError(
            f"Rust/NumPy runtime layout mismatch: Rust={list(sizes)} Python={expected_sizes}"
        )
    python_offsets = [runtime_ctx_dtype.fields[name][1] for name, _ in _ctx_fields]
    if list(offsets)[: len(python_offsets)] != python_offsets:
        raise RuntimeError(
            "Rust/NumPy StrategyRuntimeContext field-offset mismatch: "
            f"Rust={list(offsets)[:len(python_offsets)]} Python={python_offsets}"
        )


_check_layout()


@jitclass(
    [
        ("bars_ptr", numba.uint64),
        ("capacity", int64),
        ("length", int64),
        ("next", int64),
        ("field", int64),
    ]
)
class BarSeries:
    """Zero-copy Python-style view over one field in a closed-bar history ring."""

    def __init__(self, bars_ptr, capacity, length, next_index, field):
        self.bars_ptr = bars_ptr
        self.capacity = capacity
        self.length = length
        self.next = next_index
        self.field = field

    def __len__(self):
        return self.length

    def __getitem__(self, index):
        logical = index
        if logical < 0:
            logical += self.length
        if logical < 0 or logical >= self.length:
            raise IndexError("bar history index out of range")
        oldest = (self.next + self.capacity - self.length) % self.capacity
        physical = (oldest + logical) % self.capacity
        bars = carray(address_as_void_pointer(self.bars_ptr), self.capacity, bar_dtype)
        bar = bars[physical]
        if self.field == 0:
            return bar["open"]
        if self.field == 1:
            return bar["high"]
        if self.field == 2:
            return bar["low"]
        if self.field == 3:
            return bar["close"]
        if self.field == 4:
            return bar["volume"]
        return bar["quote_volume"]


@jitclass([("ctx_arr", numba.from_dtype(runtime_ctx_dtype)[:])])
class Strategy:
    """Single callback argument backed by Rust's StrategyRuntimeContext.

    Pointer-backed arrays are valid only during the current callback. Copy values into a
    state array if they must survive the next callback.
    """

    def __init__(self, ctx_arr):
        self.ctx_arr = ctx_arr

    @property
    def now(self):
        return self.ctx_arr[0]["now"]

    @property
    def event_kind(self):
        return self.ctx_arr[0]["event_kind"]

    @property
    def generation(self):
        return self.ctx_arr[0]["generation"]

    @property
    def num_ticks(self):
        return self.ctx_arr[0]["num_ticks"]

    @property
    def num_bars(self):
        return self.ctx_arr[0]["num_bars"]

    @property
    def num_fills(self):
        return self.ctx_arr[0]["num_fills"]

    @property
    def num_orders(self):
        return self.ctx_arr[0]["num_orders"]

    @property
    def num_assets(self):
        return self.ctx_arr[0]["num_markets"]

    @property
    def bar_timeframe(self):
        return self.ctx_arr[0]["bar_timeframe_ns"]

    @property
    def bar_close_ts(self):
        return self.ctx_arr[0]["bar_close_ts"]

    @property
    def last_error(self):
        return self.ctx_arr[0]["last_error"]

    @property
    def state(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["state_f64_ptr"]),
            self.ctx_arr[0]["state_f64_len"],
            float64,
        )

    @property
    def state_i64(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["state_i64_ptr"]),
            self.ctx_arr[0]["state_i64_len"],
            int64,
        )

    def ticks(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["ticks_ptr"]),
            self.ctx_arr[0]["num_ticks"],
            tick_item_dtype,
        )

    def bars(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["bars_ptr"]),
            self.ctx_arr[0]["num_bars"],
            bar_item_dtype,
        )

    def fills(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["fills_ptr"]),
            self.ctx_arr[0]["num_fills"],
            fill_dtype,
        )

    def orders(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["orders_ptr"]),
            self.ctx_arr[0]["num_orders"],
            order_event_dtype,
        )

    def payload(self):
        """Returns the current future/custom event payload as a zero-copy byte view."""
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
            self.ctx_arr[0]["payload_len"],
            numba.uint8,
        )

    def timer(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, timer_dtype
        )[0]

    def funding(self):
        return carray(
            address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, funding_dtype
        )[0]

    def position(self, asset_no):
        positions = carray(
            address_as_void_pointer(self.ctx_arr[0]["positions_ptr"]),
            self.ctx_arr[0]["num_positions"],
            float64,
        )
        return positions[asset_no]

    def market(self, asset_no):
        markets = carray(
            address_as_void_pointer(self.ctx_arr[0]["markets_ptr"]),
            self.ctx_arr[0]["num_markets"],
            market_state_dtype,
        )
        return markets[asset_no]

    def best_bid(self, asset_no):
        return self.market(asset_no)["best_bid"]

    def best_ask(self, asset_no):
        return self.market(asset_no)["best_ask"]

    def best_bid_qty(self, asset_no):
        return self.market(asset_no)["best_bid_qty"]

    def best_ask_qty(self, asset_no):
        return self.market(asset_no)["best_ask_qty"]

    def _submit(
        self,
        asset_no,
        order_id,
        price,
        qty,
        side,
        time_in_force,
        order_type,
        wait,
        reduce_only=False,
        trigger_price=0.0,
        trigger_kind=0,
        gtd_expiry_ts=0,
    ):
        if wait:
            return -2
        if self.event_kind == EVENT_ERROR or self.event_kind == EVENT_STOP:
            return -3
        index = self.ctx_arr[0]["num_commands"]
        capacity = self.ctx_arr[0]["command_capacity"]
        if index >= capacity:
            return -1
        commands = carray(
            address_as_void_pointer(self.ctx_arr[0]["commands_ptr"]),
            capacity,
            order_command_dtype,
        )
        command = commands[index]
        command["kind"] = 1
        command["side"] = side
        command["time_in_force"] = time_in_force
        command["order_type"] = order_type
        command["_reserved"][0] = 1 if reduce_only else 0
        command["_reserved"][1] = trigger_kind
        command["asset_no"] = asset_no
        command["order_id"] = order_id
        command["price"] = price
        command["qty"] = qty
        command["trigger_price"] = trigger_price
        command["gtd_expiry_ts"] = gtd_expiry_ts
        self.ctx_arr[0]["num_commands"] = index + 1
        return 0

    def submit_buy_order(
        self,
        asset_no,
        order_id,
        price,
        qty,
        time_in_force,
        order_type,
        wait,
        reduce_only=False,
        gtd_expiry_ts=0,
    ):
        return self._submit(
            asset_no,
            order_id,
            price,
            qty,
            1,
            time_in_force,
            order_type,
            wait,
            reduce_only,
            0.0,
            0,
            gtd_expiry_ts,
        )

    def submit_sell_order(
        self,
        asset_no,
        order_id,
        price,
        qty,
        time_in_force,
        order_type,
        wait,
        reduce_only=False,
        gtd_expiry_ts=0,
    ):
        return self._submit(
            asset_no,
            order_id,
            price,
            qty,
            -1,
            time_in_force,
            order_type,
            wait,
            reduce_only,
            0.0,
            0,
            gtd_expiry_ts,
        )

    def submit_stop_buy_order(
        self,
        asset_no,
        order_id,
        trigger_price,
        limit_price,
        qty,
        time_in_force,
        stop_limit,
        wait,
        reduce_only=False,
        gtd_expiry_ts=0,
    ):
        return self._submit(
            asset_no,
            order_id,
            limit_price,
            qty,
            1,
            time_in_force,
            0 if stop_limit else 1,
            wait,
            reduce_only,
            trigger_price,
            2 if stop_limit else 1,
            gtd_expiry_ts,
        )

    def submit_stop_sell_order(
        self,
        asset_no,
        order_id,
        trigger_price,
        limit_price,
        qty,
        time_in_force,
        stop_limit,
        wait,
        reduce_only=False,
        gtd_expiry_ts=0,
    ):
        return self._submit(
            asset_no,
            order_id,
            limit_price,
            qty,
            -1,
            time_in_force,
            0 if stop_limit else 1,
            wait,
            reduce_only,
            trigger_price,
            2 if stop_limit else 1,
            gtd_expiry_ts,
        )

    def cancel(self, asset_no, order_id, wait):
        if wait:
            return -2
        index = self.ctx_arr[0]["num_commands"]
        capacity = self.ctx_arr[0]["command_capacity"]
        if index >= capacity:
            return -1
        commands = carray(
            address_as_void_pointer(self.ctx_arr[0]["commands_ptr"]),
            capacity,
            order_command_dtype,
        )
        command = commands[index]
        command["kind"] = 2
        command["asset_no"] = asset_no
        command["order_id"] = order_id
        self.ctx_arr[0]["num_commands"] = index + 1
        return 0

    def _series(self, asset_no, timeframe_ns, field):
        histories = carray(
            address_as_void_pointer(self.ctx_arr[0]["histories_ptr"]),
            self.ctx_arr[0]["num_histories"],
            bar_history_view_dtype,
        )
        for history in histories:
            if history["asset_no"] == asset_no and history["timeframe_ns"] == timeframe_ns:
                return BarSeries(
                    history["bars_ptr"],
                    history["capacity"],
                    history["len"],
                    history["next"],
                    field,
                )
        return BarSeries(0, 0, 0, 0, field)

    def open(self, asset_no, timeframe_ns):
        return self._series(asset_no, timeframe_ns, 0)

    def high(self, asset_no, timeframe_ns):
        return self._series(asset_no, timeframe_ns, 1)

    def low(self, asset_no, timeframe_ns):
        return self._series(asset_no, timeframe_ns, 2)

    def close(self, asset_no, timeframe_ns):
        return self._series(asset_no, timeframe_ns, 3)

    def volume(self, asset_no, timeframe_ns):
        return self._series(asset_no, timeframe_ns, 4)

    def stop(self):
        self.ctx_arr[0]["stop_requested"] = 1


@njit
def _noop(_s):
    return


def _validate_handler(name, handler):
    from numba.core.dispatcher import Dispatcher

    if handler is None:
        return _noop
    if not isinstance(handler, Dispatcher):
        raise TypeError(f"{name} must be a @njit function")
    if len(inspect.signature(handler.py_func).parameters) != 1:
        raise TypeError(f"{name} must accept exactly one parameter: {name}(s)")
    return handler


def _callback_bridge(handler):
    """Compiles a stable C ABI bridge and keeps user code Numba-to-Numba."""

    @cfunc(types.int32(types.voidptr))
    def bridge(ctx_ptr):
        ctx_arr = carray(ctx_ptr, 1, dtype=runtime_ctx_dtype)
        s = Strategy(ctx_arr)
        try:
            handler(s)
        except Exception:
            # Numba cfunc otherwise prints "Exception ignored" and reports success.
            return -1000
        return 0

    return bridge


def _tick_runtime_function(hbt):
    name = type(hbt).__name__
    supported = {
        "HashMapMarketDepthBacktest": "hashmapbt_run_tick_runtime",
        "ROIVectorMarketDepthBacktest": "roivecbt_run_tick_runtime",
        "HashMapMarketDepthLiveBot": "hashmaplive_run_tick_runtime",
        "ROIVectorMarketDepthLiveBot": "roiveclive_run_tick_runtime",
    }
    if name not in supported:
        raise TypeError(f"unsupported Rust Bot backend: {name}")
    symbol = supported[name]
    function = getattr(_lib, symbol)
    function.restype = c_int64
    function.argtypes = [
        c_void_p,
        c_void_p,
        POINTER(c_uint64),
        c_size_t,
        c_int64,
        c_size_t,
    ]
    return function


def _scheduled_tick_runtime_function(hbt):
    name = type(hbt).__name__
    supported = {
        "HashMapMarketDepthBacktest": "hashmapbt_run_scheduled_tick_runtime",
        "ROIVectorMarketDepthBacktest": "roivecbt_run_scheduled_tick_runtime",
        "HashMapMarketDepthLiveBot": "hashmaplive_run_scheduled_tick_runtime",
        "ROIVectorMarketDepthLiveBot": "roiveclive_run_scheduled_tick_runtime",
    }
    if name not in supported:
        raise TypeError(f"scheduled timers are not supported by backend: {name}")
    function = getattr(_lib, supported[name])
    function.restype = c_int64
    function.argtypes = [
        c_void_p,  # backend
        c_void_p, c_size_t,  # timers
        c_void_p, c_size_t,  # funding
        c_void_p,  # context
        POINTER(c_uint64), c_size_t,  # callbacks
        c_int64, c_size_t,  # tick batching
    ]
    return function


def _bar_runtime_function():
    function = _lib.run_materialized_bar_runtime
    function.restype = c_int64
    function.argtypes = [
        c_void_p, c_size_t,  # bars
        c_void_p,  # context
        POINTER(c_uint64), c_size_t,  # callbacks
        c_size_t,  # history capacity
    ]
    return function


def _scheduled_bar_runtime_function():
    function = _lib.run_scheduled_materialized_bar_runtime
    function.restype = c_int64
    function.argtypes = [
        c_void_p, c_size_t,  # bars
        c_void_p, c_size_t,  # timers
        c_void_p, c_size_t,  # funding
        c_void_p,  # context
        POINTER(c_uint64), c_size_t,  # callbacks
        c_size_t,  # history capacity
    ]
    return function


def _configured_bar_runtime_function():
    function = _lib.run_configured_materialized_bar_runtime_v2
    function.restype = c_int64
    function.argtypes = [
        c_void_p, c_size_t,  # bars
        c_void_p, c_size_t,  # timers
        c_void_p, c_size_t,  # funding
        c_void_p,  # context
        POINTER(c_uint64), c_size_t,  # callbacks
        c_size_t,  # history capacity
        c_uint32, c_double,  # matching model
        c_int64, c_int64, c_int64,  # feed, entry and response latency
    ]
    return function


def _hybrid_runtime_function(hbt):
    name = type(hbt).__name__
    supported = {
        "HashMapMarketDepthBacktest": "hashmapbt_run_hybrid_runtime",
        "ROIVectorMarketDepthBacktest": "roivecbt_run_hybrid_runtime",
    }
    if name not in supported:
        raise TypeError(f"unsupported Rust hybrid backend: {name}")
    function = getattr(_lib, supported[name])
    function.restype = c_int64
    function.argtypes = [
        c_void_p,
        c_void_p,
        c_size_t,
        c_void_p,
        POINTER(c_uint64),
        c_size_t,
        c_size_t,
        c_int64,
        c_size_t,
    ]
    return function


def _scheduled_hybrid_runtime_function(hbt):
    name = type(hbt).__name__
    supported = {
        "HashMapMarketDepthBacktest": "hashmapbt_run_scheduled_hybrid_runtime",
        "ROIVectorMarketDepthBacktest": "roivecbt_run_scheduled_hybrid_runtime",
    }
    if name not in supported:
        raise TypeError(f"scheduled hybrid backend is unsupported: {name}")
    function = getattr(_lib, supported[name])
    function.restype = c_int64
    function.argtypes = [
        c_void_p,  # backend
        c_void_p, c_size_t,  # bars
        c_void_p, c_size_t,  # timers
        c_void_p, c_size_t,  # funding
        c_void_p,  # context
        POINTER(c_uint64), c_size_t,  # callbacks
        c_size_t,  # history capacity
        c_int64, c_size_t,  # tick batching
    ]
    return function


def run_event_bot(
    hbt=None,
    on_tick=None,
    on_bar=None,
    *,
    on_start=None,
    on_stop=None,
    on_filled=None,
    on_order=None,
    on_position=None,
    on_funding=None,
    on_timer=None,
    on_error=None,
    callbacks=None,
    data_mode="tick",
    frame_interval=1_000_000,
    max_tick_batch=65_536,
    bars=None,
    history_capacity=1024,
    state=None,
    state_i64=None,
    timers=None,
    funding=None,
    bar_matching="next_open",
    volume_participation=1.0,
    feed_latency=0,
    entry_latency=0,
    response_latency=0,
):
    """Runs callbacks under the Rust-owned event loop.

    Every handler is ``@njit def handler(s)``. ``on_tick`` receives one global
    TickBatch. Future event kinds can be registered with ``callbacks={event_id: fn}``.

    Tick mode waits for the next Rust feed/order-response boundary and delivers all assets at
    that timestamp as one global batch; ``frame_interval`` is the maximum wait duration, not a
    Python polling loop. Bar mode consumes an explicit ``timed_bar_dtype`` array and jumps
    directly between scheduler boundaries. The default is conservative NextOpen execution;
    ``bar_matching='signal_close'`` explicitly opts into same-close execution for parity with
    engines whose close-of-Bar callback can fill at that same close. ``feed_latency``
    controls when a closed Bar becomes visible, while ``entry_latency`` and ``response_latency``
    control exchange arrival and local execution-report delivery; all are nanoseconds and live
    outside the Bar payload. Hybrid deterministically merges
    Bar signal batches into the Rust Tick clock and executes every order only through the Tick
    backend. Optional ``timer_dtype`` records remain active after market data ends in every mode.
    Backtests accept ``funding_dtype`` records, settled by the Rust account engine and delivered
    through ``on_funding(s)`` after their configured report latency.
    """

    if data_mode not in ("tick", "bar", "hybrid"):
        raise ValueError("data_mode must be 'tick', 'bar' or 'hybrid'")
    matching_modes = {
        "next_open": 0,
        "touch": 1,
        "conservative_ohlc": 2,
        "signal_close": 3,
    }
    if bar_matching not in matching_modes:
        raise ValueError(
            "bar_matching must be 'next_open', 'touch', 'conservative_ohlc' "
            "or 'signal_close'"
        )
    if not np.isfinite(volume_participation) or not 0.0 <= volume_participation <= 1.0:
        raise ValueError("volume_participation must be finite and in [0, 1]")
    if any(latency < 0 for latency in (feed_latency, entry_latency, response_latency)):
        raise ValueError("feed_latency, entry_latency and response_latency cannot be negative")
    if data_mode != "bar" and any(
        latency != 0 for latency in (feed_latency, entry_latency, response_latency)
    ):
        raise ValueError("explicit Bar latencies are only valid in data_mode='bar'")
    if data_mode != "bar" and bar_matching != "next_open":
        raise ValueError("explicit Bar matching is only valid when Bar is the execution source")
    if bar_matching == "signal_close" and (feed_latency != 0 or entry_latency != 0):
        raise ValueError("signal_close requires zero feed_latency and entry_latency")
    if data_mode in ("tick", "hybrid") and (frame_interval <= 0 or max_tick_batch <= 0):
        raise ValueError("frame_interval and max_tick_batch must be positive")
    if data_mode in ("tick", "hybrid") and hbt is None:
        raise ValueError(f"{data_mode} mode requires hbt")
    if data_mode in ("bar", "hybrid"):
        if bars is None:
            raise ValueError(f"{data_mode} mode requires bars")
        bars = np.ascontiguousarray(bars, dtype=timed_bar_dtype)
        if history_capacity < 0:
            raise ValueError("history_capacity cannot be negative")
    if timers is not None:
        timers = np.ascontiguousarray(timers, dtype=timer_dtype)
    if funding is not None:
        funding = np.ascontiguousarray(funding, dtype=funding_dtype)
        # Zero preserves source compatibility for arrays created with np.zeros before explicit
        # rounding was added; the normalized buffer passed to Rust is always explicit.
        funding["rounding_increment"][funding["rounding_increment"] == 0.0] = 1e-12
        if (
            np.any(~np.isfinite(funding["rounding_increment"]))
            or np.any(funding["rounding_increment"] <= 0.0)
            or np.any(~np.isin(funding["position_snapshot"], (0, 1)))
            or np.any(funding["formula"] != 0)
            or np.any(~np.isin(funding["rounding_mode"], (0, 1, 2, 3)))
            or np.any(~np.isin(funding["boundary"], (0, 1)))
            or np.any(funding["position_snapshot"] != funding["boundary"])
        ):
            raise ValueError("invalid explicit funding configuration")
        if hbt is not None and type(hbt).__name__.endswith("LiveBot"):
            raise NotImplementedError(
                "scheduled backtest funding cannot be injected into a live backend; "
                "live connector funding must enter through LiveExecutionAdapter"
            )
    unsupported = []
    if on_funding is not None and funding is None:
        unsupported.append("on_funding")
    if on_timer is not None and timers is None:
        unsupported.append("on_timer")
    if unsupported:
        raise NotImplementedError(
            f"callbacks not connected to a Rust event source yet: {', '.join(unsupported)}"
        )

    handlers = {
        EVENT_START: _validate_handler("on_start", on_start),
        EVENT_ORDER: _validate_handler("on_order", on_order),
        EVENT_FILLED: _validate_handler("on_filled", on_filled),
        EVENT_POSITION: _validate_handler("on_position", on_position),
        EVENT_FUNDING: _validate_handler("on_funding", on_funding),
        EVENT_BAR: _validate_handler("on_bar", on_bar),
        EVENT_TICK: _validate_handler("on_tick", on_tick),
        EVENT_TIMER: _validate_handler("on_timer", on_timer),
        EVENT_ERROR: _validate_handler("on_error", on_error),
        EVENT_STOP: _validate_handler("on_stop", on_stop),
    }
    if callbacks:
        for event_id, handler in callbacks.items():
            if not 0 <= int(event_id) < EVENT_SLOT_COUNT:
                raise ValueError(f"custom event id must be in [0, {EVENT_SLOT_COUNT})")
            handlers[int(event_id)] = _validate_handler(f"callback_{event_id}", handler)

    bridges = {event_id: _callback_bridge(handler) for event_id, handler in handlers.items()}
    addresses = np.zeros(EVENT_SLOT_COUNT, dtype=np.uint64)
    for event_id, bridge in bridges.items():
        addresses[event_id] = bridge.address

    if state is None:
        state = np.zeros(64, dtype=np.float64)
    else:
        state = np.ascontiguousarray(state, dtype=np.float64)
    if state_i64 is None:
        state_i64 = np.zeros(64, dtype=np.int64)
    else:
        state_i64 = np.ascontiguousarray(state_i64, dtype=np.int64)

    ctx = np.zeros(1, dtype=runtime_ctx_dtype)
    ctx[0]["abi_version"] = ABI_VERSION
    ctx[0]["struct_size"] = runtime_ctx_dtype.itemsize
    ctx[0]["state_f64_ptr"] = state.ctypes.data
    ctx[0]["state_f64_len"] = len(state)
    ctx[0]["state_i64_ptr"] = state_i64.ctypes.data
    ctx[0]["state_i64_len"] = len(state_i64)

    if data_mode == "tick":
        if timers is None and funding is None:
            function = _tick_runtime_function(hbt)
            result = function(
                int(hbt.ptr),
                ctx.ctypes.data,
                addresses.ctypes.data_as(POINTER(c_uint64)),
                len(addresses),
                int(frame_interval),
                int(max_tick_batch),
            )
        else:
            function = _scheduled_tick_runtime_function(hbt)
            result = function(
                int(hbt.ptr),
                0 if timers is None else timers.ctypes.data,
                0 if timers is None else len(timers),
                0 if funding is None else funding.ctypes.data,
                0 if funding is None else len(funding),
                ctx.ctypes.data,
                addresses.ctypes.data_as(POINTER(c_uint64)),
                len(addresses),
                int(frame_interval),
                int(max_tick_batch),
            )
    elif data_mode == "hybrid":
        if timers is None and funding is None:
            function = _hybrid_runtime_function(hbt)
            result = function(
                int(hbt.ptr),
                bars.ctypes.data,
                len(bars),
                ctx.ctypes.data,
                addresses.ctypes.data_as(POINTER(c_uint64)),
                len(addresses),
                int(history_capacity),
                int(frame_interval),
                int(max_tick_batch),
            )
        else:
            function = _scheduled_hybrid_runtime_function(hbt)
            result = function(
                int(hbt.ptr),
                bars.ctypes.data,
                len(bars),
                0 if timers is None else timers.ctypes.data,
                0 if timers is None else len(timers),
                0 if funding is None else funding.ctypes.data,
                0 if funding is None else len(funding),
                ctx.ctypes.data,
                addresses.ctypes.data_as(POINTER(c_uint64)),
                len(addresses),
                int(history_capacity),
                int(frame_interval),
                int(max_tick_batch),
            )
    else:
        function = _configured_bar_runtime_function()
        result = function(
            bars.ctypes.data,
            len(bars),
            0 if timers is None else timers.ctypes.data,
            0 if timers is None else len(timers),
            0 if funding is None else funding.ctypes.data,
            0 if funding is None else len(funding),
            ctx.ctypes.data,
            addresses.ctypes.data_as(POINTER(c_uint64)),
            len(addresses),
            int(history_capacity),
            matching_modes[bar_matching],
            float(volume_participation),
            int(feed_latency),
            int(entry_latency),
            int(response_latency),
        )
    if result != 0:
        raise RuntimeError(
            f"Rust strategy runtime failed with code {result}; last_error={ctx[0]['last_error']}"
        )
    return state


run_strategy = run_event_bot
