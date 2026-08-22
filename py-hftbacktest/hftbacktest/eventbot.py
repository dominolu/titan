"""Rust-owned event runtime with single-argument Numba callbacks.

Rust owns the event queue, clock, market state and matching. Python performs cold-path
configuration only. Each user handler is ``@njit`` and has exactly one argument ``s``;
a Numba ``cfunc`` bridge lets Rust invoke it without returning to the Python interpreter.

The implementation supports the Rust-owned global TickBatch loop for backtests and live
bots, plus event-jump materialized Bar backtests with conservative NextOpen matching.
Hybrid is rejected until its exact Rust merge source is connected; it never falls back to
Python-owned aggregation.
"""

import inspect
from ctypes import CDLL, POINTER, c_int64, c_size_t, c_uint64, c_void_p

import numba
import numpy as np
from numba import carray, cfunc, float64, int64, njit, types
from numba.experimental import jitclass

from . import _hftbacktest
from .intrinsic import address_as_void_pointer
from .types import event_dtype

ABI_VERSION = 5
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
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("price", "f8"),
        ("qty", "f8"),
        ("side", "i1"),
        ("maker", "u1"),
        ("_reserved", "u1", (6,)),
    ],
    align=True,
)

order_event_dtype = np.dtype(
    [
        ("asset_no", "u8"),
        ("order_id", "u8"),
        ("exch_ts", "i8"),
        ("local_ts", "i8"),
        ("price", "f8"),
        ("qty", "f8"),
        ("exec_price", "f8"),
        ("exec_qty", "f8"),
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

    def _submit(self, asset_no, order_id, price, qty, side, time_in_force, order_type, wait):
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
        command["asset_no"] = asset_no
        command["order_id"] = order_id
        command["price"] = price
        command["qty"] = qty
        self.ctx_arr[0]["num_commands"] = index + 1
        return 0

    def submit_buy_order(
        self, asset_no, order_id, price, qty, time_in_force, order_type, wait
    ):
        return self._submit(
            asset_no, order_id, price, qty, 1, time_in_force, order_type, wait
        )

    def submit_sell_order(
        self, asset_no, order_id, price, qty, time_in_force, order_type, wait
    ):
        return self._submit(
            asset_no, order_id, price, qty, -1, time_in_force, order_type, wait
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


def _bar_runtime_function():
    function = _lib.run_materialized_bar_runtime
    function.restype = c_int64
    function.argtypes = [
        c_void_p,
        c_size_t,
        c_void_p,
        POINTER(c_uint64),
        c_size_t,
        c_size_t,
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
):
    """Runs callbacks under the Rust-owned event loop.

    Every handler is ``@njit def handler(s)``. ``on_tick`` receives one global
    TickBatch. Future event kinds can be registered with ``callbacks={event_id: fn}``.

    Tick mode waits for the next Rust feed/order-response boundary and delivers all assets at
    that timestamp as one global batch; ``frame_interval`` is the maximum wait duration, not a
    Python polling loop. Bar mode consumes an explicit ``timed_bar_dtype`` array, jumps directly
    between closes, and uses conservative NextOpen execution. Hybrid is rejected until the
    deterministic Rust merge source is connected. Funding and timer callbacks are likewise
    rejected until their Rust sources are connected.
    """

    if data_mode not in ("tick", "bar", "hybrid"):
        raise ValueError("data_mode must be 'tick', 'bar' or 'hybrid'")
    if data_mode == "hybrid":
        raise NotImplementedError(
            "Hybrid Rust event source is not connected yet; implicit aggregation is disabled"
        )
    if data_mode == "tick" and (frame_interval <= 0 or max_tick_batch <= 0):
        raise ValueError("frame_interval and max_tick_batch must be positive")
    if data_mode == "tick" and hbt is None:
        raise ValueError("tick mode requires hbt")
    if data_mode == "bar":
        if bars is None:
            raise ValueError("bar mode requires bars")
        bars = np.ascontiguousarray(bars, dtype=timed_bar_dtype)
        if history_capacity < 0:
            raise ValueError("history_capacity cannot be negative")
    unsupported = [
        name
        for name, handler in (("on_funding", on_funding), ("on_timer", on_timer))
        if handler is not None
    ]
    if unsupported:
        raise NotImplementedError(
            f"callbacks not connected to a Rust event source yet: {', '.join(unsupported)}"
        )
    if data_mode == "bar" and (on_order is not None or on_position is not None):
        raise NotImplementedError(
            "bar mode currently supports on_filled but not on_order/on_position"
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
        function = _bar_runtime_function()
        result = function(
            bars.ctypes.data,
            len(bars),
            ctx.ctypes.data,
            addresses.ctypes.data_as(POINTER(c_uint64)),
            len(addresses),
            int(history_capacity),
        )
    if result != 0:
        raise RuntimeError(
            f"Rust strategy runtime failed with code {result}; last_error={ctx[0]['last_error']}"
        )
    return state


run_strategy = run_event_bot
