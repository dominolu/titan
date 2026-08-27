"""Numba callback context backed exclusively by Rust-owned ABI memory."""

from __future__ import annotations

import inspect

import numba
import numpy as np
from numba import carray, cfunc, float64, int64, njit, types
from numba.experimental import jitclass

from .intrinsic import address_as_void_pointer

EVENT_ERROR = 8
EVENT_STOP = 9

event_dtype = np.dtype(
    [("ev", "u8"), ("exch_ts", "i8"), ("local_ts", "i8"), ("px", "f8"),
     ("qty", "f8"), ("order_id", "u8"), ("ival", "i8"), ("fval", "f8")],
    align=True,
)
bar_dtype = np.dtype(
    [("open_ts", "i8"), ("close_ts", "i8"), ("open", "f8"), ("high", "f8"),
     ("low", "f8"), ("close", "f8"), ("volume", "f8"), ("quote_volume", "f8"),
     ("buy_volume", "f8"), ("trade_count", "u8"), ("flags", "u8")],
    align=True,
)
tick_item_dtype = np.dtype(
    {"names": ["asset_no", "event"], "formats": ["u8", event_dtype],
     "offsets": [0, 64], "itemsize": 128}, align=True,
)
bar_item_dtype = np.dtype([("asset_no", "u8"), ("bar", bar_dtype)], align=True)
timed_bar_item_dtype = np.dtype(
    [("asset_no", "u8"), ("timeframe_ns", "i8"), ("bar", bar_dtype)], align=True,
)
timer_dtype = np.dtype(
    [("deadline_ts", "i8"), ("owner_id", "u8"), ("timer_id", "u8")], align=True,
)
funding_dtype = np.dtype(
    [("event_id", "u8"), ("asset_no", "u4"), ("venue_no", "u4"),
     ("instrument_id", "u4"), ("currency", "u4"), ("price_source", "u4"),
     ("position_snapshot", "u1"), ("formula", "u1"), ("rounding_mode", "u1"),
     ("boundary", "u1"), ("publication_ts", "i8"), ("effective_ts", "i8"),
     ("settlement_ts", "i8"), ("delivery_ts", "i8"), ("rate", "f8"),
     ("mark_price", "f8"), ("position_qty", "f8"), ("amount", "f8"),
     ("rounding_increment", "f8")], align=True,
)
bar_history_view_dtype = np.dtype(
    [("asset_no", "u8"), ("timeframe_ns", "i8"), ("bars_ptr", "u8"),
     ("capacity", "u8"), ("len", "u8"), ("next", "u8")], align=True,
)
fill_dtype = np.dtype(
    [("asset_no", "u8"), ("order_id", "u8"), ("venue_order_id", "u8"),
     ("exch_ts", "i8"), ("local_ts", "i8"), ("sequence", "u8"),
     ("price", "f8"), ("qty", "f8"), ("venue_no", "u4"),
     ("instrument_id", "u4"), ("reason", "u4"), ("side", "i1"),
     ("maker", "u1"), ("_reserved", "u1", (2,))], align=True,
)
order_event_dtype = np.dtype(
    [("asset_no", "u8"), ("order_id", "u8"), ("venue_order_id", "u8"),
     ("exch_ts", "i8"), ("local_ts", "i8"), ("sequence", "u8"),
     ("price", "f8"), ("qty", "f8"), ("exec_price", "f8"),
     ("exec_qty", "f8"), ("venue_no", "u4"), ("instrument_id", "u4"),
     ("reason", "u4"), ("side", "i1"), ("status", "u1"),
     ("request", "u1"), ("maker", "u1"), ("_reserved", "u1", (4,))], align=True,
)
market_state_dtype = np.dtype(
    [("best_bid", "f8"), ("best_ask", "f8"), ("best_bid_qty", "f8"),
     ("best_ask_qty", "f8"), ("tick_size", "f8"), ("lot_size", "f8")], align=True,
)
order_command_dtype = np.dtype(
    [("kind", "u1"), ("side", "i1"), ("time_in_force", "u1"),
     ("order_type", "u1"), ("_reserved", "u1", (4,)), ("asset_no", "u8"),
     ("order_id", "u8"), ("price", "f8"), ("qty", "f8"),
     ("trigger_price", "f8"), ("gtd_expiry_ts", "i8")], align=True,
)

_ctx_fields = [
    ("abi_version", "u4"), ("struct_size", "u4"), ("event_kind", "u4"),
    ("stop_requested", "u4"), ("now", "i8"), ("generation", "u8"),
    ("user_data", "u8"), ("bot_ptr", "u8"), ("ticks_ptr", "u8"),
    ("num_ticks", "u8"), ("bars_ptr", "u8"), ("num_bars", "u8"),
    ("bar_timeframe_ns", "i8"), ("bar_close_ts", "i8"), ("fills_ptr", "u8"),
    ("num_fills", "u8"), ("orders_ptr", "u8"), ("num_orders", "u8"),
    ("histories_ptr", "u8"), ("num_histories", "u8"), ("payload_ptr", "u8"),
    ("payload_len", "u8"), ("state_f64_ptr", "u8"), ("state_f64_len", "u8"),
    ("state_i64_ptr", "u8"), ("state_i64_len", "u8"), ("commands_ptr", "u8"),
    ("num_commands", "u8"), ("command_capacity", "u8"), ("positions_ptr", "u8"),
    ("num_positions", "u8"), ("markets_ptr", "u8"), ("num_markets", "u8"),
    ("last_error", "i8"),
]
runtime_ctx_dtype = np.dtype(_ctx_fields, align=True)

_ABI_DTYPES = {
    "Bar": bar_dtype,
    "Event": event_dtype,
    "FillEvent": fill_dtype,
    "OrderEvent": order_event_dtype,
    "MarketState": market_state_dtype,
    "BarItem": bar_item_dtype,
    "TimedBarItem": timed_bar_item_dtype,
    "RuntimeTimer": timer_dtype,
    "BarHistoryView": bar_history_view_dtype,
    "TickItem": tick_item_dtype,
    "OrderCommand": order_command_dtype,
    "RuntimeFunding": funding_dtype,
    "StrategyRuntimeContext": runtime_ctx_dtype,
}
_ABI_ALIGNMENTS = {name: dtype.alignment for name, dtype in _ABI_DTYPES.items()}
_ABI_ALIGNMENTS.update({"Event": 64, "TickItem": 64})

_PRIMITIVE_KINDS = {
    "u8": ("u", 1), "i8": ("i", 1), "u32": ("u", 4), "i32": ("i", 4),
    "u64": ("u", 8), "i64": ("i", 8), "f64": ("f", 8),
}


def _validate_kind(name: str, field: str, kind: object, dtype: np.dtype,
                   pointer_width: int) -> None:
    if isinstance(kind, str):
        if kind in ("pointer", "usize"):
            expected = pointer_width // 8
            if dtype.kind != "u" or dtype.itemsize != expected:
                raise RuntimeError(f"{name}.{field} must be a {pointer_width}-bit {kind}")
            return
        expected = _PRIMITIVE_KINDS.get(kind)
        if expected is None or (dtype.kind, dtype.itemsize) != expected:
            raise RuntimeError(f"{name}.{field} type mismatch: expected {kind}")
        return
    if "struct" in kind:
        nested = kind["struct"]
        if nested not in _ABI_DTYPES or dtype != _ABI_DTYPES[nested]:
            raise RuntimeError(f"{name}.{field} nested struct mismatch: expected {nested}")
        return
    if "array" in kind:
        subdtype = dtype.subdtype
        array = kind["array"]
        if subdtype is None or int(np.prod(subdtype[1])) != int(array["len"]):
            raise RuntimeError(f"{name}.{field} array length mismatch")
        _validate_kind(name, field, array["element"], subdtype[0], pointer_width)
        return
    raise RuntimeError(f"{name}.{field} has unsupported ABI kind {kind!r}")


def _kind_alignment(kind: object, dtype: np.dtype) -> int:
    if isinstance(kind, dict) and "struct" in kind:
        return _ABI_ALIGNMENTS[kind["struct"]]
    if isinstance(kind, dict) and "array" in kind and dtype.subdtype is not None:
        return _kind_alignment(kind["array"]["element"], dtype.subdtype[0])
    return dtype.alignment


def _fingerprint(runtime_abi: dict) -> str:
    value = 0xcbf29ce484222325

    def add(data: bytes) -> None:
        nonlocal value
        for byte in data:
            value ^= byte
            value = (value * 0x100000001b3) & 0xffffffffffffffff

    def u8(item: int) -> None: add(int(item).to_bytes(1, "little"))
    def u32(item: int) -> None: add(int(item).to_bytes(4, "little"))
    def u64(item: int) -> None: add(int(item).to_bytes(8, "little"))
    def string(item: str) -> None:
        encoded = item.encode()
        u64(len(encoded))
        add(encoded)
    def abi_kind(kind: object) -> None:
        tags = {"u8": 0, "i8": 1, "u32": 2, "i32": 3, "u64": 4,
                "i64": 5, "f64": 6, "usize": 7, "pointer": 8}
        if isinstance(kind, str):
            u8(tags[kind])
        elif "array" in kind:
            u8(9)
            abi_kind(kind["array"]["element"])
            u64(kind["array"]["len"])
        elif "struct" in kind:
            u8(10)
            string(kind["struct"])
        else:
            raise RuntimeError(f"unsupported ABI kind {kind!r}")

    u32(runtime_abi["abi_version"])
    u64(runtime_abi["event_slot_count"])
    u8(runtime_abi["pointer_width"])
    u8(bool(runtime_abi["little_endian"]))
    for slot in runtime_abi["event_slots"]:
        string(slot["name"])
        u32(slot["id"])
    for item in runtime_abi["structs"]:
        string(item["name"])
        u64(item["size"])
        u64(item["alignment"])
        for field in item["fields"]:
            string(field["name"])
            abi_kind(field["kind"])
            u64(field["offset"])
            u64(field["size"])
            u64(field["alignment"])
    return f"fnv1a64:{value:016x}"


def validate_runtime_descriptor(runtime_abi: dict) -> None:
    """Reject any Rust/Python shared-ABI or descriptor fingerprint disagreement."""
    pointer_width = int(runtime_abi.get("pointer_width", 0))
    if pointer_width != np.dtype(np.uintp).itemsize * 8:
        raise RuntimeError("Runtime ABI pointer width mismatch")
    if bool(runtime_abi.get("little_endian")) != bool(np.little_endian):
        raise RuntimeError("Runtime ABI endianness mismatch")
    if runtime_abi.get("fingerprint") != _fingerprint(runtime_abi):
        raise RuntimeError("Runtime ABI descriptor fingerprint mismatch")
    structs = {item["name"]: item for item in runtime_abi.get("structs", ())}
    if set(structs) != set(_ABI_DTYPES):
        missing = sorted(set(_ABI_DTYPES) - set(structs))
        extra = sorted(set(structs) - set(_ABI_DTYPES))
        raise RuntimeError(f"Runtime ABI struct set mismatch: missing={missing}, extra={extra}")
    for name, dtype in _ABI_DTYPES.items():
        descriptor = structs[name]
        if int(descriptor["size"]) != dtype.itemsize:
            raise RuntimeError(f"{name} size mismatch")
        if int(descriptor["alignment"]) != _ABI_ALIGNMENTS[name]:
            raise RuntimeError(f"{name} alignment mismatch")
        rust_fields = {field["name"]: field for field in descriptor.get("fields", ())}
        if set(rust_fields) != set(dtype.fields):
            raise RuntimeError(f"{name} field set mismatch")
        for field, field_info in dtype.fields.items():
            field_dtype, offset = field_info[:2]
            rust = rust_fields[field]
            if int(rust["offset"]) != offset or int(rust["size"]) != field_dtype.itemsize:
                raise RuntimeError(f"{name}.{field} layout mismatch")
            if int(rust["alignment"]) != _kind_alignment(rust["kind"], field_dtype):
                raise RuntimeError(f"{name}.{field} alignment mismatch")
            _validate_kind(name, field, rust["kind"], field_dtype, pointer_width)


@jitclass([("ctx_arr", numba.from_dtype(runtime_ctx_dtype)[:])])
class Strategy:
    def __init__(self, ctx_arr):
        self.ctx_arr = ctx_arr

    @property
    def now(self): return self.ctx_arr[0]["now"]
    @property
    def event_kind(self): return self.ctx_arr[0]["event_kind"]
    @property
    def generation(self): return self.ctx_arr[0]["generation"]
    @property
    def num_ticks(self): return self.ctx_arr[0]["num_ticks"]
    @property
    def num_bars(self): return self.ctx_arr[0]["num_bars"]
    @property
    def num_fills(self): return self.ctx_arr[0]["num_fills"]
    @property
    def num_orders(self): return self.ctx_arr[0]["num_orders"]
    @property
    def num_assets(self): return self.ctx_arr[0]["num_markets"]
    @property
    def bar_timeframe(self): return self.ctx_arr[0]["bar_timeframe_ns"]
    @property
    def bar_close_ts(self): return self.ctx_arr[0]["bar_close_ts"]
    @property
    def last_error(self): return self.ctx_arr[0]["last_error"]
    @property
    def state(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["state_f64_ptr"]),
                      self.ctx_arr[0]["state_f64_len"], float64)
    @property
    def state_i64(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["state_i64_ptr"]),
                      self.ctx_arr[0]["state_i64_len"], int64)
    def ticks(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["ticks_ptr"]),
                      self.ctx_arr[0]["num_ticks"], tick_item_dtype)
    def bars(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["bars_ptr"]),
                      self.ctx_arr[0]["num_bars"], bar_item_dtype)
    def fills(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["fills_ptr"]),
                      self.ctx_arr[0]["num_fills"], fill_dtype)
    def orders(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["orders_ptr"]),
                      self.ctx_arr[0]["num_orders"], order_event_dtype)
    def payload(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      self.ctx_arr[0]["payload_len"], numba.uint8)
    def timer(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, timer_dtype)[0]
    def funding(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, funding_dtype)[0]
    def position(self, asset_no):
        return carray(address_as_void_pointer(self.ctx_arr[0]["positions_ptr"]),
                      self.ctx_arr[0]["num_positions"], float64)[asset_no]
    def market(self, asset_no):
        return carray(address_as_void_pointer(self.ctx_arr[0]["markets_ptr"]),
                      self.ctx_arr[0]["num_markets"], market_state_dtype)[asset_no]
    def best_bid(self, asset_no): return self.market(asset_no)["best_bid"]
    def best_ask(self, asset_no): return self.market(asset_no)["best_ask"]

    def _submit(self, asset_no, order_id, price, qty, side, time_in_force, order_type,
                wait, reduce_only=False, trigger_price=0.0, trigger_kind=0,
                gtd_expiry_ts=0):
        if wait: return -2
        if self.event_kind == EVENT_ERROR or self.event_kind == EVENT_STOP: return -3
        index = self.ctx_arr[0]["num_commands"]
        capacity = self.ctx_arr[0]["command_capacity"]
        if index >= capacity: return -1
        command = carray(address_as_void_pointer(self.ctx_arr[0]["commands_ptr"]),
                         capacity, order_command_dtype)[index]
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

    def submit_buy_order(self, asset_no, order_id, price, qty, time_in_force, order_type,
                         wait, reduce_only=False, gtd_expiry_ts=0):
        return self._submit(asset_no, order_id, price, qty, 1, time_in_force, order_type,
                            wait, reduce_only, 0.0, 0, gtd_expiry_ts)
    def submit_sell_order(self, asset_no, order_id, price, qty, time_in_force, order_type,
                          wait, reduce_only=False, gtd_expiry_ts=0):
        return self._submit(asset_no, order_id, price, qty, -1, time_in_force, order_type,
                            wait, reduce_only, 0.0, 0, gtd_expiry_ts)
    def cancel(self, asset_no, order_id, wait):
        if wait: return -2
        index = self.ctx_arr[0]["num_commands"]
        capacity = self.ctx_arr[0]["command_capacity"]
        if index >= capacity: return -1
        command = carray(address_as_void_pointer(self.ctx_arr[0]["commands_ptr"]),
                         capacity, order_command_dtype)[index]
        command["kind"] = 2
        command["asset_no"] = asset_no
        command["order_id"] = order_id
        self.ctx_arr[0]["num_commands"] = index + 1
        return 0
    def stop(self): self.ctx_arr[0]["stop_requested"] = 1


@njit
def _noop(_strategy):
    return


def validate_handler(name, handler):
    from numba.core.dispatcher import Dispatcher

    if not isinstance(handler, Dispatcher):
        raise TypeError(f"{name} must be a @njit function")
    if len(inspect.signature(handler.py_func).parameters) != 1:
        raise TypeError(f"{name} must accept exactly one parameter: {name}(s)")
    return handler


def callback_bridge(handler):
    @cfunc(types.int32(types.voidptr))
    def bridge(ctx_ptr):
        ctx_arr = carray(ctx_ptr, 1, dtype=runtime_ctx_dtype)
        strategy = Strategy(ctx_arr)
        try:
            handler(strategy)
        except Exception:
            return -1000
        return 0

    return bridge
