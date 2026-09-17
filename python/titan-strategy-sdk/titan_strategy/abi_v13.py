"""Canonical Python description of the standalone Strategy ABI V13."""

from __future__ import annotations

import hashlib
import json
import sys
from typing import Final

import numpy as np

from .types import CallbackCode, EventKind


ABI_VERSION: Final = 13
POINTER_WIDTH: Final = 64
MAX_ALIGNMENT: Final = 8
CALLBACK_ORDER: Final = (
    "on_start", "on_tick", "on_bar", "on_depth", "on_fill", "on_order", "on_cancel",
    "on_position", "on_balance", "on_account_state", "on_timer", "on_stop",
)
CALLBACK_BITS: Final = {name: index for index, name in enumerate(CALLBACK_ORDER)}


def _dtype(fields: list[tuple]) -> np.dtype:
    return np.dtype(fields, align=True)


tick_dtype = _dtype([
    ("asset_no", "<u4"), ("kind", "u1"), ("side", "u1"), ("reserved", "u1", (2,)),
    ("exchange_ts_ns", "<i8"), ("receive_ts_ns", "<i8"), ("price_ticks", "<i8"),
    ("qty_lots", "<i8"), ("source_sequence", "<u8"),
])
bar_dtype = _dtype([
    ("asset_no", "<u4"), ("reserved", "<u4"), ("timeframe_ns", "<i8"),
    ("open_ts_ns", "<i8"), ("close_ts_ns", "<i8"), ("open_ticks", "<i8"),
    ("high_ticks", "<i8"), ("low_ticks", "<i8"), ("close_ticks", "<i8"),
    ("volume_lots", "<i8"),
])
depth_dtype = _dtype([
    ("asset_no", "<u4"), ("level", "<u4"), ("exchange_ts_ns", "<i8"),
    ("receive_ts_ns", "<i8"), ("price_ticks", "<i8"), ("qty_lots", "<i8"),
    ("source_sequence", "<u8"), ("side", "u1"), ("action", "u1"),
    ("is_snapshot", "u1"), ("reserved", "u1", (5,)),
])
fill_dtype = _dtype([
    ("order_id", "<u8"), ("asset_no", "<u4"), ("account_no", "<u4"),
    ("fill_price_ticks", "<i8"), ("fill_qty_lots", "<i8"),
    ("cumulative_filled_lots", "<i8"), ("exchange_ts_ns", "<i8"),
    ("receive_ts_ns", "<i8"), ("account_sequence", "<u8"), ("side", "u1"),
    ("liquidity", "u1"), ("final_fill", "u1"), ("reserved", "u1", (5,)),
])
order_event_dtype = _dtype([
    ("order_id", "<u8"), ("asset_no", "<u4"), ("account_no", "<u4"),
    ("price_ticks", "<i8"), ("qty_lots", "<i8"), ("cumulative_filled_lots", "<i8"),
    ("event_ts_ns", "<i8"), ("account_sequence", "<u8"), ("status", "u1"),
    ("reason", "u1"), ("reserved", "u1", (6,)),
])
cancel_event_dtype = _dtype([
    ("order_id", "<u8"), ("asset_no", "<u4"), ("account_no", "<u4"),
    ("event_ts_ns", "<i8"), ("account_sequence", "<u8"), ("request_result", "u1"),
    ("final_status", "u1"), ("reserved", "u1", (6,)),
])
position_event_dtype = _dtype([
    ("asset_no", "<u4"), ("account_no", "<u4"), ("qty_lots", "<i8"),
    ("average_price_ticks", "<i8"), ("realized_pnl_ticks", "<i8"),
    ("event_ts_ns", "<i8"), ("account_sequence", "<u8"),
])
balance_event_dtype = _dtype([
    ("account_no", "<u4"), ("currency_no", "<u4"), ("total_units", "<i8"),
    ("available_units", "<i8"), ("event_ts_ns", "<i8"), ("account_sequence", "<u8"),
])
account_state_event_dtype = _dtype([
    ("account_no", "<u4"), ("reserved0", "<u4"), ("account_epoch", "<u8"),
    ("event_ts_ns", "<i8"), ("account_sequence", "<u8"), ("state", "u1"),
    ("reason", "u1"), ("reserved", "u1", (6,)),
])
timer_dtype = _dtype([
    ("timer_id", "<u8"), ("scheduled_ts_ns", "<i8"), ("fired_ts_ns", "<i8"),
])
market_dtype = _dtype([
    ("asset_no", "<u4"), ("flags", "<u4"), ("best_bid_ticks", "<i8"),
    ("best_bid_qty_lots", "<i8"), ("best_ask_ticks", "<i8"),
    ("best_ask_qty_lots", "<i8"), ("tick_size", "<i8"), ("lot_size", "<i8"),
    ("source_sequence", "<u8"),
])
position_dtype = _dtype([
    ("asset_no", "<u4"), ("account_no", "<u4"), ("qty_lots", "<i8"),
    ("average_price_ticks", "<i8"), ("realized_pnl_ticks", "<i8"),
    ("account_sequence", "<u8"),
])
balance_dtype = _dtype([
    ("account_no", "<u4"), ("currency_no", "<u4"), ("total_units", "<i8"),
    ("available_units", "<i8"), ("account_sequence", "<u8"),
])
account_dtype = _dtype([
    ("account_no", "<u4"), ("reserved", "<u4"), ("account_epoch", "<u8"),
    ("account_sequence", "<u8"), ("state", "u1"), ("reason", "u1"),
    ("reserved2", "u1", (6,)),
])
active_order_dtype = _dtype([
    ("order_id", "<u8"), ("asset_no", "<u4"), ("account_no", "<u4"),
    ("price_ticks", "<i8"), ("qty_lots", "<i8"), ("cumulative_filled_lots", "<i8"),
    ("created_ts_ns", "<i8"), ("updated_ts_ns", "<i8"), ("account_sequence", "<u8"),
    ("side", "u1"), ("order_type", "u1"), ("time_in_force", "u1"), ("status", "u1"),
    ("reduce_only", "u1"), ("reserved", "u1", (3,)),
])
submit_order_request_dtype = _dtype([
    ("asset_no", "<u4"), ("account_no", "<u4"), ("price_ticks", "<i8"),
    ("qty_lots", "<i8"), ("trigger_price_ticks", "<i8"), ("gtd_expiry_ns", "<i8"),
    ("side", "u1"), ("order_type", "u1"), ("time_in_force", "u1"),
    ("reduce_only", "u1"), ("trigger_kind", "u1"), ("reserved", "u1", (3,)),
])
cancel_order_request_dtype = _dtype([
    ("order_id", "<u8"), ("asset_no", "<u4"), ("account_no", "<u4"),
])

_context_fields: list[tuple] = [
    ("struct_size", "<u4"), ("abi_version", "<u4"), ("event_kind", "<u4"),
    ("event_schema_version", "<u4"), ("flags", "<u4"), ("reserved0", "<u4"),
    ("now_ns", "<i8"), ("generation", "<u8"), ("strategy_instance_id", "<u8"),
    ("state_ptr", "<u8"), ("state_len", "<u8"), ("state_alignment", "<u4"),
    ("state_schema_version", "<u4"), ("state_schema_hash", "u1", (32,)),
]
for prefix in (
    "ticks", "bars", "depth", "fills", "order_events", "cancel_events", "position_events",
    "balance_events", "account_state_events", "timer", "markets", "positions", "balances",
    "accounts", "active_orders",
):
    _context_fields.extend(((f"{prefix}_ptr", "<u8"), (f"{prefix}_len", "<u8")))
_context_fields.extend([
    ("event_payload_ptr", "<u8"), ("event_payload_len", "<u8"),
    ("command_context", "<u8"), ("submit_order", "<u8"), ("cancel_order", "<u8"),
    ("last_error_code", "<i4"), ("reserved", "<u4"),
])
runtime_context_dtype = _dtype(_context_fields)

native_descriptor_dtype = _dtype([
    ("struct_size", "<u4"), ("abi_version", "<u4"), ("abi_fingerprint", "u1", (32,)),
    ("state_schema_version", "<u4"), ("state_alignment", "<u4"), ("state_len", "<u8"),
    ("state_schema_hash", "u1", (32,)), ("callback_mask", "<u8"),
])

ABI_DTYPES: Final = {
    "TitanTickView": tick_dtype, "TitanBarView": bar_dtype, "TitanDepthView": depth_dtype,
    "TitanFillView": fill_dtype, "TitanOrderEventView": order_event_dtype,
    "TitanCancelEventView": cancel_event_dtype, "TitanPositionEventView": position_event_dtype,
    "TitanBalanceEventView": balance_event_dtype,
    "TitanAccountStateEventView": account_state_event_dtype, "TitanTimerView": timer_dtype,
    "TitanMarketView": market_dtype, "TitanPositionView": position_dtype,
    "TitanBalanceView": balance_dtype, "TitanAccountView": account_dtype,
    "TitanActiveOrderView": active_order_dtype,
    "TitanSubmitOrderRequest": submit_order_request_dtype,
    "TitanCancelOrderRequest": cancel_order_request_dtype,
    "StrategyRuntimeContext": runtime_context_dtype,
    "NativeStrategyDescriptor": native_descriptor_dtype,
}


def _dtype_descriptor(dtype: np.dtype) -> dict[str, object]:
    fields = []
    assert dtype.names is not None
    for name in dtype.names:
        field_dtype, offset = dtype.fields[name][:2]
        field_dtype = np.dtype(field_dtype)
        shape: list[int] = []
        if field_dtype.subdtype is not None:
            field_dtype, raw_shape = field_dtype.subdtype
            shape = [int(value) for value in raw_shape]
        fields.append({
            "name": name, "offset": int(offset), "type": field_dtype.str,
            "shape": shape, "size": int(np.dtype(dtype.fields[name][0]).itemsize),
        })
    return {"size": dtype.itemsize, "alignment": dtype.alignment, "fields": fields}


def canonical_abi_descriptor() -> bytes:
    if sys.byteorder != "little" or np.dtype(np.uintp).itemsize != 8:
        raise RuntimeError("Strategy ABI V13 requires a little-endian 64-bit host")
    payload = {
        "abi_version": ABI_VERSION,
        "pointer_width": POINTER_WIDTH,
        "callbacks": list(CALLBACK_ORDER),
        "callback_codes": {name: int(value) for name, value in CallbackCode.__members__.items()},
        "event_kinds": {name: int(value) for name, value in EventKind.__members__.items()},
        "function_signatures": {
            "submit_order": "i32(void*,const TitanSubmitOrderRequest*,u64*)",
            "cancel_order": "i32(void*,const TitanCancelOrderRequest*,u64*)",
            "callback": "i32(void*)",
        },
        "structs": {name: _dtype_descriptor(dtype) for name, dtype in ABI_DTYPES.items()},
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


ABI_FINGERPRINT: Final = hashlib.sha256(canonical_abi_descriptor()).digest()


def callback_mask(handler_names: object) -> int:
    result = 0
    for name in handler_names:
        if name not in CALLBACK_BITS:
            raise ValueError(f"unknown V13 handler {name!r}")
        result |= 1 << CALLBACK_BITS[name]
    return result


def validate_abi_descriptor(descriptor: dict[str, object]) -> None:
    if int(descriptor.get("abi_version", -1)) != ABI_VERSION:
        raise RuntimeError("runtime ABI version mismatch")
    raw = descriptor.get("fingerprint")
    expected = ABI_FINGERPRINT.hex()
    if isinstance(raw, bytes):
        actual = raw.hex()
    else:
        actual = str(raw).removeprefix("sha256:")
    if actual != expected:
        raise RuntimeError("runtime ABI fingerprint mismatch")


__all__ = [
    "ABI_DTYPES", "ABI_FINGERPRINT", "ABI_VERSION", "CALLBACK_BITS", "CALLBACK_ORDER",
    "MAX_ALIGNMENT", "POINTER_WIDTH", "account_dtype", "account_state_event_dtype",
    "active_order_dtype", "balance_dtype", "balance_event_dtype", "bar_dtype", "callback_mask",
    "cancel_event_dtype", "cancel_order_request_dtype", "canonical_abi_descriptor", "depth_dtype",
    "fill_dtype", "market_dtype", "native_descriptor_dtype", "order_event_dtype", "position_dtype",
    "position_event_dtype", "runtime_context_dtype", "submit_order_request_dtype", "tick_dtype",
    "timer_dtype", "validate_abi_descriptor",
]
