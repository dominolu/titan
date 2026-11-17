"""Canonical Python description of the Strategy ABI.

One context, one module. The root `StrategyRuntimeContext` keeps the frozen field sequence and
appends the object-table tail. Payload dtypes carry their own `(schema_major, schema_minor)`, so a
payload may grow by appending fields without invalidating compiled artifacts.

Version identity lives in `ABI_VERSION` / `ABI_FINGERPRINT`, never in a module or type name.
"""

from __future__ import annotations

import hashlib
import json
import sys
from typing import Final

import numpy as np

from .types import CallbackCode, EventKind


ABI_VERSION: Final = 14
POINTER_WIDTH: Final = 64
MAX_ALIGNMENT: Final = 8
CALLBACK_ORDER: Final = (
    "on_start", "on_tick", "on_bar", "on_depth", "on_fill", "on_order", "on_cancel",
    "on_position", "on_balance", "on_account_state", "on_timer", "on_stop", "on_recover",
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
    ("reason", "u1"), ("position_side", "u1"), ("reserved", "u1", (5,)),
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
    ("position_side", "u1"), ("reserved", "u1", (7,)),
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
    ("account_sequence", "<u8"), ("position_side", "u1"), ("reserved", "u1", (7,)),
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
    ("reduce_only", "u1"), ("position_side", "u1"), ("reserved", "u1", (2,)),
])
submit_order_request_dtype = _dtype([
    ("asset_no", "<u4"), ("account_no", "<u4"), ("price_ticks", "<i8"),
    ("qty_lots", "<i8"), ("trigger_price_ticks", "<i8"), ("gtd_expiry_ns", "<i8"),
    ("side", "u1"), ("order_type", "u1"), ("time_in_force", "u1"),
    ("reduce_only", "u1"), ("trigger_kind", "u1"), ("position_side", "u1"), ("reserved", "u1", (2,)),
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
_context_fields.extend([
    ("objects_ptr", "<u8"), ("objects_len", "<u8"),
    ("object_schema_minor", "<u4"), ("reserved_tail", "<u4"),
    ("object_query", "<u8"), ("object_context", "<u8"), ("reserved_ext", "<u8"),
])
runtime_context_dtype = _dtype(_context_fields)

native_descriptor_dtype = _dtype([
    ("struct_size", "<u4"), ("abi_version", "<u4"), ("abi_fingerprint", "u1", (32,)),
    ("state_schema_version", "<u4"), ("state_alignment", "<u4"), ("state_len", "<u8"),
    ("state_schema_hash", "u1", (32,)), ("callback_mask", "<u8"),
])


# Object domains (ABI-frozen).
DOMAIN_SYMBOL: Final = 1
DOMAIN_ACCOUNT: Final = 2
DOMAIN_CURRENCY: Final = 3
DOMAIN_PAIR: Final = 4
DOMAIN_STATUS: Final = 5
DOMAIN_FEE: Final = 6



decimal_dtype = _dtype([
    ("coefficient", "<u8"), ("scale", "u1"), ("reserved", "u1", (7,)),
])
object_ref_dtype = _dtype([
    ("domain", "<u4"), ("object_id", "<u4"), ("flags", "<u4"),
    ("schema_major", "<u2"), ("schema_minor", "<u2"),
    ("payload", "<u8"), ("payload_len", "<u8"),
])

_symbol_field_spec: list[tuple[str, object, str]] = [
    ("local_asset_no", "<u4", "u32"),
    ("asset_id", "<u4", "u32"),
    ("quote_currency_no", "<u4", "u32"),
    ("margin_currency_no", "<u4", "u32"),
    ("valuation", "<u4", "u32"),
    ("flags", "<u4", "u32"),
    ("instrument_ref", "<u8", "u64"),
    ("price_tick", decimal_dtype, "decimal"),
    ("quantity_lot", decimal_dtype, "decimal"),
    ("contract_multiplier", decimal_dtype, "decimal"),
    ("base_delta_per_lot", decimal_dtype, "decimal"),
    ("min_quantity", decimal_dtype, "decimal"),
    ("min_notional_quote", decimal_dtype, "decimal"),
    ("min_qty_lots", "<u8", "u64"),
    ("min_notional_tick_lots", "<u8", "u64"),
    ("metadata_version", "<u4", "u32"),
    ("reserved0", "<u4", "u32"),
]
_account_field_spec: list[tuple[str, object, str]] = [
    ("local_account_no", "<u4", "u32"),
    ("account_id", "<u4", "u32"),
    ("quote_currency_no", "<u4", "u32"),
    ("margin_currency_no", "<u4", "u32"),
    ("position_mode", "<u4", "u32"),
    ("margin_mode", "<u4", "u32"),
    ("margin_domain", "<u4", "u32"),
    ("flags", "<u4", "u32"),
    ("currency_start", "<u4", "u32"),
    ("currency_count", "<u4", "u32"),
    ("reserved0", "<u4", "u32"),
    ("reserved1", "<u4", "u32"),
]
_currency_field_spec: list[tuple[str, object, str]] = [
    ("local_account_no", "<u4", "u32"),
    ("currency_no", "<u4", "u32"),
    ("flags", "<u4", "u32"),
    ("reserved0", "<u4", "u32"),
    ("amount_unit", decimal_dtype, "decimal"),
]
_pair_field_spec: list[tuple[str, object, str]] = [
    ("maker_local_asset_no", "<u4", "u32"),
    ("hedge_local_asset_no", "<u4", "u32"),
    ("maker_local_account_no", "<u4", "u32"),
    ("hedge_local_account_no", "<u4", "u32"),
    ("quote_currency_no", "<u4", "u32"),
    ("flags", "<u4", "u32"),
    ("hedge_lots_per_maker_lot", "<u8", "u64"),
    ("hedge_ticks_per_maker_tick_num", "<u8", "u64"),
    ("hedge_ticks_per_maker_tick_den", "<u8", "u64"),
    ("min_hedge_lots", "<u8", "u64"),
    ("min_hedge_notional_quote", decimal_dtype, "decimal"),
    ("pair_revision", "<u4", "u32"),
    ("reserved0", "<u4", "u32"),
]
_status_field_spec: list[tuple[str, object, str]] = [
    ("lifecycle", "<u4", "u32"),
    ("entry_flags", "<u4", "u32"),
    ("last_return_code", "<i4", "i32"),
    ("last_flags", "<u4", "u32"),
    ("last_error_code", "<i4", "i32"),
    ("error_category", "<u4", "u32"),
    ("stop_deadline_ns", "<i8", "i64"),
    ("reserved0", "<u4", "u32"),
    ("reserved1", "<u4", "u32"),
]
_fee_field_spec: list[tuple[str, object, str]] = [
    ("local_account_no", "<u4", "u32"),
    ("flags", "<u4", "u32"),
    ("maker_fee_bps", "<i4", "i32"),
    ("taker_fee_bps", "<i4", "i32"),
    ("source", "<u4", "u32"),
    ("reserved0", "<u4", "u32"),
    ("observed_at_ns", "<u8", "u64"),
]


def _payload(spec: list[tuple[str, object, str]]) -> np.dtype:
    return _dtype([(name, dtype) for name, dtype, _ in spec])


symbol_payload_dtype = _payload(_symbol_field_spec)
account_payload_dtype = _payload(_account_field_spec)
currency_payload_dtype = _payload(_currency_field_spec)
pair_payload_dtype = _payload(_pair_field_spec)
status_payload_dtype = _payload(_status_field_spec)
fee_payload_dtype = _payload(_fee_field_spec)

# `(domain, name, dtype, schema_major, schema_minor, field spec)` in domain order.
OBJECT_PAYLOADS: Final = (
    (DOMAIN_SYMBOL, "TitanSymbolPayloadV1", symbol_payload_dtype, 1, 1, _symbol_field_spec),
    (DOMAIN_ACCOUNT, "TitanAccountPayloadV1", account_payload_dtype, 1, 1, _account_field_spec),
    (DOMAIN_CURRENCY, "TitanCurrencyPayloadV1", currency_payload_dtype, 1, 1, _currency_field_spec),
    (DOMAIN_PAIR, "TitanPairPayloadV1", pair_payload_dtype, 1, 1, _pair_field_spec),
    (DOMAIN_STATUS, "TitanStatusPayloadV1", status_payload_dtype, 1, 1, _status_field_spec),
    (DOMAIN_FEE, "TitanFeePayloadV1", fee_payload_dtype, 1, 1, _fee_field_spec),
)
OBJECT_PAYLOAD_DTYPES: Final = {name: dtype for _, name, dtype, _, _, _ in OBJECT_PAYLOADS}
PAYLOAD_MAJOR: Final = 1
PAYLOAD_MINOR: Final = 1
# Root-level schema facts (mirrors Rust `OBJECT_SCHEMA_MAJOR/MINOR`).
OBJECT_SCHEMA_MAJOR: Final = PAYLOAD_MAJOR
OBJECT_SCHEMA_MINOR: Final = PAYLOAD_MINOR


def object_schema_descriptor() -> dict[str, object]:
    """Canonical payload schema description; must match Rust `object_schema_descriptor`."""
    payloads = []
    for domain, name, dtype, major, minor, spec in OBJECT_PAYLOADS:
        fields = []
        for field_name, _, type_name in spec:
            field_dtype, offset = dtype.fields[field_name][:2]
            fields.append({
                "name": field_name,
                "type": type_name,
                "offset": int(offset),
                "size": int(field_dtype.itemsize),
            })
        payloads.append({
            "domain": domain,
            "name": name,
            "schema_major": major,
            "schema_minor": minor,
            "size": int(dtype.itemsize),
            "alignment": int(dtype.alignment),
            "fields": fields,
        })
    return {
        "object_schema_version": 1,
        "object_ref_size": int(object_ref_dtype.itemsize),
        "context_size": int(runtime_context_dtype.itemsize),
        "payloads": payloads,
    }


def object_schema_bytes() -> bytes:
    """Byte-exact canonical encoding shared with the Rust golden fixture."""
    return json.dumps(object_schema_descriptor(), separators=(",", ":"),
                      sort_keys=True).encode("utf-8")


OBJECT_SCHEMA_HASH: Final = hashlib.sha256(object_schema_bytes()).digest()

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
    "TitanObjectRef": object_ref_dtype,
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
            raise ValueError(f"unknown strategy handler {name!r}")
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
