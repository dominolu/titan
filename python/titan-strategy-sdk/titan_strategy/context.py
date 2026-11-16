"""Numba extension type backing the single-argument Strategy ABI context.

This module is compiler infrastructure.  Strategy source receives ``ctx`` from the native bridge;
it never constructs a Python context object.
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
from llvmlite import ir
from numba import from_dtype
from numba.core import cgutils, types
from numba.extending import (
    intrinsic,
    lower_getattr,
    models,
    overload_method,
    register_model,
)
from numba.core.typing.templates import AttributeTemplate, infer_getattr

from . import abi


@dataclass(frozen=True)
class StrategyContextIdentity:
    state_descriptor: str
    abi_fingerprint: bytes


class StrategyContextType(types.Type):
    def __init__(self, state_dtype: np.dtype):
        self.state_dtype = np.dtype(state_dtype)
        self.state_type = from_dtype(self.state_dtype)
        self.identity = StrategyContextIdentity(self.state_dtype.descr.__repr__(), abi.ABI_FINGERPRINT)
        super().__init__(name=f"StrategyContext[{self.state_dtype.descr!r}]")

    @property
    def key(self):
        return self.identity


@register_model(StrategyContextType)
class StrategyContextModel(models.PrimitiveModel):
    def __init__(self, dmm, fe_type):
        super().__init__(dmm, fe_type, ir.IntType(8).as_pointer())


@infer_getattr
class StrategyContextAttributes(AttributeTemplate):
    key = StrategyContextType

    def resolve_state(self, typ):
        return typ.state_type

    def resolve_is_paused(self, typ):
        return types.boolean

    def resolve_now(self, typ):
        return types.int64


def _field_offset(name: str) -> int:
    return int(abi.runtime_context_dtype.fields[name][1])


def _load_integer(builder, raw_context, field: str, llvm_type):
    address = builder.gep(raw_context, [ir.Constant(ir.IntType(64), _field_offset(field))])
    return builder.load(builder.bitcast(address, llvm_type.as_pointer()))


def _cast_integer(builder, value, target, *, signed=False):
    if value.type.width == target.width:
        return value
    if value.type.width < target.width:
        return builder.sext(value, target) if signed else builder.zext(value, target)
    return builder.trunc(value, target)


def _store_request_field(builder, storage, dtype: np.dtype, name: str, value, llvm_type,
                         *, signed=False):
    address = builder.gep(storage, [ir.Constant(ir.IntType(64), int(dtype.fields[name][1]))])
    builder.store(_cast_integer(builder, value, llvm_type, signed=signed),
                  builder.bitcast(address, llvm_type.as_pointer()))


@lower_getattr(StrategyContextType, "state")
def lower_context_state(context, builder, typ, value):
    address = _load_integer(builder, value, "state_ptr", ir.IntType(64))
    pointer = builder.inttoptr(address, ir.IntType(8).as_pointer())
    return context.data_model_manager[typ.state_type].load_from_data_pointer(builder, pointer)


@lower_getattr(StrategyContextType, "now")
def lower_context_now(context, builder, typ, value):
    del context, typ
    return _load_integer(builder, value, "now_ns", ir.IntType(64))


@lower_getattr(StrategyContextType, "is_paused")
def lower_context_is_paused(context, builder, typ, value):
    del context, typ
    flags = _load_integer(builder, value, "flags", ir.IntType(32))
    return builder.icmp_unsigned("!=", builder.and_(flags, ir.Constant(ir.IntType(32), 2)), ir.Constant(ir.IntType(32), 0))


@intrinsic
def _complete_stop(typingctx, context_value):
    if not isinstance(context_value, StrategyContextType):
        return None
    signature = types.void(context_value)

    def codegen(context, builder, sig, args):
        del context, sig
        raw = args[0]
        address = builder.gep(raw, [ir.Constant(ir.IntType(64), _field_offset("flags"))])
        pointer = builder.bitcast(address, ir.IntType(32).as_pointer())
        builder.store(builder.or_(builder.load(pointer), ir.Constant(ir.IntType(32), 1)), pointer)
        return cgutils.get_null_value(ir.IntType(8).as_pointer())

    return signature, codegen


@overload_method(StrategyContextType, "complete_stop", inline="always")
def overload_complete_stop(context_value):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value):
        _complete_stop(context_value)

    return implementation


def make_context_from_pointer(context_type: StrategyContextType):
    @intrinsic
    def cast(typingctx, pointer):
        if pointer != types.voidptr:
            return None
        signature = context_type(pointer)

        def codegen(context, builder, sig, args):
            del context, sig
            return builder.bitcast(args[0], ir.IntType(8).as_pointer())

        return signature, codegen

    return cast


@intrinsic
def _submit_order(typingctx, context_value, account_no, asset_no, side, order_type, qty_lots,
                  price_ticks, time_in_force, reduce_only, trigger_price_ticks, gtd_expiry_ns,
                  position_side, purpose):
    if not isinstance(context_value, StrategyContextType):
        return None
    signature = types.uint64(
        context_value, account_no, asset_no, side, order_type, qty_lots, price_ticks,
        time_in_force, reduce_only, trigger_price_ticks, gtd_expiry_ns, position_side, purpose,
    )

    def codegen(context, builder, sig, args):
        del context, sig
        raw = args[0]
        request = builder.alloca(ir.ArrayType(ir.IntType(8), abi.submit_order_request_dtype.itemsize))
        request_bytes = builder.bitcast(request, ir.IntType(8).as_pointer())
        builder.store(ir.Constant(request.type.pointee, None), request)
        names = (
            ("account_no", args[1], ir.IntType(32), False),
            ("asset_no", args[2], ir.IntType(32), False),
            ("side", args[3], ir.IntType(8), False),
            ("order_type", args[4], ir.IntType(8), False),
            ("qty_lots", args[5], ir.IntType(64), True),
            ("price_ticks", args[6], ir.IntType(64), True),
            ("time_in_force", args[7], ir.IntType(8), False),
            ("reduce_only", args[8], ir.IntType(8), False),
            ("trigger_price_ticks", args[9], ir.IntType(64), True),
            ("gtd_expiry_ns", args[10], ir.IntType(64), True),
            ("position_side", args[11], ir.IntType(8), False),
            # Declared intent travels in reserved[0]; a host that ignores it behaves as before.
            ("reserved", args[12], ir.IntType(8), False),
        )
        for name, value, llvm_type, signed in names:
            _store_request_field(builder, request_bytes, abi.submit_order_request_dtype,
                                 name, value, llvm_type, signed=signed)
        output = builder.alloca(ir.IntType(64))
        builder.store(ir.Constant(ir.IntType(64), 0), output)
        command_context = _load_integer(builder, raw, "command_context", ir.IntType(64))
        function_address = _load_integer(builder, raw, "submit_order", ir.IntType(64))
        function_type = ir.FunctionType(
            ir.IntType(32),
            [ir.IntType(8).as_pointer(), ir.IntType(8).as_pointer(), ir.IntType(64).as_pointer()],
        )
        result = builder.call(
            builder.inttoptr(function_address, function_type.as_pointer()),
            [builder.inttoptr(command_context, ir.IntType(8).as_pointer()), request_bytes, output],
        )
        failed = builder.icmp_signed("<", result, ir.Constant(ir.IntType(32), 0))
        error_address = builder.gep(
            raw, [ir.Constant(ir.IntType(64), _field_offset("last_error_code"))]
        )
        previous = builder.load(builder.bitcast(error_address, ir.IntType(32).as_pointer()))
        builder.store(builder.select(failed, result, previous),
                      builder.bitcast(error_address, ir.IntType(32).as_pointer()))
        return builder.load(output)

    return signature, codegen


@intrinsic
def _cancel_order(typingctx, context_value, account_no, asset_no, order_id):
    if not isinstance(context_value, StrategyContextType):
        return None
    signature = types.uint64(context_value, account_no, asset_no, order_id)

    def codegen(context, builder, sig, args):
        del context, sig
        raw = args[0]
        request = builder.alloca(ir.ArrayType(ir.IntType(8), abi.cancel_order_request_dtype.itemsize))
        request_bytes = builder.bitcast(request, ir.IntType(8).as_pointer())
        builder.store(ir.Constant(request.type.pointee, None), request)
        for name, value, llvm_type in (
            ("account_no", args[1], ir.IntType(32)),
            ("asset_no", args[2], ir.IntType(32)),
            ("order_id", args[3], ir.IntType(64)),
        ):
            _store_request_field(builder, request_bytes, abi.cancel_order_request_dtype,
                                 name, value, llvm_type)
        output = builder.alloca(ir.IntType(64))
        builder.store(ir.Constant(ir.IntType(64), 0), output)
        command_context = _load_integer(builder, raw, "command_context", ir.IntType(64))
        function_address = _load_integer(builder, raw, "cancel_order", ir.IntType(64))
        function_type = ir.FunctionType(
            ir.IntType(32),
            [ir.IntType(8).as_pointer(), ir.IntType(8).as_pointer(), ir.IntType(64).as_pointer()],
        )
        result = builder.call(
            builder.inttoptr(function_address, function_type.as_pointer()),
            [builder.inttoptr(command_context, ir.IntType(8).as_pointer()), request_bytes, output],
        )
        failed = builder.icmp_signed("<", result, ir.Constant(ir.IntType(32), 0))
        error_address = builder.gep(
            raw, [ir.Constant(ir.IntType(64), _field_offset("last_error_code"))]
        )
        previous = builder.load(builder.bitcast(error_address, ir.IntType(32).as_pointer()))
        builder.store(builder.select(failed, result, previous),
                      builder.bitcast(error_address, ir.IntType(32).as_pointer()))
        return builder.load(output)

    return signature, codegen


@overload_method(StrategyContextType, "submit_order", inline="always")
def overload_submit_order(context_value, account_no, asset_no, side, order_type, qty_lots,
                          price_ticks, time_in_force, reduce_only=False, position_side=0,
                          purpose=0):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, asset_no, side, order_type, qty_lots,
                       price_ticks, time_in_force, reduce_only=False, position_side=0,
                       purpose=0):
        return _submit_order(context_value, account_no, asset_no, side, order_type, qty_lots,
                             price_ticks, time_in_force, reduce_only, 0, 0, position_side,
                             purpose)

    return implementation


@overload_method(StrategyContextType, "cancel_order", inline="always")
def overload_cancel_order(context_value, account_no, asset_no, order_id):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, asset_no, order_id):
        return _cancel_order(context_value, account_no, asset_no, order_id)

    return implementation


def _borrowed_array_type(dtype: np.dtype) -> types.Array:
    return types.Array(from_dtype(dtype), 1, "C", readonly=True, aligned=True)


def _borrow_array_intrinsic(method_name: str, dtype: np.dtype, *, recovery_payload=False, payload_event=12):
    array_type = _borrowed_array_type(dtype)
    pointer_field = "event_payload_ptr" if recovery_payload else f"{method_name}_ptr"
    length_field = "event_payload_len" if recovery_payload else f"{method_name}_len"

    @intrinsic
    def borrow(typingctx, context_value):
        if not isinstance(context_value, StrategyContextType):
            return None
        signature = array_type(context_value)

        def codegen(context, builder, sig, args):
            raw = args[0]
            data_address = _load_integer(builder, raw, pointer_field, ir.IntType(64))
            length = _load_integer(builder, raw, length_field, ir.IntType(64))
            if recovery_payload:
                size = ir.Constant(ir.IntType(64), dtype.itemsize)
                valid = builder.and_(
                    builder.icmp_unsigned("==", _load_integer(builder, raw, "event_kind", ir.IntType(32)), ir.Constant(ir.IntType(32), payload_event)),
                    builder.icmp_unsigned("==", builder.urem(length, size), ir.Constant(ir.IntType(64), 0)),
                )
                valid = builder.and_(valid, builder.or_(
                    builder.icmp_unsigned("==", length, ir.Constant(ir.IntType(64), 0)),
                    builder.icmp_unsigned("!=", data_address, ir.Constant(ir.IntType(64), 0)),
                ))
                error = builder.gep(raw, [ir.Constant(ir.IntType(64), _field_offset("last_error_code"))])
                error = builder.bitcast(error, ir.IntType(32).as_pointer())
                builder.store(builder.select(valid, builder.load(error), ir.Constant(ir.IntType(32), -2)), error)
                length = builder.select(valid, builder.udiv(length, size), ir.Constant(ir.IntType(64), 0))
            array = context.make_array(array_type)(context, builder)
            array.data = builder.bitcast(
                builder.inttoptr(data_address, ir.IntType(8).as_pointer()),
                context.get_data_type(array_type.dtype).as_pointer(),
            )
            array.shape = cgutils.pack_array(builder, [length])
            array.strides = cgutils.pack_array(
                builder, [ir.Constant(ir.IntType(64), dtype.itemsize)]
            )
            array.nitems = length
            array.itemsize = ir.Constant(ir.IntType(64), dtype.itemsize)
            array.meminfo = cgutils.get_null_value(array.meminfo.type)
            array.parent = cgutils.get_null_value(array.parent.type)
            return array._getvalue()

        return signature, codegen

    return borrow


_BORROW = {
    name: _borrow_array_intrinsic(name, dtype)
    for name, dtype in {
        "ticks": abi.tick_dtype,
        "bars": abi.bar_dtype,
        "depth": abi.depth_dtype,
        "fills": abi.fill_dtype,
        "order_events": abi.order_event_dtype,
        "cancel_events": abi.cancel_event_dtype,
        "position_events": abi.position_event_dtype,
        "balance_events": abi.balance_event_dtype,
        "account_state_events": abi.account_state_event_dtype,
        "active_orders": abi.active_order_dtype,
    }.items()
}
_BORROW["recovery_orders"] = _borrow_array_intrinsic("recovery_orders", abi.active_order_dtype,
                                                   recovery_payload=True)


# HOST_DEPTH_100 payload version 1; not part of the frozen item/context ABI.
_DEPTH_BATCH_DTYPE = np.dtype([
    ("version", "<u4"), ("asset_no", "<u4"), ("stream_epoch", "<u8"),
    ("first_update_sequence", "<u8"), ("last_update_sequence", "<u8"),
    ("publication_sequence", "<u8"), ("valid", "u1"), ("reset", "u1"),
    ("reserved", "u1", (6,)),
], align=True)
_BORROW["depth_batch"] = _borrow_array_intrinsic("depth_batch", _DEPTH_BATCH_DTYPE,
                                              recovery_payload=True, payload_event=3)


def _register_borrow_method(name: str) -> None:
    intrinsic_function = _BORROW[name]

    @overload_method(StrategyContextType, name, inline="always")
    def overload(context_value):
        if not isinstance(context_value, StrategyContextType):
            return None

        def implementation(context_value):
            return intrinsic_function(context_value)

        return implementation


for _name in _BORROW:
    _register_borrow_method(_name)


_borrow_markets = _borrow_array_intrinsic("markets", abi.market_dtype)
_borrow_positions = _borrow_array_intrinsic("positions", abi.position_dtype)
_borrow_balances = _borrow_array_intrinsic("balances", abi.balance_dtype)
_borrow_accounts = _borrow_array_intrinsic("accounts", abi.account_dtype)
_borrow_timer = _borrow_array_intrinsic("timer", abi.timer_dtype)


def _missing_view_intrinsic(dtype: np.dtype):
    record_type = from_dtype(dtype)

    @intrinsic
    def missing(typingctx, context_value):
        if not isinstance(context_value, StrategyContextType):
            return None
        signature = record_type(context_value)

        def codegen(context, builder, sig, args):
            raw = args[0]
            error_address = builder.gep(
                raw, [ir.Constant(ir.IntType(64), _field_offset("last_error_code"))]
            )
            builder.store(
                ir.Constant(ir.IntType(32), -2),
                builder.bitcast(error_address, ir.IntType(32).as_pointer()),
            )
            model = context.data_model_manager[sig.return_type]
            storage = cgutils.alloca_once(builder, model.get_data_type())
            builder.store(cgutils.get_null_value(storage.type.pointee), storage)
            return model.load_from_data_pointer(builder, storage)

        return signature, codegen

    return missing


_missing_timer = _missing_view_intrinsic(abi.timer_dtype)
_missing_market = _missing_view_intrinsic(abi.market_dtype)
_missing_position = _missing_view_intrinsic(abi.position_dtype)
_missing_balance = _missing_view_intrinsic(abi.balance_dtype)
_missing_account = _missing_view_intrinsic(abi.account_dtype)


@overload_method(StrategyContextType, "timer", inline="always")
def overload_timer(context_value):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value):
        values = _borrow_timer(context_value)
        if len(values) == 0:
            return _missing_timer(context_value)
        return values[0]

    return implementation


@overload_method(StrategyContextType, "market", inline="always")
def overload_market(context_value, asset_no):
    if not isinstance(context_value, StrategyContextType) or not isinstance(asset_no, types.Integer):
        return None

    def implementation(context_value, asset_no):
        values = _borrow_markets(context_value)
        if asset_no < 0 or asset_no >= len(values):
            return _missing_market(context_value)
        return values[asset_no]

    return implementation


@overload_method(StrategyContextType, "position", inline="always")
def overload_position(context_value, account_no, asset_no, position_side=0):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, asset_no, position_side=0):
        values = _borrow_positions(context_value)
        for index in range(len(values)):
            if values[index]["account_no"] == account_no and values[index]["asset_no"] == asset_no and values[index]["position_side"] == position_side:
                return values[index]
        return _missing_position(context_value)

    return implementation


@overload_method(StrategyContextType, "balance", inline="always")
def overload_balance(context_value, account_no, currency_no):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, currency_no):
        values = _borrow_balances(context_value)
        for index in range(len(values)):
            if values[index]["account_no"] == account_no and values[index]["currency_no"] == currency_no:
                return values[index]
        return _missing_balance(context_value)

    return implementation


@overload_method(StrategyContextType, "account", inline="always")
def overload_account(context_value, account_no):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no):
        values = _borrow_accounts(context_value)
        for index in range(len(values)):
            if values[index]["account_no"] == account_no:
                return values[index]
        return _missing_account(context_value)

    return implementation


@overload_method(StrategyContextType, "best_bid_ticks", inline="always")
def overload_best_bid(context_value, asset_no):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, asset_no):
        return context_value.market(asset_no)["best_bid_ticks"]

    return implementation


@overload_method(StrategyContextType, "best_ask_ticks", inline="always")
def overload_best_ask(context_value, asset_no):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, asset_no):
        return context_value.market(asset_no)["best_ask_ticks"]

    return implementation


def make_strategy_context_type(state_dtype: np.dtype, abi: dict[str, object]) -> StrategyContextType:
    abi.validate_abi_descriptor(abi)
    return StrategyContextType(state_dtype)


__all__ = ["StrategyContextIdentity", "StrategyContextType", "make_context_from_pointer", "make_strategy_context_type"]


# --- Object table accessors (V14 tail) -------------------------------------

# Object lifecycle error: the public view required by the accessor is missing.
_MISSING_VIEW_ERROR = -2


def _set_error(builder, raw_context, code: int) -> None:
    address = builder.gep(
        raw_context, [ir.Constant(ir.IntType(64), _field_offset("last_error_code"))]
    )
    builder.store(
        ir.Constant(ir.IntType(32), code),
        builder.bitcast(address, ir.IntType(32).as_pointer()),
    )


def _ref_array_type() -> types.Array:
    return types.Array(from_dtype(abi.object_ref_dtype), 1, "C", readonly=True, aligned=True)


@intrinsic
def _borrow_refs(typingctx, context_value):
    """Borrow the object reference array; invalid pointer/length reads as an empty array."""
    if not isinstance(context_value, StrategyContextType):
        return None
    array_type = _ref_array_type()
    signature = array_type(context_value)

    def codegen(context, builder, sig, args):
        raw = args[0]
        data_address = _load_integer(builder, raw, "objects_ptr", ir.IntType(64))
        length = _load_integer(builder, raw, "objects_len", ir.IntType(64))
        size = ir.Constant(ir.IntType(64), abi.object_ref_dtype.itemsize)
        valid = builder.and_(
            builder.icmp_unsigned("==", builder.urem(length, size), ir.Constant(ir.IntType(64), 0)),
            builder.or_(
                builder.icmp_unsigned("==", length, ir.Constant(ir.IntType(64), 0)),
                builder.icmp_unsigned("!=", data_address, ir.Constant(ir.IntType(64), 0)),
            ),
        )
        with builder.if_then(builder.not_(valid), likely=False):
            _set_error(builder, raw, _MISSING_VIEW_ERROR)
        # Use a select, not a branch-local assignment: the packed shape must see the guarded value.
        length = builder.select(valid, length, ir.Constant(ir.IntType(64), 0))
        array = context.make_array(array_type)(context, builder)
        array.data = builder.bitcast(
            builder.inttoptr(data_address, ir.IntType(8).as_pointer()),
            context.get_data_type(array_type.dtype).as_pointer(),
        )
        array.shape = cgutils.pack_array(builder, [length])
        array.strides = cgutils.pack_array(builder, [size])
        array.nitems = length
        array.itemsize = size
        array.meminfo = cgutils.get_null_value(array.meminfo.type)
        array.parent = cgutils.get_null_value(array.parent.type)
        return array._getvalue()

    return signature, codegen


def _make_payload_reader(dtype: np.dtype):
    """Intrinsic returning one payload record, or a zero record when the bytes are not proven."""
    record_type = from_dtype(dtype)

    @intrinsic
    def read(typingctx, address, length):
        if not isinstance(address, types.Integer) or not isinstance(length, types.Integer):
            return None
        signature = record_type(address, length)

        def codegen(context, builder, sig, args):
            addr, size = args
            model = context.data_model_manager[sig.return_type]
            storage = cgutils.alloca_once(builder, model.get_data_type())
            builder.store(cgutils.get_null_value(storage.type.pointee), storage)
            valid = builder.and_(
                builder.icmp_unsigned("!=", addr, ir.Constant(ir.IntType(64), 0)),
                builder.icmp_unsigned(">=", size, ir.Constant(ir.IntType(64), dtype.itemsize)),
            )
            with builder.if_then(valid):
                source = builder.inttoptr(addr, model.get_data_type().as_pointer())
                builder.store(builder.load(source), storage)
            return model.load_from_data_pointer(builder, storage)

        return signature, codegen

    return read


_read_symbol = _make_payload_reader(abi.symbol_payload_dtype)
_read_account = _make_payload_reader(abi.account_payload_dtype)
_read_currency = _make_payload_reader(abi.currency_payload_dtype)
_read_pair = _make_payload_reader(abi.pair_payload_dtype)
_read_status = _make_payload_reader(abi.status_payload_dtype)
_read_fee = _make_payload_reader(abi.fee_payload_dtype)


def _make_missing(dtype: np.dtype, *, raise_error: bool):
    record_type = from_dtype(dtype)

    @intrinsic
    def missing(typingctx, context_value):
        if not isinstance(context_value, StrategyContextType):
            return None
        signature = record_type(context_value)

        def codegen(context, builder, sig, args):
            raw = args[0]
            if raise_error:
                _set_error(builder, raw, _MISSING_VIEW_ERROR)
            model = context.data_model_manager[sig.return_type]
            storage = cgutils.alloca_once(builder, model.get_data_type())
            builder.store(cgutils.get_null_value(storage.type.pointee), storage)
            return model.load_from_data_pointer(builder, storage)

        return signature, codegen

    return missing


_missing_object_symbol = _make_missing(abi.symbol_payload_dtype, raise_error=True)
_missing_object_account = _make_missing(abi.account_payload_dtype, raise_error=True)
_missing_object_currency = _make_missing(abi.currency_payload_dtype, raise_error=True)
_missing_object_pair = _make_missing(abi.pair_payload_dtype, raise_error=True)
_missing_object_status = _make_missing(abi.status_payload_dtype, raise_error=True)
# Fees are optional: absence is `valid=0`, never an error.
_missing_object_fee = _make_missing(abi.fee_payload_dtype, raise_error=False)

_SYMBOL = abi.DOMAIN_SYMBOL
_ACCOUNT = abi.DOMAIN_ACCOUNT
_CURRENCY = abi.DOMAIN_CURRENCY
_PAIR = abi.DOMAIN_PAIR
_STATUS = abi.DOMAIN_STATUS
_FEE = abi.DOMAIN_FEE
_REF_VALID = 1
_SCHEMA_MAJOR = 1
_SCHEMA_MINOR = 1


@overload_method(StrategyContextType, "symbol", inline="always")
def overload_symbol(context_value, asset_no):
    if not isinstance(context_value, StrategyContextType) or not isinstance(asset_no, types.Integer):
        return None

    def implementation(context_value, asset_no):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _SYMBOL
                and reference["object_id"] == asset_no
                and reference["flags"] & _REF_VALID != 0
                and reference["schema_major"] == _SCHEMA_MAJOR
                and reference["schema_minor"] >= _SCHEMA_MINOR
                and reference["payload"] != 0
                and reference["payload"] & 7 == 0
            ):
                return _read_symbol(reference["payload"], reference["payload_len"])
        return _missing_object_symbol(context_value)

    return implementation


@overload_method(StrategyContextType, "account_info", inline="always")
def overload_account_info(context_value, account_no):
    if not isinstance(context_value, StrategyContextType) or not isinstance(account_no, types.Integer):
        return None

    def implementation(context_value, account_no):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _ACCOUNT
                and reference["object_id"] == account_no
                and reference["flags"] & _REF_VALID != 0
                and reference["schema_major"] == _SCHEMA_MAJOR
                and reference["schema_minor"] >= _SCHEMA_MINOR
                and reference["payload"] != 0
                and reference["payload"] & 7 == 0
            ):
                return _read_account(reference["payload"], reference["payload_len"])
        return _missing_object_account(context_value)

    return implementation


@overload_method(StrategyContextType, "currency", inline="always")
def overload_currency(context_value, account_no, currency_no):
    if not isinstance(context_value, StrategyContextType):
        return None
    if not isinstance(account_no, types.Integer) or not isinstance(currency_no, types.Integer):
        return None

    def implementation(context_value, account_no, currency_no):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _ACCOUNT
                and reference["object_id"] == account_no
                and reference["flags"] & _REF_VALID != 0
                and reference["schema_major"] == _SCHEMA_MAJOR
                and reference["schema_minor"] >= _SCHEMA_MINOR
                and reference["payload"] != 0
                and reference["payload"] & 7 == 0
            ):
                account = _read_account(reference["payload"], reference["payload_len"])
                start = account["currency_start"]
                end = start + account["currency_count"]
                # The account's currency block is contiguous in the currency domain; scan it without
                # carrying loop state so Numba's SSA sees a straight-line data flow.
                for other in range(len(refs)):
                    currency_reference = refs[other]
                    if (
                        currency_reference["domain"] == _CURRENCY
                        and currency_reference["object_id"] >= start
                        and currency_reference["object_id"] < end
                        and currency_reference["flags"] & _REF_VALID != 0
                        and currency_reference["schema_major"] == _SCHEMA_MAJOR
                        and currency_reference["schema_minor"] >= _SCHEMA_MINOR
                    ):
                        payload = _read_currency(
                            currency_reference["payload"], currency_reference["payload_len"]
                        )
                        if (
                            payload["local_account_no"] == account_no
                            and payload["currency_no"] == currency_no
                        ):
                            return payload
                return _missing_object_currency(context_value)
        return _missing_object_currency(context_value)

    return implementation


@overload_method(StrategyContextType, "pair", inline="always")
def overload_pair(context_value, pair_no):
    if not isinstance(context_value, StrategyContextType) or not isinstance(pair_no, types.Integer):
        return None

    def implementation(context_value, pair_no):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _PAIR
                and reference["object_id"] == pair_no
                and reference["flags"] & _REF_VALID != 0
                and reference["schema_major"] == _SCHEMA_MAJOR
                and reference["schema_minor"] >= _SCHEMA_MINOR
                and reference["payload"] != 0
                and reference["payload"] & 7 == 0
            ):
                return _read_pair(reference["payload"], reference["payload_len"])
        return _missing_object_pair(context_value)

    return implementation


@overload_method(StrategyContextType, "status", inline="always")
def overload_status(context_value):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _STATUS
                and reference["object_id"] == 0
                and reference["flags"] & _REF_VALID != 0
                and reference["schema_major"] == _SCHEMA_MAJOR
                and reference["schema_minor"] >= _SCHEMA_MINOR
                and reference["payload"] != 0
                and reference["payload"] & 7 == 0
            ):
                return _read_status(reference["payload"], reference["payload_len"])
        return _missing_object_status(context_value)

    return implementation


@overload_method(StrategyContextType, "fee", inline="always")
def overload_fee(context_value, account_no):
    if not isinstance(context_value, StrategyContextType) or not isinstance(account_no, types.Integer):
        return None

    def implementation(context_value, account_no):
        refs = _borrow_refs(context_value)
        for index in range(len(refs)):
            reference = refs[index]
            if (
                reference["domain"] == _FEE
                and reference["object_id"] == account_no
                and reference["flags"] & _REF_VALID != 0
            ):
                payload = _read_fee(reference["payload"], reference["payload_len"])
                if payload["flags"] & 1 != 0:
                    return payload
        return _missing_object_fee(context_value)

    return implementation


__all__ = ["StrategyContextType", "make_strategy_context_type"]
