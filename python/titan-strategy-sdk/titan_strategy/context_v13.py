"""Numba extension type backing the single-argument Strategy ABI V13 context.

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

from . import abi_v13


@dataclass(frozen=True)
class StrategyContextIdentity:
    state_descriptor: str
    abi_fingerprint: bytes


class StrategyContextType(types.Type):
    def __init__(self, state_dtype: np.dtype):
        self.state_dtype = np.dtype(state_dtype)
        self.state_type = from_dtype(self.state_dtype)
        self.identity = StrategyContextIdentity(self.state_dtype.descr.__repr__(), abi_v13.ABI_FINGERPRINT)
        super().__init__(name=f"StrategyContextV13[{self.state_dtype.descr!r}]")

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

    def resolve_now(self, typ):
        return types.int64


def _field_offset(name: str) -> int:
    return int(abi_v13.runtime_context_dtype.fields[name][1])


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
                  price_ticks, time_in_force, reduce_only, trigger_price_ticks, gtd_expiry_ns):
    if not isinstance(context_value, StrategyContextType):
        return None
    signature = types.uint64(
        context_value, account_no, asset_no, side, order_type, qty_lots, price_ticks,
        time_in_force, reduce_only, trigger_price_ticks, gtd_expiry_ns,
    )

    def codegen(context, builder, sig, args):
        del context, sig
        raw = args[0]
        request = builder.alloca(ir.ArrayType(ir.IntType(8), abi_v13.submit_order_request_dtype.itemsize))
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
        )
        for name, value, llvm_type, signed in names:
            _store_request_field(builder, request_bytes, abi_v13.submit_order_request_dtype,
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
        request = builder.alloca(ir.ArrayType(ir.IntType(8), abi_v13.cancel_order_request_dtype.itemsize))
        request_bytes = builder.bitcast(request, ir.IntType(8).as_pointer())
        builder.store(ir.Constant(request.type.pointee, None), request)
        for name, value, llvm_type in (
            ("account_no", args[1], ir.IntType(32)),
            ("asset_no", args[2], ir.IntType(32)),
            ("order_id", args[3], ir.IntType(64)),
        ):
            _store_request_field(builder, request_bytes, abi_v13.cancel_order_request_dtype,
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
                          price_ticks, time_in_force, reduce_only=False,
                          trigger_price_ticks=0, gtd_expiry_ns=0):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, asset_no, side, order_type, qty_lots,
                       price_ticks, time_in_force, reduce_only=False,
                       trigger_price_ticks=0, gtd_expiry_ns=0):
        return _submit_order(context_value, account_no, asset_no, side, order_type, qty_lots,
                             price_ticks, time_in_force, reduce_only, trigger_price_ticks,
                             gtd_expiry_ns)

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


def _borrow_array_intrinsic(method_name: str, dtype: np.dtype):
    array_type = _borrowed_array_type(dtype)
    pointer_field = f"{method_name}_ptr"
    length_field = f"{method_name}_len"

    @intrinsic
    def borrow(typingctx, context_value):
        if not isinstance(context_value, StrategyContextType):
            return None
        signature = array_type(context_value)

        def codegen(context, builder, sig, args):
            raw = args[0]
            data_address = _load_integer(builder, raw, pointer_field, ir.IntType(64))
            length = _load_integer(builder, raw, length_field, ir.IntType(64))
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
        "ticks": abi_v13.tick_dtype,
        "bars": abi_v13.bar_dtype,
        "depth": abi_v13.depth_dtype,
        "fills": abi_v13.fill_dtype,
        "order_events": abi_v13.order_event_dtype,
        "cancel_events": abi_v13.cancel_event_dtype,
        "position_events": abi_v13.position_event_dtype,
        "balance_events": abi_v13.balance_event_dtype,
        "account_state_events": abi_v13.account_state_event_dtype,
        "active_orders": abi_v13.active_order_dtype,
    }.items()
}


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


_borrow_markets = _borrow_array_intrinsic("markets", abi_v13.market_dtype)
_borrow_positions = _borrow_array_intrinsic("positions", abi_v13.position_dtype)
_borrow_balances = _borrow_array_intrinsic("balances", abi_v13.balance_dtype)
_borrow_accounts = _borrow_array_intrinsic("accounts", abi_v13.account_dtype)
_borrow_timer = _borrow_array_intrinsic("timer", abi_v13.timer_dtype)


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


_missing_timer = _missing_view_intrinsic(abi_v13.timer_dtype)
_missing_market = _missing_view_intrinsic(abi_v13.market_dtype)
_missing_position = _missing_view_intrinsic(abi_v13.position_dtype)
_missing_balance = _missing_view_intrinsic(abi_v13.balance_dtype)
_missing_account = _missing_view_intrinsic(abi_v13.account_dtype)


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
def overload_position(context_value, account_no, asset_no):
    if not isinstance(context_value, StrategyContextType):
        return None

    def implementation(context_value, account_no, asset_no):
        values = _borrow_positions(context_value)
        for index in range(len(values)):
            if values[index]["account_no"] == account_no and values[index]["asset_no"] == asset_no:
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
    abi_v13.validate_abi_descriptor(abi)
    return StrategyContextType(state_dtype)


__all__ = ["StrategyContextIdentity", "StrategyContextType", "make_context_from_pointer", "make_strategy_context_type"]
