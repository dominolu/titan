"""ABI callback bridges and execution host calls.

Layering: ``abi_v10.py <- callbacks.py <- context.py <- strategies``. This module owns everything
about *emitting* commands and *invoking* strategy callbacks:

* the two execution host function calls (``execution_submit`` / ``execution_cancel``);
* the live-host request structs and the backtest command buffer encoding;
* return-code constants and their meaning;
* ``validate_handler`` / ``callback_bridge`` / ``_noop`` used by the compiler.

No dtype is defined here: every structure comes from :mod:`titan_strategy.abi_v10`, so the ABI
layout keeps a single owner.
"""

from __future__ import annotations

import inspect

from numba import carray, cfunc, njit, types

from .abi_v10 import (
    EVENT_ERROR,
    EVENT_STOP,
    ORDER_COMMAND_CANCEL,
    ORDER_COMMAND_NEW,
    backtest_command_buffer_dtype,
    cancel_order_request_dtype,
    new_order_request_dtype,
    order_command_dtype,
    runtime_ctx_dtype,
    address_as_void_pointer,
)
from .intrinsic import call_execution_host

# --- host call return codes -----------------------------------------------------------------

SUBMIT_OK = 0
SUBMIT_BACKTEST_BUFFER_FULL = -1
SUBMIT_WAIT_UNSUPPORTED = -2
SUBMIT_NO_EXECUTION_PATH = -3

_RESULT_TEXT = {
    SUBMIT_OK: "accepted",
    SUBMIT_BACKTEST_BUFFER_FULL: "backtest command buffer is full",
    SUBMIT_WAIT_UNSUPPORTED: "synchronous wait is not supported",
    SUBMIT_NO_EXECUTION_PATH: "no execution host and no backtest buffer",
}


def describe_result(code: int) -> str:
    """Human-readable meaning of a submit/cancel return code (cold path / logging)."""

    return _RESULT_TEXT.get(code, f"host rejected the request ({code})")


# --- command encoding -----------------------------------------------------------------------


@njit
def write_new_order_request(ctx_arr, asset_no, order_id, price, qty, side, order_type,
                            time_in_force):
    """Fill the runtime-owned new-order request struct; returns its address."""

    request = carray(address_as_void_pointer(ctx_arr[0]["execution_request_ptr"]),
                     1, new_order_request_dtype)[0]
    request["asset_no"] = asset_no
    request["order_id"] = order_id
    request["price"] = price
    request["qty"] = qty
    request["side"] = side
    request["order_type"] = order_type
    request["time_in_force"] = time_in_force


@njit
def write_cancel_order_request(ctx_arr, asset_no, order_id):
    """Fill the runtime-owned cancel request struct."""

    request = carray(address_as_void_pointer(ctx_arr[0]["execution_request_ptr"]),
                     1, cancel_order_request_dtype)[0]
    request["asset_no"] = asset_no
    request["order_id"] = order_id


@njit
def _append_backtest_command(ctx_arr, kind, asset_no, order_id, price, qty, side, order_type,
                            time_in_force, reduce_only, trigger_kind, trigger_price, gtd_expiry_ts,
                            local_account_no):
    buffer = carray(address_as_void_pointer(ctx_arr[0]["backtest_commands"]),
                    1, backtest_command_buffer_dtype)[0]
    index = buffer["num_commands"]
    capacity = buffer["command_capacity"]
    if index >= capacity:
        return SUBMIT_BACKTEST_BUFFER_FULL
    command = carray(address_as_void_pointer(buffer["commands_ptr"]),
                     capacity, order_command_dtype)[index]
    command["kind"] = kind
    command["side"] = side
    command["time_in_force"] = time_in_force
    command["order_type"] = order_type
    command["_reserved"][0] = 1 if reduce_only else 0
    command["_reserved"][1] = trigger_kind
    command["local_account_no"] = local_account_no
    command["asset_no"] = asset_no
    command["order_id"] = order_id
    command["price"] = price
    command["qty"] = qty
    command["trigger_price"] = trigger_price
    command["gtd_expiry_ts"] = gtd_expiry_ts
    buffer["num_commands"] = index + 1
    return SUBMIT_OK


@njit
def submit_order(ctx_arr, asset_no, order_id, price, qty, side, time_in_force, order_type,
                 wait, reduce_only=False, trigger_price=0.0, trigger_kind=0, gtd_expiry_ts=0,
                 local_account_no=0):
    """Submit one order through the live execution host or the backtest command buffer."""

    if wait:
        return SUBMIT_WAIT_UNSUPPORTED
    event_kind = ctx_arr[0]["event_kind"]
    if event_kind == EVENT_ERROR or event_kind == EVENT_STOP:
        return SUBMIT_NO_EXECUTION_PATH
    if ctx_arr[0]["execution_submit"] != 0:
        write_new_order_request(ctx_arr, asset_no, order_id, price, qty, side, order_type,
                                time_in_force)
        return call_execution_host(
            ctx_arr[0]["execution_submit"],
            ctx_arr[0]["execution_context"],
            local_account_no,
            ctx_arr[0]["execution_request_ptr"],
            ctx_arr[0]["execution_task_id_ptr"],
        )
    if ctx_arr[0]["backtest_commands"] == 0:
        return SUBMIT_NO_EXECUTION_PATH
    return _append_backtest_command(
        ctx_arr, ORDER_COMMAND_NEW, asset_no, order_id, price, qty, side, order_type,
        time_in_force, reduce_only, trigger_kind, trigger_price, gtd_expiry_ts,
        local_account_no,
    )


@njit
def cancel_order(ctx_arr, asset_no, order_id, wait, local_account_no=0):
    """Cancel one order through the live execution host or the backtest command buffer."""

    if wait:
        return SUBMIT_WAIT_UNSUPPORTED
    if ctx_arr[0]["execution_cancel"] != 0:
        write_cancel_order_request(ctx_arr, asset_no, order_id)
        return call_execution_host(
            ctx_arr[0]["execution_cancel"],
            ctx_arr[0]["execution_context"],
            local_account_no,
            ctx_arr[0]["execution_request_ptr"],
            ctx_arr[0]["execution_task_id_ptr"],
        )
    if ctx_arr[0]["backtest_commands"] == 0:
        return SUBMIT_NO_EXECUTION_PATH
    return _append_backtest_command(
        ctx_arr, ORDER_COMMAND_CANCEL, asset_no, order_id, 0.0, 0.0, 0, 0, 0, False, 0, 0.0, 0,
        local_account_no,
    )


# --- callback bridges -----------------------------------------------------------------------


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


def callback_bridge(handler, strategy_factory):
    """Wrap one validated handler into the ``i32(voidptr)`` C callback the runtime expects.

    ``strategy_factory`` is the facade type the handler receives; it is passed in by the caller
    (the compiler) so that this module never has to import :mod:`titan_strategy.context`.
    """

    @cfunc(types.int32(types.voidptr))
    def bridge(ctx_ptr):
        ctx_arr = carray(ctx_ptr, 1, dtype=runtime_ctx_dtype)
        strategy = strategy_factory(ctx_arr)
        try:
            handler(strategy)
        except Exception:
            return -1000
        return 0

    return bridge
