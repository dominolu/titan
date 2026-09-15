"""Strategy-facing facade over Rust-owned ABI memory.

Layering: ``abi_v10.py <- callbacks.py <- context.py <- strategies``. This module owns the two
things a strategy needs:

* read-only views: clock, strategy state arrays, ticks/bars/fills/orders, market, position,
  balance, account state, depth, timer and funding payloads;
* order intent: ``submit_maker_order`` / ``submit_taker_order`` / ``submit_market_order`` /
  ``cancel_order`` (plus the historical ``submit_buy_order`` / ``submit_sell_order`` / ``cancel``
  signatures kept for compatibility).

It deliberately contains no dtype definition, no pointer arithmetic and no order-code table: those
live in :mod:`titan_strategy.abi_v10` (layout and vocabulary) and
:mod:`titan_strategy.callbacks` (host bridge and command encoding). A strategy therefore never
handles ABI details itself.
"""

from __future__ import annotations

import numba
from numba import carray, float64, int64
from numba.experimental import jitclass

from .abi_v10 import *  # noqa: F401,F403  (ABI content is re-exported for compatibility)
from .abi_v10 import (
    ORD_TYPE_LIMIT,
    ORD_TYPE_MARKET,
    SIDE_BUY,
    SIDE_SELL,
    TIME_IN_FORCE_IOC,
    TIME_IN_FORCE_POST_ONLY,
    account_state_dtype,
    address_as_void_pointer,
    balance_event_dtype,
    bar_item_dtype,
    depth_batch_dtype,
    depth_item_dtype,
    fill_dtype,
    funding_dtype,
    market_state_dtype,
    order_event_dtype,
    position_event_dtype,
    runtime_ctx_dtype,
    tick_item_dtype,
    timer_dtype,
    validate_abi_layout,  # noqa: F401  (new canonical name)
    validate_runtime_descriptor,  # noqa: F401  (re-exported for compatibility)
)
from .callbacks import (
    cancel_order as _cancel_order,
    callback_bridge as _callback_bridge,
    submit_order as _submit_order,
    validate_handler,  # noqa: F401  (re-exported for compatibility)
)
@jitclass([("ctx_arr", numba.from_dtype(runtime_ctx_dtype)[:])])
class Strategy:
    """Read-only ABI views plus the broker facade used by every Numba strategy."""

    def __init__(self, ctx_arr):
        self.ctx_arr = ctx_arr

    # -- clock / lifecycle ----------------------------------------------------------------

    @property
    def now(self): return self.ctx_arr[0]["now"]
    @property
    def event_kind(self): return self.ctx_arr[0]["event_kind"]
    @property
    def generation(self): return self.ctx_arr[0]["generation"]
    @property
    def last_error(self): return self.ctx_arr[0]["last_error"]
    @property
    def bar_timeframe(self): return self.ctx_arr[0]["bar_timeframe_ns"]
    @property
    def bar_close_ts(self): return self.ctx_arr[0]["bar_close_ts"]

    def stop(self): self.ctx_arr[0]["stop_requested"] = 1

    # -- event batches --------------------------------------------------------------------

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

    # -- payload views --------------------------------------------------------------------

    def payload(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      self.ctx_arr[0]["payload_len"], numba.uint8)

    def depth(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      1, depth_batch_dtype)[0]

    def depth_items(self):
        batch = self.depth()
        return carray(address_as_void_pointer(batch["items_ptr"]),
                      batch["num_items"], depth_item_dtype)

    def position_event(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      1, position_event_dtype)[0]

    def balance_event(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      1, balance_event_dtype)[0]

    def account_state(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]),
                      1, account_state_dtype)[0]

    def timer(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, timer_dtype)[0]

    def funding(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["payload_ptr"]), 1, funding_dtype)[0]

    # -- strategy state -------------------------------------------------------------------

    @property
    def state(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["state_f64_ptr"]),
                      self.ctx_arr[0]["state_f64_len"], float64)

    @property
    def state_i64(self):
        return carray(address_as_void_pointer(self.ctx_arr[0]["state_i64_ptr"]),
                      self.ctx_arr[0]["state_i64_len"], int64)

    # -- market / account views -----------------------------------------------------------

    def position(self, asset_no):
        return carray(address_as_void_pointer(self.ctx_arr[0]["positions_ptr"]),
                      self.ctx_arr[0]["num_positions"], float64)[asset_no]

    def market(self, asset_no):
        return carray(address_as_void_pointer(self.ctx_arr[0]["markets_ptr"]),
                      self.ctx_arr[0]["num_markets"], market_state_dtype)[asset_no]

    def best_bid(self, asset_no): return self.market(asset_no)["best_bid"]
    def best_ask(self, asset_no): return self.market(asset_no)["best_ask"]
    def best_bid_qty(self, asset_no): return self.market(asset_no)["best_bid_qty"]
    def best_ask_qty(self, asset_no): return self.market(asset_no)["best_ask_qty"]
    def tick_size(self, asset_no): return self.market(asset_no)["tick_size"]
    def lot_size(self, asset_no): return self.market(asset_no)["lot_size"]

    # -- broker facade --------------------------------------------------------------------

    def submit_order(self, asset_no, order_id, price, qty, side, time_in_force, order_type,
                     wait, reduce_only=False, trigger_price=0.0, trigger_kind=0,
                     gtd_expiry_ts=0, local_account_no=0):
        """Generic submit; prefer the intent-named helpers below."""

        return _submit_order(self.ctx_arr, asset_no, order_id, price, qty, side, time_in_force,
                             order_type, wait, reduce_only, trigger_price, trigger_kind,
                             gtd_expiry_ts, local_account_no)

    def submit_maker_order(self, asset_no, order_id, price, qty, side, local_account_no=0,
                           reduce_only=False, gtd_expiry_ts=0):
        """Post-only limit order (GTX): the passive leg of a paired execution."""

        return self.submit_order(asset_no, order_id, price, qty, side, TIME_IN_FORCE_POST_ONLY,
                                 ORD_TYPE_LIMIT, False, reduce_only, 0.0, 0, gtd_expiry_ts,
                                 local_account_no)

    def submit_taker_order(self, asset_no, order_id, price, qty, side, local_account_no=0,
                           reduce_only=False):
        """Aggressive limit order (IOC) with an explicit price protection level."""

        return self.submit_order(asset_no, order_id, price, qty, side, TIME_IN_FORCE_IOC,
                                 ORD_TYPE_LIMIT, False, reduce_only, 0.0, 0, 0,
                                 local_account_no)

    def submit_market_order(self, asset_no, order_id, price, qty, side, local_account_no=0,
                            reduce_only=False):
        """Market order (IOC market); ``price`` is only a protection hint for the venue."""

        return self.submit_order(asset_no, order_id, price, qty, side, TIME_IN_FORCE_IOC,
                                 ORD_TYPE_MARKET, False, reduce_only, 0.0, 0, 0,
                                 local_account_no)

    def submit_buy_order(self, asset_no, order_id, price, qty, time_in_force, order_type,
                         wait, reduce_only=False, gtd_expiry_ts=0, local_account_no=0):
        """Compatibility alias for callers that pass the raw direction/codes."""

        return self.submit_order(asset_no, order_id, price, qty, SIDE_BUY, time_in_force,
                                 order_type, wait, reduce_only, 0.0, 0, gtd_expiry_ts,
                                 local_account_no)

    def submit_sell_order(self, asset_no, order_id, price, qty, time_in_force, order_type,
                          wait, reduce_only=False, gtd_expiry_ts=0, local_account_no=0):
        """Compatibility alias for callers that pass the raw direction/codes."""

        return self.submit_order(asset_no, order_id, price, qty, SIDE_SELL, time_in_force,
                                 order_type, wait, reduce_only, 0.0, 0, gtd_expiry_ts,
                                 local_account_no)

    def cancel_order(self, order_id, asset_no, local_account_no=0):
        """Cancel one order of one asset."""

        return _cancel_order(self.ctx_arr, asset_no, order_id, False, local_account_no)

    def cancel(self, asset_no, order_id, wait, local_account_no=0):
        """Compatibility alias with the historical argument order."""

        return _cancel_order(self.ctx_arr, asset_no, order_id, wait, local_account_no)

    def submit_maker_bid(self, asset_no, order_id, price, qty, local_account_no=0):
        return self.submit_maker_order(asset_no, order_id, price, qty, SIDE_BUY, local_account_no)

    def submit_maker_ask(self, asset_no, order_id, price, qty, local_account_no=0):
        return self.submit_maker_order(asset_no, order_id, price, qty, SIDE_SELL, local_account_no)

    def submit_taker_buy(self, asset_no, order_id, price, qty, local_account_no=0):
        return self.submit_taker_order(asset_no, order_id, price, qty, SIDE_BUY, local_account_no)

    def submit_taker_sell(self, asset_no, order_id, price, qty, local_account_no=0):
        return self.submit_taker_order(asset_no, order_id, price, qty, SIDE_SELL, local_account_no)


def callback_bridge(handler):
    """Bind one validated handler to the ``Strategy`` facade used by the compiler."""

    return _callback_bridge(handler, Strategy)
