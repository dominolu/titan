"""The single ``pair_arb`` state machine: fixed memory, Numba-compiled, host agnostic.

``build_kernel(parameters)`` returns one kernel instance (state arrays + compiled handlers). The
same handlers are driven by two hosts:

* :mod:`strategies.pair_arb.strategy` — the ABI v12 runtime entry (``pair_arb.strategy:build``):
  synchronous callbacks, orders emitted through the execution host / backtest command buffer;
* :mod:`strategies.pair_arb.coroutine` — the coroutine host: the same handlers are invoked from
  ``async`` entrypoints, and the emitted commands are turned into ``await broker.*`` calls.

Responsibilities (requirements §6.1/§6.3/§6.4):

* ``on_tick``   — risk posture, slot advance, maker requote, cancel timeout, re-place/hedge catch-up;
* ``on_filled`` — the only place traded quantity is accumulated; a fully filled initiator submits
  the hedge IOC immediately;
* ``on_order``  — cancel confirmations, rejects and terminal order facts; clears the slot relation;
* ``on_start`` / ``on_stop`` — readiness and owned-order cleanup.

The kernel reads facts and emits commands; it never awaits, never queries and never reconciles.
Connector normalization / reconcile / audit belong to the host (:mod:`strategies.pair_arb.connector`,
:mod:`strategies.pair_arb.reconcile`). The kernel only touches the strategy facade
(``ticks``/``fills``/``orders``/``market``/``position``/``state`` + intent-named submit/cancel) and
converts every price/quantity to integer ticks/lots before submission.
"""

from __future__ import annotations

from math import ceil, floor
from types import SimpleNamespace

import numpy as np
from numba import njit

from .state_layout import (
    DIRECTION_LONG_SPREAD,
    DIRECTION_SHORT_SPREAD,
    F64_STATE_LEN,
    F_EMERGENCY_SLIPPAGE,
    F_FILLED_GROSS_NOTIONAL,
    F_GROSS_IMBALANCE_LOTS,
    F_IMBALANCE_LOTS,
    F_LAST_HEDGE_PRICE,
    F_LAST_MAKER_EDGE,
    F_LEFT_LOT_SIZE,
    F_LEFT_PRICE_TICK,
    F_OPEN_ORDER_LOTS,
    F_PAIR_DUST_LOTS,
    F_PAIR_HEDGE_RATIO,
    F_PAIR_MAX_POSITION_LOTS,
    F_PAIR_REQUOTE_DISTANCE,
    F_PAIR_SLOT_UNIT_LOTS,
    F_PAIR_SPREAD,
    F_RIGHT_LOT_SIZE,
    F_RIGHT_PRICE_TICK,
    F_RISK_MAX_GROSS_NOTIONAL,
    F_RISK_MAX_UNHEDGED_HARD,
    F_RISK_MAX_UNHEDGED_SOFT,
    F_SLOT_HEDGE_FILLED_LOTS,
    F_SLOT_INIT_FILLED_LOTS,
    F_SLOT_TARGET_LOTS,
    F_TAKER_SLIPPAGE,
    F_TOTAL_HEDGE_FILLED_LOTS,
    F_TOTAL_INIT_FILLED_LOTS,
    F_UNHEDGED_LOTS,
    HISTORY_RING,
    I64_STATE_LEN,
    I_ACTIVE_COUNT,
    I_CANCEL_COUNT,
    I_CANCEL_TIMEOUT_NS,
    I_DIRECTION,
    I_FILL_COUNT,
    I_HEDGE_SUBMIT_COUNT,
    I_HISTORY_COUNT,
    I_HISTORY_CURSOR,
    I_HISTORY_WRAPPED,
    I_HIST_FILLED_LOTS,
    I_HIST_ORDER_ID,
    I_HIST_ROLE,
    I_HIST_SLOT_ID,
    I_HIST_STATUS,
    I_LAST_ERROR,
    I_LAST_EVENT_KIND,
    I_LEFT_ACCOUNT_NO,
    I_LEFT_ASSET_NO,
    I_MODE,
    I_NEXT_ORDER_ID,
    I_ORDER_CANCEL_FAILURES,
    I_ORDER_CANCEL_TS,
    I_ORDER_FILL_COUNT,
    I_ORDER_ID,
    I_ORDER_LAST_EVENT_TS,
    I_ORDER_ROLE,
    I_ORDER_SIDE,
    I_ORDER_SLOT,
    I_ORDER_STATUS,
    I_ORDER_STATUS_BEFORE_CANCEL,
    I_ORDER_SUBMIT_TS,
    I_ORDER_VENUE_ID,
    I_PAIR_STATUS,
    I_POSTURE,
    I_POSTURE_CHANGE_COUNT,
    I_POSTURE_LATCHED,
    I_READY,
    I_REJECT_COUNT,
    I_RIGHT_ACCOUNT_NO,
    I_RIGHT_ASSET_NO,
    I_SLOT_COMPLETED_TS,
    I_SLOT_CREATED_TS,
    I_SLOT_FIRST_IMBALANCE_TS,
    I_SLOT_HEDGE_ORDER_ID,
    I_SLOT_ID,
    I_SLOT_INIT_ORDER_ID,
    I_SLOT_START_COUNT,
    I_START_TIME_NS,
    I_UNKNOWN_COUNT,
    MAX_ACTIVE_ORDERS,
    MODE_MAKER_TAKER,
    MODE_TAKER_TAKER,
    ORDER_CANCEL_REQUESTED,
    ORDER_CANCELED,
    ORDER_EXPIRED,
    ORDER_FILLED,
    ORDER_PARTIAL,
    ORDER_REJECTED,
    ORDER_REQUESTING,
    ORDER_UNKNOWN,
    ORDER_WORKING,
    PAIR_CREATED,
    PAIR_DRAINING,
    PAIR_RUNNING,
    PAIR_STOPPED,
    POSTURE_EMERGENCY,
    POSTURE_HALT,
    POSTURE_NORMAL,
    POSTURE_RESTRICTED,
    ROLE_HEDGE,
    ROLE_INITIATOR,
    VENUE_CANCELED,
    VENUE_EXPIRED,
    VENUE_FILLED,
    VENUE_NEW,
    VENUE_PARTIALLY_FILLED,
    VENUE_REJECTED,
    f_order_filled,
    f_order_last_fill_price,
    f_order_price,
    f_order_qty,
    i_history_field,
    i_order_field,
)

STRATEGY_ID = "pair_arb"
STRATEGY_VERSION = "0.2.0"

# Error codes recorded in ``I_LAST_ERROR`` (kept small and stable for operator triage).
ERROR_NONE = 0
ERROR_CANCEL_TIMEOUT = 1
ERROR_SLOT_RELATION = 2
ERROR_FILL_MISMATCH = 3
ERROR_CAPACITY = 4


def _number(parameters, name, default=None):
    value = parameters.get(name, default)
    if value is None:
        raise ValueError(f"pair_arb requires parameter {name}")
    value = float(value)
    if not np.isfinite(value):
        raise ValueError(f"{name} must be finite")
    return value


def _integer(parameters, name, default=None):
    value = parameters.get(name, default)
    if value is None:
        raise ValueError(f"pair_arb requires parameter {name}")
    return int(value)


def _choice(parameters, name, default, allowed):
    value = parameters.get(name, default)
    if value is None:
        raise ValueError(f"pair_arb requires parameter {name}")
    if value not in allowed:
        raise ValueError(f"{name} must be one of {sorted(allowed)}")
    return value


def build_kernel(parameters):
    """Validate parameters once and return the kernel (state arrays + compiled handlers)."""

    left_asset_no = _integer(parameters, "left_asset_no", 0)
    right_asset_no = _integer(parameters, "right_asset_no", 1)
    left_account_no = _integer(parameters, "left_account_no", 0)
    right_account_no = _integer(parameters, "right_account_no", 1)
    if left_asset_no == right_asset_no:
        raise ValueError("left_asset_no and right_asset_no must be distinct")

    direction = _choice(parameters, "direction", "LONG_SPREAD",
                        {"LONG_SPREAD", "SHORT_SPREAD"})
    mode = _choice(parameters, "mode", "MAKER_TAKER", {"MAKER_TAKER", "TAKER_TAKER"})

    hedge_ratio = _number(parameters, "hedge_ratio_abs")
    if hedge_ratio <= 0.0:
        raise ValueError("hedge_ratio_abs must be positive (invariant I1)")

    left_price_tick = _number(parameters, "left_price_tick")
    left_lot_size = _number(parameters, "left_lot_size")
    right_price_tick = _number(parameters, "right_price_tick")
    right_lot_size = _number(parameters, "right_lot_size")
    if min(left_price_tick, left_lot_size, right_price_tick, right_lot_size) <= 0.0:
        raise ValueError("price ticks and lot sizes must be positive")

    slot_unit_lots = _number(parameters, "slot_unit_lots")
    max_position_lots = _number(parameters, "max_position_lots")
    dust_lots = _number(parameters, "dust_lots", 0.0)
    if slot_unit_lots <= 0.0 or max_position_lots <= 0.0:
        raise ValueError("slot_unit_lots and max_position_lots must be positive")
    if dust_lots < 0.0:
        raise ValueError("dust_lots must be non-negative")

    spread = _number(parameters, "spread", 0.0)
    requote_distance = _number(parameters, "requote_distance", 0.0)
    if spread < 0.0 or requote_distance < 0.0:
        raise ValueError("spread and requote_distance must be non-negative")

    cancel_timeout_ns = _integer(parameters, "cancel_timeout_ns", 1_000_000_000)
    if cancel_timeout_ns <= 0:
        raise ValueError("cancel_timeout_ns must be positive")
    start_time_ns = _integer(parameters, "start_time_ns", 0)
    cancel_retry_limit = _integer(parameters, "cancel_retry_limit", 3)

    taker_slippage = _number(parameters, "taker_slippage_bps", 5.0) / 10_000.0
    emergency_slippage = _number(parameters, "emergency_slippage_bps", 30.0) / 10_000.0
    if taker_slippage < 0.0 or emergency_slippage < 0.0:
        raise ValueError("slippage parameters must be non-negative")

    max_unhedged_soft = _number(parameters, "max_unhedged_lots_soft", float("inf"))
    max_unhedged_hard = _number(parameters, "max_unhedged_lots_hard", float("inf"))
    max_gross_notional = _number(parameters, "max_gross_notional", float("inf"))
    if max_unhedged_hard < max_unhedged_soft:
        raise ValueError("max_unhedged_lots_hard must be >= max_unhedged_lots_soft")

    state = np.zeros(F64_STATE_LEN, dtype=np.float64)
    state_i64 = np.zeros(I64_STATE_LEN, dtype=np.int64)

    state[F_PAIR_HEDGE_RATIO] = hedge_ratio
    state[F_PAIR_SPREAD] = spread
    state[F_PAIR_REQUOTE_DISTANCE] = requote_distance
    state[F_PAIR_DUST_LOTS] = dust_lots
    state[F_PAIR_SLOT_UNIT_LOTS] = slot_unit_lots
    state[F_PAIR_MAX_POSITION_LOTS] = max_position_lots
    state[F_LEFT_PRICE_TICK] = left_price_tick
    state[F_LEFT_LOT_SIZE] = left_lot_size
    state[F_RIGHT_PRICE_TICK] = right_price_tick
    state[F_RIGHT_LOT_SIZE] = right_lot_size
    state[F_TAKER_SLIPPAGE] = taker_slippage
    state[F_EMERGENCY_SLIPPAGE] = emergency_slippage
    state[F_RISK_MAX_UNHEDGED_SOFT] = max_unhedged_soft
    state[F_RISK_MAX_UNHEDGED_HARD] = max_unhedged_hard
    state[F_RISK_MAX_GROSS_NOTIONAL] = max_gross_notional

    state_i64[I_PAIR_STATUS] = PAIR_CREATED
    state_i64[I_POSTURE] = POSTURE_NORMAL
    state_i64[I_MODE] = MODE_MAKER_TAKER if mode == "MAKER_TAKER" else MODE_TAKER_TAKER
    state_i64[I_DIRECTION] = (
        DIRECTION_LONG_SPREAD if direction == "LONG_SPREAD" else DIRECTION_SHORT_SPREAD
    )
    state_i64[I_NEXT_ORDER_ID] = 1
    state_i64[I_CANCEL_TIMEOUT_NS] = cancel_timeout_ns
    state_i64[I_START_TIME_NS] = start_time_ns
    state_i64[I_LEFT_ASSET_NO] = left_asset_no
    state_i64[I_RIGHT_ASSET_NO] = right_asset_no
    state_i64[I_LEFT_ACCOUNT_NO] = left_account_no
    state_i64[I_RIGHT_ACCOUNT_NO] = right_account_no

    # ---------------------------------------------------------------- internal helpers

    @njit
    def posture(s):
        return s.state_i64[I_POSTURE]

    @njit
    def set_posture(s, value):
        if s.state_i64[I_POSTURE] != value:
            s.state_i64[I_POSTURE] = value
            s.state_i64[I_POSTURE_CHANGE_COUNT] += 1

    @njit
    def active_index(s, order_id):
        if order_id <= 0:
            return -1
        for index in range(MAX_ACTIVE_ORDERS):
            if s.state_i64[i_order_field(index, I_ORDER_ID)] == order_id:
                return index
        return -1

    @njit
    def first_free_index(s):
        for index in range(MAX_ACTIVE_ORDERS):
            if s.state_i64[i_order_field(index, I_ORDER_ID)] == 0:
                return index
        return -1

    @njit
    def role_index(s, role):
        slot_id = s.state_i64[I_SLOT_ID]
        if slot_id <= 0:
            return -1
        for index in range(MAX_ACTIVE_ORDERS):
            if (
                s.state_i64[i_order_field(index, I_ORDER_ID)] != 0
                and s.state_i64[i_order_field(index, I_ORDER_SLOT)] == slot_id
                and s.state_i64[i_order_field(index, I_ORDER_ROLE)] == role
            ):
                return index
        return -1

    @njit
    def order_status(s, index):
        return s.state_i64[i_order_field(index, I_ORDER_STATUS)]

    @njit
    def set_order_status(s, index, value):
        s.state_i64[i_order_field(index, I_ORDER_STATUS)] = value

    @njit
    def remaining_lots(s, index):
        return s.state[f_order_qty(index)] - s.state[f_order_filled(index)]

    @njit
    def next_order_id(s):
        value = s.state_i64[I_NEXT_ORDER_ID]
        if value <= 0 or value >= 2_000_000_000:
            value = 1
        s.state_i64[I_NEXT_ORDER_ID] = value + 1
        return value

    @njit
    def record_history(s, order_id, slot_id, role, status, filled_lots):
        cursor = s.state_i64[I_HISTORY_CURSOR]
        if s.state_i64[I_HISTORY_COUNT] == HISTORY_RING:
            s.state_i64[I_HISTORY_WRAPPED] = 1
        s.state_i64[i_history_field(cursor, I_HIST_ORDER_ID)] = order_id
        s.state_i64[i_history_field(cursor, I_HIST_SLOT_ID)] = slot_id
        s.state_i64[i_history_field(cursor, I_HIST_ROLE)] = role
        s.state_i64[i_history_field(cursor, I_HIST_STATUS)] = status
        s.state_i64[i_history_field(cursor, I_HIST_FILLED_LOTS)] = filled_lots
        s.state_i64[I_HISTORY_CURSOR] = (cursor + 1) % HISTORY_RING
        count = s.state_i64[I_HISTORY_COUNT]
        if count < HISTORY_RING:
            s.state_i64[I_HISTORY_COUNT] = count + 1

    @njit
    def attach_order(s, order_id, role, side, price_ticks, qty_lots, ts):
        index = first_free_index(s)
        if index < 0:
            s.state_i64[I_LAST_ERROR] = ERROR_CAPACITY
            return -1
        s.state_i64[i_order_field(index, I_ORDER_ID)] = order_id
        s.state_i64[i_order_field(index, I_ORDER_SLOT)] = s.state_i64[I_SLOT_ID]
        s.state_i64[i_order_field(index, I_ORDER_ROLE)] = role
        s.state_i64[i_order_field(index, I_ORDER_STATUS)] = ORDER_WORKING
        s.state_i64[i_order_field(index, I_ORDER_SIDE)] = side
        s.state_i64[i_order_field(index, I_ORDER_FILL_COUNT)] = 0
        s.state_i64[i_order_field(index, I_ORDER_CANCEL_TS)] = 0
        s.state_i64[i_order_field(index, I_ORDER_STATUS_BEFORE_CANCEL)] = ORDER_WORKING
        s.state_i64[i_order_field(index, I_ORDER_CANCEL_FAILURES)] = 0
        s.state_i64[i_order_field(index, I_ORDER_SUBMIT_TS)] = ts
        s.state_i64[i_order_field(index, I_ORDER_VENUE_ID)] = 0
        s.state_i64[i_order_field(index, I_ORDER_LAST_EVENT_TS)] = ts
        s.state[f_order_price(index)] = price_ticks
        s.state[f_order_qty(index)] = qty_lots
        s.state[f_order_filled(index)] = 0.0
        s.state[f_order_last_fill_price(index)] = 0.0
        s.state_i64[I_ACTIVE_COUNT] += 1
        return index

    @njit
    def detach_order(s, index):
        if s.state_i64[i_order_field(index, I_ORDER_ID)] == 0:
            return
        s.state_i64[i_order_field(index, I_ORDER_ID)] = 0
        s.state_i64[i_order_field(index, I_ORDER_SLOT)] = 0
        s.state_i64[i_order_field(index, I_ORDER_STATUS)] = 0
        s.state_i64[i_order_field(index, I_ORDER_ROLE)] = 0
        s.state_i64[i_order_field(index, I_ORDER_SIDE)] = 0
        s.state[f_order_price(index)] = 0.0
        s.state[f_order_qty(index)] = 0.0
        s.state[f_order_filled(index)] = 0.0
        s.state[f_order_last_fill_price(index)] = 0.0
        s.state_i64[I_ACTIVE_COUNT] -= 1

    @njit
    def archive_order(s, index, status):
        order_id = s.state_i64[i_order_field(index, I_ORDER_ID)]
        slot_id = s.state_i64[i_order_field(index, I_ORDER_SLOT)]
        role = s.state_i64[i_order_field(index, I_ORDER_ROLE)]
        filled_lots = int(s.state[f_order_filled(index)])
        record_history(s, order_id, slot_id, role, status, filled_lots)
        if slot_id == s.state_i64[I_SLOT_ID]:
            if role == ROLE_INITIATOR and s.state_i64[I_SLOT_INIT_ORDER_ID] == order_id:
                s.state_i64[I_SLOT_INIT_ORDER_ID] = 0
            if role == ROLE_HEDGE and s.state_i64[I_SLOT_HEDGE_ORDER_ID] == order_id:
                s.state_i64[I_SLOT_HEDGE_ORDER_ID] = 0
        detach_order(s, index)

    @njit
    def quantize_ticks(price, tick, side):
        if tick <= 0.0:
            return 0.0
        units = price / tick
        if side > 0:
            return floor(units + 1e-9)
        return ceil(units - 1e-9)

    @njit
    def maker_reference_price(s):
        # The taker (hedge) leg is the right leg: buy reads the ask, sell reads the bid.
        if s.state_i64[I_DIRECTION] == DIRECTION_LONG_SPREAD:
            return s.best_bid(s.state_i64[I_RIGHT_ASSET_NO])
        return s.best_ask(s.state_i64[I_RIGHT_ASSET_NO])

    @njit
    def maker_edge(s, maker_price_units, reference):
        if s.state_i64[I_DIRECTION] == DIRECTION_LONG_SPREAD:
            return reference - maker_price_units
        return maker_price_units - reference

    @njit
    def maker_target_price_units(s, reference):
        spread = s.state[F_PAIR_SPREAD]
        if s.state_i64[I_DIRECTION] == DIRECTION_LONG_SPREAD:
            return reference - spread
        return reference + spread

    @njit
    def taker_price_ticks(s, side, asset_no, price_tick):
        slippage = s.state[F_TAKER_SLIPPAGE]
        if posture(s) == POSTURE_EMERGENCY:
            slippage = s.state[F_EMERGENCY_SLIPPAGE]
        if side > 0:
            raw = s.best_ask(asset_no) * (1.0 + slippage)
        else:
            raw = s.best_bid(asset_no) * (1.0 - slippage)
        if raw <= 0.0:
            return 0.0
        return quantize_ticks(raw, price_tick, side)

    @njit
    def has_unknown_order(s):
        for index in range(MAX_ACTIVE_ORDERS):
            status = order_status(s, index)
            if status == ORDER_UNKNOWN:
                return True
        return False

    @njit
    def open_order_lots(s):
        total = 0.0
        for index in range(MAX_ACTIVE_ORDERS):
            if s.state_i64[i_order_field(index, I_ORDER_ID)] != 0:
                total += remaining_lots(s, index)
        return total

    @njit
    def risk_check(s):
        """Requirements §6.2: measure facts and update the Pair posture."""
        ratio = s.state[F_PAIR_HEDGE_RATIO]
        init_filled = s.state[F_SLOT_INIT_FILLED_LOTS]
        hedge_filled = s.state[F_SLOT_HEDGE_FILLED_LOTS]
        imbalance = init_filled * ratio - hedge_filled
        s.state[F_IMBALANCE_LOTS] = imbalance
        s.state[F_UNHEDGED_LOTS] = abs(imbalance)
        s.state[F_GROSS_IMBALANCE_LOTS] = abs(imbalance)
        s.state[F_OPEN_ORDER_LOTS] = open_order_lots(s)
        s.state[F_FILLED_GROSS_NOTIONAL] = (
            s.state[F_TOTAL_INIT_FILLED_LOTS] * s.state[F_LEFT_LOT_SIZE]
            + s.state[F_TOTAL_HEDGE_FILLED_LOTS] * s.state[F_RIGHT_LOT_SIZE]
        )

        if s.state_i64[I_POSTURE_LATCHED] == 1:
            set_posture(s, POSTURE_HALT)
            return POSTURE_HALT

        unhedged = abs(imbalance)
        soft = s.state[F_RISK_MAX_UNHEDGED_SOFT]
        hard = s.state[F_RISK_MAX_UNHEDGED_HARD]
        if unhedged > hard:
            set_posture(s, POSTURE_EMERGENCY)
            return POSTURE_EMERGENCY
        if (
            unhedged > soft
            or has_unknown_order(s)
            or s.state[F_FILLED_GROSS_NOTIONAL] > s.state[F_RISK_MAX_GROSS_NOTIONAL]
            or first_free_index(s) < 0
        ):
            set_posture(s, POSTURE_RESTRICTED)
            return POSTURE_RESTRICTED
        set_posture(s, POSTURE_NORMAL)
        return POSTURE_NORMAL

    @njit
    def can_open(s, ts):
        if s.state_i64[I_READY] != 1:
            return False
        if s.state_i64[I_PAIR_STATUS] != PAIR_RUNNING:
            return False
        if ts < s.state_i64[I_START_TIME_NS]:
            return False
        if posture(s) != POSTURE_NORMAL:
            return False
        if first_free_index(s) < 0:
            return False
        if has_unknown_order(s):
            return False
        return True

    @njit
    def hedging_allowed(s, asset_no):
        if posture(s) == POSTURE_HALT:
            return False
        return s.best_bid(asset_no) > 0.0 and s.best_ask(asset_no) > 0.0

    @njit
    def slot_imbalance(s):
        return (
            s.state[F_SLOT_INIT_FILLED_LOTS] * s.state[F_PAIR_HEDGE_RATIO]
            - s.state[F_SLOT_HEDGE_FILLED_LOTS]
        )

    @njit
    def slot_is_complete(s):
        slot_id = s.state_i64[I_SLOT_ID]
        if slot_id <= 0:
            return False
        target = s.state[F_SLOT_TARGET_LOTS]
        if target <= 0.0:
            return False
        if s.state[F_SLOT_INIT_FILLED_LOTS] < target:
            return False
        if abs(slot_imbalance(s)) > s.state[F_PAIR_DUST_LOTS]:
            return False
        if s.state_i64[I_SLOT_INIT_ORDER_ID] != 0 or s.state_i64[I_SLOT_HEDGE_ORDER_ID] != 0:
            return False
        return True

    @njit
    def remaining_capacity_lots(s):
        executed = s.state[F_TOTAL_INIT_FILLED_LOTS]
        for index in range(MAX_ACTIVE_ORDERS):
            if (
                s.state_i64[i_order_field(index, I_ORDER_ID)] != 0
                and s.state_i64[i_order_field(index, I_ORDER_ROLE)] == ROLE_INITIATOR
            ):
                executed += remaining_lots(s, index)
        return max(s.state[F_PAIR_MAX_POSITION_LOTS] - executed, 0.0)

    @njit
    def submit_initiator(s, qty_lots, ts):
        order_id = next_order_id(s)
        side = 1 if s.state_i64[I_DIRECTION] == DIRECTION_LONG_SPREAD else -1
        asset_no = s.state_i64[I_LEFT_ASSET_NO]
        account_no = s.state_i64[I_LEFT_ACCOUNT_NO]
        price_tick = s.state[F_LEFT_PRICE_TICK]
        if s.state_i64[I_MODE] == MODE_MAKER_TAKER:
            reference = maker_reference_price(s)
            if reference <= 0.0:
                s.state_i64[I_LAST_ERROR] = ERROR_SLOT_RELATION
                return -1
            price_ticks = quantize_ticks(maker_target_price_units(s, reference), price_tick, side)
            if price_ticks <= 0.0:
                return -1
            result = s.submit_maker_order(asset_no, order_id, price_ticks, qty_lots, side,
                                          account_no)
        else:
            price_ticks = taker_price_ticks(s, side, asset_no, price_tick)
            if price_ticks <= 0.0:
                return -1
            result = s.submit_taker_order(asset_no, order_id, price_ticks, qty_lots, side,
                                          account_no)
        if result != 0:
            s.state_i64[I_REJECT_COUNT] += 1
            s.state_i64[I_LAST_ERROR] = ERROR_CAPACITY if result == -1 else result
            record_history(s, order_id, s.state_i64[I_SLOT_ID], ROLE_INITIATOR, ORDER_REJECTED, 0)
            return -1
        index = attach_order(s, order_id, ROLE_INITIATOR, side, price_ticks, qty_lots, ts)
        if index < 0:
            return -1
        s.state_i64[I_SLOT_INIT_ORDER_ID] = order_id
        return order_id

    @njit
    def submit_hedge(s, qty_lots, ts):
        if s.state_i64[I_SLOT_HEDGE_ORDER_ID] != 0:
            return -1
        side = -1 if s.state_i64[I_DIRECTION] == DIRECTION_LONG_SPREAD else 1
        asset_no = s.state_i64[I_RIGHT_ASSET_NO]
        account_no = s.state_i64[I_RIGHT_ACCOUNT_NO]
        price_tick = s.state[F_RIGHT_PRICE_TICK]
        price_ticks = taker_price_ticks(s, side, asset_no, price_tick)
        if price_ticks <= 0.0:
            s.state_i64[I_LAST_ERROR] = ERROR_SLOT_RELATION
            return -1
        order_id = next_order_id(s)
        result = s.submit_taker_order(asset_no, order_id, price_ticks, qty_lots, side, account_no)
        if result != 0:
            s.state_i64[I_REJECT_COUNT] += 1
            s.state_i64[I_LAST_ERROR] = ERROR_CAPACITY if result == -1 else result
            record_history(s, order_id, s.state_i64[I_SLOT_ID], ROLE_HEDGE, ORDER_REJECTED, 0)
            return -1
        index = attach_order(s, order_id, ROLE_HEDGE, side, price_ticks, qty_lots, ts)
        if index < 0:
            return -1
        s.state_i64[I_SLOT_HEDGE_ORDER_ID] = order_id
        s.state_i64[I_HEDGE_SUBMIT_COUNT] += 1
        s.state[F_LAST_HEDGE_PRICE] = price_ticks * price_tick
        return order_id

    @njit
    def start_slot(s, ts):
        capacity = remaining_capacity_lots(s)
        if capacity <= s.state[F_PAIR_DUST_LOTS]:
            s.state_i64[I_PAIR_STATUS] = PAIR_DRAINING
            return False
        target = s.state[F_PAIR_SLOT_UNIT_LOTS]
        if target > capacity:
            target = capacity
        s.state_i64[I_SLOT_ID] += 1
        s.state_i64[I_SLOT_INIT_ORDER_ID] = 0
        s.state_i64[I_SLOT_HEDGE_ORDER_ID] = 0
        s.state[F_SLOT_TARGET_LOTS] = target
        s.state[F_SLOT_INIT_FILLED_LOTS] = 0.0
        s.state[F_SLOT_HEDGE_FILLED_LOTS] = 0.0
        s.state_i64[I_SLOT_CREATED_TS] = ts
        s.state_i64[I_SLOT_FIRST_IMBALANCE_TS] = 0
        s.state_i64[I_SLOT_COMPLETED_TS] = 0
        s.state_i64[I_SLOT_START_COUNT] += 1
        submit_initiator(s, target, ts)
        if s.state_i64[I_SLOT_INIT_ORDER_ID] != 0 and s.state_i64[I_MODE] == MODE_TAKER_TAKER:
            hedge_lots = target * s.state[F_PAIR_HEDGE_RATIO]
            if hedge_lots > s.state[F_PAIR_DUST_LOTS]:
                submit_hedge(s, hedge_lots, ts)
        return True

    @njit
    def request_cancel(s, index, ts):
        order_id = s.state_i64[i_order_field(index, I_ORDER_ID)]
        status = order_status(s, index)
        if status == ORDER_CANCEL_REQUESTED or status == ORDER_UNKNOWN:
            return
        s.state_i64[i_order_field(index, I_ORDER_STATUS_BEFORE_CANCEL)] = status
        s.state_i64[i_order_field(index, I_ORDER_CANCEL_TS)] = ts
        role = s.state_i64[i_order_field(index, I_ORDER_ROLE)]
        if role == ROLE_INITIATOR:
            asset_no = s.state_i64[I_LEFT_ASSET_NO]
            account_no = s.state_i64[I_LEFT_ACCOUNT_NO]
        else:
            asset_no = s.state_i64[I_RIGHT_ASSET_NO]
            account_no = s.state_i64[I_RIGHT_ACCOUNT_NO]
        result = s.cancel_order(order_id, asset_no, account_no)
        if result == 0:
            set_order_status(s, index, ORDER_CANCEL_REQUESTED)
            s.state_i64[I_CANCEL_COUNT] += 1
        else:
            failures = s.state_i64[i_order_field(index, I_ORDER_CANCEL_FAILURES)] + 1
            s.state_i64[i_order_field(index, I_ORDER_CANCEL_FAILURES)] = failures
            s.state_i64[I_LAST_ERROR] = result
            if failures > cancel_retry_limit:
                set_order_status(s, index, ORDER_UNKNOWN)
                s.state_i64[I_UNKNOWN_COUNT] += 1

    @njit
    def cancel_timeout_scan(s, ts):
        timeout = s.state_i64[I_CANCEL_TIMEOUT_NS]
        for index in range(MAX_ACTIVE_ORDERS):
            if order_status(s, index) != ORDER_CANCEL_REQUESTED:
                continue
            requested = s.state_i64[i_order_field(index, I_ORDER_CANCEL_TS)]
            if ts - requested > timeout:
                set_order_status(s, index, ORDER_UNKNOWN)
                s.state_i64[I_UNKNOWN_COUNT] += 1
                s.state_i64[I_LAST_ERROR] = ERROR_CANCEL_TIMEOUT

    @njit
    def flag_relation_conflict(s, index):
        """Freeze the slot instead of guessing: no replacement, posture latches to HALT."""
        s.state_i64[I_LAST_ERROR] = ERROR_SLOT_RELATION
        s.state_i64[I_POSTURE_LATCHED] = 1
        set_posture(s, POSTURE_HALT)
        if index >= 0:
            set_order_status(s, index, ORDER_UNKNOWN)
            s.state_i64[I_UNKNOWN_COUNT] += 1

    @njit
    def tick_initiator(s, ts):
        index = role_index(s, ROLE_INITIATOR)
        if index < 0:
            remaining = s.state[F_SLOT_TARGET_LOTS] - s.state[F_SLOT_INIT_FILLED_LOTS]
            if remaining <= s.state[F_PAIR_DUST_LOTS]:
                return
            if not can_open(s, ts):
                return
            submit_initiator(s, remaining, ts)
            return
        status = order_status(s, index)
        if status == ORDER_CANCEL_REQUESTED or status == ORDER_UNKNOWN:
            return
        if status == ORDER_WORKING or status == ORDER_PARTIAL:
            if s.state_i64[I_MODE] != MODE_MAKER_TAKER:
                return
            reference = maker_reference_price(s)
            if reference <= 0.0:
                return
            price_ticks = s.state[f_order_price(index)]
            edge = maker_edge(s, price_ticks * s.state[F_LEFT_PRICE_TICK], reference)
            s.state[F_LAST_MAKER_EDGE] = edge
            if abs(edge - s.state[F_PAIR_SPREAD]) > s.state[F_PAIR_REQUOTE_DISTANCE]:
                request_cancel(s, index, ts)
            return
        flag_relation_conflict(s, index)

    @njit
    def tick_hedge(s, ts):
        index = role_index(s, ROLE_HEDGE)
        if index >= 0:
            status = order_status(s, index)
            if status == ORDER_CANCEL_REQUESTED or status == ORDER_UNKNOWN:
                return
            if status == ORDER_WORKING or status == ORDER_PARTIAL:
                return
            flag_relation_conflict(s, index)
            return
        gap = slot_imbalance(s)
        if gap <= s.state[F_PAIR_DUST_LOTS]:
            return
        if not hedging_allowed(s, s.state_i64[I_RIGHT_ASSET_NO]):
            return
        submit_hedge(s, gap, ts)

    @njit
    def halt_actions(s, ts):
        for index in range(MAX_ACTIVE_ORDERS):
            status = order_status(s, index)
            if status == ORDER_WORKING or status == ORDER_PARTIAL:
                request_cancel(s, index, ts)

    @njit
    def on_start(s):
        s.state_i64[I_PAIR_STATUS] = PAIR_RUNNING
        s.state_i64[I_READY] = 1
        s.state_i64[I_LAST_EVENT_KIND] = 0

    @njit
    def on_tick(s):
        ts = s.now
        s.state_i64[I_LAST_EVENT_KIND] = s.event_kind
        cancel_timeout_scan(s, ts)
        risk_check(s)
        if posture(s) == POSTURE_HALT:
            halt_actions(s, ts)
            return
        if s.state_i64[I_SLOT_ID] == 0:
            if can_open(s, ts):
                start_slot(s, ts)
            return
        if slot_is_complete(s):
            if s.state_i64[I_SLOT_COMPLETED_TS] == 0:
                s.state_i64[I_SLOT_COMPLETED_TS] = ts
            if can_open(s, ts):
                start_slot(s, ts)
            return
        tick_initiator(s, ts)
        tick_hedge(s, ts)

    @njit
    def apply_fill(s, order_id, fill_qty, fill_price, cumulative, ts):
        index = active_index(s, order_id)
        if index < 0:
            return
        filled = s.state[f_order_filled(index)]
        if cumulative > 0.0:
            delta = cumulative - filled
        else:
            delta = fill_qty
        if delta <= 0.0:
            return
        if delta > remaining_lots(s, index) + s.state[F_PAIR_DUST_LOTS]:
            s.state_i64[I_LAST_ERROR] = ERROR_FILL_MISMATCH
            s.state_i64[I_POSTURE_LATCHED] = 1
            set_posture(s, POSTURE_HALT)
            return

        s.state[f_order_filled(index)] += delta
        s.state[f_order_last_fill_price(index)] = fill_price
        s.state_i64[i_order_field(index, I_ORDER_FILL_COUNT)] += 1
        s.state_i64[i_order_field(index, I_ORDER_LAST_EVENT_TS)] = ts
        s.state_i64[I_FILL_COUNT] += 1

        role = s.state_i64[i_order_field(index, I_ORDER_ROLE)]
        if role == ROLE_INITIATOR:
            s.state[F_SLOT_INIT_FILLED_LOTS] += delta
            s.state[F_TOTAL_INIT_FILLED_LOTS] += delta
        else:
            s.state[F_SLOT_HEDGE_FILLED_LOTS] += delta
            s.state[F_TOTAL_HEDGE_FILLED_LOTS] += delta
        if s.state_i64[I_SLOT_FIRST_IMBALANCE_TS] == 0 and slot_imbalance(s) != 0.0:
            s.state_i64[I_SLOT_FIRST_IMBALANCE_TS] = ts

        # Fill records carry no venue status: an order is done when its cumulative quantity
        # reaches the requested quantity (within dust).
        filled_now = s.state[f_order_filled(index)]
        if filled_now + s.state[F_PAIR_DUST_LOTS] >= s.state[f_order_qty(index)]:
            if role == ROLE_INITIATOR:
                archive_order(s, index, ORDER_FILLED)
                gap = slot_imbalance(s)
                if (
                    gap > s.state[F_PAIR_DUST_LOTS]
                    and s.state_i64[I_SLOT_HEDGE_ORDER_ID] == 0
                    and hedging_allowed(s, s.state_i64[I_RIGHT_ASSET_NO])
                ):
                    submit_hedge(s, gap, ts)
            else:
                archive_order(s, index, ORDER_FILLED)
                if slot_is_complete(s):
                    if s.state_i64[I_SLOT_COMPLETED_TS] == 0:
                        s.state_i64[I_SLOT_COMPLETED_TS] = ts
                else:
                    s.state_i64[I_LAST_ERROR] = ERROR_FILL_MISMATCH
        else:
            set_order_status(s, index, ORDER_PARTIAL)

    @njit
    def on_filled(s):
        fills = s.fills()
        for i in range(len(fills)):
            apply_fill(
                s,
                fills[i]["order_id"],
                fills[i]["last_fill_qty"],
                fills[i]["price"],
                fills[i]["cumulative_filled_qty"],
                fills[i]["exch_ts"],
            )

    @njit
    def apply_order_event(s, order_id, venue_status, ts):
        index = active_index(s, order_id)
        if index < 0:
            return
        s.state_i64[i_order_field(index, I_ORDER_LAST_EVENT_TS)] = ts
        if venue_status == VENUE_NEW:
            if order_status(s, index) == ORDER_REQUESTING:
                set_order_status(s, index, ORDER_WORKING)
            return
        if venue_status == VENUE_PARTIALLY_FILLED:
            if order_status(s, index) != ORDER_CANCEL_REQUESTED:
                set_order_status(s, index, ORDER_PARTIAL)
            return
        if venue_status == VENUE_CANCELED:
            archive_order(s, index, ORDER_CANCELED)
            return
        if venue_status == VENUE_FILLED:
            # Quantities arrive through on_filled; the relation ends here.
            archive_order(s, index, ORDER_FILLED)
            return
        if venue_status == VENUE_REJECTED or venue_status == VENUE_EXPIRED:
            s.state_i64[I_REJECT_COUNT] += 1
            archive_order(s, index, ORDER_REJECTED if venue_status == VENUE_REJECTED
                          else ORDER_EXPIRED)
            return

    @njit
    def on_order(s):
        orders = s.orders()
        for i in range(len(orders)):
            apply_order_event(s, orders[i]["order_id"], orders[i]["status"],
                              orders[i]["exch_ts"])

    @njit
    def on_stop(s):
        ts = s.now
        for index in range(MAX_ACTIVE_ORDERS):
            status = order_status(s, index)
            if status == ORDER_WORKING or status == ORDER_PARTIAL:
                request_cancel(s, index, ts)
        s.state_i64[I_PAIR_STATUS] = PAIR_STOPPED

    return SimpleNamespace(
        strategy_id=STRATEGY_ID,
        strategy_version=STRATEGY_VERSION,
        handlers={
            "on_start": on_start,
            "on_tick": on_tick,
            "on_filled": on_filled,
            "on_order": on_order,
            "on_stop": on_stop,
            "risk_check": risk_check,
        },
        state=state,
        state_i64=state_i64,
        metadata={
            "pair_arb_layout": {
                "f64_len": int(F64_STATE_LEN),
                "i64_len": int(I64_STATE_LEN),
                "max_active_orders": int(MAX_ACTIVE_ORDERS),
                "history_ring": int(HISTORY_RING),
            },
            "implementation": "shared-fixed-memory-kernel",
        },
    )
