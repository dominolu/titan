"""Private fixed-memory layout for the Numba ``pair_arb`` strategy.

These offsets are **strategy-private**: they are not part of the public strategy ABI. The ABI only
guarantees the two state segments (``state_f64`` / ``state_i64``) handed to every strategy; this
module documents how ``pair_arb`` uses them, mirroring the reference kernel in
:mod:`strategies.pair_arb.reference` (Pair + one current Slot + a fixed active-order array).

Memory contract (requirements §8):

* one Pair: scalar fields spread over the two segments;
* one current Slot: scalar fields;
* ``MAX_ACTIVE_ORDERS`` active orders: fixed per-order field blocks, never reallocated;
* a bounded terminal-order ring: the runtime has no append-only ``OrdersList`` yet, so the ring
  keeps finished order facts for audit/dedupe. A wrapped ring is recorded in
  ``I_HISTORY_WRAPPED``; it never overwrites a still-active order.

Units: prices are integer *ticks* kept in float64 (the ABI rejects fractional request prices),
quantities are integer *lots* kept in float64, timestamps are ns in int64.
"""

from numba import njit

# --- codes shared with the requirements document --------------------------------------------

ROLE_INITIATOR = 0
ROLE_HEDGE = 1

MODE_MAKER_TAKER = 1
MODE_TAKER_TAKER = 2

DIRECTION_LONG_SPREAD = 1
DIRECTION_SHORT_SPREAD = 2

PAIR_CREATED = 1
PAIR_RUNNING = 2
PAIR_DRAINING = 3
PAIR_RECONCILING = 4
PAIR_STOPPED = 5
PAIR_ERROR = 6

POSTURE_NORMAL = 0
POSTURE_RESTRICTED = 1
POSTURE_EMERGENCY = 2
POSTURE_HALT = 3

# Local order status (mirrors the reference kernel).
ORDER_REQUESTING = 1
ORDER_UNKNOWN = 2
ORDER_WORKING = 3
ORDER_PARTIAL = 4
ORDER_FILLED = 5
ORDER_CANCEL_REQUESTED = 6
ORDER_CANCELED = 7
ORDER_REJECTED = 8
ORDER_EXPIRED = 9

# Venue order status (connector::account_plugin::api_status).
VENUE_NEW = 1
VENUE_EXPIRED = 2
VENUE_FILLED = 3
VENUE_CANCELED = 4
VENUE_PARTIALLY_FILLED = 5
VENUE_REJECTED = 6
VENUE_UNKNOWN = 255

# --- capacity ------------------------------------------------------------------------------

MAX_ACTIVE_ORDERS = 4
HISTORY_RING = 64

F64_ORDER_BLOCK = 4
"""Per active order f64 fields: price, qty, filled, last fill price."""

I64_ORDER_BLOCK = 12
"""Per active order i64 fields, see ``I_ORDER_*`` below."""

I64_HISTORY_BLOCK = 5
"""Per terminal order i64 fields, see ``I_HIST_*`` below."""

# --- state_f64 layout ----------------------------------------------------------------------

F_SLOT_TARGET_LOTS = 0
F_SLOT_INIT_FILLED_LOTS = 1
F_SLOT_HEDGE_FILLED_LOTS = 2
F_IMBALANCE_LOTS = 3
F_UNHEDGED_LOTS = 4
F_GROSS_IMBALANCE_LOTS = 5
F_OPEN_ORDER_LOTS = 6
F_FILLED_GROSS_NOTIONAL = 7
F_PAIR_HEDGE_RATIO = 8
F_PAIR_SPREAD = 9
F_PAIR_REQUOTE_DISTANCE = 10
F_PAIR_DUST_LOTS = 11
F_PAIR_SLOT_UNIT_LOTS = 12
F_PAIR_MAX_POSITION_LOTS = 13
F_TOTAL_INIT_FILLED_LOTS = 14
F_TOTAL_HEDGE_FILLED_LOTS = 15
F_LEFT_PRICE_TICK = 16
F_LEFT_LOT_SIZE = 17
F_RIGHT_PRICE_TICK = 18
F_RIGHT_LOT_SIZE = 19
F_TAKER_SLIPPAGE = 20
F_EMERGENCY_SLIPPAGE = 21
F_RISK_MAX_UNHEDGED_SOFT = 22
F_RISK_MAX_UNHEDGED_HARD = 23
F_RISK_MAX_GROSS_NOTIONAL = 24
F_LAST_MAKER_EDGE = 25
F_LAST_HEDGE_PRICE = 26

F_ORDER_BASE = 32

F64_STATE_LEN = F_ORDER_BASE + F64_ORDER_BLOCK * MAX_ACTIVE_ORDERS


@njit
def f_order_price(index):
    return F_ORDER_BASE + F64_ORDER_BLOCK * index + 0


@njit
def f_order_qty(index):
    return F_ORDER_BASE + F64_ORDER_BLOCK * index + 1


@njit
def f_order_filled(index):
    return F_ORDER_BASE + F64_ORDER_BLOCK * index + 2


@njit
def f_order_last_fill_price(index):
    return F_ORDER_BASE + F64_ORDER_BLOCK * index + 3


# --- state_i64 layout ----------------------------------------------------------------------

I_PAIR_ID = 0
I_PAIR_STATUS = 1
I_POSTURE = 2
I_POSTURE_LATCHED = 3
I_MODE = 4
I_DIRECTION = 5
I_READY = 6
I_SLOT_ID = 7
I_SLOT_INIT_ORDER_ID = 8
I_SLOT_HEDGE_ORDER_ID = 9
I_ACTIVE_COUNT = 10
I_NEXT_ORDER_ID = 11
I_SLOT_CREATED_TS = 12
I_SLOT_FIRST_IMBALANCE_TS = 13
I_SLOT_COMPLETED_TS = 14
I_CANCEL_TIMEOUT_NS = 15
I_START_TIME_NS = 16
I_LAST_ERROR = 17
I_FILL_COUNT = 18
I_CANCEL_COUNT = 19
I_REJECT_COUNT = 20
I_HEDGE_SUBMIT_COUNT = 21
I_SLOT_START_COUNT = 22
I_POSTURE_CHANGE_COUNT = 23
I_UNKNOWN_COUNT = 24
I_ACTIVE_ORDER_NO = 25
I_LEFT_ASSET_NO = 26
I_RIGHT_ASSET_NO = 27
I_LEFT_ACCOUNT_NO = 28
I_RIGHT_ACCOUNT_NO = 29
I_HISTORY_CURSOR = 30
I_HISTORY_COUNT = 31
I_HISTORY_WRAPPED = 32
I_LAST_EVENT_KIND = 33

I_ORDER_BASE = 40

# Per-order i64 field offsets inside one block.
I_ORDER_ID = 0
I_ORDER_SLOT = 1
I_ORDER_ROLE = 2
I_ORDER_STATUS = 3
I_ORDER_SIDE = 4
I_ORDER_FILL_COUNT = 5
I_ORDER_CANCEL_TS = 6
I_ORDER_STATUS_BEFORE_CANCEL = 7
I_ORDER_CANCEL_FAILURES = 8
I_ORDER_SUBMIT_TS = 9
I_ORDER_VENUE_ID = 10
I_ORDER_LAST_EVENT_TS = 11

I_HISTORY_BASE = I_ORDER_BASE + I64_ORDER_BLOCK * MAX_ACTIVE_ORDERS

I64_STATE_LEN = I_HISTORY_BASE + I64_HISTORY_BLOCK * HISTORY_RING


@njit
def i_order_field(index, field):
    return I_ORDER_BASE + I64_ORDER_BLOCK * index + field


@njit
def i_history_field(slot, field):
    return I_HISTORY_BASE + I64_HISTORY_BLOCK * slot + field


# History ring fields.
I_HIST_ORDER_ID = 0
I_HIST_SLOT_ID = 1
I_HIST_ROLE = 2
I_HIST_STATUS = 3
I_HIST_FILLED_LOTS = 4
