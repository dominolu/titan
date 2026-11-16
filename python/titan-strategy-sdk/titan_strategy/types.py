"""Public, stable scalar vocabulary for Strategy ABI V13.

The enums are ``IntEnum``/``IntFlag`` so Numba sees their values as fixed-width integers.  Native
layouts never contain Python enum objects.
"""

from __future__ import annotations

from enum import IntEnum


class EventKind(IntEnum):
    START = 0
    BBO = 1
    BAR = 2
    DEPTH = 3
    FILL = 4
    ORDER = 5
    CANCEL = 6
    POSITION = 7
    BALANCE = 8
    ACCOUNT_STATE = 9
    TIMER = 10
    STOP = 11
    RECOVER = 12


class EventQos(IntEnum):
    LATEST = 1
    RELIABLE_ORDERED = 2
    BEST_EFFORT = 3


class Side(IntEnum):
    BUY = 1
    SELL = 2


class CommandPurpose(IntEnum):
    """Declared intent of a submit, carried in the request's first reserved byte.

    Without a multi-binding execution envelope these declarations grant no admission
    relaxation; the host still verifies that the byte is a known value.
    """

    UNSPECIFIED = 0
    MAKER_QUOTE = 1
    KNOWN_FILL_HEDGE = 2
    RESCUE_EXIT = 3
    OWNED_EXIT = 4


class PositionSide(IntEnum):
    NET = 0
    LONG = 1
    SHORT = 2


class OrderType(IntEnum):
    LIMIT = 1
    MARKET = 2


class TimeInForce(IntEnum):
    GTC = 1
    IOC = 2
    FOK = 3
    POST_ONLY = 4


class OrderStatus(IntEnum):
    PENDING = 1
    ACCEPTED = 2
    PARTIALLY_FILLED = 3
    FILLED = 4
    CANCEL_PENDING = 5
    CANCELED = 6
    REJECTED = 7
    EXPIRED = 8
    UNKNOWN = 255


class CallbackCode(IntEnum):
    OK = 0
    HANDLER_ERROR = -1
    INVALID_CONTEXT = -2
    STATE_SCHEMA_MISMATCH = -3
    COMMAND_ERROR = -4


__all__ = [
    "CallbackCode",
    "CommandPurpose",
    "EventKind",
    "EventQos",
    "OrderStatus",
    "OrderType",
    "Side",
    "TimeInForce",
]
