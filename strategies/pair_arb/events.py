"""Host-side event and audit vocabulary for ``pair_arb``.

This is *not* ABI content: the wire/layout definitions live in ``titan_strategy.abi_v10`` and are
never duplicated here. These are the runtime-side facts a host produces while driving the kernel:
raw venue reports, normalized fills, broker call results, reconcile requests and audit records.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntEnum
from typing import List, Optional

from .state_layout import (
    ORDER_CANCELED,
    ORDER_EXPIRED,
    ORDER_FILLED,
    ORDER_PARTIAL,
    ORDER_REJECTED,
    ORDER_UNKNOWN,
    ORDER_WORKING,
)


class VenueOrderStatus(IntEnum):
    """``connector::account_plugin::api_status`` encoding."""

    NEW = 1
    EXPIRED = 2
    FILLED = 3
    CANCELED = 4
    PARTIALLY_FILLED = 5
    REJECTED = 6
    UNKNOWN = 255


class CommandOutcome(IntEnum):
    """Result of one broker request."""

    ACCEPTED = 0
    REJECTED = 1
    TIMEOUT = 2
    TRANSPORT_ERROR = 3
    UNKNOWN = 4

    @property
    def accepted(self) -> bool:
        return self is CommandOutcome.ACCEPTED

    @property
    def unknown(self) -> bool:
        """Requests whose outcome cannot be confirmed must never be re-sent (invariant I8)."""

        return self in (CommandOutcome.TIMEOUT, CommandOutcome.TRANSPORT_ERROR,
                        CommandOutcome.UNKNOWN)


def local_status_for_venue(status: VenueOrderStatus) -> int:
    """Map a venue status onto the kernel's local order status."""

    return {
        VenueOrderStatus.NEW: ORDER_WORKING,
        VenueOrderStatus.PARTIALLY_FILLED: ORDER_PARTIAL,
        VenueOrderStatus.FILLED: ORDER_FILLED,
        VenueOrderStatus.CANCELED: ORDER_CANCELED,
        VenueOrderStatus.REJECTED: ORDER_REJECTED,
        VenueOrderStatus.EXPIRED: ORDER_EXPIRED,
        VenueOrderStatus.UNKNOWN: ORDER_UNKNOWN,
    }[status]


@dataclass
class BrokerResult:
    """Broker response for one ``create_order`` / ``cancel_order`` call."""

    order_id: int
    outcome: CommandOutcome
    request_kind: str = "create"  # "create" | "cancel"
    venue_order_id: int = 0
    error_code: int = 0
    message: str = ""
    ts_ns: int = 0

    @property
    def accepted(self) -> bool:
        return self.outcome.accepted

    @property
    def unknown(self) -> bool:
        return self.outcome.unknown


@dataclass
class VenueFillReport:
    """Raw fill report exactly as the venue adapter received it."""

    order_id: int
    ts_ns: int
    fill_qty: Optional[float] = None
    cumulative_filled_qty: Optional[float] = None
    fill_price: float = 0.0
    venue_order_id: int = 0
    sequence: int = 0
    symbol: str = ""
    side: int = 0


@dataclass
class VenueOrderReport:
    """Raw order-state report (cancel confirmation, reject, expire, fill race)."""

    order_id: int
    status: VenueOrderStatus
    ts_ns: int
    venue_order_id: int = 0
    error_code: int = 0
    message: str = ""


@dataclass
class NormalizedFill:
    """Incremental fill fact ready for the kernel (requirements §6.3)."""

    order_id: int
    fill_qty: float
    fill_price: float
    event_ts_ns: int
    cumulative_filled_qty: Optional[float] = None
    venue_order_id: int = 0
    sequence: int = 0


class CancelEventKind(IntEnum):
    """Cancel request phases the host can report."""

    REQUEST_ACCEPTED = 1
    REQUEST_REJECTED = 2
    REQUEST_TIMEOUT = 3
    REQUEST_UNKNOWN = 4
    CANCELED = 5
    FILLED = 6
    ENDED_OTHER = 7


class ReconcileReason(IntEnum):
    """Why a Pair/order had to leave the normal event path (requirements §6.6.2)."""

    UNKNOWN_ORDER = 1
    STALE_SLOT_FILL = 2
    CUMULATIVE_REGRESSION = 3
    CUMULATIVE_JUMP = 4
    FILL_EXCEEDS_ORDER = 5
    FILL_WITHOUT_QUANTITY = 6
    DUPLICATE_OR_OUT_OF_ORDER = 7
    CANCEL_TIMEOUT = 8
    CANCEL_STATE_CONFLICT = 9
    BROKER_OUTCOME_UNKNOWN = 10
    SLOT_ORDER_CONFLICT = 11
    CAPACITY_EXHAUSTED = 12
    RISK_STAT_INCONSISTENT = 13
    ACCOUNT_MISMATCH = 14
    RECONNECT = 15
    VENUE_ONLY_ORDER = 16


class AuditConclusion(IntEnum):
    """Audit conclusions allowed by requirements §6.6.4."""

    RESOLVED = 1
    RETRY_REQUIRED = 2
    MANUAL_REQUIRED = 3


@dataclass
class ReconcileRequest:
    """Request for the host reconcile path; never a kernel concern."""

    reason: ReconcileReason
    ts_ns: int
    pair_id: str = ""
    order_id: int = 0
    slot_id: int = 0
    event_text: str = ""


@dataclass
class AuditRecord:
    """Reconcile audit record required by requirements §6.6.4."""

    reason: ReconcileReason
    ts_ns: int
    pair_id: str
    order_id: int = 0
    slot_id: int = 0
    event_text: str = ""
    local_snapshot: dict = field(default_factory=dict)
    venue_snapshot: dict = field(default_factory=dict)
    account_snapshot: dict = field(default_factory=dict)
    difference: dict = field(default_factory=dict)
    actions: List[str] = field(default_factory=list)
    conclusion: AuditConclusion = AuditConclusion.RETRY_REQUIRED
    reviewer: str = ""
    reviewed_ts_ns: int = 0
