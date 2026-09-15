"""Slot-based paired-leg execution kernel (``pair_arb``).

The package ships two implementations of the same state machine:

* :mod:`strategies.pair_arb.strategy` — the ABI v12 Numba strategy the runtime loads
  (``pair_arb.strategy:build``), fixed-memory, driven by ``on_tick``/``on_filled``/``on_order``;
* :mod:`strategies.pair_arb.reference` — the coroutine reference kernel with the four async
  entrypoints (``on_tick`` / ``risk_check`` / ``on_fill`` / ``on_cancel``), full reconcile and the
  audit trail, used by the requirements-level test suite.

``build`` below is the reference constructor. The Numba entrypoint lives in
``strategies.pair_arb.strategy`` and is imported explicitly by the runtime and its tests.
"""

from .abi_v10 import (
    AuditConclusion,
    AuditRecord,
    CancelEvent,
    CancelEventKind,
    CommandOutcome,
    Direction,
    ExecutionMode,
    MarketView,
    NormalizedFill,
    OrderCommandResult,
    OrderEvent,
    OrderRequest,
    OrderStatus,
    OrderType,
    PairStatus,
    Posture,
    ReconcileReason,
    ReconcileRequest,
    Role,
    Side,
    SlotState,
    TickSnapshot,
    TimeInForce,
    VenueOrderStatus,
)
from .callbacks import StrategyCallbacks, cancel_event_from_result
from .connector import (
    ConnectorNormalizer,
    IntakeAction,
    IntakeDecision,
    VenueFillReport,
    VenueOrderReport,
)
from .context import (
    MAX_ACTIVE_ORDERS,
    ActiveOrder,
    BrokerFacade,
    OrdersList,
    OrdersListItem,
    Pair,
    PairContext,
    Slot,
    slot_state,
)
from .engine import ActionResult, PairArbEngine
from .risk import RiskLimits, RiskSnapshot, clear_halt, compute_risk, posture_for, risk_check
from .reference import PairArbStrategy, build
from .sim import SimulatedBroker

__all__ = [
    "ActionResult",
    "ActiveOrder",
    "AuditConclusion",
    "AuditRecord",
    "BrokerFacade",
    "CancelEvent",
    "CancelEventKind",
    "CommandOutcome",
    "ConnectorNormalizer",
    "Direction",
    "ExecutionMode",
    "IntakeAction",
    "IntakeDecision",
    "MAX_ACTIVE_ORDERS",
    "MarketView",
    "NormalizedFill",
    "OrderCommandResult",
    "OrderEvent",
    "OrderRequest",
    "OrderStatus",
    "OrderType",
    "OrdersList",
    "OrdersListItem",
    "Pair",
    "PairArbEngine",
    "PairArbStrategy",
    "PairContext",
    "PairStatus",
    "Posture",
    "ReconcileReason",
    "ReconcileRequest",
    "RiskLimits",
    "RiskSnapshot",
    "Role",
    "Side",
    "SimulatedBroker",
    "Slot",
    "SlotState",
    "StrategyCallbacks",
    "TickSnapshot",
    "TimeInForce",
    "VenueOrderStatus",
    "VenueFillReport",
    "VenueOrderReport",
    "build",
    "cancel_event_from_result",
    "clear_halt",
    "compute_risk",
    "posture_for",
    "risk_check",
    "slot_state",
]
