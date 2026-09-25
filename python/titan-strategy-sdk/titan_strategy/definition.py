"""Strategy declarations shared by the V13 authoring SDK and static compiler."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import IntFlag
import re
from types import MappingProxyType
from typing import Mapping

import numpy as np

from .parameters import Parameter, parameter_json_schema, validate_parameters
from .types import EventKind, EventQos


class Capability(IntFlag):
    NONE = 0
    MARKET_DATA = 1 << 0
    ACCOUNT_DATA = 1 << 1
    ORDER_EXECUTION = 1 << 2
    TIMER = 1 << 3


_HANDLERS = (
    "on_start", "on_tick", "on_bar", "on_depth", "on_fill", "on_order", "on_cancel",
    "on_position", "on_balance", "on_account_state", "on_timer", "on_stop",
)
_EVENT_HANDLER = {
    EventKind.START: "on_start", EventKind.BBO: "on_tick", EventKind.BAR: "on_bar",
    EventKind.DEPTH: "on_depth", EventKind.FILL: "on_fill", EventKind.ORDER: "on_order",
    EventKind.CANCEL: "on_cancel", EventKind.POSITION: "on_position",
    EventKind.BALANCE: "on_balance", EventKind.ACCOUNT_STATE: "on_account_state",
    EventKind.TIMER: "on_timer", EventKind.STOP: "on_stop",
}
_RELIABLE = {
    EventKind.FILL, EventKind.ORDER, EventKind.CANCEL, EventKind.POSITION,
    EventKind.BALANCE, EventKind.ACCOUNT_STATE,
}


@dataclass(frozen=True)
class EventSubscription:
    event_kind: EventKind
    handler: str
    schema_version: int
    qos: EventQos

    def __post_init__(self) -> None:
        try:
            kind = EventKind(self.event_kind)
            qos = EventQos(self.qos)
        except ValueError as exc:
            raise ValueError("subscription uses an unknown event kind or QoS") from exc
        object.__setattr__(self, "event_kind", kind)
        object.__setattr__(self, "qos", qos)
        if self.handler not in _HANDLERS or _EVENT_HANDLER[kind] != self.handler:
            raise ValueError(f"{kind.name.lower()} must use handler {_EVENT_HANDLER[kind]!r}")
        if isinstance(self.schema_version, bool) or self.schema_version <= 0:
            raise ValueError("event schema_version must be positive")
        if kind in _RELIABLE and qos != EventQos.RELIABLE_ORDERED:
            raise ValueError(f"{kind.name.lower()} must use RELIABLE_ORDERED QoS")


@dataclass(frozen=True)
class StrategySpec:
    strategy_id: str
    strategy_version: str
    state_schema_version: int
    parameters: tuple[Parameter, ...] = ()
    subscriptions: tuple[EventSubscription, ...] = ()
    capabilities: Capability = Capability.NONE

    def __post_init__(self) -> None:
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", self.strategy_id):
            raise ValueError("strategy_id must be lowercase snake_case")
        if not re.fullmatch(r"(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:[-+][0-9A-Za-z.-]+)?", self.strategy_version):
            raise ValueError("strategy_version must be semantic version text")
        if isinstance(self.state_schema_version, bool) or self.state_schema_version <= 0:
            raise ValueError("state_schema_version must be positive")
        parameters = tuple(self.parameters)
        subscriptions = tuple(self.subscriptions)
        if len({parameter.name for parameter in parameters}) != len(parameters):
            raise ValueError("parameter names must be unique")
        if len({subscription.event_kind for subscription in subscriptions}) != len(subscriptions):
            raise ValueError("event subscriptions must be unique")
        if any(subscription.event_kind in (EventKind.START, EventKind.STOP) for subscription in subscriptions):
            raise ValueError("lifecycle handlers are declared by handlers, not subscriptions")
        if any(subscription.event_kind == EventKind.BAR for subscription in subscriptions):
            raise ValueError("ABI V13 Bar/Hybrid is unavailable until the canonical producer and backtest adapter exist")
        capabilities = Capability(self.capabilities)
        if any(s.event_kind in (EventKind.BBO, EventKind.BAR, EventKind.DEPTH) for s in subscriptions):
            if not capabilities & Capability.MARKET_DATA:
                raise ValueError("market subscriptions require MARKET_DATA capability")
        object.__setattr__(self, "parameters", parameters)
        object.__setattr__(self, "subscriptions", subscriptions)
        object.__setattr__(self, "capabilities", capabilities)

    def parameter_schema(self) -> dict[str, object]:
        return parameter_json_schema(self.parameters)

    def validate_parameters(self, values: dict[str, object]) -> dict[str, object]:
        return validate_parameters(self.parameters, values)


@dataclass(frozen=True)
class StrategyDefinition:
    spec: StrategySpec
    state: np.ndarray
    handlers: Mapping[str, object]
    metadata: Mapping[str, object] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not isinstance(self.state, np.ndarray) or self.state.shape != (1,):
            raise TypeError("state must be a length-one ndarray")
        if self.state.dtype.fields is None or not self.state.flags.c_contiguous:
            raise TypeError("state must be a C-contiguous structured ndarray")
        handlers = dict(self.handlers)
        if not handlers or any(name not in _HANDLERS for name in handlers):
            raise ValueError("handlers contain an unknown or empty handler name")
        for subscription in self.spec.subscriptions:
            if subscription.handler not in handlers:
                raise ValueError(f"subscription handler {subscription.handler!r} is missing")
        metadata = dict(self.metadata)
        object.__setattr__(self, "handlers", MappingProxyType(handlers))
        object.__setattr__(self, "metadata", MappingProxyType(metadata))

    def __reduce__(self):
        return (
            StrategyDefinition,
            (self.spec, self.state, dict(self.handlers), dict(self.metadata)),
        )


EVENT_HANDLERS = _EVENT_HANDLER
STANDARD_HANDLERS = _HANDLERS

__all__ = [
    "Capability", "EVENT_HANDLERS", "EventSubscription", "STANDARD_HANDLERS",
    "StrategyDefinition", "StrategySpec",
]
