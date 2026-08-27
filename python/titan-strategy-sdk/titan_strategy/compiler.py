"""Cold-path Numba strategy compiler used by the Rust worker."""

from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any

import numpy as np

from .context import callback_bridge, validate_handler, validate_runtime_descriptor

ABI_VERSION = 8
EVENT_SLOT_COUNT = 32

EVENTS = (
    (0, "on_start", "start"),
    (1, "on_order", "order"),
    (2, "on_filled", "filled"),
    (3, "on_position", "position"),
    (4, "on_funding", "funding"),
    (5, "on_bar", "bar"),
    (6, "on_tick", "tick"),
    (7, "on_timer", "timer"),
    (8, "on_error", "error"),
    (9, "on_stop", "stop"),
)


@dataclass
class CompiledStrategy:
    strategy_id: str
    strategy_version: str
    abi_version: int
    callback_addresses: tuple[int, ...]
    state_f64: np.ndarray
    state_i64: np.ndarray
    capabilities: tuple[str, ...]
    metadata: dict[str, Any]
    keepalive: tuple[object, ...]


def _member(value: object, name: str, default: Any = None) -> Any:
    if isinstance(value, dict):
        return value.get(name, default)
    return getattr(value, name, default)


def _state(value: object, name: str, dtype: np.dtype) -> np.ndarray:
    state = _member(value, name)
    if state is None:
        state = np.zeros(64, dtype=dtype)
    array = np.asarray(state)
    if array.ndim != 1 or array.dtype != dtype or not array.flags.c_contiguous:
        raise TypeError(f"{name} must be a one-dimensional C-contiguous {dtype} array")
    return array


def compile_strategy(
    entrypoint: str,
    parameters: dict[str, Any],
    runtime_abi: dict[str, Any],
) -> CompiledStrategy:
    """Import, validate and eagerly compile one process-local Numba strategy."""

    if int(runtime_abi.get("abi_version", -1)) != ABI_VERSION:
        raise RuntimeError(
            f"Runtime ABI mismatch: SDK={ABI_VERSION} "
            f"runtime={runtime_abi.get('abi_version')}"
        )
    if int(runtime_abi.get("event_slot_count", -1)) != EVENT_SLOT_COUNT:
        raise RuntimeError("Runtime callback slot count mismatch")
    if not runtime_abi.get("fingerprint"):
        raise RuntimeError("Runtime ABI descriptor is missing its fingerprint")
    validate_runtime_descriptor(runtime_abi)

    module_name, separator, function_name = entrypoint.partition(":")
    if not separator or not module_name or not function_name:
        raise ValueError("entrypoint must use 'module:function' syntax")
    module = importlib.import_module(module_name)
    build = getattr(module, function_name)
    strategy = build(dict(parameters))

    addresses = [0] * EVENT_SLOT_COUNT
    bridges: list[object] = []
    capabilities: list[str] = []
    for slot, attribute, capability in EVENTS:
        handler = _member(strategy, attribute)
        if handler is None:
            continue
        validated = validate_handler(attribute, handler)
        bridge = callback_bridge(validated)
        addresses[slot] = int(bridge.address)  # forces eager JIT compilation
        bridges.append(bridge)
        capabilities.append(capability)

    custom_callbacks = _member(strategy, "callbacks", {}) or {}
    for raw_slot, handler in custom_callbacks.items():
        slot = int(raw_slot)
        if slot < 0 or slot >= EVENT_SLOT_COUNT:
            raise ValueError(f"custom callback slot {slot} is outside the ABI table")
        if addresses[slot] != 0:
            raise ValueError(f"custom callback slot {slot} is already occupied")
        bridge = callback_bridge(validate_handler(f"callback_{slot}", handler))
        addresses[slot] = int(bridge.address)
        bridges.append(bridge)
        capabilities.append(f"custom:{slot}")

    state_f64 = _state(strategy, "state", np.dtype(np.float64))
    state_i64 = _state(strategy, "state_i64", np.dtype(np.int64))
    strategy_id = str(_member(strategy, "strategy_id", module_name.rsplit(".", 1)[-1]))
    strategy_version = str(_member(strategy, "strategy_version", "0.0.0"))
    metadata = dict(_member(strategy, "metadata", {}) or {})
    metadata["abi_fingerprint"] = str(runtime_abi["fingerprint"])

    return CompiledStrategy(
        strategy_id=strategy_id,
        strategy_version=strategy_version,
        abi_version=ABI_VERSION,
        callback_addresses=tuple(addresses),
        state_f64=state_f64,
        state_i64=state_i64,
        capabilities=tuple(capabilities),
        metadata=metadata,
        keepalive=(strategy, tuple(bridges), state_f64, state_i64),
    )
