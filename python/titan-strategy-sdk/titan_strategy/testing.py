"""Declarative, offline regression scenes executed against the linked native callbacks.

This module never sends exchange requests. Command functions only record requests and
assign deterministic local IDs. Rust owns compilation, process bounds and publication.
"""
from __future__ import annotations

import ctypes
import json
import re
from pathlib import Path

import numpy as np

from . import abi_v13
from .context_v13 import _DEPTH_BATCH_DTYPE
from .types import EventKind


def depth_book(*, bid=1000, ask=1001, quantity=10, levels=100,
               now_ns=1_000_000_000, snapshot=True):
    """JSON-compatible complete book fixture in ABI ticks/lots."""
    if not 1 <= levels <= 4096:
        raise ValueError("test book levels must be between 1 and 4096")
    return [dict(side=side, price_ticks=price, qty_lots=quantity,
                 action=1, is_snapshot=int(snapshot), exchange_ts_ns=now_ns,
                 receive_ts_ns=now_ns, level=level)
            for side in (1, 2) for level in range(levels)
            for price in ((bid - level if side == 1 else ask + level),)]


def validate_tests(cases):
    if not isinstance(cases, (list, tuple)) or len(cases) > 1024:
        raise ValueError("TESTS must be a sequence of at most 1024 scenes")
    if len(json.dumps(cases, allow_nan=False).encode()) > 8 * 1024 * 1024:
        raise ValueError("TESTS exceed the 8 MiB scene budget")
    names = set()
    for case in cases:
        if not isinstance(case, dict) or set(case) - {"name", "state", "steps", "expect"}:
            raise ValueError("invalid TESTS scene fields")
        name = case.get("name")
        if not isinstance(name, str) or not name or name in names:
            raise ValueError("TESTS names must be nonempty and unique")
        names.add(name)
        steps = case.get("steps", [])
        if not isinstance(steps, (list, tuple)) or not steps or len(steps) > 10000:
            raise ValueError("a TESTS scene must contain 1..10000 callback steps")
        if not all(isinstance(step, dict) for step in steps):
            raise ValueError("TESTS steps must be objects")
        if not case.get("expect") and not any(step.get("expect") for step in steps):
            raise ValueError("every TESTS scene needs at least one assertion")
        for step in steps:
            if not isinstance(step, dict) or set(step) - {
                "callback", "now_ns", "state", "views", "depth_batch", "recovery_orders",
                "flags", "status", "expect",
            }:
                raise ValueError("invalid TESTS step fields")
            callback = step.get("callback")
            if callback not in {"on_" + kind.name.lower() for kind in EventKind} | {"on_tick"}:
                raise ValueError("invalid TESTS callback")
    return list(cases)


def _path(root, path):
    parts = re.findall(r"[A-Za-z_]\w*|\d+", path)
    if not parts or ".".join(parts) != path.replace("[", ".").replace("]", ""):
        raise ValueError(f"invalid test state path {path!r}")
    current = root
    for part in parts[:-1]:
        current = current[int(part) if part.isdigit() else part]
    last = parts[-1]
    return current, int(last) if last.isdigit() else last


def _set_state(state, values):
    if not isinstance(values, dict):
        raise ValueError("test state overrides must be an object")
    for path, value in values.items():
        parent, key = _path(state, path)
        parent[key] = value


def _assert(values, expected):
    for path, wanted in expected.items():
        parent, key = _path(values, path)
        actual = parent[key]
        operations = wanted if isinstance(wanted, dict) else {"eq": wanted}
        if not operations:
            raise ValueError("test assertion must not be empty")
        for op, target in operations.items():
            if op == "eq": passed = np.all(actual == target)
            elif op == "ne": passed = np.all(actual != target)
            elif op == "gt": passed = np.all(actual > target)
            elif op == "ge": passed = np.all(actual >= target)
            elif op == "lt": passed = np.all(actual < target)
            elif op == "le": passed = np.all(actual <= target)
            elif op == "sum": passed = np.sum(actual) == target
            elif op == "max": passed = np.max(actual) == target
            elif op == "contains": passed = target in actual
            elif op == "not_contains": passed = target not in actual
            else: raise ValueError(f"unknown test comparison {op!r}")
            if not passed:
                raise AssertionError(f"{path}: expected {op} {target!r}, got {actual!r}")


_VIEW_DTYPES = {name: getattr(abi_v13, dtype + "_dtype") for name, dtype in (
    ("ticks", "tick"), ("depth", "depth"), ("fills", "fill"),
    ("order_events", "order_event"), ("cancel_events", "cancel_event"),
    ("position_events", "position_event"), ("balance_events", "balance_event"),
    ("account_state_events", "account_state_event"), ("timer", "timer"),
    ("markets", "market"), ("positions", "position"), ("balances", "balance"),
    ("accounts", "account"), ("active_orders", "active_order"),
)}
_KINDS = {"on_" + kind.name.lower(): int(kind) for kind in EventKind}
_KINDS["on_tick"] = int(EventKind.BBO)


def _rows(dtype, rows):
    if not isinstance(rows, list) or len(rows) > 65535:
        raise ValueError("test view must contain at most 65535 rows")
    result = np.zeros(len(rows), dtype=dtype)
    for i, row in enumerate(rows):
        for key, value in row.items():
            if key not in dtype.names:
                raise ValueError(f"unknown test view field {key!r}")
            result[i][key] = value
    return result


def run_native_tests(strategy, library_path: Path, cases):
    """Run each scene with fresh initial bytes and synchronous real native callbacks."""
    cases = validate_tests(cases)
    library = ctypes.CDLL(str(library_path))
    reports = []
    request_fn = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p,
                                 ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
    for case in cases:
        state = np.frombuffer(strategy.initial_state, dtype=strategy.state_layout.dtype).copy()
        context = np.zeros(1, dtype=abi_v13.runtime_context_dtype)
        row = context[0]
        row["struct_size"], row["abi_version"] = context.nbytes, 13
        row["state_ptr"], row["state_len"] = state.ctypes.data, state.nbytes
        row["state_alignment"] = strategy.state_layout.alignment
        row["state_schema_version"] = strategy.definition.spec.recovery_state_schema_version
        row["state_schema_hash"] = list(strategy.schema_hash)
        row["now_ns"], row["generation"], row["strategy_instance_id"] = 1_000_000_000, 1, 1
        schemas = {int(sub.event_kind): sub.schema_version for sub in strategy.definition.spec.subscriptions}
        commands = {"submit_count": 0, "cancel_count": 0}
        def submit(_context, _request, output):
            commands["submit_count"] += 1
            output[0] = commands["submit_count"]
            return 0
        def cancel(_context, _request, output):
            commands["cancel_count"] += 1
            output[0] = commands["cancel_count"]
            return 0
        submit_callback, cancel_callback = request_fn(submit), request_fn(cancel)
        row["submit_order"] = ctypes.cast(submit_callback, ctypes.c_void_p).value
        row["cancel_order"] = ctypes.cast(cancel_callback, ctypes.c_void_p).value
        keepalive = {}
        _set_state(state[0], case.get("state", {}))
        def expect(checks):
            roots = {**{key: state[0][key] for key in state.dtype.names},
                     "context": row, "commands": commands, "state_bytes": state.nbytes}
            _assert(roots, checks)
        for index, step in enumerate(case.get("steps", [])):
            try:
                _set_state(state[0], step.get("state", {}))
                row["now_ns"] = step.get("now_ns", int(row["now_ns"]))
                row["flags"] = step.get("flags", 0)
                callback = step["callback"]
                if callback not in strategy.definition.handlers:
                    raise ValueError(f"undeclared callback {callback}")
                row["event_kind"], row["last_error_code"] = _KINDS[callback], 0
                row["event_schema_version"] = schemas.get(_KINDS[callback], 1)
                for name, rows in step.get("views", {}).items():
                    if name not in _VIEW_DTYPES:
                        raise ValueError(f"unknown test view {name!r}")
                    array = _rows(_VIEW_DTYPES[name], rows)
                    keepalive[name] = array
                    row[name + "_ptr"], row[name + "_len"] = array.ctypes.data, len(array)
                row["event_payload_ptr"], row["event_payload_len"] = 0, 0
                if "depth_batch" in step:
                    payload = _rows(_DEPTH_BATCH_DTYPE, [{"version": 1, "valid": 1, **step["depth_batch"]}])
                    keepalive["payload"] = payload
                    row["event_payload_ptr"], row["event_payload_len"] = payload.ctypes.data, payload.nbytes
                if "recovery_orders" in step:
                    payload = _rows(abi_v13.active_order_dtype, step["recovery_orders"])
                    keepalive["payload"] = payload
                    row["event_payload_ptr"], row["event_payload_len"] = payload.ctypes.data, payload.nbytes
                function = getattr(library, "titan_strategy_" + callback)
                function.argtypes, function.restype = [ctypes.c_void_p], ctypes.c_int32
                status = function(context.ctypes.data)
                if status != step.get("status", 0):
                    raise AssertionError(f"callback returned {status}, expected {step.get('status', 0)}")
                expect(step.get("expect", {}))
            except (ValueError, AssertionError, KeyError, IndexError, TypeError) as exc:
                raise AssertionError(f"{case['name']} step {index}: {exc}") from exc
        try:
            expect(case.get("expect", {}))
        except (ValueError, AssertionError, KeyError, IndexError, TypeError) as exc:
            raise AssertionError(f"{case['name']}: {exc}") from exc
        reports.append({"name": case["name"], "passed": True, "steps": len(case.get("steps", []))})
    return {"declared": len(cases), "passed": len(reports), "cases": reports,
            "mode": "native_callbacks" if cases else "not_declared"}
