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

from . import abi

_abi = abi
from .context import _DEPTH_BATCH_DTYPE
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
        if not isinstance(case, dict) or set(case) - {"name", "state", "steps", "expect", "objects"}:
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
                "flags", "status", "expect", "object_status",
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


_VIEW_DTYPES = {name: getattr(abi, dtype + "_dtype") for name, dtype in (
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


_OBJECT_DOMAIN_MAX = {"symbols": 64, "accounts": 16, "currencies": 128, "pairs": 16, "fees": 16}


def _fill_records(dtype, rows):
    result = np.zeros(len(rows), dtype=dtype)
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValueError("object payload rows must be dicts")
        for key, value in row.items():
            if key not in dtype.names:
                raise ValueError(f"unknown object payload field {key!r}")
            if isinstance(value, dict):
                for nested_key, nested_value in value.items():
                    if nested_key not in dtype[key].names:
                        raise ValueError(f"unknown nested object field {key}.{nested_key}")
                    result[i][key][nested_key] = nested_value
            else:
                result[i][key] = value
    return result


def objects(*, symbols=(), accounts=(), currencies=(), pairs=(), fees=(), status=None):
    """Declare the object table for one TESTS scene.

    The returned description is JSON-compatible; the harness turns it into payload buffers plus a
    reference array and keeps them alive for the duration of the native callback. Object ids follow
    the ABI conventions: symbol/account use their local numbers, currency uses its position in the
    currency list, pair its position, status is 0, fee its account number.
    """
    for name, rows in (("symbols", symbols), ("accounts", accounts), ("currencies", currencies),
                       ("pairs", pairs), ("fees", fees)):
        if not isinstance(rows, (list, tuple)) or len(rows) > _OBJECT_DOMAIN_MAX[name]:
            raise ValueError(f"objects.{name} must be a list of at most {_OBJECT_DOMAIN_MAX[name]} rows")
    return {
        "symbols": [dict(row) for row in symbols],
        "accounts": [dict(row) for row in accounts],
        "currencies": [dict(row) for row in currencies],
        "pairs": [dict(row) for row in pairs],
        "fees": [dict(row) for row in fees],
        "status": dict(status or {}),
    }


def _build_object_table(description):
    """Materialize payloads + `TitanObjectRef[]`; returns (refs, buffers, status)."""
    

    if not isinstance(description, dict):
        raise ValueError("objects must be an `objects(...)` description")
    unknown = set(description) - {"symbols", "accounts", "currencies", "pairs", "fees", "status"}
    if unknown:
        raise ValueError(f"unknown objects keys {sorted(unknown)}")
    payloads = {
        "symbols": _fill_records(abi.symbol_payload_dtype, description.get("symbols", [])),
        "accounts": _fill_records(abi.account_payload_dtype, description.get("accounts", [])),
        "currencies": _fill_records(abi.currency_payload_dtype, description.get("currencies", [])),
        "pairs": _fill_records(abi.pair_payload_dtype, description.get("pairs", [])),
        "fees": _fill_records(abi.fee_payload_dtype, description.get("fees", [])),
    }
    status = np.zeros(1, dtype=abi.status_payload_dtype)
    for key, value in description.get("status", {}).items():
        if key not in status.dtype.names:
            raise ValueError(f"unknown status field {key!r}")
        status[0][key] = value
    entries = []

    def add(domain, object_id, flags, array, index):
        entries.append((domain, int(object_id), flags, array, index))

    for index in range(len(payloads["symbols"])):
        add(abi.DOMAIN_SYMBOL, payloads["symbols"][index]["local_asset_no"], 1,
            payloads["symbols"], index)
    for index in range(len(payloads["accounts"])):
        add(abi.DOMAIN_ACCOUNT, payloads["accounts"][index]["local_account_no"], 1,
            payloads["accounts"], index)
    for index in range(len(payloads["currencies"])):
        add(abi.DOMAIN_CURRENCY, index, 1, payloads["currencies"], index)
    for index in range(len(payloads["pairs"])):
        add(abi.DOMAIN_PAIR, index, 1, payloads["pairs"], index)
    add(abi.DOMAIN_STATUS, 0, 1 | 2, status, 0)
    fee_refs = []
    seen_accounts = set()
    for index in range(len(payloads["fees"])):
        row = payloads["fees"][index]
        account_no = int(row["local_account_no"])
        if account_no in seen_accounts:
            raise ValueError(f"duplicate fee object for account {account_no}")
        seen_accounts.add(account_no)
        valid = int(row["flags"]) & 1 != 0
        fee_refs.append((account_no, 1 if valid else 0, payloads["fees"], index))
    fee_refs.sort(key=lambda entry: entry[0])
    for account_no, flags, array, index in fee_refs:
        add(abi.DOMAIN_FEE, account_no, flags, array, index)
    # References must be ordered by (domain, object_id).
    entries.sort(key=lambda entry: (entry[0], entry[1], entry[4]))
    refs = np.zeros(len(entries), dtype=abi.object_ref_dtype)
    for index, (domain, object_id, flags, array, row_index) in enumerate(entries):
        refs[index]["domain"] = domain
        refs[index]["object_id"] = object_id
        refs[index]["flags"] = flags
        refs[index]["schema_major"] = abi.PAYLOAD_MAJOR
        refs[index]["schema_minor"] = abi.PAYLOAD_MINOR
        refs[index]["payload"] = array.ctypes.data + row_index * array.dtype.itemsize
        refs[index]["payload_len"] = array.dtype.itemsize
    buffers = dict(payloads)
    buffers["status"] = status
    return refs, buffers, status


def run_native_tests(strategy, library_path: Path, cases, *, abi=None):
    """Run each scene with fresh initial bytes and synchronous real native callbacks."""
    abi = abi or _abi
    cases = validate_tests(cases)
    library = ctypes.CDLL(str(library_path))
    reports = []
    request_fn = ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p,
                                 ctypes.c_void_p, ctypes.POINTER(ctypes.c_uint64))
    for case in cases:
        state = np.frombuffer(strategy.initial_state, dtype=strategy.state_layout.dtype).copy()
        context = np.zeros(1, dtype=abi.runtime_context_dtype)
        row = context[0]
        row["struct_size"], row["abi_version"] = context.nbytes, abi.ABI_VERSION
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
        object_status = None
        if case.get("objects"):
            refs, buffers, object_status = _build_object_table(case["objects"])
            keepalive["objects"] = refs
            keepalive["object_payloads"] = buffers
            row["objects_ptr"], row["objects_len"] = refs.ctypes.data, refs.nbytes
            row["object_schema_minor"] = abi.PAYLOAD_MINOR
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
                if object_status is not None and "object_status" in step:
                    for key, value in step["object_status"].items():
                        if key not in object_status.dtype.names:
                            raise ValueError(f"unknown object status field {key!r}")
                        object_status[0][key] = value
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
                    payload = _rows(abi.active_order_dtype, step["recovery_orders"])
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
