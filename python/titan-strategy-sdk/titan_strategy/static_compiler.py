"""Numba-only object emission and typed strategy validation for the Rust compiler."""

from __future__ import annotations

import ast
import builtins
from dataclasses import dataclass
import hashlib
import importlib.util
import inspect
import json
import os
from pathlib import Path
import platform
import re
import subprocess
import sys

import llvmlite
from llvmlite import binding, ir
from numba import cfunc, types
from numba.core.registry import CPUDispatcher
from numba.extending import intrinsic
import numba
import numpy as np

from . import abi_v13
from .context_v13 import StrategyContextType, make_context_from_pointer
from .definition import Capability, StrategyDefinition, StrategySpec
from .state import (
    StateFieldLayout, StateLayout, canonical_state_schema, describe_state_schema, state_schema_hash,
    validate_state_dtype,
)


class StrategyCompileError(RuntimeError):
    pass


@dataclass(frozen=True)
class ValidatedHandler:
    name: str
    dispatcher: CPUDispatcher
    context_calls: frozenset[str]


@dataclass(frozen=True)
class ValidatedStrategy:
    definition: StrategyDefinition
    state_layout: StateLayout
    canonical_schema: bytes
    schema_hash: bytes
    handlers: tuple[ValidatedHandler, ...]
    callback_mask: int
    initial_state: bytes


_FORBIDDEN_CALLS = {
    "eval", "exec", "open", "compile", "__import__", "input", "breakpoint",
    "sleep", "system", "popen", "fork", "spawn",
}
_PUBLIC_ACCESSORS = {
    "market", "position", "balance", "account", "active_orders", "ticks", "bars", "depth",
    "fills", "order_events", "cancel_events", "position_events", "balance_events",
    "account_state_events", "timer", "recovery_orders", "depth_batch",
}


def _module_ast(source_file: Path) -> ast.Module:
    try:
        return ast.parse(source_file.read_text(encoding="utf-8"), filename=str(source_file))
    except (OSError, SyntaxError) as exc:
        raise StrategyCompileError(f"cannot parse strategy source: {exc}") from exc


def _validate_source(source_file: Path) -> None:
    tree = _module_ast(source_file)
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.level:
            raise StrategyCompileError("relative imports are forbidden; strategy.py is the only source")
        if isinstance(node, (ast.Import, ast.ImportFrom)):
            names = [alias.name for alias in node.names] if isinstance(node, ast.Import) else [node.module or ""]
            if any(name.split(".", 1)[0] not in {"numpy", "numba", "titan_strategy"} for name in names):
                raise StrategyCompileError(f"strategy import is outside the public SDK: {names!r}")
        if isinstance(node, (ast.Global, ast.Nonlocal, ast.AsyncFunctionDef, ast.Await, ast.Yield, ast.YieldFrom)):
            raise StrategyCompileError(f"unsupported strategy construct: {type(node).__name__}")


def _rooted_public_call(node: ast.AST) -> bool:
    while isinstance(node, (ast.Subscript, ast.Attribute)):
        node = node.value
    return (
        isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name) and node.func.value.id == "ctx"
        and node.func.attr in _PUBLIC_ACCESSORS
    )


def _root_name(node: ast.AST) -> str | None:
    while isinstance(node, (ast.Subscript, ast.Attribute)):
        node = node.value
    return node.id if isinstance(node, ast.Name) else None


def _validate_handler_ast(handler: CPUDispatcher, name: str) -> frozenset[str]:
    try:
        tree = ast.parse(inspect.getsource(handler.py_func))
    except (OSError, TypeError, IndentationError) as exc:
        raise StrategyCompileError(f"{name}: source must be available for static validation") from exc
    tainted: set[str] = set()
    context_calls: set[str] = set()
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            public_value = _rooted_public_call(value) or _root_name(value) in tainted
            if not public_value:
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if isinstance(target, ast.Name) and target.id not in tainted:
                    tainted.add(target.id)
                    changed = True
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            called = node.func.id if isinstance(node.func, ast.Name) else (
                node.func.attr if isinstance(node.func, ast.Attribute) else ""
            )
            if (
                isinstance(node.func, ast.Attribute)
                and isinstance(node.func.value, ast.Name)
                and node.func.value.id == "ctx"
            ):
                context_calls.add(node.func.attr)
            if called in _FORBIDDEN_CALLS:
                raise StrategyCompileError(f"{name}: call to {called!r} is forbidden")
        if isinstance(node, ast.Raise):
            raise StrategyCompileError(f"{name}: exceptions are unsupported at the native ABI boundary")
        if isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            if any(
                _rooted_public_call(target)
                or (not isinstance(target, ast.Name) and _root_name(target) in tainted)
                for target in targets
            ):
                raise StrategyCompileError(f"{name}: public runtime views are read-only")
        if isinstance(node, ast.Return) and node.value is not None:
            value = node.value
            if isinstance(value, ast.Constant) and value.value is None:
                continue
            sign = 1
            if isinstance(value, ast.UnaryOp) and isinstance(value.op, ast.USub):
                sign, value = -1, value.operand
            if not (isinstance(value, ast.Constant) and type(value.value) is int
                    and sign * value.value in (0, -1, -2, -3, -4)):
                raise StrategyCompileError(f"{name}: return None or a literal ABI callback status (0, -1..-4)")
    return frozenset(context_calls)


def _load_strategy_definition_in_worker(
    source_file: Path, parameters: dict[str, object]
) -> StrategyDefinition:
    source_file = Path(source_file).resolve()
    _validate_source(source_file)
    module_name = f"_titan_strategy_{hashlib.sha256(source_file.read_bytes()).hexdigest()}"
    spec = importlib.util.spec_from_file_location(module_name, source_file)
    if spec is None or spec.loader is None:
        raise StrategyCompileError("strategy source cannot be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
        build = getattr(module, "build", None)
        if not callable(build):
            raise StrategyCompileError("strategy.py must define build(parameters)")
        declared_spec = getattr(module, "SPEC", None)
        normalized = (
            declared_spec.validate_parameters(dict(parameters))
            if isinstance(declared_spec, StrategySpec)
            else dict(parameters)
        )
        definition = build(normalized)
    finally:
        sys.modules.pop(module_name, None)
    if not isinstance(definition, StrategyDefinition):
        raise StrategyCompileError("build(parameters) must return StrategyDefinition")
    declared_dtype = getattr(module, "state_dtype", None)
    if declared_dtype is None or np.dtype(declared_dtype) != definition.state.dtype:
        raise StrategyCompileError("state_dtype must be a module-level constant used by initial state")
    return definition


def load_strategy_definition(source_file: Path, parameters: dict[str, object]) -> StrategyDefinition:
    """Load under the read-only guard in a Rust-supervised disposable Python worker.

    This function does not create processes. Direct calls are for SDK unit tests only.
    """
    previous_environment = dict(os.environ)
    preserved = {
        name: value for name, value in os.environ.items()
        if name in {"PATH", "PYTHONPATH", "PYTHONHOME", "LANG", "LC_ALL", "TMPDIR"}
    }
    os.environ.clear()
    os.environ.update(preserved)

    original_open = builtins.open
    original_os_open = os.open
    original_system = os.system
    original_popen = os.popen
    original_fork = getattr(os, "fork", None)
    original_spawn = {name: getattr(os, name) for name in dir(os) if name.startswith("spawn")}
    original_subprocess = {
        name: getattr(subprocess, name)
        for name in ("Popen", "run", "call", "check_call", "check_output")
        if hasattr(subprocess, name)
    }
    import socket
    original_socket = socket.socket

    def readonly_open(file, mode="r", *args, **kwargs):
        if any(flag in mode for flag in "wax+"):
            raise PermissionError("strategy build worker filesystem is read-only")
        return original_open(file, mode, *args, **kwargs)

    def readonly_os_open(path, flags, *args, **kwargs):
        write_flags = os.O_WRONLY | os.O_RDWR | os.O_CREAT | os.O_TRUNC | os.O_APPEND
        if flags & write_flags:
            raise PermissionError("strategy build worker filesystem is read-only")
        return original_os_open(path, flags, *args, **kwargs)

    def blocked(*_args, **_kwargs):
        raise PermissionError("operation is disabled in the strategy build worker")

    builtins.open = readonly_open
    os.open = readonly_os_open
    os.system = blocked
    os.popen = blocked
    if original_fork is not None:
        os.fork = blocked
    for name in original_spawn:
        setattr(os, name, blocked)
    for name in original_subprocess:
        setattr(subprocess, name, blocked)
    socket.socket = blocked
    try:
        definition = _load_strategy_definition_in_worker(Path(source_file), parameters)
    except StrategyCompileError:
        raise
    except BaseException as exc:
        raise StrategyCompileError(f"strategy definition rejected: {type(exc).__name__}: {exc}") from exc
    finally:
        builtins.open = original_open
        os.open = original_os_open
        os.system = original_system
        os.popen = original_popen
        if original_fork is not None:
            os.fork = original_fork
        for name, value in original_spawn.items():
            setattr(os, name, value)
        for name, value in original_subprocess.items():
            setattr(subprocess, name, value)
        socket.socket = original_socket
        os.environ.clear()
        os.environ.update(previous_environment)
    return definition


def validate_handler(name: str, handler: object) -> ValidatedHandler:
    if not isinstance(handler, CPUDispatcher):
        raise StrategyCompileError(f"{name}: handler must be a module-level @njit dispatcher")
    function = handler.py_func
    if "<locals>" in function.__qualname__:
        raise StrategyCompileError(f"{name}: closures and dynamically defined handlers are forbidden")
    signature = inspect.signature(function)
    parameters = tuple(signature.parameters.values())
    if len(parameters) != 1 or parameters[0].name != "ctx" or any(
        parameter.kind not in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        for parameter in parameters
    ):
        raise StrategyCompileError(f"{name}: ABI V13 requires exactly (ctx)")
    context_calls = _validate_handler_ast(handler, name)
    return ValidatedHandler(name, handler, context_calls)


def validate_strategy_definition(
    definition: StrategyDefinition,
    runtime_abi: dict[str, object],
) -> ValidatedStrategy:
    abi_v13.validate_abi_descriptor(runtime_abi)
    layout = validate_state_dtype(definition.state.dtype, max_state_bytes=65_536, max_alignment=8)
    if definition.state.shape != (1,) or not definition.state.flags.c_contiguous:
        raise StrategyCompileError("initial state must be one C-contiguous record")
    try:
        json.dumps(dict(definition.metadata), sort_keys=True, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise StrategyCompileError("metadata must be JSON-safe") from exc
    validated = tuple(validate_handler(name, value) for name, value in definition.handlers.items())
    handler_names = {item.name for item in validated}
    subscribed_handlers = {item.handler for item in definition.spec.subscriptions}
    for subscription in definition.spec.subscriptions:
        if subscription.handler not in handler_names:
            raise StrategyCompileError(f"missing subscribed handler {subscription.handler!r}")
    undeclared = handler_names - subscribed_handlers - {"on_start", "on_stop", "on_recover"}
    if undeclared:
        raise StrategyCompileError(f"event handlers are missing subscriptions: {sorted(undeclared)!r}")
    if any(name in {"on_tick", "on_bar", "on_depth"} for name in handler_names):
        if not definition.spec.capabilities & Capability.MARKET_DATA:
            raise StrategyCompileError("market handlers require MARKET_DATA capability")
    account_handlers = {
        "on_fill", "on_order", "on_cancel", "on_position", "on_balance", "on_account_state",
    }
    if handler_names & account_handlers and not definition.spec.capabilities & Capability.ACCOUNT_DATA:
        raise StrategyCompileError("account event handlers require ACCOUNT_DATA capability")
    context_calls = set().union(*(item.context_calls for item in validated))
    if context_calls & {"submit_order", "cancel_order"}:
        if not definition.spec.capabilities & Capability.ORDER_EXECUTION:
            raise StrategyCompileError("order commands require ORDER_EXECUTION capability")
    if "timer" in context_calls or "on_timer" in handler_names:
        if not definition.spec.capabilities & Capability.TIMER:
            raise StrategyCompileError("timer access requires TIMER capability")
    if "complete_stop" in context_calls and not definition.spec.capabilities & Capability.STOP_BARRIER:
        raise StrategyCompileError("complete_stop requires STOP_BARRIER capability")
    if definition.spec.capabilities & Capability.STOP_BARRIER and "on_stop" not in handler_names:
        raise StrategyCompileError("STOP_BARRIER requires on_stop")
    if "depth_batch" in context_calls and not definition.spec.capabilities & Capability.HOST_DEPTH_100:
        raise StrategyCompileError("depth_batch requires HOST_DEPTH_100 capability")
    if "recovery_orders" in context_calls and not definition.spec.capabilities & Capability.RECOVERY_ORDER_RESULTS:
        raise StrategyCompileError("recovery_orders requires RECOVERY_ORDER_RESULTS capability")
    if definition.spec.capabilities & Capability.RECOVERY_ORDER_RESULTS and "on_recover" not in handler_names:
        raise StrategyCompileError("RECOVERY_ORDER_RESULTS requires on_recover")
    source_schema = canonical_state_schema(layout, schema_version=definition.spec.recovery_state_schema_version)
    initial_state = bytearray(definition.state.tobytes(order="C"))
    occupied = bytearray(layout.itemsize)

    def mark(field: StateFieldLayout, base_offset: int) -> None:
        count = int(np.prod(field.shape or (1,)))
        stride = field.dtype.itemsize
        for element in range(count):
            element_offset = base_offset + field.offset + element * stride
            if field.fields:
                for child in field.fields:
                    mark(child, element_offset)
            else:
                occupied[element_offset:element_offset + field.dtype.itemsize] = b"\x01" * field.dtype.itemsize

    for root_field in layout.fields:
        mark(root_field, 0)
    for index, used in enumerate(occupied):
        if not used:
            initial_state[index] = 0
    return ValidatedStrategy(
        definition=definition,
        state_layout=layout,
        canonical_schema=source_schema,
        schema_hash=state_schema_hash(source_schema),
        handlers=validated,
        callback_mask=abi_v13.callback_mask(handler_names),
        initial_state=bytes(initial_state),
    )


def _bridge(handler: ValidatedHandler, context_type: StrategyContextType, strategy: ValidatedStrategy):
    cast = make_context_from_pointer(context_type)
    dispatcher = handler.dispatcher
    returns_status = any(isinstance(node, ast.Return) and node.value is not None
                         and not (isinstance(node.value, ast.Constant) and node.value.value is None)
                         for node in ast.walk(ast.parse(inspect.getsource(dispatcher.py_func))))
    expected_len = strategy.state_layout.itemsize
    expected_alignment = strategy.state_layout.alignment
    expected_version = strategy.definition.spec.recovery_state_schema_version
    expected_hash = tuple(strategy.schema_hash)
    context_size = abi_v13.runtime_context_dtype.itemsize

    def field_offset(name: str) -> int:
        return int(abi_v13.runtime_context_dtype.fields[name][1])

    @intrinsic
    def validate_native_context(typingctx, pointer):
        if pointer != types.voidptr:
            return None
        signature = types.int32(pointer)

        def codegen(context, builder, sig, args):
            del context, sig
            raw = builder.bitcast(args[0], ir.IntType(8).as_pointer())

            def load(name, llvm_type):
                address = builder.gep(raw, [ir.Constant(ir.IntType(64), field_offset(name))])
                return builder.load(builder.bitcast(address, llvm_type.as_pointer()))

            zero = ir.Constant(ir.IntType(32), 0)
            invalid = ir.Constant(ir.IntType(32), -2)
            schema = ir.Constant(ir.IntType(32), -3)
            invalid_condition = builder.or_(
                builder.icmp_unsigned("<", load("struct_size", ir.IntType(32)),
                                      ir.Constant(ir.IntType(32), context_size)),
                builder.icmp_unsigned("!=", load("abi_version", ir.IntType(32)),
                                      ir.Constant(ir.IntType(32), abi_v13.ABI_VERSION)),
            )
            schema_condition = builder.or_(
                builder.icmp_unsigned("!=", load("state_len", ir.IntType(64)),
                                      ir.Constant(ir.IntType(64), expected_len)),
                builder.icmp_unsigned("!=", load("state_alignment", ir.IntType(32)),
                                      ir.Constant(ir.IntType(32), expected_alignment)),
            )
            schema_condition = builder.or_(
                schema_condition,
                builder.icmp_unsigned("!=", load("state_schema_version", ir.IntType(32)),
                                      ir.Constant(ir.IntType(32), expected_version)),
            )
            hash_base = builder.gep(
                raw, [ir.Constant(ir.IntType(64), field_offset("state_schema_hash"))]
            )
            for index, expected in enumerate(expected_hash):
                actual = builder.load(builder.gep(hash_base, [ir.Constant(ir.IntType(64), index)]))
                schema_condition = builder.or_(
                    schema_condition,
                    builder.icmp_unsigned("!=", actual, ir.Constant(ir.IntType(8), expected)),
                )
            return builder.select(invalid_condition, invalid,
                                  builder.select(schema_condition, schema, zero))

        return signature, codegen

    @intrinsic
    def last_error_code(typingctx, pointer):
        if pointer != types.voidptr:
            return None
        signature = types.int32(pointer)

        def codegen(context, builder, sig, args):
            del context, sig
            raw = builder.bitcast(args[0], ir.IntType(8).as_pointer())
            address = builder.gep(
                raw, [ir.Constant(ir.IntType(64), field_offset("last_error_code"))]
            )
            return builder.load(builder.bitcast(address, ir.IntType(32).as_pointer()))

        return signature, codegen

    @cfunc(types.int32(types.voidptr))
    def native(raw):
        validation = validate_native_context(raw)
        if validation != 0:
            return validation
        context_value = cast(raw)
        if returns_status:
            status = dispatcher(context_value)
            if status is not None and status != 0:
                return status
        else:
            dispatcher(context_value)
        if last_error_code(raw) != 0:
            return int(-4)
        return int(0)

    return native


def _emit_pic_object(llvm_ir: str, output: Path, cpu_baseline: str,
                     *, discard_functions: tuple[str, ...] = ()) -> None:
    binding.initialize_native_target()
    binding.initialize_native_asmprinter()
    module = binding.parse_assembly(llvm_ir)
    module.verify()
    target = binding.Target.from_default_triple()
    cpu = "generic"
    features = ""
    if cpu_baseline == "x86-64-v2":
        features = "+sse3,+ssse3,+sse4.1,+sse4.2,+popcnt,+cx16"
    machine = target.create_target_machine(cpu=cpu, features=features, reloc="pic")
    for name in discard_functions:
        module.get_function(name).linkage = "internal"
    passes = binding.ModulePassManager()
    if hasattr(passes, "add_global_dead_code_eliminate_pass"):
        passes.add_global_dead_code_eliminate_pass()
    else:
        passes.add_dead_code_elimination_pass()
    if hasattr(passes, "add_strip_dead_prototype_pass"):
        passes.add_strip_dead_prototype_pass()
    else:
        passes.add_strip_dead_prototypes_pass()
    llvmlite_version = tuple(int(part) for part in llvmlite.__version__.split(".")[:2])
    if llvmlite_version < (0, 45):
        passes.run(module)
    else:
        pass_builder = binding.create_pass_builder(machine, binding.PipelineTuningOptions())
        passes.run(module, pass_builder)
    output.write_bytes(machine.emit_object(module))


def emit_callback_bridge(
    handler: ValidatedHandler,
    state_dtype: np.dtype,
    strategy: ValidatedStrategy,
    work_dir: Path,
    cpu_baseline: str,
) -> str:
    context_type = StrategyContextType(state_dtype)
    native = _bridge(handler, context_type, strategy)
    llvm_ir = native._library.get_llvm_str()
    match = re.search(r"^define[^@]*@([^\s(]+)\(", llvm_ir, re.MULTILINE)
    if match is None:
        raise StrategyCompileError(f"{handler.name}: Numba did not emit a native implementation")
    numba_implementation = match.group(1).strip('"')
    object_file = work_dir / f"{handler.name}.o"
    _emit_pic_object(llvm_ir, object_file, cpu_baseline, discard_functions=(native.native_name,))
    return numba_implementation


def describe_strategy(strategy: ValidatedStrategy) -> dict[str, object]:
    """Typed bridge facts. Rust validates them and builds the compiled strategy."""
    spec = strategy.definition.spec
    return {
        "protocol_version": 2, "abi_version": 13,
        "abi_fingerprint": list(abi_v13.ABI_FINGERPRINT),
        "strategy_type": spec.strategy_type, "source_version": spec.source_version,
        "callbacks": [handler.name for handler in strategy.handlers],
        "callback_mask": strategy.callback_mask,
        "recovery_state_schema_version": spec.recovery_state_schema_version,
        "recovery_state_schema_hash": list(strategy.schema_hash),
        "state_len": strategy.state_layout.itemsize,
        "state_alignment": strategy.state_layout.alignment,
        "initial_state": list(strategy.initial_state),
        "recovery_state_schema": describe_state_schema(strategy.state_layout),
        "business_observation": None if strategy.definition.business_observation is None
            else strategy.definition.business_observation.describe(),
        "parameter_schema": spec.parameter_schema(),
        "subscriptions": [{"event": sub.event_kind.name.lower(), "handler": sub.handler,
                           "schema_version": sub.schema_version, "qos": sub.qos.name.lower()}
                          for sub in spec.subscriptions],
        "capabilities": [item.name.lower() for item in Capability if item and spec.capabilities & item],
        "build": {"python": platform.python_version(), "numba": numba.__version__,
                  "llvmlite": list(binding.llvm_version_info), "compiler": "titan-numba-bridge/2"},
    }


def emit_objects(strategy: ValidatedStrategy, work_dir: Path, cpu_baseline: str) -> list[dict[str, str]]:
    return [{"callback": handler.name, "implementation": emit_callback_bridge(
        handler, strategy.state_layout.dtype, strategy, work_dir, cpu_baseline,
    )} for handler in strategy.handlers]


__all__ = [
    "StrategyCompileError", "ValidatedHandler", "ValidatedStrategy",
    "load_strategy_definition", "validate_strategy_definition", "validate_handler",
    "emit_callback_bridge", "describe_strategy", "emit_objects",
]
