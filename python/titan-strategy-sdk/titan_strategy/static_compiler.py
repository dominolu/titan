"""Offline native artifact compiler for Strategy ABI V13."""

from __future__ import annotations

import ast
import builtins
from dataclasses import dataclass
import hashlib
import importlib.util
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
from types import ModuleType
from typing import Any, Mapping
import zipfile

import llvmlite
from llvmlite import binding, ir
from numba import cfunc, types
from numba.core.registry import CPUDispatcher
from numba.extending import intrinsic
import numba
import numpy as np

from . import abi_v13, cbor
from .context_v13 import StrategyContextType, make_context_from_pointer
from .definition import Capability, StrategyDefinition, StrategySpec
from .state import (
    StateFieldLayout, StateLayout, canonical_state_schema, describe_state_schema, state_schema_hash,
    validate_state_dtype,
)


class StrategyCompileError(RuntimeError):
    pass


@dataclass(frozen=True)
class CompileRequest:
    source_file: Path
    parameters: dict[str, object]
    target_triple: str
    cpu_baseline: str
    artifact_format: str
    output_path: Path
    runtime_abi: dict[str, object]


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


@dataclass(frozen=True)
class NativeSymbol:
    handler: str
    export_name: str
    implementation_name: str
    object_file: Path
    llvm_ir: str


@dataclass(frozen=True)
class NativeBuild:
    library: Path
    symbols: tuple[NativeSymbol, ...]
    undefined_symbols: tuple[str, ...]
    build: Mapping[str, object]


@dataclass(frozen=True)
class CompiledArtifact:
    paths: tuple[Path, ...]
    artifact_digest: bytes
    native_digest: bytes


@dataclass(frozen=True)
class CompileResult:
    artifact: CompiledArtifact
    strategy: ValidatedStrategy


_FORBIDDEN_CALLS = {
    "eval", "exec", "open", "compile", "__import__", "input", "breakpoint",
    "sleep", "system", "popen", "fork", "spawn",
}
_PUBLIC_ACCESSORS = {
    "market", "position", "balance", "account", "active_orders", "ticks", "bars", "depth",
    "fills", "order_events", "cancel_events", "position_events", "balance_events",
    "account_state_events", "timer",
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
            raise StrategyCompileError(f"{name}: ABI V13 handlers must return None")
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


def _definition_worker(
    connection, source_file: str, parameters: dict[str, object]
) -> None:
    try:
        try:
            import resource

            resource.setrlimit(resource.RLIMIT_CPU, (30, 30))
            memory_limit = 4 * 1024 * 1024 * 1024
            resource.setrlimit(resource.RLIMIT_AS, (memory_limit, memory_limit))
            if hasattr(resource, "RLIMIT_NPROC"):
                resource.setrlimit(resource.RLIMIT_NPROC, (0, 0))
        except (ImportError, OSError, ValueError):
            pass

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
        connection.send((True, definition))
    except BaseException as exc:
        connection.send((False, f"{type(exc).__name__}: {exc}"))
    finally:
        connection.close()


def load_strategy_definition(source_file: Path, parameters: dict[str, object]) -> StrategyDefinition:
    source_file = Path(source_file).resolve()
    _validate_source(source_file)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_definition_worker,
        args=(child, str(source_file), dict(parameters)),
        name="titan-strategy-compiler-worker",
    )
    previous_hash_seed = os.environ.get("PYTHONHASHSEED")
    os.environ["PYTHONHASHSEED"] = "0"
    try:
        process.start()
    finally:
        if previous_hash_seed is None:
            os.environ.pop("PYTHONHASHSEED", None)
        else:
            os.environ["PYTHONHASHSEED"] = previous_hash_seed
    child.close()
    try:
        if not parent.poll(60):
            process.kill()
            process.join()
            raise StrategyCompileError("strategy compiler worker exceeded the 60 second limit")
        success, payload = parent.recv()
    except EOFError as exc:
        process.join()
        raise StrategyCompileError(
            f"strategy compiler worker exited without a result (exit={process.exitcode})"
        ) from exc
    finally:
        parent.close()
    process.join()
    if process.exitcode != 0:
        raise StrategyCompileError(f"strategy compiler worker failed with exit code {process.exitcode}")
    if not success:
        raise StrategyCompileError(f"strategy compiler worker rejected the strategy: {payload}")
    if not isinstance(payload, StrategyDefinition):
        raise StrategyCompileError("strategy compiler worker returned an invalid definition")
    return payload


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
    undeclared = handler_names - subscribed_handlers - {"on_start", "on_stop"}
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
    source_schema = canonical_state_schema(layout, schema_version=definition.spec.state_schema_version)
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
    expected_len = strategy.state_layout.itemsize
    expected_alignment = strategy.state_layout.alignment
    expected_version = strategy.definition.spec.state_schema_version
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
    abi: dict[str, object],
    strategy: ValidatedStrategy,
    work_dir: Path,
    cpu_baseline: str,
) -> NativeSymbol:
    context_type = StrategyContextType(state_dtype)
    native = _bridge(handler, context_type, strategy)
    llvm_ir = native._library.get_llvm_str()
    match = re.search(r"^define[^@]*@([^\s(]+)\(", llvm_ir, re.MULTILINE)
    if match is None:
        raise StrategyCompileError(f"{handler.name}: Numba did not emit a native implementation")
    numba_implementation = match.group(1).strip('"')
    object_file = work_dir / f"{handler.name}.o"
    _emit_pic_object(llvm_ir, object_file, cpu_baseline, discard_functions=(native.native_name,))
    export_name = f"titan_strategy_{handler.name}"
    implementation_name = f"titan_numba_impl_{handler.name}"
    defined = subprocess.run(
        ["nm", "--defined-only", str(object_file)], check=True, capture_output=True, text=True,
    ).stdout
    environment_symbols = sorted(
        line.split()[-1]
        for line in defined.splitlines()
        if line.split() and line.split()[-1].startswith("_ZN08NumbaEnv")
    )
    rename_arguments = [f"--redefine-sym={numba_implementation}={implementation_name}"]
    rename_arguments.extend(
        f"--redefine-sym={name}=titan_numba_env_{handler.name}_{index}"
        for index, name in enumerate(environment_symbols)
    )
    subprocess.run(
        ["objcopy", *rename_arguments, str(object_file)],
        check=True, capture_output=True, text=True,
    )
    undefined = subprocess.run(["nm", "-u", str(object_file)], check=True, capture_output=True,
                               text=True).stdout
    forbidden = sorted({name for name in re.findall(
        r"\b(?:Py[A-Za-z0-9_]*|numba_[A-Za-z0-9_]*|NRT_[A-Za-z0-9_]*)\b", undefined
    )})
    if forbidden:
        raise StrategyCompileError(f"{handler.name}: native object depends on forbidden symbols {forbidden}")
    return NativeSymbol(handler.name, export_name, implementation_name, object_file, llvm_ir)


def _descriptor_source(strategy: ValidatedStrategy, symbols: tuple[NativeSymbol, ...]) -> str:
    fingerprint = ",".join(str(value) for value in abi_v13.ABI_FINGERPRINT)
    schema_hash = ",".join(str(value) for value in strategy.schema_hash)
    callbacks = "\n".join(
        f"extern int32_t {symbol.implementation_name}(int32_t *, void **, void *);\n"
        f"TITAN_EXPORT int32_t {symbol.export_name}(void *context) {{\n"
        f"  int32_t result = -1; void *exception = 0;\n"
        f"  int32_t status = {symbol.implementation_name}(&result, &exception, context);\n"
        f"  return status == 0 ? result : -1;\n}}"
        for symbol in symbols
    )
    return f"""
#include <stdint.h>
#if defined(__GNUC__)
#define TITAN_EXPORT __attribute__((visibility("default")))
#else
#define TITAN_EXPORT
#endif
struct NativeStrategyDescriptor {{
  uint32_t struct_size; uint32_t abi_version; uint8_t abi_fingerprint[32];
  uint32_t state_schema_version; uint32_t state_alignment; uint64_t state_len;
  uint8_t state_schema_hash[32]; uint64_t callback_mask;
}};
static const struct NativeStrategyDescriptor DESCRIPTOR = {{
  sizeof(struct NativeStrategyDescriptor), 13, {{{fingerprint}}},
  {strategy.definition.spec.state_schema_version}, {strategy.state_layout.alignment},
  {strategy.state_layout.itemsize}, {{{schema_hash}}}, {strategy.callback_mask}ULL
}};
TITAN_EXPORT uint32_t titan_strategy_abi_version(void) {{ return 13; }}
TITAN_EXPORT const struct NativeStrategyDescriptor *titan_strategy_descriptor(void) {{ return &DESCRIPTOR; }}
{callbacks}
"""


def link_native_library(
    objects: tuple[Path, ...],
    *,
    target_triple: str,
    output: Path,
    exported_symbols: tuple[str, ...],
    descriptor_object: Path,
) -> Path:
    del target_triple
    version_script = output.with_suffix(".exports")
    exports = "\n".join(f"    {name};" for name in (*exported_symbols, "titan_strategy_abi_version", "titan_strategy_descriptor"))
    version_script.write_text(f"{{ global:\n{exports}\n  local: *;\n}};\n", encoding="utf-8")
    subprocess.run(
        ["cc", "-shared", "-Wl,--build-id=none", f"-Wl,--version-script={version_script}",
         "-o", str(output), *(str(path) for path in objects), str(descriptor_object)],
        check=True, capture_output=True, text=True,
    )
    return output


class NumbaAotBackend:
    def compile(self, strategy: ValidatedStrategy, *, target_triple: str,
                cpu_baseline: str, work_dir: Path) -> NativeBuild:
        symbols = tuple(
            emit_callback_bridge(handler, strategy.state_layout.dtype, {
                "abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex(),
            }, strategy, work_dir, cpu_baseline)
            for handler in strategy.handlers
        )
        descriptor_c = work_dir / "descriptor.c"
        descriptor_c.write_text(_descriptor_source(strategy, symbols), encoding="utf-8")
        descriptor_object = work_dir / "descriptor.o"
        subprocess.run(
            ["cc", "-std=c11", "-fPIC", "-O2", "-c", str(descriptor_c), "-o", str(descriptor_object)],
            check=True, capture_output=True, text=True,
        )
        library = work_dir / f"{strategy.definition.spec.strategy_id}.so"
        link_native_library(tuple(symbol.object_file for symbol in symbols), target_triple=target_triple,
                            output=library, exported_symbols=tuple(s.export_name for s in symbols),
                            descriptor_object=descriptor_object)
        nm = subprocess.run(["nm", "-D", "--undefined-only", str(library)], check=True,
                            capture_output=True, text=True).stdout
        undefined = tuple(sorted({line.split()[-1] for line in nm.splitlines() if line.split()}))
        forbidden = [name for name in undefined if name.startswith(("Py", "numba_", "NRT_"))]
        if forbidden:
            raise StrategyCompileError(f"native library has forbidden dynamic symbols: {forbidden}")
        return NativeBuild(
            library=library, symbols=symbols, undefined_symbols=undefined,
            build={"python": platform.python_version(), "numba": numba.__version__,
                   "llvmlite": binding.llvm_version_info, "compiler": "titan-strategy-sdk/0.1.0"},
        )


def _capability_names(value: Capability) -> list[str]:
    return [item.name.lower() for item in Capability if item and value & item]


def _manifest(strategy: ValidatedStrategy, native: NativeBuild, request: CompileRequest,
              library_name: str) -> dict[str, object]:
    source = request.source_file.read_bytes()
    parameters = json.dumps(request.parameters, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    native_bytes = native.library.read_bytes()
    spec = strategy.definition.spec
    return {
        "artifact_format_version": 1, "strategy_id": spec.strategy_id,
        "strategy_version": spec.strategy_version, "abi_version": 13,
        "abi_fingerprint": abi_v13.ABI_FINGERPRINT, "target_triple": request.target_triple,
        "cpu_baseline": request.cpu_baseline, "native_library": library_name,
        "callback_mask": strategy.callback_mask, "state_schema_version": spec.state_schema_version,
        "state_schema_hash": strategy.schema_hash, "state_len": strategy.state_layout.itemsize,
        "state_alignment": strategy.state_layout.alignment, "initial_state": strategy.initial_state,
        "state_schema": describe_state_schema(strategy.state_layout),
        "parameter_schema": spec.parameter_schema(),
        "subscriptions": [{"event": s.event_kind.name.lower(), "handler": s.handler,
                           "schema_version": s.schema_version, "qos": s.qos.name.lower()}
                          for s in spec.subscriptions],
        "capabilities": _capability_names(spec.capabilities),
        "source_digest": hashlib.sha256(source).digest(),
        "parameters_digest": hashlib.sha256(parameters).digest(), "build": dict(native.build),
        "native_digest": hashlib.sha256(native_bytes).digest(), "signature": None,
    }


def write_artifact(strategy: ValidatedStrategy, native: NativeBuild, request: CompileRequest,
                   staging_dir: Path) -> CompiledArtifact:
    base = request.output_path
    stem = base.stem if base.suffix in (".so", ".titan", ".cbor") else base.name
    library_name = f"{stem}.so"
    manifest = _manifest(strategy, native, request, library_name)
    unsigned = dict(manifest)
    unsigned.pop("signature")
    artifact_digest = hashlib.sha256(cbor.dumps(unsigned)).digest()
    manifest_bytes = cbor.dumps(manifest)
    if request.artifact_format == "pair":
        final_library = base.with_name(library_name)
        final_manifest = base.with_name(f"{stem}.manifest.cbor")
        staged_library = staging_dir / library_name
        staged_manifest = staging_dir / final_manifest.name
        if native.library != staged_library:
            shutil.copyfile(native.library, staged_library)
        staged_manifest.write_bytes(manifest_bytes)
        final_library.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_library, final_library)
        os.replace(staged_manifest, final_manifest)
        paths = (final_library, final_manifest)
    else:
        final_bundle = base if base.suffix == ".titan" else base.with_suffix(".titan")
        staged_bundle = staging_dir / final_bundle.name
        with zipfile.ZipFile(staged_bundle, "w", compression=zipfile.ZIP_STORED) as archive:
            for name, payload in sorted(((library_name, native.library.read_bytes()),
                                         (f"{stem}.manifest.cbor", manifest_bytes))):
                info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
                info.external_attr = 0o444 << 16
                archive.writestr(info, payload)
        final_bundle.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_bundle, final_bundle)
        paths = (final_bundle,)
    return CompiledArtifact(paths, artifact_digest, manifest["native_digest"])


def _host_target() -> str:
    machine = platform.machine().lower()
    os_name = platform.system().lower()
    if machine in ("x86_64", "amd64") and os_name == "linux":
        return "x86_64-unknown-linux-gnu"
    if machine in ("aarch64", "arm64") and os_name == "linux":
        return "aarch64-unknown-linux-gnu"
    if machine in ("arm64", "aarch64") and os_name == "darwin":
        return "aarch64-apple-darwin"
    return f"{machine}-unknown-{os_name}"


def _validate_compile_request(request: CompileRequest) -> None:
    if request.artifact_format not in ("pair", "bundle"):
        raise StrategyCompileError("artifact_format must be 'pair' or 'bundle'")
    if request.target_triple != _host_target():
        raise StrategyCompileError("V13 only supports compilation for the compiler host target")
    allowed_baselines = (
        {"generic", "x86-64", "x86-64-v2"}
        if request.target_triple.startswith("x86_64-")
        else {"generic", "armv8-a"}
    )
    if request.cpu_baseline not in allowed_baselines:
        raise StrategyCompileError(f"unsupported CPU baseline {request.cpu_baseline!r}")
    abi_v13.validate_abi_descriptor(request.runtime_abi)


def _compile_artifact_in_process(request: CompileRequest) -> CompiledArtifact:
    _validate_compile_request(request)
    definition = load_strategy_definition(request.source_file, request.parameters)
    definition.spec.validate_parameters(request.parameters)
    strategy = validate_strategy_definition(definition, request.runtime_abi)
    parent = request.output_path.parent.resolve()
    parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".titan-v13-", dir=parent) as temporary:
        work_dir = Path(temporary)
        native = NumbaAotBackend().compile(strategy, target_triple=request.target_triple,
                                           cpu_baseline=request.cpu_baseline, work_dir=work_dir)
        artifact = write_artifact(strategy, native, request, work_dir)
    return artifact


def _compile_worker(connection, request: CompileRequest) -> None:
    try:
        connection.send((True, _compile_artifact_in_process(request)))
    except BaseException as exc:
        connection.send((False, f"{type(exc).__name__}: {exc}"))
    finally:
        connection.close()


def compile_package(request: CompileRequest) -> CompileResult:
    """Compile in a fresh process so Numba process-global symbol counters cannot affect bytes."""
    _validate_compile_request(request)
    definition = load_strategy_definition(request.source_file, request.parameters)
    definition.spec.validate_parameters(request.parameters)
    strategy = validate_strategy_definition(definition, request.runtime_abi)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_compile_worker,
        args=(child, request),
        name="titan-strategy-aot-worker",
    )
    previous_hash_seed = os.environ.get("PYTHONHASHSEED")
    os.environ["PYTHONHASHSEED"] = "0"
    try:
        process.start()
    finally:
        if previous_hash_seed is None:
            os.environ.pop("PYTHONHASHSEED", None)
        else:
            os.environ["PYTHONHASHSEED"] = previous_hash_seed
    child.close()
    try:
        if not parent.poll(300):
            process.kill()
            process.join()
            raise StrategyCompileError("strategy AOT compiler exceeded the 300 second limit")
        success, payload = parent.recv()
    except EOFError as exc:
        process.join()
        raise StrategyCompileError(
            f"strategy AOT compiler exited without a result (exit={process.exitcode})"
        ) from exc
    finally:
        parent.close()
    process.join()
    if process.exitcode != 0:
        raise StrategyCompileError(f"strategy AOT compiler failed with exit code {process.exitcode}")
    if not success:
        raise StrategyCompileError(f"strategy AOT compiler failed: {payload}")
    if not isinstance(payload, CompiledArtifact):
        raise StrategyCompileError("strategy AOT compiler returned an invalid artifact")
    return CompileResult(payload, strategy)


def verify_artifact(artifact_path: Path, runtime_abi: dict[str, object]) -> dict[str, object]:
    abi_v13.validate_abi_descriptor(runtime_abi)
    path = Path(artifact_path)
    if path.suffix == ".titan":
        with zipfile.ZipFile(path) as archive:
            names = archive.namelist()
            if len(names) != 2 or any(name.startswith(("/", "../")) or "/../" in name for name in names):
                raise StrategyCompileError("unsafe or invalid bundle directory")
            manifest_names = [name for name in names if name.endswith(".manifest.cbor")]
            if len(manifest_names) != 1:
                raise StrategyCompileError("bundle must contain one manifest")
            manifest = cbor.loads(archive.read(manifest_names[0]))
            library = archive.read(manifest["native_library"])
    else:
        manifest_path = path if path.name.endswith(".manifest.cbor") else path.with_suffix(".manifest.cbor")
        manifest = cbor.loads(manifest_path.read_bytes())
        library = (manifest_path.parent / manifest["native_library"]).read_bytes()
    if not isinstance(manifest, dict):
        raise StrategyCompileError("artifact manifest must be a map")
    if manifest.get("abi_version") != 13 or manifest.get("abi_fingerprint") != abi_v13.ABI_FINGERPRINT:
        raise StrategyCompileError("artifact ABI identity mismatch")
    if hashlib.sha256(library).digest() != manifest.get("native_digest"):
        raise StrategyCompileError("native library digest mismatch")
    if len(manifest.get("initial_state", b"")) != manifest.get("state_len"):
        raise StrategyCompileError("initial state length mismatch")
    return manifest


__all__ = [
    "CompileRequest", "CompileResult", "CompiledArtifact", "NativeBuild", "NativeSymbol",
    "NumbaAotBackend", "StrategyCompileError", "ValidatedHandler", "ValidatedStrategy",
    "compile_package", "emit_callback_bridge", "link_native_library", "load_strategy_definition",
    "validate_handler", "validate_strategy_definition", "verify_artifact", "write_artifact",
]
