from pathlib import Path
import ctypes
import json
import tempfile
import unittest
import os
import subprocess
from dataclasses import dataclass
from types import SimpleNamespace

from titan_strategy import abi_v13
from titan_strategy.static_compiler import (
    StrategyCompileError, load_strategy_definition, validate_strategy_definition,
)


@dataclass
class _Request:
    source_file: Path
    parameters: dict
    target_triple: str
    cpu_baseline: str
    strategy_format: str
    output_path: Path
    runtime_abi: dict


def _rust(*args):
    root = Path(__file__).parents[3]
    binary = os.environ.get("TITAN_TEST_BINARY", str(root / "target/debug/titan"))
    result = subprocess.run([binary, "strategy", *map(str, args), "--json"],
                            check=True, capture_output=True, text=True, timeout=360)
    return json.loads(result.stdout)


def _compile_with_rust(request):
    # Test adapter only: exercise the production Rust controller and keep dtype
    # access here for ctypes tests of the Numba ABI. No artifact logic lives in SDK.
    parameters = request.output_path.parent / "parameters.json"
    parameters.write_text(json.dumps(request.parameters))
    result = _rust("compile", "--strategy", request.source_file, "--parameters", parameters,
                   "--target", request.target_triple, "--cpu-baseline", request.cpu_baseline,
                   "--strategy-format", request.strategy_format, "--output", request.output_path)
    strategy = validate_strategy_definition(load_strategy_definition(request.source_file, request.parameters), request.runtime_abi)
    return SimpleNamespace(strategy=strategy, compiled_strategy=SimpleNamespace(
        paths=tuple(Path(path) for path in result["paths"]), compiled_strategy_digest=result["compiled_strategy_digest"]))


def _inspect_with_rust(path, _runtime_abi):
    return _rust("show", path)["strategy"]


class TestV13StaticCompiler(unittest.TestCase):
    def test_strategy_definition_load_runs_in_restricted_worker(self):
        fixture = Path(__file__).parent / "fixtures" / "v13_build_side_effect.py"
        with self.assertRaisesRegex(StrategyCompileError, "read-only"):
            load_strategy_definition(fixture, {})

    def test_readonly_view_and_bad_signature_are_rejected(self):
        fixtures = Path(__file__).parent / "fixtures"
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        for name in (
            "v13_readonly_violation.py",
            "v13_bad_signature.py",
            "v13_missing_capability.py",
        ):
            with self.subTest(name=name), self.assertRaises(StrategyCompileError):
                definition = load_strategy_definition(fixtures / name, {})
                validate_strategy_definition(definition, runtime_abi)

    def test_smoke_strategy_builds_pair_and_bundle(self):
        source = Path(__file__).parents[3] / "strategies" / "v13_smoke" / "strategy.py"
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "pair").mkdir()
            (root / "bundle").mkdir()
            pair = _compile_with_rust(_Request(
                source, {"initial_counter": 4}, "x86_64-unknown-linux-gnu", "x86-64",
                "pair", root / "pair" / "v13_smoke", runtime_abi,
            ))
            self.assertEqual(len(pair.compiled_strategy.paths), 2)
            self.assertFalse(hasattr(pair, "native"))
            manifest = _inspect_with_rust(root / "pair" / "v13_smoke.manifest.cbor", runtime_abi)
            self.assertEqual(manifest["callback_mask"], 4098)
            bundle = _compile_with_rust(_Request(
                source, {"initial_counter": 4}, "x86_64-unknown-linux-gnu", "x86-64",
                "bundle", root / "bundle" / "v13_smoke.titan", runtime_abi,
            ))
            self.assertEqual(len(bundle.compiled_strategy.paths), 1)
            self.assertEqual(_inspect_with_rust(bundle.compiled_strategy.paths[0], runtime_abi)["state_len"], 48)
            self.assertEqual(pair.compiled_strategy.compiled_strategy_digest, bundle.compiled_strategy.compiled_strategy_digest)

    def test_missing_public_view_returns_stable_callback_error(self):
        for fixture, expected in (("v13_missing_view.py", -4), ("v13_handler_error.py", -1)):
            with self.subTest(fixture=fixture):
                source = Path(__file__).parent / "fixtures" / fixture
                runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
                with tempfile.TemporaryDirectory() as temporary:
                    result = _compile_with_rust(_Request(
                        source, {}, "x86_64-unknown-linux-gnu", "x86-64", "pair",
                        Path(temporary) / "missing_view", runtime_abi,
                    ))
                    library = ctypes.CDLL(str(result.compiled_strategy.paths[0]))
                    callback = library.titan_strategy_on_tick
                    callback.argtypes = [ctypes.c_void_p]
                    callback.restype = ctypes.c_int32
                    context = __import__("numpy").zeros(1, dtype=abi_v13.runtime_context_dtype)
                    state = (ctypes.c_uint8 * result.strategy.state_layout.itemsize)()
                    context[0]["struct_size"] = abi_v13.runtime_context_dtype.itemsize
                    context[0]["abi_version"] = 13
                    context[0]["state_ptr"] = ctypes.addressof(state)
                    context[0]["state_len"] = result.strategy.state_layout.itemsize
                    context[0]["state_alignment"] = result.strategy.state_layout.alignment
                    context[0]["state_schema_version"] = 1
                    context[0]["state_schema_hash"] = list(result.strategy.schema_hash)
                    self.assertEqual(callback(context.ctypes.data), expected)

    def test_native_hedge_views_and_order_side(self):
        import numpy as np
        source = Path(__file__).parent / "fixtures" / "v13_hedge_views.py"
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        with tempfile.TemporaryDirectory() as temporary:
            result = _compile_with_rust(_Request(
                source, {}, "x86_64-unknown-linux-gnu", "x86-64", "pair",
                Path(temporary) / "hedge", runtime_abi,
            ))
            library = ctypes.CDLL(str(result.compiled_strategy.paths[0]))
            callback = library.titan_strategy_on_tick
            callback.argtypes = [ctypes.c_void_p]
            callback.restype = ctypes.c_int32
            context = np.zeros(1, dtype=abi_v13.runtime_context_dtype)
            state = np.zeros(1, dtype=result.strategy.state_layout.dtype)
            positions = np.zeros(2, dtype=abi_v13.position_dtype)
            positions[0]["position_side"], positions[0]["qty_lots"] = 1, 2
            positions[1]["position_side"], positions[1]["qty_lots"] = 2, -1
            observed = []
            @ctypes.CFUNCTYPE(ctypes.c_int32, ctypes.c_void_p, ctypes.c_void_p,
                             ctypes.POINTER(ctypes.c_uint64))
            def submit(command_context, request, output):
                data = ctypes.string_at(request, abi_v13.submit_order_request_dtype.itemsize)
                row = np.frombuffer(data, dtype=abi_v13.submit_order_request_dtype)[0]
                observed.append((int(row["position_side"]), int(row["side"]), int(row["reduce_only"])))
                output[0] = 7
                return 0
            context[0]["struct_size"] = abi_v13.runtime_context_dtype.itemsize
            context[0]["abi_version"] = 13
            context[0]["state_ptr"] = state.ctypes.data
            context[0]["state_len"] = state.nbytes
            context[0]["state_alignment"] = result.strategy.state_layout.alignment
            context[0]["state_schema_version"] = 1
            context[0]["state_schema_hash"] = list(result.strategy.schema_hash)
            context[0]["positions_ptr"] = positions.ctypes.data
            context[0]["positions_len"] = len(positions)
            context[0]["submit_order"] = ctypes.cast(submit, ctypes.c_void_p).value
            self.assertEqual(callback(context.ctypes.data), 0)
            self.assertEqual((int(state[0]["long_qty"]), int(state[0]["short_qty"])), (2, -1))
            self.assertEqual(observed, [(2, 1, 1)])
            # A missing requested side fails instead of returning the other side.
            context[0]["positions_len"] = 1
            self.assertEqual(callback(context.ctypes.data), -4)

    def test_pair_arb_full_state_machine_aot_compiles(self):
        root = Path(__file__).parents[3]
        source = root / "strategies" / "pair_arb" / "strategy.py"
        parameters = json.loads(
            (root / "strategies" / "pair_arb" / "parameters.json").read_text()
        )
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        with tempfile.TemporaryDirectory() as temporary:
            result = _compile_with_rust(_Request(
                source, parameters, "x86_64-unknown-linux-gnu", "x86-64-v2",
                "bundle", Path(temporary) / "pair_arb.titan", runtime_abi,
            ))
            manifest = _inspect_with_rust(result.compiled_strategy.paths[0], runtime_abi)
            self.assertEqual(manifest["strategy_type"], "pair_arb")
            self.assertEqual(manifest["callback_mask"], 8179)

    def test_all_repository_v13_examples_aot_compile(self):
        root = Path(__file__).parents[3]
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        for name in ("dual_ma", "event_counter"):
            with self.subTest(strategy=name), tempfile.TemporaryDirectory() as temporary:
                strategy_root = root / "strategies" / name
                parameters = json.loads((strategy_root / "parameters.json").read_text())
                result = _compile_with_rust(_Request(
                    strategy_root / "strategy.py", parameters,
                    "x86_64-unknown-linux-gnu", "x86-64-v2", "bundle",
                    Path(temporary) / f"{name}.titan", runtime_abi,
                ))
                manifest = _inspect_with_rust(result.compiled_strategy.paths[0], runtime_abi)
                self.assertEqual(manifest["strategy_type"], name)
                self.assertEqual(manifest["abi_version"], 13)


if __name__ == "__main__":
    unittest.main()
