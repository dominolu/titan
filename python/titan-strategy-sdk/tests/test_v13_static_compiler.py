from pathlib import Path
import ctypes
import json
import tempfile
import unittest

from titan_strategy import abi_v13
from titan_strategy.static_compiler import (
    CompileRequest, StrategyCompileError, compile_package, load_strategy_definition,
    validate_strategy_definition, verify_artifact,
)


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
            pair = compile_package(CompileRequest(
                source, {"initial_counter": 4}, "x86_64-unknown-linux-gnu", "x86-64",
                "pair", root / "pair" / "v13_smoke", runtime_abi,
            ))
            self.assertEqual(len(pair.artifact.paths), 2)
            self.assertFalse(hasattr(pair, "native"))
            manifest = verify_artifact(root / "pair" / "v13_smoke.manifest.cbor", runtime_abi)
            self.assertEqual(manifest["callback_mask"], 2)
            bundle = compile_package(CompileRequest(
                source, {"initial_counter": 4}, "x86_64-unknown-linux-gnu", "x86-64",
                "bundle", root / "bundle" / "v13_smoke.titan", runtime_abi,
            ))
            self.assertEqual(len(bundle.artifact.paths), 1)
            self.assertEqual(verify_artifact(bundle.artifact.paths[0], runtime_abi)["state_len"], 48)
            self.assertEqual(pair.artifact.artifact_digest, bundle.artifact.artifact_digest)

    def test_missing_public_view_returns_stable_callback_error(self):
        source = Path(__file__).parent / "fixtures" / "v13_missing_view.py"
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        with tempfile.TemporaryDirectory() as temporary:
            result = compile_package(CompileRequest(
                source, {}, "x86_64-unknown-linux-gnu", "x86-64", "pair",
                Path(temporary) / "missing_view", runtime_abi,
            ))
            library = ctypes.CDLL(str(result.artifact.paths[0]))
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
            self.assertEqual(callback(context.ctypes.data), -4)

    def test_pair_arb_full_state_machine_aot_compiles(self):
        root = Path(__file__).parents[3]
        source = root / "strategies" / "pair_arb" / "strategy.py"
        parameters = json.loads(
            (root / "strategies" / "pair_arb" / "parameters.json").read_text()
        )
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        with tempfile.TemporaryDirectory() as temporary:
            result = compile_package(CompileRequest(
                source, parameters, "x86_64-unknown-linux-gnu", "x86-64-v2",
                "bundle", Path(temporary) / "pair_arb.titan", runtime_abi,
            ))
            manifest = verify_artifact(result.artifact.paths[0], runtime_abi)
            self.assertEqual(manifest["strategy_id"], "pair_arb")
            self.assertEqual(manifest["callback_mask"], 4083)

    def test_all_repository_v13_examples_aot_compile(self):
        root = Path(__file__).parents[3]
        runtime_abi = {"abi_version": 13, "fingerprint": abi_v13.ABI_FINGERPRINT.hex()}
        for name in ("dual_ma", "event_counter"):
            with self.subTest(strategy=name), tempfile.TemporaryDirectory() as temporary:
                strategy_root = root / "strategies" / name
                parameters = json.loads((strategy_root / "parameters.json").read_text())
                result = compile_package(CompileRequest(
                    strategy_root / "strategy.py", parameters,
                    "x86_64-unknown-linux-gnu", "x86-64-v2", "bundle",
                    Path(temporary) / f"{name}.titan", runtime_abi,
                ))
                manifest = verify_artifact(result.artifact.paths[0], runtime_abi)
                self.assertEqual(manifest["strategy_id"], name)
                self.assertEqual(manifest["abi_version"], 13)


if __name__ == "__main__":
    unittest.main()
