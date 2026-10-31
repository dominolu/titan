"""Internal Numba bridge protocol. Use `titan strategy compile` for artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .static_compiler import (
    describe_strategy, emit_objects, load_strategy_definition, validate_strategy_definition,
)
from .testing import run_native_tests, validate_tests


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("describe", "emit-objects", "test-native"))
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding="utf-8"))
    if request.get("protocol_version") != 1:
        raise SystemExit("unsupported Numba bridge protocol")
    definition = load_strategy_definition(Path(request["source"]), request["parameters"])
    (args.work_dir / "normalized-parameters.json").write_text(
        json.dumps(definition.spec.validate_parameters(request["parameters"]), sort_keys=True, allow_nan=False),
        encoding="utf-8",
    )
    strategy = validate_strategy_definition(definition, request["runtime_abi"])
    cases = validate_tests(definition.tests)
    (args.work_dir / (args.action + "-tests.json")).write_text(
        json.dumps(cases, sort_keys=True, allow_nan=False), encoding="utf-8",
    )
    if args.action == "test-native":
        report = run_native_tests(strategy, args.work_dir / "native.so", cases)
        (args.work_dir / "native-tests.json").write_text(
            json.dumps(report, sort_keys=True, allow_nan=False), encoding="utf-8",
        )
        (args.work_dir / "test-native.json").write_text(
            json.dumps(describe_strategy(strategy), sort_keys=True, allow_nan=False), encoding="utf-8",
        )
        return
    if args.action == "emit-objects":
        objects = emit_objects(strategy, args.work_dir, request["cpu_baseline"])
        (args.work_dir / "objects.json").write_text(json.dumps(objects, sort_keys=True), encoding="utf-8")
    report = args.work_dir / (args.action + ".json")
    report.write_text(json.dumps(describe_strategy(strategy), sort_keys=True, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    main()
