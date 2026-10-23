"""Internal Numba bridge protocol. Use `titan strategy compile` for artifacts."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from .static_compiler import (
    describe_strategy, emit_objects, load_strategy_definition, validate_strategy_definition,
)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("describe", "emit-objects"))
    parser.add_argument("--request", required=True, type=Path)
    parser.add_argument("--work-dir", required=True, type=Path)
    args = parser.parse_args()
    request = json.loads(args.request.read_text(encoding="utf-8"))
    if request.get("protocol_version") != 1:
        raise SystemExit("unsupported Numba bridge protocol")
    definition = load_strategy_definition(Path(request["source"]), request["parameters"])
    strategy = validate_strategy_definition(definition, request["runtime_abi"])
    if args.action == "emit-objects":
        objects = emit_objects(strategy, args.work_dir, request["cpu_baseline"])
        (args.work_dir / "objects.json").write_text(json.dumps(objects, sort_keys=True), encoding="utf-8")
    report = args.work_dir / (args.action + ".json")
    report.write_text(json.dumps(describe_strategy(strategy), sort_keys=True, allow_nan=False), encoding="utf-8")


if __name__ == "__main__":
    main()
