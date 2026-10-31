"""Compile-only helper for the historical native grid replay. Run on 192.168.3.88."""
from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path

from strategies import grid
from titan_strategy.state import canonical_state_schema, state_schema_hash, validate_state_dtype


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--binary", type=Path, default=Path("target/debug/titan"))
    parser.add_argument("--regime", action="append", choices=("sideways", "up", "down"))
    parser.add_argument("--direction", action="append", choices=("neutral", "long_bias", "short_bias"))
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    layout = validate_state_dtype(grid.state_dtype, max_state_bytes=65536, max_alignment=8)
    fields = {}

    def visit(dtype, prefix="", base=0):
        for name in dtype.names:
            child, offset = dtype.fields[name]
            key = prefix + name
            if child.names:
                visit(child, key + ".", base + offset)
            elif child.subdtype is None:
                fields[key] = {"offset": base + offset, "size": child.itemsize, "kind": child.kind}

    visit(grid.state_dtype)
    schema = {"state_len": layout.itemsize, "schema_version": grid.SPEC.state_schema_version,
              "schema_hash": state_schema_hash(canonical_state_schema(layout, schema_version=grid.SPEC.state_schema_version)).hex(),
              "fields": fields}
    (args.output / "layout.json").write_text(json.dumps(schema, indent=2))
    runs = []
    for regime, day in (("sideways", "2023-09-02"), ("up", "2023-10-23"), ("down", "2023-08-17")):
        if args.regime and regime not in args.regime:
            continue
        summary = json.loads((args.data / (day + ".json")).read_text())
        initial_mid = summary["start_mid_ticks"]
        # Only the first available BBO chooses the interval; no later high/low or optimized bounds.
        lower, upper = int(initial_mid * 0.96), int(initial_mid * 1.04)
        for direction in ("neutral", "long_bias", "short_bias"):
            if args.direction and direction not in args.direction:
                continue
            name = regime + "-" + direction
            parameters = {
                "direction": direction, "neutral_mode": "two_sided", "spacing_mode": "arithmetic",
                "order_policy": "reset_pair", "anchor_mode": "order_average",
                "lower_price_ticks": lower, "upper_price_ticks": upper, "grid_levels": 40,
                "buy_levels": 1, "sell_levels": 1, "size_mode": "allocated_fixed_qty",
                "allocated_capital_tick_lots": 100_000_000, "leverage": 3,
                "max_notional_tick_lots": 300_000_000, "max_loss_tick_lots": 10_000_000,
                "stop_below_ticks": int(initial_mid * 0.95), "stop_above_ticks": int(initial_mid * 1.05),
                "min_order_lots": 1, "min_notional_tick_lots": 50_000,
                "quote_units_per_tick_lot": 1.0, "max_slippage_ticks": 50,
                "order_timeout_ns": 10_000_000_000, "market_stale_after_ns": 5_000_000_000,
                "flatten_timeout_ns": 120_000_000_000,
                "retry_backoff_ns": 500_000_000, "max_rejections": 5, "stop_action": "flatten",
            }
            parameters_path = args.output / (name + ".parameters.json")
            parameters_path.write_text(json.dumps(parameters, indent=2))
            artifact = args.output / (name + ".titan")
            command = [str(args.binary.resolve()), "strategy", "compile", "--strategy", "strategies/grid.py",
                       "--parameters", str(parameters_path), "--target", "x86_64-unknown-linux-gnu",
                       "--cpu-baseline", "x86-64-v2", "--artifact-format", "bundle", "--output", str(artifact), "--json"]
            result = subprocess.run(command, capture_output=True, text=True, timeout=360)
            (args.output / (name + ".compile.txt")).write_text(result.stdout + result.stderr)
            if result.returncode:
                raise SystemExit(f"native compilation failed for {name}: {result.stdout}{result.stderr}")
            runs.append({"name": name, "regime": regime, "date": day, "direction": direction,
                         "parameters": str(parameters_path), "strategy_version": str(artifact), "data": str(args.data / (day + ".bin"))})
            print("compiled", name, flush=True)
    (args.output / "runs.json").write_text(json.dumps(runs, indent=2))


if __name__ == "__main__":
    main()
