"""Build-server-only CLI replay checks for explicit host depth metadata (no accounts)."""
import argparse
import json
import os
from pathlib import Path
import subprocess


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--strategy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    addresses = subprocess.check_output(["hostname", "-I"], text=True).split()
    if "192.168.3.88" not in addresses:
        raise SystemExit("Run only on build server 192.168.3.88")
    args.output.mkdir(parents=True, exist_ok=False)
    image = [dict(asset_no=0, side=side, price_ticks=price, qty_lots=10, action=1,
                  is_snapshot=1, exchange_ts_ns=1_100_000_000, receive_ts_ns=1_100_000_000)
             for i in range(1000) for side, price in [(1, 10000-i), (2, 10001+i)]]
    info = dict(version=1, asset_no=0, stream_epoch=1, first_update_sequence=10,
                last_update_sequence=10, publication_sequence=1, valid=1, reset=1)
    trace = dict(schema_version=1, initial=dict(now_ns=1_000_000_000, markets=[dict(asset_no=0)]),
                 events=[dict(kind="depth", now_ns=1_100_000_000, depth=image, depth_batch=info),
                         dict(kind="stop", now_ns=1_200_000_000)])
    results = []
    for name, metadata in [("explicit-metadata", True), ("missing-metadata", False)]:
        directory = args.output / name
        directory.mkdir()
        data = json.loads(json.dumps(trace))
        if not metadata:
            del data["events"][0]["depth_batch"]
        (directory / "trace.json").write_text(json.dumps(data))
        config = directory / "runtime.toml"
        config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 8\nallow_unsigned_strategy = true\n')
        result = subprocess.run([str(args.binary.resolve()), "run", str(args.strategy.resolve()),
                                 "-e", "backtest", "-m", "tick", "-c", str(config.resolve()), "--json"],
                                env=dict(os.environ, TITAN_HOME=str((directory / "home").resolve())),
                                capture_output=True, text=True, timeout=30)
        (directory / "stdout.log").write_text(result.stdout)
        (directory / "stderr.log").write_text(result.stderr)
        if metadata:
            assert result.returncode == 0, result.stdout + result.stderr
        else:
            assert result.returncode != 0, "missing metadata was silently accepted"
            assert "on_depth_batch" in result.stdout + result.stderr, result.stdout + result.stderr
        results.append(dict(case=name, exit_code=result.returncode, passed=True))
    (args.output / "summary.json").write_text(json.dumps(results, indent=2) + "\n")
    print(json.dumps(results))


if __name__ == "__main__":
    main()
