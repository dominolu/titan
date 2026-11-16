"""Build-server-only CLI replay checks for explicit host depth metadata (no accounts)."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess


def canonical_snapshot_digest(snapshot):
    """Mirror of titan-market-service MetadataSnapshot::canonical_bytes."""
    instruments = [{key: item.get(key) for key in (
        "asset_no", "contract_multiplier", "linear_quote_currency", "min_notional_quote",
        "min_quantity", "native_symbol", "price_tick", "quantity_lot", "source_key",
        "synthetic")} for item in snapshot["instruments"]]
    instruments.sort(key=lambda item: (item["source_key"] or "", item["asset_no"],
                                       item["native_symbol"]))
    canonical = json.dumps({"captured_at_ns": snapshot["captured_at_ns"],
                            "instruments": instruments,
                            "schema_version": snapshot["schema_version"]},
                           sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return list(hashlib.sha256(canonical.encode()).digest())


def schema_4_envelope(repo_root):
    """Trace schema 4 requires the ABI identity, a metadata snapshot and account definitions."""
    fixture = json.loads((repo_root / "fixtures" / "grid_v13_metadata.json").read_text())
    metadata = {"schema_version": 1, "captured_at_ns": 1700000000000, "instruments": [{
        "asset_no": 0, "native_symbol": "TEST-SYNTH-BTC",
        "price_tick": {"coefficient": 1, "scale": 1},
        "quantity_lot": {"coefficient": 1, "scale": 2},
        "contract_multiplier": {"coefficient": 1, "scale": 0},
        "linear_quote_currency": "USDT", "synthetic": True}]}
    metadata["digest"] = canonical_snapshot_digest(metadata)
    units = {"price_tick": metadata["instruments"][0]["price_tick"],
             "quantity_lot": metadata["instruments"][0]["quantity_lot"],
             "contract_multiplier": metadata["instruments"][0]["contract_multiplier"]}
    return {
        "schema_version": 4,
        "runtime_abi": fixture["runtime_abi"],
        "metadata": metadata,
        "accounts": [{
            "account_key": "trace-account-0", "account_id": 1, "connector_type": "trace",
            "credential_ref": "secret://trace", "connector_config": "",
            "instruments": [dict(native_symbol="TEST-SYNTH-BTC", asset_id=1,
                                 linear_quote_currency=1, **units)],
            "currencies": [{"native_currency": "USDT", "currency_id": 1,
                            "amount_unit": "0.000001"}],
            "ownership": "ObserveAll", "shutdown_order_policy": "LeaveOpen",
            "enabled": True, "definition_version": 1}],
        "account_instruments": [dict(account_no=0, asset_id=1,
                                     native_symbol="TEST-SYNTH-BTC", **units)],
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--strategy", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    addresses = subprocess.check_output(["hostname", "-I"], text=True).split()
    if __import__("os").environ["TITAN_BUILD_ADDRESS"] not in addresses:
        raise SystemExit("Run only on build server titan-build")
    args.output.mkdir(parents=True, exist_ok=False)
    image = [dict(asset_no=0, side=side, price_ticks=price, qty_lots=10, action=1,
                  is_snapshot=1, exchange_ts_ns=1_100_000_000, receive_ts_ns=1_100_000_000)
             for i in range(1000) for side, price in [(1, 10000-i), (2, 10001+i)]]
    info = dict(version=1, asset_no=0, stream_epoch=1, first_update_sequence=10,
                last_update_sequence=10, publication_sequence=1, valid=1, reset=1)
    trace = schema_4_envelope(Path(__file__).resolve().parent.parent)
    trace.update(initial=dict(now_ns=1_000_000_000, markets=[dict(asset_no=0)]),
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
        config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 8\n')
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
