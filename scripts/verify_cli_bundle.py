"""Non-live CLI run/report, integrity, and SIGTERM integration verification."""
import hashlib
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from titan_reporting import BundleError, load_bundle

binary, root = Path(sys.argv[1]), Path(sys.argv[2])


def canonical_digest(snapshot):
    """Same canonical bytes as titan-market-service MetadataSnapshot::canonical_bytes."""
    instruments = []
    for instrument in snapshot["instruments"]:
        instruments.append({key: instrument.get(key) for key in (
            "asset_no", "contract_multiplier", "linear_quote_currency", "min_notional_quote",
            "min_quantity", "native_symbol", "price_tick", "quantity_lot", "source_key",
            "synthetic")})
    instruments.sort(key=lambda item: (item["source_key"] or "", item["asset_no"],
                                       item["native_symbol"]))
    document = {"captured_at_ns": snapshot["captured_at_ns"], "instruments": instruments,
                "schema_version": snapshot["schema_version"]}
    canonical = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return list(hashlib.sha256(canonical.encode()).digest())


def trace_schema_4():
    """Schema 4 carries the ABI identity, the metadata snapshot and the account definitions.

    The ABI fingerprint is taken from a committed fixture so this script never has to recompile a
    strategy to learn the host constants; the host still rejects the trace if they diverge.
    """
    fixture = json.loads((Path(__file__).resolve().parent.parent
                          / "fixtures" / "grid_v13_metadata.json").read_text())
    metadata = {
        "schema_version": 1, "captured_at_ns": 1700000000000,
        "instruments": [{
            "asset_no": 0, "native_symbol": "TEST-SYNTH-BTC",
            "price_tick": {"coefficient": 1, "scale": 1},
            "quantity_lot": {"coefficient": 1, "scale": 2},
            "contract_multiplier": {"coefficient": 1, "scale": 0},
            "linear_quote_currency": "USDT", "synthetic": True}],
    }
    metadata["digest"] = canonical_digest(metadata)
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
        "initial": {"now_ns": 1000, "markets": [{"asset_no": 0, "best_bid_ticks": 2}],
                    "accounts": [{"account_no": 0, "state": 4}],
                    "positions": [{"account_no": 0, "asset_no": 0}]},
        "events": [{"kind": "tick", "now_ns": 2000,
                    "ticks": [{"asset_no": 0, "price_ticks": 2, "qty_lots": 1}]}],
    }


trace = trace_schema_4()
(root / "trace.json").write_text(json.dumps(trace))
config = root / "backtest.toml"
config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 8\n')
run = subprocess.run([str(binary), "run", str(root / "smoke.titan"), "-e", "backtest", "-m", "tick", "-c", str(config), "--json"], text=True, capture_output=True, timeout=30)
assert run.returncode == 0, (run.stdout, run.stderr)
records = []
remaining = run.stdout.lstrip()
while remaining:
    value, length = json.JSONDecoder().raw_decode(remaining)
    records.append(value)
    remaining = remaining[length:].lstrip()
run_id = next(record.get("run_id", record.get("id")) for record in records if "run_id" in record or "id" in record)
shown = subprocess.run([str(binary), "show", run_id, "--json"], check=True, text=True, capture_output=True)
record = json.loads(shown.stdout)
if "run" in record:
    record = record["run"]
bundle_path = Path(record["result_path"]).parent
bundle = load_bundle(bundle_path)
assert bundle.result["event_count"] == 1
assert bundle.result["submit_count"] == 1
assert bundle.result["returns"] == []
for renderer in ("native", "quantstats"):
    output = root / f"{run_id}-{renderer}.html"
    subprocess.run([str(binary), "report", run_id, "--renderer", renderer, "--output", str(output), "--json"], check=True, text=True, capture_output=True, timeout=30)
    assert output.is_file() and output.stat().st_size > 0
copy = root / f"{run_id}-corrupt"
shutil.copytree(bundle_path, copy)
(copy / "result.json").write_text("{}")
try:
    load_bundle(copy)
except BundleError:
    pass
else:
    raise AssertionError("corrupt result was accepted")
(copy / "manifest.json").unlink()
try:
    load_bundle(copy)
except BundleError:
    pass
else:
    raise AssertionError("uncommitted bundle was accepted")
# No markets, accounts, credentials, or strategies: only exercise local runtime lifecycle.
core_config = root / "empty-core.toml"
core_config.write_text("schema_version = 1\n")
process = subprocess.Popen([str(binary), "core-run", "-c", str(core_config), "--json"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
try:
    import selectors
    selector = selectors.DefaultSelector()
    selector.register(process.stdout, selectors.EVENT_READ)
    assert selector.select(timeout=15), "core runtime did not become ready"
    initial = process.stdout.readline()
    assert json.loads(initial)["status"] == "RUNNING", initial
    process.send_signal(signal.SIGTERM)
    stdout, stderr = process.communicate(timeout=15)
    assert process.returncode == 0, (stdout, stderr)
    assert '"status":"STOPPED"' in stdout and '"reason":"SIGTERM"' in stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
print(f"CLI run -> report, corrupt/incomplete bundle, and non-live SIGTERM passed: {run_id}")
