"""Non-live CLI run/report, integrity, and SIGTERM integration verification."""
import json
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

from titan_reporting import BundleError, load_bundle

binary, root = Path(sys.argv[1]), Path(sys.argv[2])
trace = {
    "schema_version": 1,
    "initial": {"now_ns": 1000, "markets": [{"asset_no": 0, "best_bid_ticks": 2}],
                "accounts": [{"account_no": 0, "state": 4}],
                "positions": [{"account_no": 0, "asset_no": 0}]},
    "events": [{"kind": "tick", "now_ns": 2000, "ticks": [{"asset_no": 0, "price_ticks": 2, "qty_lots": 1}]}],
}
(root / "trace.json").write_text(json.dumps(trace))
config = root / "backtest.toml"
config.write_text('schema_version = 1\n[backtest]\ndata = "trace.json"\ncommand_capacity = 8\nallow_unsigned_artifact = true\n')
run = subprocess.run([str(binary), "run", str(root / "v13_smoke.titan"), "-e", "backtest", "-m", "tick", "-c", str(config), "--json"], text=True, capture_output=True, timeout=30)
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
