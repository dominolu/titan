"""Verify an actual previous binary's strategy/recovery bytes using the current native host."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--fixture", type=Path, required=True)
parser.add_argument("--repo", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--cargo-config", type=Path)
args = parser.parse_args()
assert __import__("os").environ["TITAN_BUILD_ADDRESS"] in subprocess.check_output(["hostname", "-I"]).decode()
point = args.fixture / "live/checkpoints/synthetic-gate-1/77/strategy-77.json"
strategy = next((args.fixture / "build/operations").glob("*/strategy.titan"))
paths = (point, strategy)
before = {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
env = dict(os.environ, TITAN_TEST_PREVIOUS_RECOVERY_POINT=str(point),
           TITAN_TEST_PREVIOUS_STRATEGY_FILE=str(strategy))
cargo = ["cargo"] + (["--config", str(args.cargo_config)] if args.cargo_config else [])
command = cargo + ["test", "--locked", "-p", "titan-strategy-runtime",
           "historical_recovery_point_loads_original_native_strategy", "--", "--ignored", "--nocapture"]
subprocess.run(command, cwd=args.repo, env=env, check=True)
boundary_command = cargo + ["test", "--locked", "-p", "titan-strategy-runtime",
                    "--test", "observation_boundaries", "--", "--ignored", "--nocapture", "--test-threads=1"]
subprocess.run(boundary_command, cwd=args.repo, env=env, check=True)
assert before == {str(path): hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
args.output.write_text(json.dumps({"passed": True, "original_files_unchanged": True,
                                  "inputs_sha256": before, "command": command,
                                  "boundary_command": boundary_command}, indent=2))
