#!/usr/bin/env python3
"""Exercise M1 maintenance against original previous-binary bytes. Build server only."""
import argparse
import fcntl
import hashlib
import json
from pathlib import Path
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument("--binary", type=Path, required=True)
parser.add_argument("--fixture", type=Path, required=True)
parser.add_argument("--output", type=Path, required=True)
args = parser.parse_args()
assert "192.168.3.88" in subprocess.check_output(["hostname", "-I"]).decode()
args.output.mkdir(parents=True, exist_ok=False)
source = args.fixture / "live/checkpoints/synthetic-gate-1/77"
point = source / "strategy-77.json"
watermark = source / "strategy-77.generation"
original = point.read_bytes()
original_watermark = watermark.read_bytes()
sha = hashlib.sha256(original).hexdigest()
database = args.output / "runtime.db"
checks = []


def call(command, *, target=database, success=True):
    result = subprocess.run([str(args.binary), "recovery", "--database", str(target), *command],
                            capture_output=True, text=True)
    assert (result.returncode == 0) == success, (command, result.stdout, result.stderr)
    return json.loads(result.stdout) if result.returncode == 0 else result.stdout + result.stderr


import_command = ["import-legacy", "--point", str(point), "--generation-watermark", str(watermark),
                  "--expected-sha256", sha]
busy_source = args.output / "busy-source"
busy_source.mkdir()
(busy_source / point.name).write_bytes(original)
(busy_source / watermark.name).write_bytes(original_watermark)
with (busy_source / "owner.lock").open("w") as owner:
    fcntl.flock(owner, fcntl.LOCK_EX | fcntl.LOCK_NB)
    failure = call(["import-legacy", "--point", str(busy_source / point.name),
                    "--generation-watermark", str(busy_source / watermark.name),
                    "--expected-sha256", sha], target=args.output / "busy.db", success=False)
    assert "still owned" in failure
checks.append("live-legacy-owner-lock-prevents-migration")
reference = call(import_command)["reference"]
assert call(import_command)["reference"] == reference
checks.append("original-byte-import-and-idempotent-receipt")
assert call(["inspect"])["heads"][0]["reference"] == reference
encoded = json.dumps(reference)
call(["pin", "--reference", encoded, "--operation-id", "maintenance-verification"])
call(["pin", "--reference", encoded, "--operation-id", "maintenance-verification"])
assert call(["inspect"])["pins"] == 1
checks.append("fixed-reference-and-idempotent-pin")
output = args.output / "exported.json"
call(["export", "--reference", encoded, "--output", str(output)])
assert output.read_bytes() == original
call(["export", "--reference", encoded, "--output", str(output)], success=False)
checks.append("byte-identical-export-and-no-overwrite")
wrong = {**reference, "store_id": "wrong"}
call(["export", "--reference", json.dumps(wrong), "--output", str(args.output / "wrong.json")], success=False)
call([*import_command[:-1], "0" * 64], success=False)
checks.append("wrong-store-and-changed-source-rejected")
backup = args.output / "backup.db"
call(["backup", "--output", str(backup)])
assert call(["inspect"], target=backup)["activation_allowed"] is False
call(["backup", "--output", str(backup)], success=False)
checks.append("consistent-backup-quarantine-and-no-overwrite")
call(["prune", "--strategy-no", "77", "--retain", "1"])
assert call(["inspect"])["pins"] == 1
call(["unpin", "--reference", encoded, "--operation-id", "maintenance-verification"])
assert call(["inspect"])["pins"] == 0
checks.append("retention-respects-fixed-operation-reference")
assert point.read_bytes() == original and watermark.read_bytes() == original_watermark
checks.append("legacy-source-and-generation-watermark-unchanged")
generation_only = args.output / "generation-only-source"
generation_only.mkdir()
generation_file = generation_only / watermark.name
generation_file.write_bytes(original_watermark)
generation_database = args.output / "generation-only.db"
generation_command = ["import-generation", "--strategy-no", "77", "--generation-watermark", str(generation_file),
                      "--expected-sha256", hashlib.sha256(original_watermark).hexdigest()]
assert call(generation_command, target=generation_database)["recovery_point_created"] is False
call(generation_command, target=generation_database)
generation_info = call(["inspect"], target=generation_database)
assert generation_info["records"] == 0 and generation_info["generation_watermarks"][0]["generation"] == str(int.from_bytes(original_watermark, "little"))
checks.append("generation-only-migration-preserves-reservations-without-forging-state")
result = {"passed": True, "checks": checks, "reference": reference,
          "source_sha256": sha, "generation_floor": str(int.from_bytes(original_watermark, "little"))}
(args.output / "results.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result))
