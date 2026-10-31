#!/usr/bin/env bash
# Non-live verification on 192.168.3.88. Never supplies account credentials.
set -euo pipefail
if [[ $# != 1 || $1 != /* ]]; then echo 'usage: verify_native_venues.sh ABSOLUTE_REPORT_DIRECTORY' >&2; exit 2; fi
verification_report=$1
if [[ -e "$verification_report" ]]; then echo 'report directory must be new' >&2; exit 2; fi
mkdir -p "$verification_report"
: "${TITAN_LIGHTER_SIGNER_TEST_PATH:?server-built official signer is required}"
: "${TITAN_LIGHTER_SIGNER_TEST_SHA256:?verified signer SHA256 is required}"
: "${TITAN_VENUES_SOURCE_LIST:?source list is required}"
cargo_checked() {
  local command_name=$1
  shift
  if [[ -n ${TITAN_CARGO_REGISTRY_CONFIG:-} ]]; then
    cargo "$command_name" --config "$TITAN_CARGO_REGISTRY_CONFIG" "$@"
  else
    cargo "$command_name" "$@"
  fi
}
run_check() {
  local check_name=$1
  shift
  local check_status=0
  "$@" > "$verification_report/$check_name.log" 2>&1 || check_status=$?
  printf '%s\t%s\n' "$check_name" "$check_status" >> "$verification_report/status.tsv"
  if [[ $check_status != 0 ]]; then tail -60 "$verification_report/$check_name.log"; return "$check_status"; fi
}
source_manifest() {
 python3 - "$TITAN_VENUES_SOURCE_LIST" "$1" <<'PY'
import hashlib,json,pathlib,sys
files=pathlib.Path(sys.argv[1]).read_bytes().split(b'\0')
values={}
for raw in files:
 if not raw: continue
 path=pathlib.Path(raw.decode())
 values[str(path)]=hashlib.sha256(path.read_bytes()).hexdigest()
pathlib.Path(sys.argv[2]).write_text(json.dumps(values,sort_keys=True,indent=2)+'\n')
PY
}
{
  printf 'baseline=%s\n' "${TITAN_BASE_REVISION:-unknown}"
  date --iso-8601=seconds
  rustc --version
  cargo --version
  python3 --version
  uname -sm
  sha256sum "$TITAN_LIGHTER_SIGNER_TEST_PATH"
} > "$verification_report/environment.txt"
source_manifest "$verification_report/sources-before.json"
# Cargo config may be supplied by the build server environment; keep registry and lockfile intact.
run_check format cargo fmt --all -- --check
if [[ -n ${TITAN_OFFICIAL_API_RESEARCH:-} ]]; then
  run_check official-coverage python3 scripts/verify_native_api_coverage.py "$TITAN_OFFICIAL_API_RESEARCH"
fi
run_check clippy cargo_checked clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
run_check workspace cargo_checked test --offline --locked --workspace
run_check official-signer cargo_checked test --offline --locked -p connector official_signer_offline_abi_roundtrip -- --ignored
source_manifest "$verification_report/sources-after.json"
run_check source-identity cmp "$verification_report/sources-before.json" "$verification_report/sources-after.json"
# These three checks are independent. Preserve per-venue failures rather than hiding them.
public_status=0
for venue in lighter variational backpack; do
  run_check "public-$venue" cargo_checked run --offline --locked -p connector --example native_venues_public_probe -- "$venue" || public_status=1
done
source_manifest "$verification_report/sources-final.json"
run_check source-identity-final cmp "$verification_report/sources-before.json" "$verification_report/sources-final.json" || public_status=1
exit "$public_status"
