#!/usr/bin/env bash
# Non-live verification only, on the designated build server.
set -euo pipefail
case " $(hostname -I 2>/dev/null) " in
  *" 192.168.3.88 "*) ;;
  *) echo 'Run this verification only on build server 192.168.3.88.' >&2; exit 2 ;;
esac
repo_root=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo_root"
verification_root="$repo_root/verification-artifacts/final"
mkdir -p "$verification_root"
export CARGO_BUILD_JOBS=8
task_cargo=/home/dominolu/.cargo/bin/cargo
task_python="$repo_root/.venv/bin/python"
export TITAN_STRATEGY_PYTHON="$task_python"
export TITAN_STRATEGY_SDK="$repo_root/python/titan-strategy-sdk"
"$task_cargo" +1.94.1 --version > "$verification_root/toolchain.log"
"$task_python" --version >> "$verification_root/toolchain.log"
"$task_cargo" +1.94.1 fmt --all -- --check > "$verification_root/fmt.log" 2>&1
"$task_cargo" +1.94.1 check --workspace --all-targets --all-features > "$verification_root/check.log" 2>&1
"$task_cargo" +1.94.1 clippy --workspace --all-targets --all-features -- -D warnings > "$verification_root/clippy.log" 2>&1
"$task_cargo" +1.94.1 test --workspace --all-targets --all-features > "$verification_root/rust-test.log" 2>&1
"$task_cargo" +1.94.1 test --workspace --all-features --doc > "$verification_root/rust-doc-test.log" 2>&1
"$task_cargo" +1.94.1 build -p titan-cli --bin titan > "$verification_root/cli-build.log" 2>&1
export TITAN_TEST_BINARY="$repo_root/target/debug/titan"
PYTHONPATH=.:python/titan-strategy-sdk "$task_python" -m unittest discover -s python/titan-strategy-sdk/tests -v > "$verification_root/sdk-test.log" 2>&1
PYTHONPATH=python/titan-reporting "$task_python" -m unittest discover -s python/titan-reporting/tests -v > "$verification_root/reporting-test.log" 2>&1
bash scripts/check_single_runtime.sh > "$verification_root/single-runtime.log" 2>&1
for strategy in v13_smoke pair_arb; do
  "$TITAN_TEST_BINARY" strategy compile \
    --strategy "strategies/$strategy.py" \
    --artifact-format bundle --output "$verification_root/$strategy.titan" > "$verification_root/$strategy-build.log" 2>&1
done
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_artifact_smoke -- "$verification_root/v13_smoke.titan" "$verification_root/cache" > "$verification_root/artifact-smoke.log" 2>&1
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_pair_arb_smoke -- "$verification_root/pair_arb.titan" "$verification_root/cache" > "$verification_root/pair-smoke.log" 2>&1
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_service_recovery_smoke -- "$verification_root/v13_smoke.titan" "$verification_root/service-recovery" > "$verification_root/recovery-smoke.log" 2>&1
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_common_recovery_smoke -- "$verification_root/v13_smoke.titan" > "$verification_root/common-recovery-smoke.log" 2>&1
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_order_drain_smoke -- "$verification_root/v13_smoke.titan" > "$verification_root/drain-smoke.log" 2>&1
"$TITAN_TEST_BINARY" strategy compile --strategy python/titan-strategy-sdk/tests/fixtures/v13_timer_submit.py --set initial_counter=4 --artifact-format bundle --output "$verification_root/timer-submit.titan" > "$verification_root/timer-submit-build.log" 2>&1
"$task_cargo" +1.94.1 run -p titan-strategy-runtime --example v13_timer_failure_smoke -- "$verification_root/timer-submit.titan" > "$verification_root/timer-failure-smoke.log" 2>&1
TITAN_HOME="$verification_root/cli-home" TITAN_REPORT_PYTHON="$task_python" PYTHONPATH=python/titan-reporting \
  "$task_python" scripts/verify_cli_bundle.py "$repo_root/target/debug/titan" "$verification_root" > "$verification_root/cli-integration.log" 2>&1
sha256sum Cargo.lock "$verification_root/v13_smoke.titan" "$verification_root/pair_arb.titan" > "$verification_root/artifact-checksums.txt"
echo "Verification passed: $verification_root"
