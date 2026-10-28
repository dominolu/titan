#!/usr/bin/env bash
# M1/M2: real native runtimes and local storage, simulated venue, no real accounts.
set -euo pipefail
case " $(hostname -I 2>/dev/null) " in
  *" 192.168.3.88 "*) ;;
  *) echo 'Run only on build server 192.168.3.88.' >&2; exit 2 ;;
esac
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
out=${1:?provide a new evidence directory}
test ! -e "$out"
mkdir -p "$out"
out=$(cd "$out" && pwd)
: "${TITAN_PREVIOUS_RECOVERY_FIXTURE:?provide previous-binary synthetic fixture}"
: "${TITAN_BROWSER_EXECUTABLE:?provide an installed Chromium executable}"
: "${TITAN_SOURCE_BASELINE:?provide the source Git baseline for this isolated snapshot}"
test -x "$TITAN_BROWSER_EXECUTABLE"
: "${TITAN_VERIFY_PYTHON:=/home/dominolu/dev/titan/.venv/bin/python}"
: "${CARGO_TARGET_DIR:=/tmp/titan-deplugin-target}"
export CARGO_TARGET_DIR CARGO_NET_OFFLINE=true
: "${TITAN_PERSISTENCE_STAGE:=M1}"
case "$TITAN_PERSISTENCE_STAGE" in M1|M2|M3) ;; *) echo 'Unknown persistence stage' >&2; exit 2 ;; esac
export TITAN_PERSISTENCE_STAGE
if [ "$TITAN_PERSISTENCE_STAGE" != M1 ]; then
  : "${TITAN_HISTORY_DATA:?provide frozen public historical CSV fixtures}"
  : "${TITAN_HISTORY_PARAMETERS:?provide frozen historical parameter fixtures}"
  test -f "$TITAN_HISTORY_DATA/BTCUSDT-aggTrades-2023-09-02.csv"
  test -f "$TITAN_HISTORY_PARAMETERS/sideways-neutral.parameters.json"
fi
export TITAN_STRATEGY_PYTHON="$TITAN_VERIFY_PYTHON" TITAN_REPORT_PYTHON="$TITAN_VERIFY_PYTHON"
export TITAN_STRATEGY_SDK="$repo/python/titan-strategy-sdk" TITAN_REPORTING_PATH="$repo/python/titan-reporting"
export PYTHONPATH="$TITAN_STRATEGY_SDK:$TITAN_REPORTING_PATH"
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
# Reuse the build server's cached source registry without changing the lockfile or global config.
cat > "$out/cargo-source.toml" <<'TOML'
[source.crates-io]
replace-with = "rsproxy"
[source.rsproxy]
registry = "sparse+https://rsproxy.cn/index/"
TOML
cargo_cmd=(cargo --config "$out/cargo-source.toml")
run() {
  local name=$1; shift
  printf '%s\n' "$*" > "$out/$name.command.txt"
  if "$@" > "$out/$name.log" 2>&1; then
    printf '{"name":"%s","exit_code":0}\n' "$name" >> "$out/steps.jsonl"
    printf '%s: passed\n' "$name"
  else
    local result=$?
    printf '{"name":"%s","exit_code":%s}\n' "$name" "$result" >> "$out/steps.jsonl"
    tail -80 "$out/$name.log"
    printf '%s: failed (%s)\n' "$name" "$result"
    return "$result"
  fi
}
run rust-format cargo fmt --all -- --check
run rust-check "${cargo_cmd[@]}" check --locked --workspace --all-targets --all-features
run rust-clippy cargo clippy --config "$out/cargo-source.toml" --locked --workspace --all-targets --all-features -- -D warnings
run rust-tests "${cargo_cmd[@]}" test --locked --workspace --all-targets --all-features
run rust-doctests "${cargo_cmd[@]}" test --locked --workspace --all-features --doc
if [ "$TITAN_PERSISTENCE_STAGE" != M1 ]; then
  run recovery-latency "${cargo_cmd[@]}" test --locked -p titan-strategy-runtime recovery_commit_latency_probe -- --nocapture --test-threads=1
fi
if [ "$TITAN_PERSISTENCE_STAGE" = M3 ]; then
  : "${TITAN_M3_PROFILE:?provide the isolated loopback PostgreSQL/S3 profile}"
  export TITAN_M3_OUTPUT="$out/external"
  run external-backends "${cargo_cmd[@]}" test --locked -p titan-persistence --test external_backends -- --ignored --nocapture --test-threads=1
fi
run cli-build "${cargo_cmd[@]}" build --locked -p titan-cli --bin titan
if [ "$TITAN_PERSISTENCE_STAGE" = M3 ]; then
  : "${TITAN_M3_SERVICES:?provide the labelled isolated services manifest}"
  run external-faults "$TITAN_VERIFY_PYTHON" scripts/verify_external_persistence.py --binary "$TITAN_TEST_BINARY" --profile "$TITAN_M3_PROFILE" --services "$TITAN_M3_SERVICES" --contract "$out/external" --output "$out/external-faults"
  run recovery-contention env TITAN_M3_PERFORMANCE="$out/recovery-contention.json" "${cargo_cmd[@]}" test --locked -p titan-strategy-runtime recovery_contention_budget_probe -- --ignored --nocapture --test-threads=1
fi
run sdk-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v
run report-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-reporting/tests -v
run original-native-sqlite-recovery "$TITAN_VERIFY_PYTHON" scripts/verify_observation_recovery_compat.py --repo "$repo" --fixture "$TITAN_PREVIOUS_RECOVERY_FIXTURE" --output "$out/original-native-sqlite-recovery.json" --cargo-config "$out/cargo-source.toml"
run maintenance "$TITAN_VERIFY_PYTHON" scripts/verify_recovery_maintenance.py --binary "$TITAN_TEST_BINARY" --fixture "$TITAN_PREVIOUS_RECOVERY_FIXTURE" --output "$out/maintenance"
run native-service-build "${cargo_cmd[@]}" build --locked -p titan-strategy-runtime --example v13_service_recovery_smoke
run smoke-compile "$TITAN_TEST_BINARY" strategy compile --strategy strategies/v13_smoke/strategy.py --parameters strategies/v13_smoke/parameters.json --artifact-format bundle --output "$out/smoke.titan"
run native-service "$CARGO_TARGET_DIR/debug/examples/v13_service_recovery_smoke" "$out/smoke.titan" "$out/native-service"
run native-negative-build "${cargo_cmd[@]}" build --locked -p titan-strategy-runtime --example v13_common_recovery_smoke --example v13_order_drain_smoke --example v13_timer_failure_smoke
run common-recovery "$CARGO_TARGET_DIR/debug/examples/v13_common_recovery_smoke" "$out/smoke.titan"
run order-drain "$CARGO_TARGET_DIR/debug/examples/v13_order_drain_smoke" "$out/smoke.titan"
run timer-compile "$TITAN_TEST_BINARY" strategy compile --strategy python/titan-strategy-sdk/tests/fixtures/v13_timer_submit.py --parameters strategies/v13_smoke/parameters.json --artifact-format bundle --output "$out/timer.titan"
run timer-failure "$CARGO_TARGET_DIR/debug/examples/v13_timer_failure_smoke" "$out/timer.titan"
run dto-export "$TITAN_TEST_BINARY" console export-types --output web/console/src/generated
(
  cd web/console
  run web-types pnpm typecheck
  if [ "$TITAN_PERSISTENCE_STAGE" != M1 ]; then
    run web-format pnpm exec prettier --check src/views/console src/api/console.ts src/store/modules/console.ts src/store/resource-cache.ts
    run web-lint pnpm exec eslint src/views/console src/api/console.ts --max-warnings 0
    run web-cache pnpm test:cache
  fi
  run web-build pnpm build
)
run loopback-grid "$TITAN_VERIFY_PYTHON" scripts/management_control_smoke.py --repo "$repo" --binary "$TITAN_TEST_BINARY" --output "$out/loopback" --browser
run history-query-isolation "$TITAN_VERIFY_PYTHON" scripts/management_research_query_test.py --repo "$repo" --binary "$TITAN_TEST_BINARY" --output "$out/history-query"
if [ "$TITAN_PERSISTENCE_STAGE" != M1 ]; then
  run readonly-image env TITAN_OBSERVATION_FIXTURE="$out/readonly-image.json" "${cargo_cmd[@]}" test --locked -p titan-account-service account_quantity_bound_counts_only_unfilled_remainder_and_keeps_hedge_gross -- --exact observation::tests::account_quantity_bound_counts_only_unfilled_remainder_and_keeps_hedge_gross
  run readonly-observation "$TITAN_VERIFY_PYTHON" scripts/management_observation_smoke.py --repo "$repo" --binary "$TITAN_TEST_BINARY" --fixture "$out/readonly-image.json" --output "$out/readonly" --browser
  run history-build "${cargo_cmd[@]}" rustc --locked -p hftbacktest --example v13_grid_history -- -C opt-level=2
  mkdir "$out/cli-bundle"
  cp "$out/smoke.titan" "$out/cli-bundle/v13_smoke.titan"
  run cli-result-bundle env TITAN_HOME="$out/cli-home" "$TITAN_VERIFY_PYTHON" scripts/verify_cli_bundle.py "$TITAN_TEST_BINARY" "$out/cli-bundle"
  run historical-matrix "$TITAN_VERIFY_PYTHON" scripts/management_console_smoke.py --repo "$repo" --binary "$TITAN_TEST_BINARY" --history "$CARGO_TARGET_DIR/debug/examples/v13_grid_history" --data "$TITAN_HISTORY_DATA" --parameters "$TITAN_HISTORY_PARAMETERS" --output "$out/historical" --matrix --browser
  run local-persistence "$TITAN_VERIFY_PYTHON" scripts/verify_local_persistence.py --binary "$TITAN_TEST_BINARY" --history "$CARGO_TARGET_DIR/debug/examples/v13_grid_history" --matrix "$out/historical" --loopback "$out/loopback" --output "$out/local-persistence" --smoke-strategy "$out/smoke.titan"
  sha256sum "$CARGO_TARGET_DIR/debug/examples/v13_grid_history" > "$out/history-build-sha256.txt"
fi
sha256sum "$TITAN_TEST_BINARY" Cargo.lock "$out/smoke.titan" > "$out/build-sha256.txt"
{ rustc --version; cargo --version; "$TITAN_VERIFY_PYTHON" --version; node --version; pnpm --version; } > "$out/toolchain.txt"
run source-identity "$TITAN_VERIFY_PYTHON" - "$out/source-identity.json" <<'PY'
import hashlib,json,os,sys
from pathlib import Path
root=Path.cwd()
excluded={".git",".codegraph","target","evidence","verification-artifacts","node_modules","dist","__pycache__",".venv",".secrets","secrets"}
paths=[root/name for name in ("Cargo.toml","Cargo.lock","rust-toolchain.toml","rustfmt.toml",".cargo/config.toml","web/console/package.json","web/console/pnpm-lock.yaml")]
for name in ("crates","connector","collector","hftbacktest","hftbacktest-derive","python","configs","strategies","deploy","examples","scripts","web/console/src"):
    directory=root/name
    if directory.exists(): paths.extend(p for p in directory.rglob("*") if not (set(p.relative_to(root).parts)&excluded))
files={str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted(set(paths)) if p.is_file() and not p.is_symlink()}
Path(sys.argv[1]).write_text(json.dumps({"baseline":os.environ["TITAN_SOURCE_BASELINE"],"root":str(root),"source_sha256":files},indent=2))
PY
printf '%s non-live verification passed. No real accounts or production storage were accessed.\n' "$TITAN_PERSISTENCE_STAGE"
