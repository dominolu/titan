#!/usr/bin/env bash
# Full non-live strategy observation verification. Execute on the build server.
set -euo pipefail
case " $(hostname -I 2>/dev/null) " in
  *" 192.168.3.88 "*) ;;
  *) echo 'Run only on build server 192.168.3.88.' >&2; exit 2 ;;
esac
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"
out=${1:?provide an evidence directory}
mkdir -p "$out"
out=$(cd "$out" && pwd)
: "${CARGO_TARGET_DIR:=$repo/target}"
: "${TITAN_VERIFY_PYTHON:=/home/dominolu/dev/titan/.venv/bin/python}"
export CARGO_TARGET_DIR CARGO_NET_OFFLINE=true
export TITAN_STRATEGY_PYTHON="$TITAN_VERIFY_PYTHON" TITAN_REPORT_PYTHON="$TITAN_VERIFY_PYTHON"
export TITAN_STRATEGY_SDK="$repo/python/titan-strategy-sdk" TITAN_REPORTING_PATH="$repo/python/titan-reporting"
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
export PYTHONPATH="$TITAN_STRATEGY_SDK:$TITAN_REPORTING_PATH"
run() {
  local name=$1; shift
  printf '%s\n' "$*" > "$out/$name.command.txt"
  if "$@" > "$out/$name.log" 2>&1; then
    printf '%s: passed\n' "$name"
  else
    local status=$?
    tail -80 "$out/$name.log"
    printf '%s: failed (%s)\n' "$name" "$status"
    return "$status"
  fi
}
run rust-format cargo fmt --all -- --check
run rust-clippy cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
run rust-tests cargo test --locked --workspace --all-targets --all-features
run rust-doc-tests cargo test --locked --workspace --all-features --doc
run publication-cost cargo test --locked --release -p titan-strategy-runtime publication_performance_probe -- --nocapture
if [[ -n "${TITAN_PREVIOUS_OBSERVATION_FIXTURE:-}" ]]; then
  run recovery-compat "$TITAN_VERIFY_PYTHON" scripts/verify_observation_recovery_compat.py --repo "$repo" --fixture "$TITAN_PREVIOUS_OBSERVATION_FIXTURE" --output "$out/recovery-compat.json"
fi
run cli-build cargo build --locked -p titan-cli --bin titan
run history-build cargo rustc --locked -p hftbacktest --example v13_grid_history -- -C opt-level=2
run sdk-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v
run reporting-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-reporting/tests -v
run dto-export "$TITAN_TEST_BINARY" console export-types --output web/console/src/generated
cd web/console
run web-types pnpm typecheck
run web-format pnpm exec prettier --check src/views/console src/api/console.ts src/store/modules/console.ts src/store/resource-cache.ts src/router/modules/home.ts src/utils/auth.ts src/utils/http/index.ts src/views/login/index.vue types/router.d.ts
run web-lint pnpm exec eslint src/views/console src/api/console.ts src/store/modules/console.ts src/store/resource-cache.ts src/router/modules/home.ts src/utils/auth.ts src/utils/http/index.ts src/views/login/index.vue --max-warnings 0
run web-build pnpm build
cd "$repo"
run web-cache node scripts/management_web_cache_test.mjs
run run-view node scripts/management_run_view_test.mjs
run strategy-view node scripts/management_research_view_test.mjs
run loopback-grid "$TITAN_VERIFY_PYTHON" scripts/management_control_smoke.py --repo "$repo" --binary "$TITAN_TEST_BINARY" --output "$out/loopback" --browser
sha256sum "$TITAN_TEST_BINARY" "$CARGO_TARGET_DIR/debug/examples/v13_grid_history" Cargo.lock web/console/pnpm-lock.yaml web/console/src/generated/management.ts > "$out/build-sha256.txt"
{ rustc --version; cargo --version; node --version; pnpm --version; "$TITAN_VERIFY_PYTHON" --version; } > "$out/toolchain.txt"
printf 'Strategy observation verification passed. No real accounts were used.\n'
