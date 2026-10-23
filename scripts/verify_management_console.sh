#!/usr/bin/env bash
# Build-server verification only. Run from an isolated checkout on 192.168.3.88.
set -euo pipefail
out=${1:?provide a fresh evidence directory}
mkdir -p "$out"
out=$(cd "$out" && pwd)
repo=$(pwd)
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
    tail -60 "$out/$name.log"
    printf '%s: failed (%s)\n' "$name" "$status"
    return "$status"
  fi
}
run rust-format cargo fmt -p titan-cli -p titan-account-service -p titan-domain-types -p titan-strategy-runtime -p connector -- --check
run rust-clippy cargo clippy --locked --no-deps -p titan-cli -p titan-account-service -p titan-domain-types -p titan-strategy-runtime -p connector --all-targets -- -D warnings
run account-tests cargo test --locked -p titan-account-service --lib
run strategy-tests cargo test --locked -p titan-strategy-runtime --lib --tests
run connector-tests cargo test --locked -p connector gatecrossex --lib
run cli-tests cargo test --locked -p titan-cli --lib --tests
run history-tests cargo test --locked -p hftbacktest --example v13_grid_history
run cli-build cargo build --locked -p titan-cli --bin titan
run history-build cargo rustc --locked -p hftbacktest --example v13_grid_history -- -C opt-level=2
run sdk-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -p test_v13_static_compiler.py -v
run reporting-tests "$TITAN_VERIFY_PYTHON" -m unittest discover -s python/titan-reporting/tests -v
run dto-export "$CARGO_TARGET_DIR/debug/titan" console export-types --output web/console/src/generated
cd web/console
run web-types pnpm typecheck
run web-format pnpm exec prettier --check src/views/console src/api/console.ts src/store/modules/console.ts src/store/modules/user.ts src/utils/auth.ts src/utils/http/index.ts src/utils/http/types.d.ts src/utils/precision.ts src/router/index.ts src/router/modules/home.ts src/views/login/index.vue
run web-lint pnpm exec eslint src/views/console src/api/console.ts src/store/modules/console.ts src/store/modules/user.ts src/utils/auth.ts src/utils/http/index.ts src/utils/precision.ts src/router/index.ts src/router/modules/home.ts src/views/login/index.vue src/layout/components/lay-navbar/index.vue src/layout/components/lay-sidebar/NavMix.vue src/layout/components/lay-sidebar/NavHorizontal.vue src/layout/hooks/useNav.ts --max-warnings 0
run web-build pnpm build
cd "$repo"
sha256sum "$CARGO_TARGET_DIR/debug/titan" "$CARGO_TARGET_DIR/debug/examples/v13_grid_history" Cargo.lock web/console/pnpm-lock.yaml web/console/src/generated/management.ts > "$out/build-sha256.txt"
{ rustc --version; cargo --version; node --version; pnpm --version; "$TITAN_VERIFY_PYTHON" --version; } > "$out/toolchain.txt"
printf 'All verification checks passed. Run the real HTTP/browser fixture separately.\n'
