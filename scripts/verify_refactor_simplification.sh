#!/usr/bin/env bash
# Non-live gates; invoke from the isolated source directory on the build server.
set -euo pipefail
case " $(hostname -I) " in
  *" 192.168.3.88 "*) ;;
  *) echo "Run on build server 192.168.3.88" >&2; exit 2 ;;
esac
export CARGO_BUILD_JOBS=8 CARGO_TARGET_DIR=/tmp/titan-single-file-target CARGO_NET_OFFLINE=true
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export PYTHONPATH="$PWD:$TITAN_STRATEGY_SDK"
task_logs="$(dirname "$PWD")/logs"
mkdir -p "$task_logs"
task_cargo=(/home/dominolu/.cargo/bin/cargo +1.94.1 --config 'source.crates-io.replace-with="titan-mirror"' --config 'source.titan-mirror.registry="sparse+https://rsproxy.cn/index/"')
run_gate() {
  local name="$1"
  shift
  local result=0
  "$@" > "$task_logs/$name.log" 2>&1 || result=$?
  printf "%s\t%s\n" "$name" "$result" >> "$task_logs/status.tsv"
  if [ "$result" -ne 0 ]; then
    tail -n 80 "$task_logs/$name.log"
    return "$result"
  fi
  printf "%s passed\n" "$name"
}
: > "$task_logs/status.tsv"
run_gate format "${task_cargo[@]}" fmt --all
run_gate fmt-check "${task_cargo[@]}" fmt --all -- --check
run_gate targeted-api "${task_cargo[@]}" test --locked -p titan-cli --lib version_page_projects_private_state_before_response_limit
run_gate targeted-collector "${task_cargo[@]}" test --locked -p collector binance_transport::tests
for attempt in {1..10}; do
  run_gate "heartbeat-$attempt" "${task_cargo[@]}" test --locked -p collector binance_transport::tests::loopback_ping_refreshes_deadline_then_missing_ping_times_out -- --exact
done
run_gate clippy "${task_cargo[@]}" clippy --locked --workspace --all-targets --all-features -- -D warnings
run_gate rust "${task_cargo[@]}" test --locked --workspace --all-targets --all-features
run_gate doctest "${task_cargo[@]}" test --locked --workspace --all-features --doc
run_gate cli-build "${task_cargo[@]}" build --locked -p titan-cli --all-features --bin titan
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
run_gate sdk "$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v
run_gate reporting env PYTHONPATH="$PWD/python/titan-reporting" "$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-reporting/tests -v
run_gate web-check pnpm -C web/console check
run_gate web-lint pnpm -C web/console exec eslint src/api/console.ts src/store/modules/console.ts src/main.ts --max-warnings 0
run_gate web-cache pnpm -C web/console test:cache
run_gate web-api node scripts/refactor_console_api_test.mjs
run_gate web-run-view node scripts/management_run_view_test.mjs
run_gate web-research-view node scripts/management_research_view_test.mjs
run_gate web-build pnpm -C web/console build
