#!/usr/bin/env bash
set -euo pipefail
cd /home/dominolu/dev/titan-merge-20261023-a055af91
mkdir verification
sha256sum -c source.sha256 > verification/source-check.txt
export PATH="/home/dominolu/.cargo/bin:$PATH"
export CARGO_BUILD_JOBS=8
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_TEST_BINARY="$PWD/target/debug/titan"
export PYTHONPATH=".:python/titan-strategy-sdk:python/titan-reporting"
run_check() {
    local task_name="$1"
    shift
    echo "START $task_name"
    if "$@" > "verification/$task_name.txt" 2>&1; then
        echo "PASS $task_name"
    else
        local task_status=$?
        tail -80 "verification/$task_name.txt"
        echo "FAIL $task_name $task_status"
        exit "$task_status"
    fi
}
run_check fmt cargo +1.94.1 fmt --all -- --check
run_check single-runtime bash scripts/check_single_runtime.sh
run_check cli-build cargo +1.94.1 build --locked -p titan-cli --bin titan
run_check event-engine cargo +1.94.1 test --locked -p titan-event-engine
run_check fairness-repeat bash -c 'for task_attempt in $(seq 1 100); do cargo +1.94.1 test --locked -p titan-event-engine pending_retry_round_robins_across_subscribers -- --exact tests::pending_retry_round_robins_across_subscribers || exit; done'
run_check rust-default cargo +1.94.1 test --locked --workspace
run_check rust-all cargo +1.94.1 test --locked --workspace --all-targets --all-features
run_check clippy cargo +1.94.1 clippy --locked --workspace --all-targets --all-features -- -D warnings
run_check sdk .venv/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -v
run_check reporting .venv/bin/python -m unittest discover -s python/titan-reporting/tests -v
run_check python-syntax .venv/bin/python -m compileall -q python/titan-strategy-sdk python/titan-reporting strategies
echo 'ALL CHECKS PASSED'
