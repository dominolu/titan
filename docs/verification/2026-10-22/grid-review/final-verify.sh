#!/usr/bin/env bash
set -euo pipefail
cd /home/dominolu/dev/titan-grid-review-20261022-2aafc598
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_TEST_BINARY="$PWD/target/debug/titan"
export CARGO_BUILD_JOBS=8
export PYTHONPATH=.:python/titan-strategy-sdk
logs=verification-artifacts/grid-review
run() {
    local name=$1
    shift
    printf 'COMMAND:' > "$logs/$name.log"
    printf ' %q' "$@" >> "$logs/$name.log"
    printf '\n' >> "$logs/$name.log"
    local status=0
    "$@" >> "$logs/$name.log" 2>&1 || status=$?
    printf '\nEXIT_CODE=%s\n' "$status" >> "$logs/$name.log"
    tail -n 8 "$logs/$name.log"
    return "$status"
}
run cli-build /home/dominolu/.cargo/bin/cargo +1.94.1 build -p titan-cli
run sdk .venv/bin/python -m unittest discover -s python/titan-strategy-sdk/tests -v
run doctests /home/dominolu/.cargo/bin/cargo +1.94.1 test --workspace --doc
run grid-build target/debug/titan strategy compile --strategy strategies/grid/strategy.py --parameters strategies/grid/parameters.json --target x86_64-unknown-linux-gnu --cpu-baseline x86-64-v2 --artifact-format bundle --output "$logs/grid.titan"
run gate-arb-build target/debug/titan strategy compile --strategy strategies/grid/strategy.py --parameters strategies/grid/parameters.gate_arb.json --target x86_64-unknown-linux-gnu --cpu-baseline x86-64-v2 --artifact-format bundle --output "$logs/grid-gate-arb.titan"
target/debug/titan strategy validate "$logs/grid.titan" --json > "$logs/artifact-validation.json"
target/debug/titan strategy show "$logs/grid.titan" --json > "$logs/artifact-manifest.json"
target/debug/titan strategy validate "$logs/grid-gate-arb.titan" --json > "$logs/gate-arb-validation.json"
target/debug/titan strategy show "$logs/grid-gate-arb.titan" --json > "$logs/gate-arb-manifest.json"
run host-smoke /home/dominolu/.cargo/bin/cargo +1.94.1 run -p titan-strategy-runtime --example v13_grid_stop_smoke -- "$logs/grid.titan"
export TITAN_HOME="$PWD/$logs/cli-final"
test ! -e "$TITAN_HOME"
run cli-run target/debug/titan run "$logs/grid.titan" -e backtest -m tick -c configs/grid_v13_backtest.toml --json
find "$TITAN_HOME" -name result.json -exec cat {} \; > "$logs/cli-result.json"
sha256sum "$logs/grid.titan" "$logs/grid-gate-arb.titan" > "$logs/artifact-checksums.txt"
{
    uname -a
    /home/dominolu/.cargo/bin/rustc +1.94.1 --version
    /home/dominolu/.cargo/bin/cargo +1.94.1 --version
    .venv/bin/python -c 'import sys, numba, numpy; print(sys.version); print("Numba", numba.__version__); print("NumPy", numpy.__version__)'
} > "$logs/toolchain.txt"
printf 'FINAL_VERIFICATION_OK\n'
