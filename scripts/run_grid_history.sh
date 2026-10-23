#!/usr/bin/env bash
# Execute only on 192.168.3.88. Inputs are public data and unsigned offline fixtures.
set -euo pipefail
data_dir=${1:-data/grid-history}
output=${2:-verification-artifacts/grid-history/final}
runner=target/debug/examples/v13_grid_history
export TITAN_STRATEGY_PYTHON="$PWD/.venv/bin/python"
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export PYTHONPATH=.:python/titan-strategy-sdk
mkdir -p "$output"
for day in 2023-09-02 2023-10-23 2023-08-17; do
    "$runner" prepare --trades "$data_dir/BTCUSDT-aggTrades-$day.csv" \
        --quotes "$data_dir/BTCUSDT-bookTicker-$day.csv" \
        --funding "$data_dir/BTCUSDT-fundingRate-${day:0:7}.csv" \
        --output "$data_dir/$day.bin" > "$output/prepare-$day.txt"
done
.venv/bin/python scripts/compile_grid_history.py --data "$data_dir" --output "$output/artifacts" > "$output/compile.txt" 2>&1
failed=0
for regime in sideways up down; do
    case "$regime" in sideways) day=2023-09-02 ;; up) day=2023-10-23 ;; down) day=2023-08-17 ;; esac
    for model in base stress; do
        pids=()
        for direction in neutral long_bias short_bias; do
            name=$regime-$direction
            options=()
            [[ "$model" != stress ]] || options+=(--stress)
            "$runner" run --artifact "$output/artifacts/$name.titan" --layout "$output/artifacts/layout.json" \
                --data "$data_dir/$day.bin" --output "$output/$name-$model" "${options[@]}" \
                > "$output/$name-$model.txt" 2>&1 &
            pids+=("$!")
        done
        for pid in "${pids[@]}"; do wait "$pid" || failed=1; done
        printf 'completed %s %s\n' "$regime" "$model"
    done
done
exit "$failed"
