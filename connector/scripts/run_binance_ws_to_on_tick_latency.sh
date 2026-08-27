#!/usr/bin/env bash
set -euo pipefail

# Server-side, public-market-data-only latency test. No API key is used and the probe never sends
# orders. Run this script on the same Linux host/process namespace used by the live strategy.

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
CONNECTOR_NAME="${CONNECTOR_NAME:-binance-latency}"
SYMBOL="${SYMBOL:-btcusdt}"
TICK_SIZE="${TICK_SIZE:-0.1}"
LOT_SIZE="${LOT_SIZE:-0.001}"
FRAME_US="${FRAME_US:-1000}"
WARMUP_SECONDS="${WARMUP_SECONDS:-5}"
MEASURE_SECONDS="${MEASURE_SECONDS:-60}"
RESULT_DIR="${RESULT_DIR:-${REPO_ROOT}/latency_results}"
SKIP_BUILD="${SKIP_BUILD:-0}"
UV_BIN="${UV_BIN:-uv}"
PY_PROJECT="${REPO_ROOT}/py-hftbacktest"

mkdir -p "${RESULT_DIR}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
CONNECTOR_LOG="${RESULT_DIR}/binance_connector_${RUN_ID}.log"
RESULT_LOG="${RESULT_DIR}/ws_to_on_tick_${RUN_ID}.log"
TMP_DIR="$(mktemp -d)"
CONFIG_FILE="${TMP_DIR}/binancefutures-mainnet-public.toml"
CONNECTOR_PID=""

cleanup() {
    if [[ -n "${CONNECTOR_PID}" ]] && kill -0 "${CONNECTOR_PID}" 2>/dev/null; then
        kill -TERM "${CONNECTOR_PID}"
        wait "${CONNECTOR_PID}" || true
    fi
    rm -rf "${TMP_DIR}"
}
trap cleanup EXIT INT TERM

printf '%s\n' \
    'stream_url = "wss://fstream.binance.com/ws"' \
    'api_url = "https://fapi.binance.com"' \
    'order_prefix = "latency-probe"' \
    'api_key = ""' \
    'secret = ""' \
    'safety_timeout_ms = 0' >"${CONFIG_FILE}"

cd "${REPO_ROOT}"
if [[ "${SKIP_BUILD}" != "1" ]]; then
    cargo build --release -p connector --bin connector
    cd "${PY_PROJECT}"
    env -u CONDA_PREFIX "${UV_BIN}" sync
    env -u CONDA_PREFIX "${UV_BIN}" pip install \
        --python "${PY_PROJECT}/.venv/bin/python" 'maturin~=1.7'
    env -u CONDA_PREFIX "${PY_PROJECT}/.venv/bin/maturin" \
        develop --release --features live
    cd "${REPO_ROOT}"
fi

# iceoryx can race while creating its runtime directory on a host's very first launch. Retry a
# short-lived startup failure; persistent configuration/permission errors still fail with logs.
for attempt in 1 2 3; do
    RUST_LOG="${CONNECTOR_RUST_LOG:-info}" \
        "${REPO_ROOT}/target/release/connector" \
        "${CONNECTOR_NAME}" binancefutures "${CONFIG_FILE}" \
        >>"${CONNECTOR_LOG}" 2>&1 &
    CONNECTOR_PID=$!
    sleep 1
    if kill -0 "${CONNECTOR_PID}" 2>/dev/null; then
        break
    fi
    wait "${CONNECTOR_PID}" || true
    CONNECTOR_PID=""
    if [[ "${attempt}" == "3" ]]; then
        printf 'connector exited during startup; log: %s\n' "${CONNECTOR_LOG}" >&2
        tail -n 50 "${CONNECTOR_LOG}" >&2
        exit 1
    fi
    printf 'connector startup attempt %s failed; retrying\n' "${attempt}" >&2
done

RUST_LOG="${PROBE_RUST_LOG:-warn}" \
    PYTHONPATH="${PY_PROJECT}${PYTHONPATH:+:${PYTHONPATH}}" \
    env -u CONDA_PREFIX "${PY_PROJECT}/.venv/bin/python" \
    "${REPO_ROOT}/examples/binance_ws_to_numba_on_tick_latency.py" \
    --connector-name "${CONNECTOR_NAME}" \
    --symbol "${SYMBOL}" \
    --tick-size "${TICK_SIZE}" \
    --lot-size "${LOT_SIZE}" \
    --frame-us "${FRAME_US}" \
    --warmup-seconds "${WARMUP_SECONDS}" \
    --measure-seconds "${MEASURE_SECONDS}" | tee "${RESULT_LOG}"

printf 'result_log=%s\nconnector_log=%s\n' "${RESULT_LOG}" "${CONNECTOR_LOG}"
