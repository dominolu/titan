#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
deployment="$repository/deploy/okx_hyperliquid_xemm_testnet"
cd "$repository"

mkdir -p "$deployment/secrets" "$deployment/logs"
chmod 700 "$deployment/secrets" "$deployment/logs"

CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" cargo build --release -p titan-cli

target/release/titan validate okx_hyperliquid_xemm \
  --env live --mode tick --config "$deployment/runtime.toml" --json
