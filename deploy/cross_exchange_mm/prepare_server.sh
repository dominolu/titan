#!/usr/bin/env bash
# Prepare a cross_exchange_mm deployment directory. Builds the CLI only; it never
# starts a strategy and never enables any binding.
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
deployment="$repository/deploy/cross_exchange_mm"
cd "$repository"

mkdir -p "$deployment/artifacts" "$deployment/secrets" "$deployment/checkpoints" "$deployment/logs"
chmod 700 "$deployment/secrets" "$deployment/checkpoints" "$deployment/logs"

CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" cargo build --release -p titan-cli

echo "built titan-cli; cross_exchange_mm remains disabled until the multi-binding"
echo "controlled-execution work is verified and a live rollout is explicitly authorized"
