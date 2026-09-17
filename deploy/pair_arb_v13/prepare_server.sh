#!/usr/bin/env bash
set -euo pipefail

repository="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
deployment="$repository/deploy/pair_arb_v13"
cd "$repository"

mkdir -p "$deployment/secrets" "$deployment/logs"
chmod 700 "$deployment/secrets" "$deployment/logs"

CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}" cargo build --release -p titan-cli

echo "built titan-cli; pair_arb V13 remains disabled until an explicitly authorized live rollout"
