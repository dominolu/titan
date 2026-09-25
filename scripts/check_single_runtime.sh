#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
source_roots=("$repo_root/crates" "$repo_root/hftbacktest" "$repo_root/connector" "$repo_root/collector")
if command -v rg >/dev/null 2>&1; then
  definitions=$(rg -l '^pub struct StrategyRuntimeContextV13' "${source_roots[@]}" --glob '*.rs')
else
  definitions=$(grep -R -l --include='*.rs' '^pub struct StrategyRuntimeContextV13' "${source_roots[@]}" || true)
fi
expected="$repo_root/crates/titan-strategy-runtime/src/v13.rs"
if [[ "$definitions" != "$expected" ]]; then
  printf 'expected exactly one Strategy ABI context at %s; found:\n%s\n' "$expected" "$definitions" >&2
  exit 1
fi
legacy_pattern='StrategyRuntimeContext([^V]|$)|BacktestCommandBuffer|STRATEGY_ABI_VERSION[[:space:]]*:[^=]*=[[:space:]]*12'
if command -v rg >/dev/null 2>&1; then
  legacy_matches=$(rg -n "$legacy_pattern" "${source_roots[@]}" --glob '*.rs' || true)
else
  legacy_matches=$(grep -R -n -E --include='*.rs' "$legacy_pattern" "${source_roots[@]}" || true)
fi
if [[ -n "$legacy_matches" ]]; then
  printf '%s\n' "$legacy_matches" >&2
  printf 'legacy strategy ABI symbols remain in executable Rust sources\n' >&2
  exit 1
fi
