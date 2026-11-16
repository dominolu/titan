#!/usr/bin/env bash
# Guard (acceptance matrix item 15): the retired hftbacktest `strategy` module must not come back.
set -euo pipefail
root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

fail=0
if [[ -e hftbacktest/src/strategy.rs ]]; then
  echo "retired module is back: hftbacktest/src/strategy.rs" >&2
  fail=1
fi
for pattern in 'hftbacktest::strategy' 'hftbacktest::prelude::run_strategy' 'run_strategy_for' 'StrategyCtx'; do
  hits="$(grep -rn --include='*.rs' --include='*.py' "$pattern" hftbacktest crates connector python 2>/dev/null || true)"
  if [[ -n "$hits" ]]; then
    echo "retired hftbacktest strategy API referenced ($pattern):" >&2
    echo "$hits" >&2
    fail=1
  fi
done
if [[ $fail -eq 0 ]]; then
  echo "ok: hftbacktest strategy module retired and unreferenced"
fi
exit $fail
