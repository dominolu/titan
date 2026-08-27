#!/usr/bin/env bash
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
definitions=$(rg -l '^pub fn run_event_runtime_counted' "$repo_root" --glob '*.rs')
expected="$repo_root/crates/titan-runtime/src/runtime.rs"
if [[ "$definitions" != "$expected" ]]; then
  printf 'expected exactly one Runtime event loop at %s; found:\n%s\n' "$expected" "$definitions" >&2
  exit 1
fi
