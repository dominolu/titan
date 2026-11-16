#!/usr/bin/env bash
# One strategy ABI, one name, no version suffix anywhere.
#
# The published descriptor calls the root `StrategyRuntimeContext` and both the Rust type and the
# Python dtypes register it under that name. Version identity lives in `ABI_VERSION` /
# `ABI_FINGERPRINT` and in the `abi_version` descriptor field — never in a module, type, file or
# capability name. Keeping the version out of every identifier is what stops two ABI generations
# from coexisting and being mixed across call sites.
#
# The guard fails when:
#   * the context is defined anywhere other than titan-strategy-runtime (a second ABI definition),
#   * a versioned alias (`StrategyRuntimeContextV13`, `...V14`, ...) reappears in Rust,
#   * a versioned runtime type, capability, or module path reappears in Rust,
#   * a versioned Python ABI/context module (`abi_v13.py`, `context_v14.py`, ...) exists,
#   * a versioned library name (`native-v13`, `native-v14`, ...) reappears in any config,
#   * a versioned identifier reappears in the shipped Python SDK,
#   * retired ABI symbols from earlier runtimes come back.
#
# Historical verification records under `docs/verification/` and the private `internal-docs/`
# snapshots are deliberately out of scope: they describe what was true at the time and must not be
# rewritten.
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/.." && pwd)
rust_roots=("$repo_root/crates" "$repo_root/hftbacktest" "$repo_root/connector" "$repo_root/collector")
python_root="$repo_root/python/titan-strategy-sdk/titan_strategy"
exclude_private=(
  --glob '!target/**' --glob '!target-hlverify/**'
  --glob '!internal-docs/**' --glob '!docs/verification/**'
)

have_rg=0
command -v rg >/dev/null 2>&1 && have_rg=1

# search <path> <pattern> [--glob <pattern> ...]
search() {
  local path=$1 pattern=$2
  shift 2
  if [[ "$have_rg" == "1" ]]; then
    rg -n "$pattern" "$path" "${exclude_private[@]}" "$@" 2>/dev/null || true
  else
    # Plain grep has no --glob: translate the include globs into an extension filter.
    local ext=()
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --glob)
          case "$2" in
            '*.rs') ext+=(--include='*.rs') ;;
            '*.py') ext+=(--include='*.py') ;;
            '*.toml') ext+=(--include='*.toml') ;;
            '*.json') ext+=(--include='*.json') ;;
          esac
          shift 2
          ;;
        *) shift ;;
      esac
    done
    grep -R -n -E "${ext[@]}" "$pattern" "$path" 2>/dev/null \
      | grep -v -e '/target/' -e '/target-hlverify/' -e '/internal-docs/' -e '/docs/verification/' || true
  fi
}

fail() {
  printf '%s\n' "$1" >&2
  exit 1
}

# --- 1. exactly one Rust ABI context, at a stable path -------------------------------------------
# Two steps so a definition written with the brace on the next line is still counted.
definitions=$(search "$repo_root/crates" '^pub struct StrategyRuntimeContext\b' --glob '*.rs')
definition_count=$(printf '%s\n' "$definitions" | grep -c 'pub struct' || true)
expected_one="$repo_root/crates/titan-strategy-runtime/src/context.rs"
if [[ "$definition_count" != "1" || "$definitions" != *"$expected_one"* ]]; then
  printf 'expected exactly one ABI context at %s; found:\n%s\n' "$expected_one" "$definitions" >&2
  exit 1
fi

# --- 2. no versioned identifiers in shipped Rust -------------------------------------------------
versioned_rust=$(search "${rust_roots[@]}" \
  '\b[A-Za-z_][A-Za-z0-9_]*[vV]1[0-9]+[A-Za-z0-9_]*\b' --glob '*.rs')
if [[ -n "$versioned_rust" ]]; then
  printf '%s\n' "$versioned_rust" >&2
  fail 'versioned identifiers must not exist: the version belongs in STRATEGY_ABI/ABI_VERSION'
fi

# --- 3. no versioned strategy-ABI module files in Rust or Python ---------------------------------
# Scoped to the strategy runtime crate and the strategy SDK. Plugin ABIs (`titan_plugin_abi_v1.h`),
# protocol modules (`uniswap_v2`), and management API versions (`management/v2.rs`) are different,
# legitimate versioning and must not be renamed to satisfy a strategy guard.
versioned_files=$(find \
  "$repo_root/crates/titan-strategy-runtime" "$python_root" \
  -type f \( -name '*_v[0-9]*.rs' -o -name '*_v[0-9]*.py' -o -name 'v[0-9]*.rs' -o -name 'v[0-9]*.py' \) \
  -o -type d -name 'v[0-9]*' -path "$repo_root/crates/titan-strategy-runtime/*" \
  2>/dev/null | grep -v '/target/' || true)
if [[ -n "$versioned_files" ]]; then
  printf '%s\n' "$versioned_files" >&2
  fail 'versioned module files must not exist: use one module per responsibility'
fi

# --- 4. no versioned identifiers in the shipped Python SDK ---------------------------------------
versioned_python=$(search "$python_root" \
  '\b[A-Za-z_][A-Za-z0-9_]*[vV]1[0-9]+[A-Za-z0-9_]*\b' --glob '*.py' --glob '!**/__pycache__/**')
if [[ -n "$versioned_python" ]]; then
  printf '%s\n' "$versioned_python" >&2
  fail 'versioned identifiers must not exist in the Python SDK'
fi

# --- 5. the loader/library name carries no version ------------------------------------------------
# Retired loader names may only appear as *rejected* values: a migration hint that tells an operator
# to recompile, a refusal, or the fixture/test that proves the refusal. They are never an accepted
# loader type. Every other occurrence must use "native".
retired_loader_pattern='native-v[0-9]+'
# Retired names that are compared against, not assigned: the migration hint and the refusal gate
# must recognize the old value in order to reject it. These two lines are allowlisted by exact text.
retired_allowed_pattern='let hint = if definition\.strategy_file\.loader_type\.as_ref\(\) == "native-v[0-9]+" \{|if definition\.strategy_file\.loader_type\.as_ref\(\) == "native-v[0-9]+" \{|loader_type_migrated|was retired|definition\.strategy_file\.loader_type = Arc::from\("native-v[0-9]+"\)'
versioned_library=$(search "$repo_root" "$retired_loader_pattern" \
  --glob '*.rs' --glob '*.toml' --glob '*.json' --glob '*.py' \
  | grep -v -E "$retired_allowed_pattern" || true)
if [[ -n "$versioned_library" ]]; then
  printf '%s\n' "$versioned_library" >&2
  fail 'library/loader names must be "native": a retired name may only appear in a rejection path'
fi

# --- 6. retired ABI symbols must not come back ----------------------------------------------------
legacy=$(search "${rust_roots[@]}" \
  'BacktestCommandBuffer|STRATEGY_ABI_VERSION[[:space:]]*:[^=]*=[[:space:]]*12' --glob '*.rs')
if [[ -n "$legacy" ]]; then
  printf '%s\n' "$legacy" >&2
  fail 'legacy strategy ABI symbols remain in executable Rust sources'
fi
