#!/usr/bin/env bash
# Everything a verification run on the build server should not have to think about.
#
# Two costs dominate a verification round and both are avoidable:
#
#   * a second full compile, caused by a build pointed at any `CARGO_TARGET_DIR` other than the
#     repository's `target/` (the Python tests resolve `<repo>/target/debug/titan`);
#   * recompiling ~340 dependency crates that did not change, which the dependency snapshot in
#     `scripts/dep_cache.sh` serves in about a second.
#
# This wrapper is the single entry point that removes both: it pins the target directory, restores
# the dependency snapshot when one matches the current toolchain/lockfile, runs the command, and
# then keeps the snapshot current. `.cargo/config.toml` aliases `titan-test`/`titan-build`/
# `titan-clippy` to it, so the automatic path is also the shortest one to type.
#
# Usage:
#   scripts/verify.sh <command> [args...]
#   scripts/verify.sh --no-cache <command> [args...]   # skip restore, ignore its exit status
#
# Environment:
#   TITAN_DEP_CACHE      snapshot root      (default: $TITAN_HOME/dep-cache or ~/.cache/titan-deps)
#   TITAN_BUILD_PROFILE  debug (default) or release
#   TITAN_BUILD_PYTHON   interpreter able to import numba/llvmlite
#   TITAN_NO_DEP_CACHE=1 disables snapshot restore/save (the target pinning still applies)
#
# Exit status is the wrapped command's; a failed restore falls back to a normal build rather than
# failing the run, so a cold or stale cache never blocks work.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
# shellcheck source=scripts/build_env.sh
source "$script_dir/build_env.sh"
file="$script_dir/dep_cache.sh"

use_cache=1
if [[ "${TITAN_NO_DEP_CACHE:-}" == "1" ]]; then
    use_cache=0
fi
if [[ "${1:-}" == "--no-cache" ]]; then
    use_cache=0
    shift
fi
if [[ $# -eq 0 ]]; then
    echo "usage: $0 [--no-cache] <command> [args...]" >&2
    exit 2
fi

titan_build_env quiet || exit 2

# Check the interpreter before the build, not after it: a missing numba/llvmlite only surfaces in a
# few tests, by which point a full compile has already been paid.
if ! titan_check_python && [[ "${TITAN_ALLOW_PYTHON_WITHOUT_NUMBA:-}" != "1" ]]; then
    echo "verify: refusing to start; set TITAN_BUILD_PYTHON or TITAN_ALLOW_PYTHON_WITHOUT_NUMBA=1" >&2
    exit 3
fi

if [[ $use_cache -eq 1 ]]; then
    if "$file" status >/dev/null 2>&1 && "$file" status | grep -q '^snapshot: present'; then
        if "$file" restore >/dev/null 2>&1; then
            echo "verify: adopted dependency snapshot $("$file" key)" >&2
        else
            echo "verify: snapshot restore failed; building normally" >&2
        fi
    fi
fi

# A run that compiled nothing has nothing to contribute to the snapshot; saving then would only
# copy an incomplete tree (for example after a failed early command).
run_started=$SECONDS
status=0
"$@" || status=$?

# Keep the snapshot current: the first run on a new toolchain or lockfile pays the dependency
# compile once and then seeds the cache for every later run.
if [[ $use_cache -eq 1 && $status -eq 0 && -d "$CARGO_TARGET_DIR" && $((SECONDS - run_started)) -ge 30 ]]; then
    if ! "$file" status 2>/dev/null | grep -q '^snapshot: present'; then
        if "$file" save >/dev/null 2>&1; then
            echo "verify: saved dependency snapshot $("$file" key)" >&2
            # Bounded retention: two snapshots is one working set plus one rollback.
            "$file" prune 2 >/dev/null 2>&1 || true
        fi
    fi
fi

exit "$status"
