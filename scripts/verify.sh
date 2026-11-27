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
# On the build server it also makes the tree it builds the tree you are working in: before the
# command runs, the working tree is pushed to `$TITAN_BUILD_DIR` (see scripts/sync_build_server.sh),
# uncommitted changes included. Verification therefore always describes the code as it is now, and
# there is only ever one server directory — a per-version copy was how a stale file from an earlier
# round survived into a later build.
#
# Usage:
#   scripts/verify.sh <command> [args...]
#   scripts/verify.sh --no-cache <command> [args...]   # skip restore, ignore its exit status
#   scripts/verify.sh --no-sync <command> [args...]    # build the server tree as it is
#
# Environment:
#   TITAN_DEP_CACHE      snapshot root      (default: $TITAN_HOME/dep-cache or ~/.cache/titan-deps)
#   TITAN_BUILD_PROFILE  debug (default) or release
#   TITAN_BUILD_PYTHON   interpreter able to import numba/llvmlite
#   TITAN_NO_DEP_CACHE=1 disables snapshot restore/save (the target pinning still applies)
#   TITAN_SYNC_SOURCE=1  push this working tree to the build server first (set by titan-sync)
#   TITAN_BUILD_HOST     ssh destination of the build server, used by the sync
#   TITAN_BUILD_DIR      the one server directory verification builds (default: ~/dev/titan)
#
# Exit status is the wrapped command's; a failed restore falls back to a normal build rather than
# failing the run, so a cold or stale cache never blocks work, and a failed sync stops the run
# rather than verifying a tree that is not the one you are working in.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
# shellcheck source=scripts/build_env.sh
source "$script_dir/build_env.sh"
file="$script_dir/dep_cache.sh"

use_cache=1
sync_source="${TITAN_SYNC_SOURCE:-0}"
while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-cache) use_cache=0; shift ;;
        --no-sync) sync_source=0; shift ;;
        --sync) sync_source=1; shift ;;
        *) break ;;
    esac
done
if [[ $# -eq 0 ]]; then
    echo "usage: $0 [--no-cache] [--no-sync] <command> [args...]" >&2
    exit 2
fi

# The tree under verification is the working tree. A sync that fails stops the run: building the
# server's previous copy and calling the result a verification of this tree is exactly the mistake
# this guard exists to prevent.
if [[ "$sync_source" == 1 ]]; then
    if ! "$script_dir/sync_build_server.sh"; then
        echo "verify: syncing the working tree to the build server failed; not verifying" >&2
        exit 4
    fi
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

status=0
"$@" || status=$?

# Keep the snapshot current for the key this run used, after every successful run.
#
# Saving is keyed rather than time-based: a run that reused a complete adoptable snapshot rewrites
# it from the same tree (cheap, hard-linked), and a run that compiled a new toolchain or lockfile
# seeds the snapshot it just paid for. The old "only if missing, and only after 30 seconds" rule
# was removed from the wrong place: a short run that compiled the last few crates still produced
# the artifacts the next run restores from.
if [[ $use_cache -eq 1 && $status -eq 0 && -d "$CARGO_TARGET_DIR" ]]; then
    if "$file" save >/dev/null 2>&1; then
        echo "verify: dependency snapshot $("$file" key) is current" >&2
        # Bounded retention: two snapshots is one working set plus one rollback.
        "$file" prune 2 >/dev/null 2>&1 || true
    fi
fi

exit "$status"
