#!/usr/bin/env bash
# Build-server helper: always compile into the project's own `target/`.
#
# Why this exists: several SDK tests and verification scripts resolve the binary as
# `<repo>/target/debug/titan`. Building with `CARGO_TARGET_DIR` pointing anywhere else produces a
# second, independent artifact set — a full duplicate compile (~28 minutes on a shared host) and
# tens of gigabytes of extra disk. Every wrapper here therefore pins `CARGO_TARGET_DIR` to the
# project `target/` (or refuses to run when it points elsewhere) and exports the derived paths the
# Python side needs.
#
# Usage:
#   source scripts/build_env.sh                     # set up the environment only
#   scripts/build_env.sh cargo test -p titan-cli    # run a command inside it
#
# `TITAN_BUILD_PROFILE` selects debug (default) or release. `TITAN_BUILD_PYTHON` selects the
# interpreter for native strategy compilation; when unset, `python3` is used and the script warns
# if it cannot import numba/llvmlite (the SDK tests that compile strategies would fail).
#
# `TITAN_VENUE_PROXY` (or `TITAN_VENUE_PROXY_PORT`) routes outbound HTTP to a local proxy. Some
# exchanges are unreachable from the build host without one — the venue metadata tests and the
# connector integration tests need it. The proxy is only exported when the port is actually
# listening, so an absent proxy degrades to the direct path instead of failing every request.
set -euo pipefail

titan_repo_root() {
    local script_dir
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    (cd "$script_dir/.." && pwd)
}

# Sets CARGO_TARGET_DIR/TITAN_TEST_BINARY/TITAN_STRATEGY_* to the project `target/` layout.
# Prints the resolved target directory on stdout when `quiet` is not given.
titan_build_env() {
    local quiet="${1:-}"
    local root profile
    root="$(titan_repo_root)"
    profile="${TITAN_BUILD_PROFILE:-debug}"

    # A caller-supplied target dir is honoured only when it is the project's own; anything else
    # would silently duplicate every compile.
    local expected="$root/target"
    if [[ -n "${CARGO_TARGET_DIR:-}" && "$(cd "$CARGO_TARGET_DIR" 2>/dev/null && pwd)" != "$expected" ]]; then
        echo "build_env: refusing CARGO_TARGET_DIR=$CARGO_TARGET_DIR (expected $expected)" >&2
        return 2
    fi
    if [[ -n "${CARGO_BUILD_TARGET_DIR:-}" && "$CARGO_BUILD_TARGET_DIR" != "$expected" ]]; then
        echo "build_env: refusing CARGO_BUILD_TARGET_DIR=$CARGO_BUILD_TARGET_DIR (expected $expected)" >&2
        return 2
    fi

    export CARGO_TARGET_DIR="$expected"
    export TITAN_TEST_BINARY="$expected/$profile/titan"
    export TITAN_STRATEGY_SDK="$root/python/titan-strategy-sdk"
    export PYTHONPATH="${TITAN_STRATEGY_SDK}${PYTHONPATH:+:$PYTHONPATH}"
    if [[ -n "${TITAN_BUILD_PYTHON:-}" ]]; then
        export TITAN_STRATEGY_PYTHON="$TITAN_BUILD_PYTHON"
    fi
    titan_venue_proxy || true

    if [[ -z "$quiet" ]]; then
        echo "$expected"
    fi
}

# Verifies the interpreter can compile native strategies; failing here is much cheaper than
# discovering it after a full build.
# Selects the strategy-compilation interpreter, once, from the designations the host provides.
#
# Order: an explicit TITAN_STRATEGY_PYTHON, then TITAN_BUILD_PYTHON (set by the build server's shell
# profile), then the first candidate that can import numba/llvmlite. Exporting the result is the
# whole point: the Rust CLI spawns `python3` for `describe` and inherits this environment, so a
# check that only warns would still fail every strategy-compiling test and artifact rebuild.
# Returns 1 when nothing usable is found; the caller decides whether that is fatal.
# Exports the venue proxy when one is listening. Off-exchange hosts need this: without it, DNS for
# some venues resolves to a link-local address and every request times out.
titan_venue_proxy() {
    local port="${TITAN_VENUE_PROXY_PORT:-7893}"
    local host="${TITAN_VENUE_PROXY:-127.0.0.1}"
    # Nothing to do when the caller already set a proxy, or when none is listening.
    if [[ -n "${https_proxy:-}${HTTPS_PROXY:-}" ]]; then
        return 0
    fi
    if ! (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then
        return 1
    fi
    export https_proxy="http://$host:$port"
    export http_proxy="http://$host:$port"
    export HTTPS_PROXY="$https_proxy"
    export HTTP_PROXY="$http_proxy"
    # Loopback must never go through the proxy: the connector tests stand up local HTTP servers, and
    # the proxy answers those with 502, which fails dozens of unrelated tests.
    local bypass="127.0.0.1,localhost,::1,169.254.0.0/16"
    export no_proxy="${no_proxy:+$no_proxy,}$bypass"
    export NO_PROXY="${NO_PROXY:+$NO_PROXY,}$bypass"
    echo "build_env: routed outbound HTTP through $host:$port" >&2
    return 0
}

titan_check_python() {
    local candidate
    if [[ -n "${TITAN_STRATEGY_PYTHON:-}" ]]; then
        candidate="$TITAN_STRATEGY_PYTHON"
    elif [[ -n "${TITAN_BUILD_PYTHON:-}" ]]; then
        candidate="$TITAN_BUILD_PYTHON"
    else
        candidate=""
        for option in \
            "$HOME/.venv/titan/bin/python" \
            "$HOME/titan-build-741d2de5/.venv-pair-arb/bin/python" \
            "$HOME/miniconda3/bin/python3"; do
            if [[ -x "$option" ]] && "$option" -c "import numba, llvmlite" >/dev/null 2>&1; then
                candidate="$option"
                break
            fi
        done
    fi
    if [[ -n "$candidate" ]] && "$candidate" -c "import numba, llvmlite" >/dev/null 2>&1; then
        export TITAN_STRATEGY_PYTHON="$candidate"
        export TITAN_REPORT_PYTHON="${TITAN_REPORT_PYTHON:-$candidate}"
        return 0
    fi
    echo "build_env: no interpreter with numba/llvmlite; set TITAN_BUILD_PYTHON" \
         "(strategy-compiling tests and artifact rebuilds will fail)" >&2
    return 1
}

# The project's `target/` directory, whatever the caller asked for.
titan_target_dir() {
    titan_build_env quiet
    printf '%s' "${CARGO_TARGET_DIR:-}"
}

# Only act as a command when executed. When sourced, callers get the functions and must call
# `titan_build_env` themselves: `BASH_SOURCE[0] != $0` cannot be relied on inside `bash -c`.
if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
    titan_build_env
    if [[ $# -gt 0 ]]; then
        exec "$@"
    fi
fi
