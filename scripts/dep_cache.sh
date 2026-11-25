#!/usr/bin/env bash
# Build-server helper: keep a warm, hard-linked copy of `target/` so verification runs do not repay
# a full dependency compile.
#
# The workspace pulls in roughly a thousand dependency artifacts; a from-scratch compile of all of
# them is the single largest cost of a verification round (~28 minutes on a contended host) while
# the workspace's own fifteen crates take well under a minute. A hard-linked snapshot lets a fresh
# checkout adopt the dependency artifacts instantly: links share inodes, so the copy costs seconds
# and virtually no extra disk.
#
# Snapshots are keyed by everything that changes dependency fingerprints: the toolchain version,
# `Cargo.lock`, and the build profile. A changed key means the snapshot is not reusable, so a new
# one is taken after the next full build instead of silently reusing stale artifacts.
#
# Usage:
#   scripts/dep_cache.sh key                  # print the current snapshot key
#   scripts/dep_cache.sh save                 # snapshot the current target/ under that key
#   scripts/dep_cache.sh restore [DEST]       # adopt the snapshot into DEST (default: repo target/)
#   scripts/dep_cache.sh status               # key, snapshot presence, sizes
#   scripts/dep_cache.sh list                 # every stored key
#   scripts/dep_cache.sh prune [KEEP]         # keep the newest KEEP snapshots (default 2)
#
# The cache root defaults to `$TITAN_DEP_CACHE` or `$TITAN_HOME/dep-cache` or `~/.cache/titan-deps`.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

cache_root() {
    if [[ -n "${TITAN_DEP_CACHE:-}" ]]; then
        printf '%s' "$TITAN_DEP_CACHE"
    elif [[ -n "${TITAN_HOME:-}" ]]; then
        printf '%s' "$TITAN_HOME/dep-cache"
    else
        printf '%s' "$HOME/.cache/titan-deps"
    fi
}

# The rustc that will actually compile, not the version of the `cargo` shim in front of it.
#
# `cargo` and `rustc` on PATH are usually rustup proxies, and a proxy reports rustup's own version
# rather than the toolchain it is about to select; `rust-toolchain.toml` only switches the
# toolchain once cargo starts, so a key built from `rustc --version` through the proxy stays the
# same across a toolchain bump. That is how this repository ended up with two snapshots for two
# toolchains but a key that matches neither: every verification round found `snapshot: missing`,
# recompiled ~1000 dependency crates against a full `target/`, and never seeded a snapshot at all.
#
# `rustup run <toolchain> rustc --version` asks the selected toolchain directly; the plain call is
# the fallback for a host without rustup.
toolchain_rustc_version() {
    local channel toolchain
    channel="$(sed -n 's/^channel *= *"\(.*\)"/\1/p' "$repo_root/rust-toolchain.toml" 2>/dev/null | head -1)"
    if [[ -n "$channel" ]] && command -v rustup >/dev/null 2>&1; then
        if toolchain="$(rustup run "$channel" rustc --version 2>/dev/null)"; then
            printf '%s' "$toolchain"
            return
        fi
    fi
    rustc --version 2>/dev/null || echo unknown
}

snapshot_key() {
    local profile="${TITAN_BUILD_PROFILE:-debug}"
    local rustc_version lock_hash
    rustc_version="$(toolchain_rustc_version)"
    lock_hash="$(sha256sum "$repo_root/Cargo.lock" 2>/dev/null | cut -d' ' -f1 || echo nolock)"
    printf '%s|%s|%s' "$rustc_version" "${lock_hash:0:16}" "$profile" | sha256sum | cut -c1-16
}

snapshot_path() {
    printf '%s/%s' "$(cache_root)" "$(snapshot_key)"
}

# Hard-links the dependency artifacts of `target/` into the snapshot. Workspace crates are left out:
# they change every round and reusing them would mask a broken build.
save() {
    local source="${1:-$repo_root/target}" destination
    destination="$(snapshot_path)"
    if [[ ! -d "$source" ]]; then
        echo "dep_cache: $source does not exist; build first" >&2
        return 1
    fi
    mkdir -p "$(cache_root)"
    local staging="$destination.tmp.$$"
    rm -rf "$staging"
    mkdir -p "$staging"
    # `-a` plus `-l` copies the tree as hard links where the filesystem allows it; cross-device
    # falls back to real copies, which is slower but still correct.
    cp -al "$source/." "$staging/" 2>/dev/null || cp -a "$source/." "$staging/"
    # Drop the workspace's own build outputs so a restore cannot reuse a stale binary or test
    # binary of *our* code; only dependency artifacts are meant to survive.
    local profile="${TITAN_BUILD_PROFILE:-debug}"
    rm -f "$staging/$profile/deps"/titan-* "$staging/$profile/deps"/titan_* \
          "$staging/$profile/deps"/titan.??? "$staging/$profile/deps"/titan_*.??? 2>/dev/null || true
    rm -f "$staging/$profile"/titan "$staging/$profile"/titan_cli 2>/dev/null || true
    mv -T "$staging" "$destination" 2>/dev/null || { rm -rf "$destination"; mv "$staging" "$destination"; }
    echo "$destination"
}

restore() {
    local destination="${1:-$repo_root/target}"
    local snapshot
    snapshot="$(snapshot_path)"
    if [[ ! -d "$snapshot" ]]; then
        echo "dep_cache: no snapshot for key $(snapshot_key)" >&2
        return 1
    fi
    mkdir -p "$destination"
    # Hard links first so the adopted tree shares inodes with the snapshot; a fallback copy keeps
    # behaviour correct across filesystems (at the cost of disk).
    cp -al "$snapshot/." "$destination/" 2>/dev/null || cp -a "$snapshot/." "$destination/"
    echo "$destination"
}

status() {
    local snapshot
    snapshot="$(snapshot_path)"
    echo "rustc:    $(toolchain_rustc_version)"
    echo "key:      $(snapshot_key)"
    echo "cache:    $(cache_root)"
    if [[ -d "$snapshot" ]]; then
        echo "snapshot: present ($(du -sh "$snapshot" 2>/dev/null | cut -f1))"
    else
        echo "snapshot: missing"
    fi
    if [[ -d "$repo_root/target" ]]; then
        echo "target:   $(du -sh "$repo_root/target" 2>/dev/null | cut -f1)"
    fi
}

list_snapshots() {
    [[ -d "$(cache_root)" ]] && ls -1 "$(cache_root)" | grep -v '\.tmp\.' || true
}

# Snapshot keys are content hashes, so their order means nothing. Keeping "the last N by name" once
# deleted the snapshot that had just been written, leaving the cache empty after a successful
# verification: every round then paid a full dependency compile while `~/.cache/titan-deps` looked
# populated from the outside. The current key is never pruned, and the others go oldest first.
prune() {
    local keep="${1:-2}"
    local current entry key stamp kept=0
    local -a entries
    current="$(snapshot_key)"
    mapfile -t entries < <(
        for entry in $(list_snapshots); do
            [[ -d "$(cache_root)/$entry" ]] || continue
            stamp="$(stat -c %Y "$(cache_root)/$entry" 2>/dev/null || stat -f %m "$(cache_root)/$entry")"
            printf '%s %s\n' "$stamp" "$entry"
        done | sort -n
    )
    # Oldest first, so the newest `keep` survive. The current key counts as one of them.
    local total="${#entries[@]}"
    local index=0
    while (( index < total )); do
        key="${entries[$index]#* }"
        index=$((index + 1))
        if (( total - index + 1 <= keep )); then
            break
        fi
        [[ "$key" == "$current" ]] && continue
        rm -rf "$(cache_root)/$key"
        kept=$((kept + 1))
        echo "pruned $key"
    done
    if (( kept == 0 )); then
        echo "dep_cache: nothing to prune ($total snapshot(s), keeping $keep)"
    fi
}
case "${1:-status}" in
    key) echo "$(snapshot_key)" ;;
    save) shift; save "$@" ;;
    restore) shift; restore "$@" ;;
    status) status ;;
    list) list_snapshots ;;
    prune) shift; prune "$@" ;;
    *)
        echo "usage: $0 {key|save|restore|status|list|prune}" >&2
        exit 2
        ;;
esac
