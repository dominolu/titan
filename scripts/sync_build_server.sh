#!/usr/bin/env bash
# The build server's copy of this repository, made from this working tree.
#
# One directory, always: the server compiles `$TITAN_BUILD_DIR` and nothing else, and this script
# makes that directory say exactly what this working tree says — including changes not yet
# committed, which are usually the ones being verified. A per-version directory was the previous
# arrangement and it failed in both directions: `~/dev/titan` drifted into a mix of files from
# different dates (a stale `v13.rs` next to a November `lib.rs`), and every round left another
# copy of a 60 GB `target/` behind. Version identity is the sync manifest instead of a directory
# name; `git status` on the server says how the tree relates to the repository.
#
# The sync is exact: files the working tree does not have are deleted on the server, `target/` and
# `.git/` excepted. That is what keeps the build honest — a leftover source file from an earlier
# round can make a test pass that the current tree would fail.
#
# Usage:
#   scripts/sync_build_server.sh              # sync, then print the server's git status
#   scripts/sync_build_server.sh --check      # report what would change, transfer nothing
#   scripts/sync_build_server.sh --full       # include data/ and backtest_reports/ (large)
#
# Environment:
#   TITAN_BUILD_HOST   ssh destination (required; no default, see docs/build_verification_workflow.md)
#   TITAN_BUILD_DIR    server-side path   (default: ~/dev/titan)

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"

mode=sync
full=0
for arg in "$@"; do
    case "$arg" in
        --check) mode=check ;;
        --full) full=1 ;;
        -h|--help) sed -n '2,22p' "$0"; exit 0 ;;
        *) echo "sync: unknown argument: $arg" >&2; exit 2 ;;
    esac
done

host="${TITAN_BUILD_HOST:-}"
if [[ -z "$host" ]]; then
    echo "sync: set TITAN_BUILD_HOST to the build server's ssh destination" >&2
    exit 2
fi
# The directory the server always builds. It is a remote shell expansion, not a local path: the
# sync replaces its contents, so it must be the one directory verification uses.
destination="${TITAN_BUILD_DIR:-\$HOME/dev/titan}"
echo "sync: $repo_root -> $host:$destination" >&2

# Build outputs and local-only inputs never travel. `data/` and `backtest_reports/` hold local
# research inputs rather than anything the build reads, and they are most of the tree's size; a
# full sync transfers them only when asked.
#
# The exclusions are applied by building a staging tree, not by pattern. Both tools available here
# match patterns at any depth: `tar --exclude=./data/` and `rsync --exclude=data/` each removed
# `hftbacktest/src/backtest/data/`, which is source, and the build then failed with `file not found
# for module data` on a tree that was not the one under verification. Removing the staged root
# entries cannot make that mistake: every other file is present by construction.
staging="$(mktemp -d "${TMPDIR:-/tmp}/titan-sync-stage.XXXXXX")"
local_archive="$(mktemp "${TMPDIR:-/tmp}/titan-sync.XXXXXX")"
member_list="$(mktemp "${TMPDIR:-/tmp}/titan-sync-list.XXXXXX")"
trap 'rm -rf "$staging" "$local_archive" "$member_list"' EXIT

cp -a "$repo_root/." "$staging/"
rm -rf "$staging/target" "$staging"/target-* "$staging/.git" "$staging/web/node_modules" \
    "$staging/internal-docs" "$staging/.secrets"
if (( ! full )); then
    rm -rf "$staging/data" "$staging/backtest_reports"
fi

# One archive, one file, one scp: the local rsync is openrsync, which speaks an older protocol than
# the server's rsync 3, and a per-file round trip over this link made a one-gigabyte tree take tens
# of minutes.
tar --no-xattrs -czf "$local_archive" -C "$staging" . 2>/dev/null \
    || tar -czf "$local_archive" -C "$staging" .
# macOS writes a resource fork beside every file copied from an extended-attribute volume; the
# server's tar keeps them as `.?_name` files, which would show up in `git status` forever.
"$script_dir/sync_strip_appledouble.py" "$local_archive"

# Every tracked file must be in the archive, checked once against the member list. It is the
# difference between a sync that fails here and a build that fails on the server for a file that
# never travelled.
tar -tzf "$local_archive" > "$member_list"
if ! git -C "$repo_root" ls-files -z 2>/dev/null \
    | while IFS= read -r -d '' tracked; do
        case "$tracked" in
            data/*|backtest_reports/*) (( full )) || continue ;;
            internal-docs/*|.secrets/*) continue ;;
        esac
        grep -qxF -- "./$tracked" "$member_list" || { echo "sync: tracked file missing from the archive: $tracked" >&2; exit 1; }
    done
then
    echo "sync: refusing to sync an incomplete archive" >&2
    exit 3
fi

revision="$(git -C "$repo_root" rev-parse --short HEAD 2>/dev/null || echo unknown)"
dirty="clean"
git -C "$repo_root" diff --quiet HEAD 2>/dev/null || dirty="dirty"
archive_bytes="$(wc -c < "$local_archive" | tr -d ' ')"
echo "sync: revision $revision ($dirty), archive $((archive_bytes / 1024 / 1024)) MiB" >&2

if [[ "$mode" == check ]]; then
    echo "sync: --check requested; nothing transferred" >&2
    exit 0
fi

remote_archive="/tmp/titan-sync-$$.tar.gz"
scp -q "$local_archive" "$host:$remote_archive"

# Unpacking is a merge, so the staged files are deleted before extraction: a source file that this
# working tree no longer has must not survive on the server. `target/` and `.git/` stay.
ssh -o BatchMode=yes "$host" "set -e
    cd \"$destination\"
    find . -mindepth 1 -maxdepth 1 ! -name target ! -name .git -exec rm -rf {} +
    tar -xzf \"$remote_archive\"
    rm -f \"$remote_archive\"
    rm -rf ./target/*/incremental 2>/dev/null || true
    {
        echo \"revision: $revision\"
        echo \"tree: $dirty\"
        echo \"synced_at: \$(date -u +%Y-%m-%dT%H:%M:%SZ)\"
        echo \"archive_bytes: $archive_bytes\"
    } > .sync-manifest
"
echo "sync: done; server copy at $destination" >&2
