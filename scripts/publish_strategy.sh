#!/usr/bin/env bash
# Publish any verified Titan release over SSH. Never starts a strategy or changes a current pointer.
set -euo pipefail

release="" host="" remote_root="" remote_titan="" identity=""
trust=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --release) release="$2"; shift 2 ;;
    --host) host="$2"; shift 2 ;;
    --remote-root) remote_root="$2"; shift 2 ;;
    --remote-titan) remote_titan="$2"; shift 2 ;;
    --identity) identity="$2"; shift 2 ;;
    --trusted-key) trust+=(--trusted-key "$2"); shift 2 ;;
    --require-signature) trust+=(--require-signature); shift ;;
    -h|--help)
      echo 'publish_strategy.sh --release DIR --host SSH_HOST --remote-root /releases --remote-titan /installed/titan [--identity KEY] [--require-signature --trusted-key ID=HEX]'
      exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done
[[ -d "$release" && -x "$release/titan" && -n "$host" && "$host" != -* ]] || { echo 'release and host are required' >&2; exit 2; }
[[ "$remote_root" == /* && "$remote_titan" == /* ]] || { echo 'remote paths must be absolute' >&2; exit 2; }
"$release/titan" strategy verify-release "$release" "${trust[@]}" --json
manifest_sha=$(sha256sum "$release/release.json")
manifest_sha=${manifest_sha%% *}

quote() { printf "'%s'" "${1//\'/\'\\\'\'}"; }
remote="set -euo pipefail
mkdir -p -- $(quote "$remote_root")
incoming=\$(mktemp -d $(quote "$remote_root/.incoming.XXXXXXXX"))
trap 'rm -rf -- \"\$incoming\"' EXIT
tar xzf - -C \"\$incoming\"
printf '%s  %s\\n' $(quote "$manifest_sha") \"\$incoming/release.json\" | sha256sum -c - >/dev/null
$(quote "$remote_titan") strategy publish \"\$incoming\" --directory $(quote "$remote_root")"
for item in "${trust[@]}"; do remote+=" $(quote "$item")"; done
remote+=" --json"
ssh_args=(-o BatchMode=yes)
if [[ -n "$identity" ]]; then ssh_args+=(-o IdentitiesOnly=yes -i "$identity"); fi
tar czf - -C "$release" . | ssh "${ssh_args[@]}" "$host" "bash -c $(quote "$remote")"
