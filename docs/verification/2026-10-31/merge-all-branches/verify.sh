#!/usr/bin/env bash
set -euo pipefail
case " $(hostname -I) " in
  *" 192.168.3.88 "*) ;;
  *) echo 'Run only on compilation server 192.168.3.88' >&2; exit 2 ;;
esac
export CARGO_TARGET_DIR=/tmp/titan-single-file-target
export CARGO_BUILD_JOBS=8
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export PYTHONPATH="$PWD:$TITAN_STRATEGY_SDK"
merge_logs=/tmp/titan-merge-all-20261031-nH6qpB/logs
merge_cargo=(/home/dominolu/.cargo/bin/cargo +1.94.1 --config 'source.crates-io.replace-with="titan-mirror"' --config 'source.titan-mirror.registry="sparse+https://rsproxy.cn/index/"')
run_gate() {
  local gate_name="$1"
  shift
  local gate_result=0
  "$@" > "$merge_logs/$gate_name.log" 2>&1 || gate_result=$?
  printf '%s\t%s\n' "$gate_name" "$gate_result" >> "$merge_logs/status.tsv"
  if [[ $gate_result -ne 0 ]]; then
    tail -100 "$merge_logs/$gate_name.log"
    return "$gate_result"
  fi
  printf '%s passed\n' "$gate_name"
}
run_gate format "${merge_cargo[@]}" fmt --all
run_gate fmt-check "${merge_cargo[@]}" fmt --all -- --check
run_gate check "${merge_cargo[@]}" check --offline --locked --workspace --all-targets --all-features
run_gate clippy "${merge_cargo[@]}" clippy --offline --locked --workspace --all-targets --all-features -- -D warnings
run_gate rust "${merge_cargo[@]}" test --offline --locked --workspace --all-targets --all-features
run_gate doctest "${merge_cargo[@]}" test --offline --locked --workspace --all-features --doc
