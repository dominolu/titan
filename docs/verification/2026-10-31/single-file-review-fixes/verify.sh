#!/usr/bin/env bash
set -euo pipefail
cd /home/dominolu/dev/titan-single-file-review-20261031
mkdir src hft_quant logs
tar xzf source.tar.gz -C src
tar xzf hft-source.tar.gz -C hft_quant
mkdir .cargo
cat > .cargo/config.toml <<'MIRROR'
[source.crates-io]
replace-with = "titan-mirror"
[source.titan-mirror]
registry = "sparse+https://rsproxy.cn/index/"
MIRROR
export CARGO_BUILD_JOBS=8 CARGO_TARGET_DIR=/tmp/titan-single-file-target CARGO_NET_OFFLINE=true
export TITAN_STRATEGY_SDK="$PWD/src/python/titan-strategy-sdk"
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export PYTHONPATH="$PWD/src:$TITAN_STRATEGY_SDK"
task_cargo=(/home/dominolu/.cargo/bin/cargo +1.94.1 --config 'source.crates-io.replace-with="titan-mirror"' --config 'source.titan-mirror.registry="sparse+https://rsproxy.cn/index/"')
trap 'code=$?; printf "%s\n" "$code" > /home/dominolu/dev/titan-single-file-review-20261031/logs/exit-code; exit "$code"' EXIT
python3 - <<'PY'
from pathlib import Path
import hashlib,json,tarfile
root=Path('src')
with tarfile.open('source.tar.gz') as archive:
    names=[m.name for m in archive.getmembers() if m.isfile()]
Path('input-files.json').write_text(json.dumps(names))
Path('inputs-before-format.json').write_text(json.dumps({n:hashlib.sha256((root/n).read_bytes()).hexdigest() for n in names},indent=2,sort_keys=True))
PY
cd src
"${task_cargo[@]}" fmt --all > ../logs/format.log 2>&1
python3 - <<'PY'
from pathlib import Path
import hashlib,json,tarfile
before=json.loads(Path('../inputs-before-format.json').read_text())
after={n:hashlib.sha256(Path(n).read_bytes()).hexdigest() for n in before}
changed=[n for n in before if before[n]!=after[n]]
Path('../inputs-frozen.json').write_text(json.dumps(after,indent=2,sort_keys=True))
with tarfile.open('../formatted.tar.gz','w:gz') as archive:
    for n in changed:archive.add(n,arcname=n,recursive=False)
print('formatted='+json.dumps(changed),flush=True)
PY
"${task_cargo[@]}" test --locked -p titan-cli --test single_file_strategy --all-features > ../logs/targeted.log 2>&1 || { tail -n 100 ../logs/targeted.log; exit 1; }
printf 'targeted passed\n'
"${task_cargo[@]}" clippy --locked --workspace --all-targets --all-features -- -D warnings > ../logs/clippy.log 2>&1 || { tail -n 100 ../logs/clippy.log; exit 1; }
printf 'clippy passed\n'
"${task_cargo[@]}" test --locked --workspace --all-targets --all-features > ../logs/rust.log 2>&1 || { tail -n 100 ../logs/rust.log; exit 1; }
"${task_cargo[@]}" test --locked --workspace --all-features --doc > ../logs/doctest.log 2>&1 || { tail -n 100 ../logs/doctest.log; exit 1; }
printf 'workspace and doctest passed\n'
"${task_cargo[@]}" build --locked -p titan-cli --all-features --bin titan > ../logs/build.log 2>&1
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
"$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v > ../logs/sdk.log 2>&1 || { tail -n 100 ../logs/sdk.log; exit 1; }
printf 'SDK passed\n'
"${task_cargo[@]}" build --locked --release -p titan-cli --all-features --bin titan > ../logs/release-build.log 2>&1 || { tail -n 100 ../logs/release-build.log; exit 1; }
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/release/titan"
cd ../hft_quant
"$TITAN_TEST_BINARY" strategy build strategies/orderflow_events.py --runtime configs/titan/binance-readonly.toml --json > ../logs/orderflow-build.json 2> ../logs/orderflow-build.stderr
for name in disabled asset combined; do
  case "$name" in
    disabled) overrides=(--set e04_enabled=false) ;;
    asset) overrides=(--set asset_no=1) ;;
    combined) overrides=(--set e04_enabled=false --set asset_no=1 --set e11_lookback_ms=60000) ;;
  esac
  "$TITAN_TEST_BINARY" strategy compile strategies/orderflow_events.py "${overrides[@]}" --output "build/$name.titan" --json > "../logs/orderflow-$name.json" 2> "../logs/orderflow-$name.stderr"
done
"$TITAN_STRATEGY_PYTHON" - <<'PY'
from pathlib import Path
import json,runpy,numpy as np
dtype=runpy.run_path('strategies/orderflow_events.py')['state_dtype']
for name in ('disabled','asset','combined'):
    result=json.loads(Path('../logs/orderflow-'+name+'.json').read_text())
    state=np.frombuffer(bytes(result['metadata']['initial_state']),dtype=dtype)[0]
    assert int(state['cfg']['asset_no'])==result['normalized_parameters']['asset_no']
    assert bool(state['cfg']['e04_enabled'])==result['normalized_parameters']['e04_enabled']
    assert result['tests']['passed']==9
print('all parameter overrides passed; published state retains actual configuration')
Path('../logs/parameter-state-check.json').write_text(json.dumps({'valid':True,'cases':['disabled','asset','combined'],'native_scenes_per_case':9}))
PY
release=$(python3 -c 'import json;print(json.load(open("../logs/orderflow-build.json"))["release"])')
TITAN_STRATEGY_PYTHON=/must-not-execute "$TITAN_TEST_BINARY" strategy verify-release "$release" --json > ../logs/release-verify.json
"$TITAN_TEST_BINARY" strategy publish "$release" --directory ../published --json > ../logs/release-publish.json
"$TITAN_TEST_BINARY" strategy publish "$release" --directory ../published --json > ../logs/release-publish-repeat.json
cd ../src
"${task_cargo[@]}" fmt --all -- --check > ../logs/fmt-check.log 2>&1
python3 - <<'PY'
from pathlib import Path
import hashlib,json
before=json.loads(Path('../inputs-frozen.json').read_text())
after={n:hashlib.sha256(Path(n).read_bytes()).hexdigest() for n in before}
assert before==after,'source input changed during verification'
Path('../logs/source-check.json').write_text(json.dumps({'valid':True,'files':len(before)},indent=2))
Path('../inputs-after.json').write_text(json.dumps(after,indent=2,sort_keys=True))
PY
printf 'release and parameter regressions passed\n'
