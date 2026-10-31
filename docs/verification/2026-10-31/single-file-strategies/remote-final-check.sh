set -euo pipefail
cd /home/dominolu/dev/titan-single-file-20261031-1a7cc720
python3 - <<'PY'
import json,hashlib,pathlib
root=pathlib.Path('src');old=json.loads(pathlib.Path('source.json').read_text());new=json.loads(pathlib.Path('next-source.json').read_text())
for name in old:
 p=root/name
 if p.exists() and hashlib.sha256(p.read_bytes()).hexdigest()!=old[name]:raise SystemExit('remote input changed: '+name)
for name in set(old)-set(new):
 p=root/name
 if p.exists():p.unlink()
PY
tar xzf next-source.tgz -C src
tar xzf hft-source.tgz
mv next-source.json source.json
cd src
mkdir -p ../.cargo
cat > ../.cargo/config.toml <<'MIRROR'
[source.crates-io]
replace-with = "titan-mirror"
[source.titan-mirror]
registry = "sparse+https://rsproxy.cn/index/"
MIRROR
export CARGO_BUILD_JOBS=8 CARGO_TARGET_DIR=/tmp/titan-single-file-target CARGO_NET_OFFLINE=true
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export PYTHONPATH="$PWD:$TITAN_STRATEGY_SDK"
task_cargo=(/home/dominolu/.cargo/bin/cargo +1.94.1 --config 'source.crates-io.replace-with="titan-mirror"' --config 'source.titan-mirror.registry="sparse+https://rsproxy.cn/index/"')
"${task_cargo[@]}" fmt --all > ../logs/format.log 2>&1
python3 - <<'PY'
import json,hashlib,pathlib,tarfile
root=pathlib.Path('.');path=pathlib.Path('../source.json');files=json.loads(path.read_text());changed=[]
for name,value in files.items():
 p=root/name
 if p.exists():
  current=hashlib.sha256(p.read_bytes()).hexdigest()
  if current!=value:changed.append(name);files[name]=current
with tarfile.open('../formatted.tgz','w:gz') as archive:
 for name in changed:archive.add(name,arcname=name,recursive=False)
path.write_text(json.dumps(files,sort_keys=True,indent=2))
print('server formatting:',changed,flush=True)
PY
"${task_cargo[@]}" check -p titan-cli --all-targets > ../logs/check-round3.log 2>&1 || { tail -80 ../logs/check-round3.log; exit 1; }
"${task_cargo[@]}" clippy --locked --workspace --all-targets --all-features -- -D warnings > ../logs/clippy.log 2>&1 || { tail -100 ../logs/clippy.log; exit 1; }
# Only generated locks in this task's isolated workspaces are refreshed for the final compiler binary.
rm -f titan.lock ../hft_quant/titan.lock
"${task_cargo[@]}" test --locked --workspace --all-targets --all-features > ../logs/rust-workspace-test.log 2>&1 || { tail -120 ../logs/rust-workspace-test.log; exit 1; }
"${task_cargo[@]}" test --locked --workspace --all-features --doc > ../logs/doctest.log 2>&1 || { tail -80 ../logs/doctest.log; exit 1; }
"${task_cargo[@]}" build --locked -p titan-cli --all-features --bin titan > ../logs/build.log 2>&1
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/debug/titan"
bash -n scripts/publish_strategy.sh
"$TITAN_TEST_BINARY" strategy test strategies/grid.py --update-lock --json > ../logs/grid-inline.json 2> ../logs/grid-inline.stderr
"$TITAN_TEST_BINARY" strategy test strategies/pair_arb.py --json > ../logs/pair-inline.json 2> ../logs/pair-inline.stderr
"$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v > ../logs/sdk-test.log 2>&1 || { tail -100 ../logs/sdk-test.log; exit 1; }
cd ../hft_quant
"$TITAN_TEST_BINARY" strategy test strategies/orderflow_events.py --update-lock --json > ../logs/orderflow-inline.json 2> ../logs/orderflow-inline.stderr
"$TITAN_TEST_BINARY" strategy build strategies/orderflow_events.py --runtime configs/titan/binance-readonly.toml --strategy-key orderflow_events --json > ../logs/orderflow-build.json 2> ../logs/orderflow-build.stderr
cat ../logs/orderflow-inline.json
cat ../logs/orderflow-build.json
printf '0\n' > ../logs/pipeline.exit
