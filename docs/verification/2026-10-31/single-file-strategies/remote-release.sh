set -euo pipefail
cd /home/dominolu/dev/titan-single-file-20261031-1a7cc720/src
test "$(cat ../logs/pipeline.exit)" = 0
python3 - <<'PY'
from pathlib import Path
import json,hashlib
files=json.loads(Path('../source.json').read_text());bad=[]
for name,wanted in files.items():
 p=Path(name)
 if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=wanted:bad.append(name)
if bad:raise SystemExit('source inputs changed: '+str(bad))
Path('../logs/source-inputs-before.json').write_text(json.dumps(files,indent=2,sort_keys=True))
print(len(files),'frozen inputs verified',flush=True)
PY
export CARGO_BUILD_JOBS=8 CARGO_TARGET_DIR=/tmp/titan-single-file-target CARGO_NET_OFFLINE=true
export TITAN_STRATEGY_SDK="$PWD/python/titan-strategy-sdk"
export TITAN_STRATEGY_PYTHON=/home/dominolu/dev/titan/.venv/bin/python
export PYTHONPATH="$PWD:$TITAN_STRATEGY_SDK"
/home/dominolu/.cargo/bin/cargo +1.94.1 build --locked --release -p titan-cli --all-features --bin titan > ../logs/release-build.log 2>&1 || { tail -80 ../logs/release-build.log; exit 1; }
export TITAN_TEST_BINARY="$CARGO_TARGET_DIR/release/titan"
"$TITAN_TEST_BINARY" strategy test strategies/event_counter.py --update-lock --json > ../logs/release-inline.json
"$TITAN_STRATEGY_PYTHON" -m unittest discover -s python/titan-strategy-sdk/tests -v > ../logs/release-sdk-test.log 2>&1 || { tail -90 ../logs/release-sdk-test.log; exit 1; }
"$TITAN_TEST_BINARY" strategy discover --json > ../logs/discover.json
cd ../hft_quant
"$TITAN_TEST_BINARY" strategy build strategies/orderflow_events.py --runtime configs/titan/binance-readonly.toml --strategy-key orderflow_events --update-lock --json > ../logs/release-orderflow-build.json
release_dir=$(python3 -c 'import json;print(json.load(open("../logs/release-orderflow-build.json"))["release"])')
TITAN_STRATEGY_PYTHON=/must-not-launch-python "$TITAN_TEST_BINARY" strategy verify-release "$release_dir" --json > ../logs/release-verify.json
"$TITAN_TEST_BINARY" strategy publish "$release_dir" --directory ../published --json > ../logs/release-publish.json
"$TITAN_TEST_BINARY" strategy publish "$release_dir" --directory ../published --json > ../logs/release-publish-repeat.json
"$TITAN_TEST_BINARY" validate orderflow_events -e live -m tick -c "$release_dir/runtime.toml" --json > ../logs/release-runtime-validate.json
"$TITAN_TEST_BINARY" strategy show "$release_dir/artifacts/strategy.titan" --json > ../logs/release-orderflow-manifest.json
cd ../src
python3 - <<'PY'
from pathlib import Path
import json,hashlib,platform,re,subprocess,tarfile
import sys
sys.path.insert(0,'/home/dominolu/dev/titan/.venv/lib/python3.11/site-packages')
files=json.loads(Path('../logs/source-inputs-before.json').read_text());bad=[]
for name,wanted in files.items():
 p=Path(name)
 if not p.is_file() or hashlib.sha256(p.read_bytes()).hexdigest()!=wanted:bad.append(name)
if bad:raise SystemExit('source inputs changed during verification: '+str(bad))
Path('../logs/source-inputs-after.json').write_text(json.dumps(files,indent=2,sort_keys=True))
results=[]
for name in ('rust-workspace-test.log','doctest.log'):
 text=Path('../logs',name).read_text()
 rows=re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored',text)
 results.append({'log':name,'binaries':len(rows),'passed':sum(int(r[0]) for r in rows),'failed':sum(int(r[1]) for r in rows),'ignored':sum(int(r[2]) for r in rows)})
release=json.loads(Path('../logs/release-orderflow-build.json').read_text());release_dir=Path(release['release'])
summary={'base_revision':'1a7cc720','source_files':len(files),'rust':results,'release':release,
 'release_verify':json.loads(Path('../logs/release-verify.json').read_text()),
 'runtime_validate':json.loads(Path('../logs/release-runtime-validate.json').read_text()),
 'cargo_lock_sha256':hashlib.sha256(Path('Cargo.lock').read_bytes()).hexdigest(),
 'binary_sha256':hashlib.sha256((release_dir/'titan').read_bytes()).hexdigest(),
 'bundle_sha256':hashlib.sha256((release_dir/'artifacts/strategy.titan').read_bytes()).hexdigest(),
 'source_consistent':True,'live_started':False}
Path('../logs/summary.json').write_text(json.dumps(summary,indent=2,sort_keys=True))
with tarfile.open('../verified-release.tgz','w:gz') as archive:archive.add(release_dir,arcname=release_dir.name)
print(json.dumps(summary,indent=2),flush=True)
PY
"$TITAN_STRATEGY_PYTHON" - <<'PY' > ../logs/toolchain.json
import json,platform,subprocess,numpy,numba,llvmlite
print(json.dumps({'python':platform.python_version(),'numpy':numpy.__version__,'numba':numba.__version__,'llvmlite':llvmlite.__version__,'rustc':subprocess.check_output(['/home/dominolu/.rustup/toolchains/1.94.1-x86_64-unknown-linux-gnu/bin/rustc','--version'],text=True).strip(),'cargo':subprocess.check_output(['/home/dominolu/.cargo/bin/cargo','+1.94.1','--version'],text=True).strip()},indent=2))
PY
printf '0\n' > ../logs/release-pipeline.exit
