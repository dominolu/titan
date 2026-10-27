"""Fault injection into this task's private offline fixture, on 192.168.3.88 only."""
from __future__ import annotations
import concurrent.futures, hashlib, http.cookiejar, json, os, secrets, signal, socket, sqlite3, subprocess, sys, time, tomllib
from pathlib import Path
from urllib import request, error

root = Path(sys.argv[1]).resolve()
config = tomllib.loads((root / "console.toml").read_text())
origin = config["manager"]["public_origin"]
assert origin.startswith("http://127.0.0.1:")
client = request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
def call(path, data=None, method=None):
    headers = {"Origin": origin, "Content-Type": "application/json"}
    if "csrf" in globals(): headers["X-CSRF-Token"] = csrf
    with client.open(request.Request(origin+path, data=None if data is None else json.dumps(data).encode(), headers=headers, method=method),timeout=15) as response:
        content=response.read();return json.loads(content) if content else None
session=call("/api/v2/session",{"username":"operator","password":os.environ["TITAN_BROWSER_PASSWORD"]})
csrf=session["csrf_token"]
def submit(action):
    return call("/api/v2/operations",{"idempotency_key":secrets.token_hex(16),"node_id":"build","action":action})
def wait_terminal(op):
    deadline=time.monotonic()+900
    while time.monotonic()<deadline:
        item=call("/api/v2/operations/"+op["id"])
        if item["state"] in ("succeeded","failed","cancelled"):return item
        time.sleep(.3)
    raise AssertionError("fault scenario did not reach a confirmed terminal state")
def completed(action):
    item=wait_terminal(submit(action));assert item["state"]=="succeeded",item;return item
def resource(operation,kind):return next(row for row in operation["result"]["resources"] if row["kind"]==kind)
checks=[]
snapshot=call("/api/v2/snapshot?limit=200")
strategy_version=next(row for row in snapshot["resources"] if row["kind"]=="strategy_version")
dataset=next(row for row in snapshot["resources"] if row["kind"]=="dataset")
unproven=json.loads(json.dumps(dataset));unproven["body"]["prepared_operation_id"]=None
action={"kind":"backtest","strategy_version_id":strategy_version["id"],"dataset_id":dataset["body"]["dataset_id"],"stress":False}
material={"node_id":"build","action":action,"resources":[strategy_version,unproven]}
fixed={"api_version":1,"operation_id":secrets.token_hex(32),"action":action,"resources":material["resources"],"input_digest":hashlib.sha256(json.dumps(material,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()}
def build_node(path,data=None):
    headers={"Authorization":"Bearer "+os.environ[config["node"]["token_env"]],"Content-Type":"application/json"}
    with request.urlopen(request.Request(config["manager"]["nodes"]["build"]["endpoint"]+path,data=None if data is None else json.dumps(data).encode(),headers=headers),timeout=15) as response:return json.load(response)
build_node("/v1/operations",fixed)
for _ in range(100):
    receipt=build_node("/v1/operations/"+fixed["operation_id"])
    if receipt["state"] in ("failed","succeeded"):break
    time.sleep(.1)
assert receipt["state"]=="failed" and "no verified preparation operation" in receipt["error"],receipt
checks.append("unproven-prepared-cache-rejected")
strategy_path=Path(config["node"]["work_root"])/"operations"/strategy_version["body"]["operation_id"]/"strategy.titan"
original=strategy_path.read_bytes()
try:
    strategy_path.write_bytes(original+b"tamper")
    rejected=wait_terminal(submit({"kind":"backtest","strategy_version_id":strategy_version["id"],"dataset_id":"sideways","stress":False}))
    assert rejected["state"]=="failed" and "compiled_strategy content changed" in rejected["error"],rejected
finally:strategy_path.write_bytes(original)
assert hashlib.sha256(strategy_path.read_bytes()).hexdigest()==strategy_version["body"]["strategy_file_sha256"]
checks.append("tampered-strategy_version-rejected-valid-file-restored")
op=completed({"kind":"describe","parameter_preset_id":"sideways-neutral","parameters":json.loads(Path(config["node"]["parameter_presets"]["sideways-neutral"]["parameters"]).read_text())})
for name in ("request.json","../console.toml"):
    try:call("/api/v2/files/"+op["id"]+"/"+name);raise AssertionError("private file was downloadable")
    except error.HTTPError as failure:assert failure.code>=400
link=Path(config["node"]["work_root"])/"operations"/op["id"]/"result.json"
try:
    link.symlink_to(root/"console.toml")
    try:call("/api/v2/files/"+op["id"]+"/result.json");raise AssertionError("symlink escaped the published operation")
    except error.HTTPError as failure:assert failure.code>=400
finally:link.unlink(missing_ok=True)
checks.append("private-and-symlink-files-rejected")
# Retained inventory is a valid engine stop, but cannot count as a successful historical grid replay.
parameters=json.loads(Path(config["node"]["parameter_presets"]["sideways-long_bias"]["parameters"]).read_text())
parameters["stop_action"]="cancel"
validation=completed({"kind":"describe","parameter_preset_id":"sideways-long_bias","parameters":parameters})
version=call("/api/v2/parameter-versions",{"node_id":"build","parameter_preset_id":"sideways-long_bias","parameters":parameters,"expected_revision":None,"freeze_operation_id":validation["id"]})
built=completed({"kind":"compile","parameter_version_id":version["id"]})
retained=wait_terminal(submit({"kind":"backtest","strategy_version_id":resource(built,"strategy_version")["id"],"dataset_id":"sideways","stress":False}))
record=resource(retained,"backtest")
assert retained["state"]=="failed" and record["body"]["business_success"] is False and float(record["body"]["metrics"]["final_position_btc"])!=0,retained
checks.append("retained-inventory-keeps-metrics-with-failed-operation")
# A separate node owns a fresh directory, with a real one-second compiler deadline.
fault_root=root/("timeout-node-"+secrets.token_hex(8));fault_root.mkdir()
with socket.socket() as stream:stream.bind(("127.0.0.1",0));port=stream.getsockname()[1]
node_config=root/"timeout.toml"
q=lambda value:json.dumps(str(value))
template_settings=config["node"]["parameter_presets"]["sideways-long_bias"]
research_settings=[f"{key}={q(template_settings[key])}" for key in ("strategy_id","strategy_name") if template_settings.get(key) is not None]
node_config.write_text("\n".join(["schema_version=1","[node]",'id="timeout"',f'listen="127.0.0.1:{port}"',f'token_env={q(config["node"]["token_env"])}',f'work_root={q(fault_root)}',f'titan_program={q(config["node"]["titan_program"])}','environments=["backtest"]',"task_seconds=1","[node.parameter_presets.sideways-long_bias]",*research_settings,f'source={q(config["node"]["parameter_presets"]["sideways-long_bias"]["source"])}',f'parameters={q(config["node"]["parameter_presets"]["sideways-long_bias"]["parameters"])}'])+"\n")
node_config.chmod(0o600)
log=(fault_root/"node.log").open("ab")
process=subprocess.Popen([config["node"]["titan_program"],"console","node","--config",str(node_config)],stdout=log,stderr=log)
def node_call(path,data=None):
    headers={"Authorization":"Bearer "+os.environ[config["node"]["token_env"]],"Content-Type":"application/json"}
    with request.urlopen(request.Request(f"http://127.0.0.1:{port}"+path,data=None if data is None else json.dumps(data).encode(),headers=headers),timeout=15) as response:return json.load(response)
try:
    for _ in range(100):
        try:node_call("/v1/catalog");break
        except error.URLError:time.sleep(.1)
    action={"kind":"compile","parameter_version_id":version["id"]}
    material={"node_id":"timeout","action":action,"resources":[version]}
    fixed={"api_version":1,"operation_id":secrets.token_hex(32),"action":action,"resources":[version],"input_digest":hashlib.sha256(json.dumps(material,sort_keys=True,separators=(",",":"),ensure_ascii=False).encode()).hexdigest()}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        receipts=list(pool.map(lambda _:node_call("/v1/operations",fixed),range(16)))
    assert all(row["operation_id"]==fixed["operation_id"] for row in receipts)
    for _ in range(150):
        receipt=node_call("/v1/operations/"+fixed["operation_id"])
        if receipt["state"] in ("succeeded","failed","cancelled"):break
        time.sleep(.1)
    assert receipt["state"]=="failed" and "deadline" in receipt["error"],receipt
    with sqlite3.connect(fault_root/"runs.sqlite3") as db:assert db.execute("select count(*) from runs").fetchone()[0]==1
    checks.append("concurrent-node-dedup-and-real-compiler-timeout")
finally:
    process.send_signal(signal.SIGINT);process.wait(timeout=10);log.close()
reader=call("/api/v2/session",{"username":"reader","password":os.environ["TITAN_BROWSER_PASSWORD"]})
with sqlite3.connect(config["manager"]["database"]) as db:db.execute("update sessions set expires_ns='0' where username='reader'")
try:call("/api/v2/snapshot");raise AssertionError("expired session remained valid")
except error.HTTPError as failure:assert failure.code==401
checks.append("expired-session-rejected")
(root/"faults.json").write_text(json.dumps({"passed":True,"checks":checks},indent=2))
print(json.dumps({"name":"fault-injection","passed":True,"checks":checks}),flush=True)
