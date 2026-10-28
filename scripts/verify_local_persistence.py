#!/usr/bin/env python3
"""M2 build-server proof using real engine outputs and the loopback venue. No real accounts."""
from __future__ import annotations
import argparse, hashlib, json, os, shutil, sqlite3, subprocess, time
from pathlib import Path
from titan_reporting import load_bundle, BundleError

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ("binary","history","matrix","loopback","output","smoke-strategy"):
        parser.add_argument("--"+name,type=Path,required=True)
    args=parser.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname","-I"],text=True).split()
    args.output.mkdir(parents=True,exist_ok=False)
    root=args.output.resolve();checks=[];commands=[]
    def passed(name,**evidence):
        checks.append({"name":name,"passed":True,**evidence});print(name+": passed",flush=True)
    def execute(command,*,success=True,env=None,timeout=600,measurement=None):
        invoked=list(map(str,command))
        if measurement:invoked=["/usr/bin/time","-v","-o",str(root/(measurement+".time.txt")),*invoked]
        started=time.monotonic();p=subprocess.run(invoked,capture_output=True,text=True,env=env,timeout=timeout)
        commands.append({"argv":list(map(str,command)),"exit_code":p.returncode,"elapsed_seconds":time.monotonic()-started})
        (root/"commands.json").write_text(json.dumps(commands,indent=2))
        (root/f"command-{len(commands):03d}.log").write_text(p.stdout+"\n"+p.stderr)
        if success and p.returncode:raise AssertionError((command,p.returncode,p.stdout[-3000:],p.stderr[-3000:]))
        if not success:assert p.returncode!=0,(command,p.stdout)
        return p
    def storage(*command,success=True):
        p=execute([args.binary,"storage",*command],success=success)
        return json.loads(p.stdout) if success else None
    def digest(path):
        with path.open("rb") as stream:return hashlib.file_digest(stream,"sha256").hexdigest()
    def facts_digest(database):
        h=hashlib.sha256();count=0
        with sqlite3.connect(database) as db:
            for (blob,expected) in db.execute("SELECT payload,sha256 FROM archive_records WHERE replay=0 ORDER BY id"):
                assert hashlib.sha256(blob).hexdigest()==expected
                h.update(len(blob).to_bytes(8,"big"));h.update(blob);count+=1
        return count,h.hexdigest()
    def verify_bundle(path):
        manifest=json.loads((path/"manifest.json").read_text());names=set()
        for entry in manifest["files"]:
            name=entry["path"];assert name not in names;names.add(name)
            file=(path/name).resolve();assert file.is_relative_to(path.resolve()) and file.is_file()
            assert file.stat().st_size==entry["bytes"] and digest(file)==entry["sha256"]
        assert {"result.json","history.db","facts.parquet"}<=names
        count,sha=facts_digest(path/"history.db")
        parquet=storage("inspect-parquet","--input",path/"facts.parquet")
        assert int(parquet["records"])==count and parquet["ordered_payload_sha256"]==sha
        return manifest,count,sha
    matrix=json.loads((args.matrix/"results.json").read_text());assert matrix["passed"]
    loopback=json.loads((args.loopback/"results.json").read_text());assert loopback["passed"] and "no real account" in loopback["source"]
    assert json.loads((args.loopback/"archive-history.json").read_text())["passed"]
    results=[c for c in matrix["checks"] if c["name"].startswith("backtest-") and "operation_id" in c]
    assert len(results)==18, len(results)
    for item in results:
        bundle=args.matrix/"node/operations"/item["operation_id"]/"history"
        manifest,count,sha=verify_bundle(bundle)
        assert manifest["run_id"]==item["operation_id"]
        with sqlite3.connect(bundle/"history.db") as db:
            fills=db.execute("SELECT count(*) FROM archive_records WHERE kind='fill' AND replay=0").fetchone()[0]
            assert fills==int(item["metrics"]["fills"])
            assert db.execute("SELECT count(*) FROM archive_sources WHERE clean<>1 OR complete<>1").fetchone()[0]==0
        passed("full-facts-match-engine-"+item["name"],records=count,fills=fills,ordered_payload_sha256=sha)
    base=next(c for c in results if c["name"]=="backtest-sideways-neutral-base")
    operation=args.matrix/"node/operations"/base["operation_id"]
    request=json.loads((operation/"request.json").read_text())
    version=next(r for r in request["resources"] if r["kind"]=="strategy_version")
    dataset=next(r for r in request["resources"] if r["kind"]=="dataset")
    version_root=args.matrix/"node/operations"/version["body"]["operation_id"]
    prepared=args.matrix/"node/operations"/dataset["body"]["prepared_operation_id"]
    original=operation/"history"
    parquet_replay=root/"parquet-replay"
    before=digest(version_root/"strategy.titan")
    execute([args.history,"run","--strategy-file",version_root/"strategy.titan","--layout",version_root/"layout.json","--data",prepared/"prepared.parquet","--output",parquet_replay,"--run-id","m2-parquet-equivalence","--storage-root",root/"parquet-storage"],measurement="parquet-replay")
    expected=json.loads((original/"result.json").read_text());actual=json.loads((parquet_replay/"result.json").read_text())
    # Only the frozen physical input location differs. Engine counters, money, model and times remain exact.
    assert {k:v for k,v in expected.items() if k!="data"}=={k:v for k,v in actual.items() if k!="data"}
    assert digest(version_root/"strategy.titan")==before
    with sqlite3.connect(original/"history.db") as a,sqlite3.connect(parquet_replay/"history.db") as b:
        query="SELECT payload FROM archive_records WHERE kind NOT IN ('run_started','run_finished') ORDER BY id"
        left=a.execute(query);right=b.execute(query);compared=0
        while True:
            x,y=left.fetchone(),right.fetchone();assert (x is None)==(y is None)
            if x is None:break
            x,y=json.loads(x[0]),json.loads(y[0])
            for value in (x,y):value.pop("recording_id");value["scope"].pop("run_id")
            assert x==y;compared+=1
    verify_bundle(parquet_replay)
    passed("same-native-file-binary-and-parquet-inputs-have-identical-engine-and-fact-results",strategy_sha256=before,records_compared=compared,binary_input_sha256=digest(prepared/"prepared.bin"),parquet_input_sha256=digest(prepared/"prepared.parquet"))
    metrics={line.strip().split(": ",1)[0]:line.strip().split(": ",1)[1] for line in (root/"parquet-replay.time.txt").read_text().splitlines() if ": " in line}
    passed("bounded-full-audit-replay-resource-measurement",peak_rss_kib=int(metrics["Maximum resident set size (kbytes)"]),user_seconds=metrics["User time (seconds)"],system_seconds=metrics["System time (seconds)"],input_bytes=(prepared/"prepared.parquet").stat().st_size,history_bytes=(parquet_replay/"history.db").stat().st_size,parquet_bytes=(parquet_replay/"facts.parquet").stat().st_size,sql_chunk_records=64,parquet_chunk_records=64,fact_max_bytes=1048576)
    storage_root=args.matrix/"node/storage"
    reference=storage("register-bundle","--bundle",original,"--storage-root",storage_root)["reference"]
    assert storage("register-bundle","--bundle",original,"--storage-root",storage_root)["reference"]==reference
    storage("register-publication","--storage-root",storage_root,"--reference",json.dumps(dataset["body"]["storage_manifest"]))
    materialized=root/"materialized"
    storage("materialize-bundle","--storage-root",storage_root,"--reference",json.dumps(reference),"--output",materialized)
    storage("materialize-bundle","--storage-root",storage_root,"--reference",json.dumps(reference),"--output",materialized,success=False)
    for entry in json.loads((original/"manifest.json").read_text())["files"]:
        assert digest(original/entry["path"])==digest(materialized/entry["path"])
    assert (materialized/"manifest.json").read_bytes()==(original/"manifest.json").read_bytes()
    load_bundle(materialized)
    passed("object-publication-registration-retry-and-byte-identical-v1-materialization")
    output=root/"export.parquet"
    storage("export-facts","--database",original/"history.db","--account","simulation","--run-id",base["operation_id"],"--output",output)
    full=storage("inspect-parquet","--input",output);count,sha=facts_digest(original/"history.db")
    assert int(full["records"])==count and full["ordered_payload_sha256"]==sha
    assert json.loads(output.with_suffix(".manifest.json").read_text())["object"]["sha256"]==digest(output)
    storage("export-facts","--database",original/"history.db","--account","simulation","--run-id",base["operation_id"],"--output",output,success=False)
    passed("fixed-bound-streamed-parquet-export-preserves-all-original-envelopes",records=count)
    isolated=dict(os.environ,TITAN_HOME=str(root/"cli-home"))
    trace=root/"invalid-trace.json";trace.write_text(json.dumps({"schema_version":1,"initial":{"now_ns":1},"events":[{"kind":"unsupported-fact","now_ns":2}]}))
    config=root/"backtest.toml";config.write_text('schema_version=1\n[backtest]\ndata="invalid-trace.json"\nallow_unsigned_strategy=true\n')
    execute([args.binary,"run",args.smoke_strategy,"-e","backtest","-m","tick","-c",config,"--json"],success=False,env=isolated)
    with sqlite3.connect(root/"cli-home/runs.sqlite3") as db:failed=db.execute("SELECT state,result_path FROM runs ORDER BY rowid DESC LIMIT 1").fetchone()
    assert failed[0]=="FAILED";failed_bundle=Path(failed[1]).parent;verify_bundle(failed_bundle)
    failed_result=json.loads((failed_bundle/"result.json").read_text());assert failed_result["status"]=="FAILED" and failed_result["final_state_available"] is False and failed_result["persistence"]["status"]=="COMMITTED"
    passed("engine-failure-persists-explicit-diagnostics-without-inventing-final-state")
    corrupt=root/"corrupt";shutil.copytree(materialized,corrupt);(corrupt/"result.json").write_text("{}")
    try:load_bundle(corrupt);raise AssertionError("tampered result accepted")
    except BundleError:pass
    (corrupt/"manifest.json").unlink()
    try:load_bundle(corrupt);raise AssertionError("uncommitted result accepted")
    except BundleError:pass
    passed("report-reader-rejects-tampered-or-uncommitted-materialized-bundle")
    (root/"commands.json").write_text(json.dumps(commands,indent=2))
    (root/"results.json").write_text(json.dumps({"passed":True,"real_account_access":False,"checks":checks},indent=2))

if __name__=="__main__":main()
