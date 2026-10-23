#!/usr/bin/env python3
"""M3 finite acceptance on the explicitly authorized live host, using a server-built release.

No credentials or account balances leave its private directory. Trading is performed only by E,
through the same signed deployment and bounded management lifecycle used by the console.
"""
import argparse, hashlib, hmac, http.cookiejar, json, os, secrets, signal, socket, sqlite3, subprocess, sys, time
from decimal import Decimal
from pathlib import Path
from urllib import request, error, parse
SYMBOL = "GATE_FUTURE_BTC_USDT"

def private(path, value):
    with os.fdopen(os.open(path, os.O_WRONLY|os.O_CREAT|os.O_EXCL, 0o600), "w") as stream: stream.write(value)
def inline(v):
    if isinstance(v, dict): return "{"+",".join(json.dumps(k)+"="+inline(x) for k,x in v.items())+"}"
    if isinstance(v, list): return "["+",".join(map(inline,v))+"]"
    return json.dumps(v)
def toml(v): return "\n".join(json.dumps(k)+"="+inline(x) for k,x in v.items())+"\n"
def unit(c,s=0): return {"coefficient":c,"scale":s}
def port():
    with socket.socket() as sock: sock.bind(("127.0.0.1",0)); return sock.getsockname()[1]
def main():
    p=argparse.ArgumentParser(); p.add_argument("--root",type=Path,required=True);p.add_argument("--release",type=Path,required=True);p.add_argument("--expected-sha256",required=True);a=p.parse_args()
    assert sys.platform=="linux" and Path("/data/gate_arb").is_dir() and socket.gethostname()=="VM-0-33-ubuntu", "authorized live host required"
    assert a.root.is_absolute() and a.release.is_absolute()
    binary=a.release/"bin/titan"; source=a.release/"artifact-source"; manifest=json.loads((source/"manifest.json").read_text());artifact=manifest["artifact"]
    assert hashlib.file_digest(binary.open("rb"),"sha256").hexdigest()==a.expected_sha256
    blob=source/"operations"/artifact["body"]["operation_id"]/"strategy.titan"
    assert hashlib.file_digest(blob.open("rb"),"sha256").hexdigest()==manifest["blob_sha256"]
    a.root.mkdir(mode=0o700);secret_root=a.root/"secrets";secret_root.mkdir(mode=0o700)
    os.chdir("/data/gate_arb");sys.path.insert(0,"/data/gate_arb")
    from app.core.config import settings
    from app.services.account_credentials import AccountCredentialService
    from sqlalchemy.engine import make_url
    database=Path(make_url(settings.resolved_database_url).database).resolve()
    with sqlite3.connect("file:"+parse.quote(str(database))+"?mode=ro",uri=True) as db:
        db.row_factory=sqlite3.Row;row=db.execute("SELECT * FROM exchange_accounts WHERE id=1").fetchone()
        assert row and row["name"]=="gate" and row["account_type"]=="crossex" and row["exchange"]=="GATE" and row["enabled"]
        assert not db.execute("SELECT id FROM contract_grid_strategies WHERE account_id=1 AND symbol LIKE '%BTC%' AND status IN ('running','starting','stopping','paused')").fetchall(),"legacy BTC controller exists"
    credentials=AccountCredentialService().decrypt(row["credentials_encrypted"]);key,secret=credentials["api_key"],credentials["api_secret"]
    private(secret_root/"gate.toml",'api_key='+json.dumps(key)+'\nsecret='+json.dumps(secret)+'\n')
    def venue(path,query=(),authenticated=True):
        assert path in ("/crossex/rule/symbols","/crossex/accounts","/crossex/positions","/crossex/open_orders","/futures/usdt/order_book")
        encoded=parse.urlencode(query);headers={"Accept":"application/json"}
        if authenticated:
            stamp=str(int(time.time()));payload="\n".join(("GET","/api/v4"+path,encoded,hashlib.sha512(b"").hexdigest(),stamp));headers.update(KEY=key,Timestamp=stamp,SIGN=hmac.new(secret.encode(),payload.encode(),hashlib.sha512).hexdigest())
        with request.urlopen(request.Request("https://api.gateio.ws/api/v4"+path+("?"+encoded if encoded else ""),headers=headers),timeout=12) as response:return json.load(response)
    def facts():
        orders=[o for o in venue("/crossex/open_orders",(("symbol",SYMBOL),)) if o.get("symbol")==SYMBOL]
        positions=[o for o in venue("/crossex/positions") if o.get("symbol")==SYMBOL and Decimal(o["position_qty"])!=0]
        return orders,positions
    rule=next(r for r in venue("/crossex/rule/symbols",authenticated=False) if r["symbol"]==SYMBOL)
    assert rule["state"]=="live" and Decimal(rule["tick_size"])==Decimal("0.1") and Decimal(rule["lot_size"])==Decimal("0.0001") and Decimal(rule["min_size"])==Decimal("0.0001")
    account=venue("/crossex/accounts");assert account["account_mode"]=="CROSS_EXCHANGE" and account["position_mode"]=="SINGLE"
    assert facts()==([],[]),"preexisting BTC risk prevents acceptance"
    book=venue("/futures/usdt/order_book",(("contract","BTC_USDT"),("limit",1)),False);mid=(Decimal(book["bids"][0]["p"])+Decimal(book["asks"][0]["p"]))/2
    params=manifest["parameters"];assert params["order_qty_lots"]==1 and params["stop_action"]=="flatten" and params["quote_units_per_tick_lot"]==1000
    assert Decimal(params["lower_price_ticks"])/10<mid<Decimal(params["upper_price_ticks"])/10 and Decimal(300)/mid<Decimal("0.005")
    duration=lambda secs,nanos=0:{"secs":secs,"nanos":nanos};instrument={"native_symbol":SYMBOL,"asset_id":1,"price_tick":unit(1,1),"quantity_lot":unit(1,4)}
    runtime={"schema_version":1,"managed_by_console":True,"account_service":{"secret_root":str(secret_root),"startup_timeout_ms":30000},"strategy_service":{"trusted_ed25519_keys":{"acceptance":manifest["public_key"]}},"market_sources":[{"source_key":"gate-market","connector_type":"gate-crossex","connector_config":"","instruments":[instrument],"enabled":True,"definition_version":1}],"accounts":[{"account_key":"gate","account_id":1,"connector_type":"gate-crossex-account","credential_ref":"secret://file/gate.toml","connector_config":"","ownership":"ObserveAll","shutdown_order_policy":"LeaveOpen","instruments":[{**instrument,"contract_multiplier":unit(1)}],"currencies":[{"native_currency":"USDT","currency_id":1,"amount_unit":unit(1,8)}],"enabled":True,"definition_version":1}],"strategies":[{"strategy_key":"grid","strategy_id":int(time.time()),"package":{"loader_type":"native-v13","uri":"file://unused.titan","expected_digest":[1]*32},"markets":[{"local_market_no":0,"local_asset_no":0,"source_key":"gate-market","asset_id":1,"data_mode":"tick"}],"accounts":[{"local_account_no":0,"account_key":"gate","tradable_assets":[{"local_asset_no":0,"asset_id":1}]}],"risk_scope":"controlled-100-usdt","runtime":{"async_lane_capacity":256,"critical_reserve":32,"reliable_pending_capacity":32,"worker_policy":"park","timer_capacity":16,"max_commands_per_callback":64,"checkpoint_interval":duration(1),"timer_interval":duration(0,100000000),"max_handler_duration":duration(1),"startup_timeout":duration(30),"stop_timeout":duration(10)},"recovery":"fresh","enabled":True,"definition_version":1}]}
    private(a.root/"runtime.toml",toml(runtime));mp,np,bp=port(),port(),port();origin=f"http://127.0.0.1:{mp}";token,password=secrets.token_hex(32),secrets.token_hex(24);env={**os.environ,"TITAN_CONSOLE_LIVE_TOKEN":token,"RUST_LOG":"warn"}
    hashed=subprocess.check_output([str(binary),"console","password-hash"],input=password.encode(),env=env).decode().strip()
    remote=lambda port,live:{"endpoint":f"http://127.0.0.1:{port}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","environments":["live" if live else "backtest"],"live_control":live}
    binding={"account":"gate","account_identity":"crossex-gate-account-1","instrument":SYMBOL,"runtime_config":str(a.root/"runtime.toml"),"strategy_key":"grid","max_exposure_quote":"100","max_price_deviation_bps":50,"max_duration_seconds":120}
    node={"id":"live","listen":f"127.0.0.1:{np}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","work_root":str(a.root/"node"),"titan_program":str(binary),"environments":["live"],"live_control":True,"bindings":{"gate-btc":binding},"artifact_sources":{"build":remote(bp,False)}}
    build={"id":"build","listen":f"127.0.0.1:{bp}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","work_root":str(source),"titan_program":str(binary),"environments":["backtest"]}
    users=[{"username":name,"password_hash":hashed,"role":role,"nodes":["build","live"],"accounts":["gate"],"environments":["backtest","live"]} for name,role in (("operator","operator"),("reader","reader"))]
    manager={"listen":f"127.0.0.1:{mp}","public_origin":origin,"database":str(a.root/"manager.sqlite3"),"web_root":str(a.release/"web"),"secure_cookie":False,"nodes":{"build":remote(bp,False),"live":remote(np,True)},"users":users}
    for name,value in (("live",{"node":node}),("build",{"node":build}),("manager",{"manager":manager})):private(a.root/(name+".toml"),toml({"schema_version":1,**value}))
    processes=[];csrf=None;opener=request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()));checks=[];run=None;active=None
    def start(name,kind):
        log=(a.root/(name+".log")).open("ab");proc=subprocess.Popen([str(binary),"console",kind,"--config",str(a.root/(name+".toml"))],env=env,stdin=subprocess.DEVNULL,stdout=log,stderr=log);processes.append(proc);return proc
    def stop(proc):
        if proc.poll() is None:
            proc.send_signal(signal.SIGINT)
            try:proc.wait(timeout=8)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
    def call(path,data=None):
        headers={"Origin":origin,"Content-Type":"application/json"}
        if csrf:headers["X-CSRF-Token"]=csrf
        with opener.open(request.Request(origin+path,data=None if data is None else json.dumps(data).encode(),headers=headers),timeout=10) as response:return json.load(response)
    def wait(fn,timeout=35):
        deadline=time.monotonic()+timeout
        while time.monotonic()<deadline:
            try:
                value=fn()
                if value:return value
            except (error.URLError,TimeoutError,ConnectionError):pass
            time.sleep(.2)
        raise RuntimeError("finite live acceptance timed out; inspect private evidence")
    def submit(action):return call("/api/v1/operations",{"idempotency_key":secrets.token_hex(16),"node_id":"live","action":action})
    def completed(op):
        op=wait(lambda: (v if v["state"] in ("succeeded","failed","cancelled") else None) if (v:=call("/api/v1/operations/"+op["id"])) else None)
        assert op["state"]=="succeeded",op.get("error");return op
    def resources(kind):return [r for r in call("/api/v1/snapshot")["resources"] if r["kind"]==kind]
    def resource(op,kind):return next(r for r in op["result"]["resources"] if r["kind"]==kind)
    def current():return next((r for r in resources("run") if active and r["id"]==active["id"]),None)
    def control(kind):return completed(submit({"kind":kind,"run_id":active["id"],"generation":active["body"]["generation"]}))
    def passed(name):checks.append(name);print(name+": passed",flush=True)
    try:
        buildp=start("build","node");nodep=start("live","node");managerp=start("manager","manager");session=wait(lambda:call("/api/v1/session",{"username":"operator","password":password}));csrf=session["csrf_token"]
        wait(lambda:resources("artifact") and resources("binding"));deployment=resource(completed(submit({"kind":"deploy","artifact_id":artifact["id"],"binding_id":"gate-btc"})),"deployment");passed("signed-remote-deployment")
        started=time.monotonic();run=resource(completed(submit({"kind":"start","deployment_id":deployment["id"]})),"run");active=run;passed("start-engine-running")
        wait(lambda:facts()[0],timeout=15);passed("minimum-post-only-order-confirmed")
        control("pause");wait(lambda:facts()==([],[]),timeout=15);time.sleep(1.5);passed("pause-private-terminal-and-flat")
        stop(managerp);stop(nodep);nodep=start("live","node");managerp=start("manager","manager")
        adopted=wait(lambda:(r if r["body"]["available"] else None) if (r:=current()) else None,timeout=15)
        assert adopted["body"]["observation"]["pid"]==run["body"]["observation"]["pid"];passed("A-N-restart-same-worker")
        control("resume");wait(lambda:facts()[0],timeout=15);control("pause");wait(lambda:facts()==([],[]),timeout=15);time.sleep(1.5);passed("same-generation-resume-and-confirmed-cancel")
        observation=current()["body"]["observation"];pid=int(observation["pid"]);assert int(Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()[19])==int(observation["start_time"]);os.kill(pid,signal.SIGKILL)
        old=wait(lambda:(r if r["body"]["checkpoint_id"] and r["body"]["process_state"]=="UNKNOWN" else None) if (r:=current()) else None,timeout=15);passed("controlled-flat-crash-preserves-checkpoint")
        active=resource(completed(submit({"kind":"recover","run_id":old["id"],"generation":old["body"]["generation"],"checkpoint_id":old["body"]["checkpoint_id"]})),"run")
        assert int(active["body"]["generation"])>int(run["body"]["generation"]) and active["body"]["expires_at_ns"]==run["body"]["expires_at_ns"];passed("pinned-recovery-original-budget")
        control("stop");wait(lambda:(r if r["body"]["release_proof"]["safe"] else None) if (r:=current()) else None,timeout=20);assert facts()==([],[]);passed("engine-stop-proof-zero-residual-risk")
        with sqlite3.connect(a.root/"manager.sqlite3") as db:assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]==0
        history=call(f"/api/v1/resources/{run['id']}/history?kind=order&generation={run['body']['generation']}");assert history["items"]
        elapsed=round(time.monotonic()-started,3);assert elapsed<=120
        result={"passed":True,"checks":checks,"host":"43.130.234.54","account":"gate","account_id":1,"instrument":SYMBOL,"binary_sha256":a.expected_sha256,"artifact_sha256":manifest["blob_sha256"],"max_exposure_quote":"100","max_price_deviation_bps":50,"maximum_run_seconds":120,"minimum_order_base":"0.0001","stop_action":"flatten","elapsed_seconds":elapsed,"final_target_orders":0,"final_target_positions":[],"order_facts_recorded":len(history["items"])}
        private(a.root/"results.json",json.dumps(result,indent=2));print(json.dumps(result),flush=True)
    finally:
        # Keep A/N alive for a normal bounded Stop before shutting down management processes.
        if active:
            try:
                if current()["body"]["available"]:control("stop")
            except Exception:pass
        for proc in reversed(processes):stop(proc)
        # Exact owned identity only; E retains its original 120 second finite budget after SIGTERM.
        registry=a.root/"node/runs.sqlite3"
        if registry.exists():
            with sqlite3.connect(registry) as db:
                rows=db.execute("SELECT pid,process_start_time FROM runs WHERE strategy_id='management-runtime'").fetchall()
            for pid,stamp in rows:
                stat=Path(f"/proc/{pid}/stat")
                if stat.exists() and int(stat.read_text().rsplit(")",1)[1].split()[19])==int(stamp):os.kill(pid,signal.SIGTERM)
        deadline=time.monotonic()+20
        while True:
            orders,positions=facts()
            if not orders and not positions or time.monotonic()>=deadline:break
            time.sleep(.5)
        private(a.root/"postflight.json",json.dumps({"target_order_count":len(orders),"target_positions":[{k:v.get(k) for k in ("symbol","position_side","position_qty")} for v in positions]},indent=2))
if __name__=="__main__":main()
