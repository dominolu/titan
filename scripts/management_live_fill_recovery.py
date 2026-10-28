#!/usr/bin/env python3
"""Finite real-fill SQLite recovery and positioned Stop acceptance on the authorized live host.

No credentials or account balances leave its private directory. Trading is performed only by E,
through the same signed deployment and bounded management lifecycle used by the console.
"""
import persistence_live_preflight as admission
from live_fill_acceptance import validate_parameters, checkpoint_has_position, position_quantity, protective_close_request, unactivated_fixture, stopped_fixture, reconciled_fixture
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
    p=argparse.ArgumentParser(); p.add_argument("--root",type=Path,required=True);p.add_argument("--release",type=Path,required=True);p.add_argument("--expected-sha256",required=True);p.add_argument("--preflight",type=Path,required=True);reuse=p.add_mutually_exclusive_group();reuse.add_argument("--reuse-unactivated",action="store_true");reuse.add_argument("--reuse-stopped",action="store_true");reuse.add_argument("--reconciliation-receipt",type=Path);a=p.parse_args()
    assert sys.platform=="linux" and Path("/data/gate_arb").is_dir() and socket.gethostname()=="VM-0-33-ubuntu", "authorized live host required"
    assert a.root.is_absolute() and a.release.is_absolute()
    preflight=json.loads(a.preflight.read_text());assert preflight["passed"] and 0<=time.time()-preflight["checked_at_unix"]<=600,"fresh GET-only preflight required"
    assert all(preflight.get(name)==value for name,value in admission.POLICY.items()),"preflight policy mismatch"
    binary=a.release/"bin/titan"; source=a.release/"strategy_version-source"; manifest=json.loads((source/"manifest.json").read_text());strategy_version=manifest["strategy_version"]
    assert hashlib.file_digest(binary.open("rb"),"sha256").hexdigest()==a.expected_sha256
    blob=source/"operations"/strategy_version["body"]["operation_id"]/"strategy.titan"
    assert hashlib.file_digest(blob.open("rb"),"sha256").hexdigest()==manifest["strategy_file_sha256"]
    reusing=a.reuse_unactivated or a.reuse_stopped or bool(a.reconciliation_receipt)
    original_no=unactivated_fixture(a.root) if a.reuse_unactivated else stopped_fixture(a.root) if a.reuse_stopped else reconciled_fixture(a.root,a.reconciliation_receipt) if a.reconciliation_receipt else None
    if not reusing:a.root.mkdir(mode=0o700)
    secret_root=a.root/"secrets"
    if not secret_root.exists():secret_root.mkdir(mode=0o700)
    if reusing:
        import shutil
        history=a.root/"attempt-history"/str(time.time_ns());history.mkdir(parents=True,mode=0o700)
        for name in ("checks-progress.json","postflight.json","runtime.toml","manager.toml","live.toml","build.toml"):
            if (a.root/name).exists():shutil.copy2(a.root/name,history/name)
        for name in ("checkpoint-proof.json","recovery-proof.json","results.json","venue-fill-proof.json","audit-summary.json"):
            if (a.root/name).exists():shutil.copy2(a.root/name,history/name);(a.root/name).unlink()
        (secret_root/"gate.toml").unlink(missing_ok=True)
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
        assert path in ("/crossex/rule/symbols","/crossex/accounts","/crossex/positions","/crossex/open_orders","/futures/usdt/order_book") or path.startswith("/crossex/orders/")
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
    assert not [line for line in subprocess.check_output(["ps","-eo","comm"]).decode().splitlines() if line.strip()=="titan"],"another Titan owner exists"
    book=venue("/futures/usdt/order_book",(("contract","BTC_USDT"),("limit",1)),False);mid=(Decimal(book["bids"][0]["p"])+Decimal(book["asks"][0]["p"]))/2
    params=manifest["parameters"];validate_parameters(params)
    assert Decimal(params["lower_price_ticks"])/10<mid<Decimal(params["upper_price_ticks"])/10 and Decimal(300)/mid<Decimal("0.005")
    duration=lambda secs,nanos=0:{"secs":secs,"nanos":nanos};instrument={"native_symbol":SYMBOL,"asset_id":1,"price_tick":unit(1,1),"quantity_lot":unit(1,4)}
    runtime={"schema_version":1,"managed_by_console":True,"account_service":{"secret_root":str(secret_root),"startup_timeout_ms":30000},"strategy_service":{"trusted_ed25519_keys":{"acceptance":manifest["public_key"]}},"market_sources":[{"source_key":"gate-market","connector_type":"gate-crossex","connector_config":"","instruments":[instrument],"enabled":True,"definition_version":1}],"accounts":[{"account_key":"gate","account_id":1,"connector_type":"gate-crossex-account","credential_ref":"secret://file/gate.toml","connector_config":"","ownership":"ObserveAll","shutdown_order_policy":"LeaveOpen","instruments":[{**instrument,"contract_multiplier":unit(1)}],"currencies":[{"native_currency":"USDT","currency_id":1,"amount_unit":unit(1,8)}],"enabled":True,"definition_version":1}],"strategies":[{"strategy_key":"grid","runtime_strategy_no":original_no if original_no is not None else int(time.time()),"strategy_file":{"loader_type":"native-v13","uri":"file://unused.titan","expected_digest":[1]*32},"markets":[{"local_market_no":0,"local_asset_no":0,"source_key":"gate-market","asset_id":1,"data_mode":"tick"}],"accounts":[{"local_account_no":0,"account_key":"gate","tradable_assets":[{"local_asset_no":0,"asset_id":1}]}],"risk_scope":"controlled-100-usdt","runtime":{"async_lane_capacity":256,"critical_reserve":32,"reliable_pending_capacity":32,"worker_policy":"park","timer_capacity":16,"max_commands_per_callback":64,"checkpoint_interval":duration(1),"timer_interval":duration(0,100000000),"max_handler_duration":duration(1),"startup_timeout":duration(30),"stop_timeout":duration(10)},"recovery":"fresh","enabled":True,"run_config_revision":1}]}
    runtime["accounts"][0]["instruments"][0]["linear_quote_currency"]=1
    runtime["strategies"][0]["runtime"]["checkpoint_interval"]=duration(60)
    if reusing:
        (a.root/"runtime.toml").unlink()
    private(a.root/"runtime.toml",toml(runtime));mp,np,bp=port(),port(),port();origin=f"http://127.0.0.1:{mp}";token,password=secrets.token_hex(32),secrets.token_hex(24);env={**os.environ,"TITAN_CONSOLE_LIVE_TOKEN":token,"RUST_LOG":"warn"}
    hashed=subprocess.check_output([str(binary),"console","password-hash"],input=password.encode(),env=env).decode().strip()
    remote=lambda port,live:{"endpoint":f"http://127.0.0.1:{port}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","environments":["live" if live else "backtest"],"live_control":live}
    binding={"account":"gate","account_identity":"crossex-gate-account-1","instrument":SYMBOL,"runtime_config":str(a.root/"runtime.toml"),"strategy_key":"grid","max_exposure_quote":"100","max_price_deviation_bps":50,"max_duration_seconds":120}
    node={"id":"live","listen":f"127.0.0.1:{np}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","work_root":str(a.root/"node"),"titan_program":str(binary),"environments":["live"],"live_control":True,"bindings":{"gate-btc":binding},"strategy_sources":{"build":remote(bp,False)}}
    build={"id":"build","listen":f"127.0.0.1:{bp}","token_env":"TITAN_CONSOLE_LIVE_TOKEN","work_root":str(source),"titan_program":str(binary),"environments":["backtest"]}
    users=[{"username":name,"password_hash":hashed,"role":role,"nodes":["build","live"],"accounts":["gate"],"environments":["backtest","live"]} for name,role in (("operator","operator"),("reader","reader"))]
    manager={"listen":f"127.0.0.1:{mp}","public_origin":origin,"database":str(a.root/"manager.sqlite3"),"web_root":str(a.release/"web"),"secure_cookie":False,"nodes":{"build":remote(bp,False),"live":remote(np,True)},"users":users}
    for name,value in (("live",{"node":node}),("build",{"node":build}),("manager",{"manager":manager})):
        if reusing:(a.root/(name+".toml")).unlink()
        private(a.root/(name+".toml"),toml({"schema_version":1,**value}))
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
    def submit(action):return call("/api/v2/operations",{"idempotency_key":secrets.token_hex(16),"node_id":"live","action":action})
    def completed(op):
        op=wait(lambda: (v if v["state"] in ("succeeded","failed","cancelled") else None) if (v:=call("/api/v2/operations/"+op["id"])) else None)
        assert op["state"]=="succeeded",op.get("error");return op
    def resources(kind):return [r for r in call("/api/v2/snapshot")["resources"] if r["kind"]==kind]
    def resource(op,kind):return next(r for r in op["result"]["resources"] if r["kind"]==kind)
    def current():return next((r for r in resources("run") if active and r["id"]==active["id"]),None)
    def control(kind):return completed(submit({"kind":kind,"run_id":active["id"],"expected_run_generation":active["body"]["run_generation"]}))
    def passed(name):
        checks.append(name);print(name+": passed",flush=True)
        progress=a.root/"checks-progress.json";pending=progress.with_suffix(".tmp");pending.write_text(json.dumps(checks));pending.chmod(0o600);pending.replace(progress)
    def fact_page(item,kind,cursor=None):
        query={"kind":kind,"limit":200,"run_generation":item["body"]["run_generation"]}
        if cursor:query["cursor"]=json.dumps(cursor)
        return call(f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/facts?"+parse.urlencode(query))
    def archived(item,kind):
        values=[];cursor=None;upper=None;recording=None
        for _ in range(32):
            page=fact_page(item,kind,cursor);recording=page["recording"]
            if upper is None:upper=page["upper_row"]
            assert page["upper_row"]==upper
            values.extend(page["items"]);cursor=page.get("next_cursor")
            if not cursor:break
        assert not cursor,"finite acceptance archive exceeds page budget"
        assert len({v["row_id"] for v in values})==len(values)
        return values,recording

    def trades():
        p=a.root/"node/account-facts.sqlite3"
        if not p.exists():return {}
        with sqlite3.connect("file:"+str(p)+"?mode=ro",uri=True) as db:
            return {key:{"digest":bytes(digest).hex(),"body":json.loads(body)} for key,digest,body in db.execute("SELECT trade_key,digest,body FROM trades")}
    prior_trades=trades()
    def acceptance_trades():
        values=trades()
        assert {k:values.get(k,{}).get("digest") for k in prior_trades}=={k:v["digest"] for k,v in prior_trades.items()},"an earlier execution or fee changed"
        return {k:v for k,v in values.items() if k not in prior_trades}
    def snapshot(item):
        return call(f"/api/v2/strategies/{item['body']['strategy_id']}/runs/{item['id']}/snapshot")
    def held_image():
        item=current()
        if not item:return None
        v=snapshot(item);public=v.get("business_observation",{})
        image=item["body"].get("observation",{}).get("account_snapshot",{}).get("image",{})
        risk=image.get("risk",{})
        if (public.get("available") and public["data"]["phase"] in ("running","paused") and int(public["data"]["last_error"])==0 and int(public["data"]["expected_inventory_lots"])==1 and int(public["data"]["last_market_at_ns"])>0
                and risk.get("complete") and image.get("max_exposure")):
            assert Decimal(risk["assets"][0]["pending_bound_notional"]["amount"])<=100
            assert Decimal(image["max_exposure"][0]["max_pending_bound_notional"])<=100
            return v
    def current_point():
        p=next((a.root/"node/checkpoints").rglob("runtime.db"))
        with sqlite3.connect("file:"+str(p)+"?mode=ro",uri=True) as db:
            record_id,digest,payload=db.execute("SELECT p.id,p.sha256,p.payload FROM recovery_heads h JOIN recovery_points p ON p.id=h.record_id").fetchone()
        assert hashlib.sha256(payload).hexdigest()==digest
        return p,record_id,digest,payload,json.loads(payload)
    successful=False;emergency_used=False
    try:
        buildp=start("build","node");nodep=start("live","node");managerp=start("manager","manager");session=wait(lambda:call("/api/v2/session",{"username":"operator","password":password}));csrf=session["csrf_token"]
        wait(lambda:resources("strategy_version") and resources("binding"));deployment=resource(completed(submit({"kind":"deploy","strategy_version_id":strategy_version["id"],"binding_id":"gate-btc"})),"deployment");passed("signed-server-built-one-lot-grid-deployed")
        began=time.monotonic();run=resource(completed(submit({"kind":"start","deployment_id":deployment["id"]})),"run");active=run
        wait(lambda:position_quantity(facts()[1])==Decimal("0.0001"),timeout=15)
        first=wait(lambda:acceptance_trades() or None,timeout=10);assert len(first)==1
        detail=next(iter(first.values()))["body"]["details"]
        assert int(detail["side"])==1 and int(detail["last_fill_quantity_lots"])==1
        first_fill_digest={k:v["digest"] for k,v in first.items()}
        wait(held_image,timeout=10);passed("real-independent-buy-fill-and-one-lot-position-confirmed")
        control("pause");wait(lambda:not facts()[0],timeout=10);assert position_quantity(facts()[1])==Decimal("0.0001")
        passed("paused-no-opening-orders-and-confirmed-one-lot-inventory")
        # Keep the original 60 s periodic checkpoint interval. Wait for an actual
        # post-fill commit, never rewrite a blob or bypass pending-fill validation.
        def post_fill_point():
            point=current_point()
            return point if checkpoint_has_position(point[-1]) else None
        checkpoint,record_id,digest,payload,point=wait(post_fill_point,timeout=70)
        assert checkpoint_has_position(point)
        assert (int(run["body"]["expires_at_ns"])-time.time_ns())/1e9>=40,"insufficient original recovery/Stop budget; keep worker alive for normal Stop"
        stable=acceptance_trades();assert {k:v["digest"] for k,v in stable.items()}==first_fill_digest
        public=wait(held_image,timeout=5);assert position_quantity(facts()[1])==Decimal("0.0001") and not facts()[0]
        observed=current();selected_reference=observed["body"]["recovery_reference"]
        assert selected_reference["record_id"]==record_id and selected_reference["payload_sha256"]==digest
        private(a.root/"checkpoint-proof.json",json.dumps({"passed":True,"reference":selected_reference,"confirmed_position_lots":1,"independent_fills":1,"pending_execution":False,"periodic_checkpoint_seconds":60,"before_crash_elapsed_seconds":round(time.monotonic()-began,3),"state_schema_version":point["state_schema_version"]},indent=2))
        passed("real-fill-consumed-into-original-periodic-local-sqlite-checkpoint")
        observation=observed["body"]["observation"];pid=int(observation["pid"]);assert int(Path(f"/proc/{pid}/stat").read_text().rsplit(")",1)[1].split()[19])==int(observation["start_time"]);os.kill(pid,signal.SIGKILL)
        old=wait(lambda:(r if r["body"]["process_state"]=="UNKNOWN" else None) if (r:=current()) else None,timeout=10)
        assert position_quantity(facts()[1])==Decimal("0.0001") and not facts()[0]
        passed("owned-worker-crash-with-confirmed-minimum-position")
        restored=completed(submit({"kind":"restore","run_id":old["id"],"expected_run_generation":old["body"]["run_generation"],"recovery_point_sha256":digest}));active=resource(restored,"run")
        assert int(active["body"]["run_generation"])==int(run["body"]["run_generation"])+1
        assert active["body"]["expires_at_ns"]==run["body"]["expires_at_ns"]
        managed=json.loads((a.root/"node/managed"/active["body"]["admission_operation_id"]/"managed.json").read_text());assert managed["recovery_reference"]==selected_reference
        wait(held_image,timeout=10);assert position_quantity(facts()[1])==Decimal("0.0001") and not facts()[0]
        assert {k:v["digest"] for k,v in acceptance_trades().items()}==first_fill_digest,"restore duplicated or rewrote original execution/fee"
        private(a.root/"recovery-proof.json",json.dumps({"passed":True,"backend":"local-sqlite","reference":selected_reference,"old_generation":run["body"]["run_generation"],"new_generation":active["body"]["run_generation"],"old_expires_at_ns":run["body"]["expires_at_ns"],"new_expires_at_ns":active["body"]["expires_at_ns"],"position_lots_before_crash":1,"position_lots_after_restore":1,"independent_fill_rows_before_and_after_restore":1,"original_trade_and_fee_digest_unchanged":True},indent=2))
        passed("real-filled-sqlite-record-restores-position-business-state-and-original-expiry")
        before_stop=position_quantity(facts()[1]);assert before_stop==Decimal("0.0001")
        stop_began=time.monotonic();stopped=control("stop");stop_seconds=time.monotonic()-stop_began
        assert stop_seconds<=10,"original 10 s Stop budget exceeded"
        wait(lambda:facts()==([],[]),timeout=10)
        after=wait(lambda:(v if len(v)==2 else None) if (v:=acceptance_trades()) else None,timeout=10)
        assert {k:v["digest"] for k,v in after.items() if k in first_fill_digest}==first_fill_digest
        closing=[v["body"]["details"] for k,v in after.items() if k not in first_fill_digest];assert len(closing)==1 and int(closing[0]["side"])==2 and int(closing[0]["last_fill_quantity_lots"])==1
        wait(lambda:(r if r["body"]["release_proof"]["safe"] else None) if (r:=current()) else None,timeout=10)
        with sqlite3.connect(a.root/"manager.sqlite3") as db:assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]==0
        passed("positioned-stop-reduce-only-real-sell-fill-and-flat-proof-within-original-10s")
        with sqlite3.connect(a.root/"node/account-facts.sqlite3") as db:
            order_rows=[json.loads(body) for (body,) in db.execute("SELECT body FROM facts WHERE kind='order'")]
        current_clients={v["body"]["details"]["client_order_id"] for v in after.values()}
        latest={v["details"]["client_order_id"]:v["details"] for v in order_rows if v["details"]["client_order_id"] in current_clients}
        opening=[v for v in latest.values() if int(v["side"])==1 and int(v["filled_quantity_lots"])==1];exits=[v for v in latest.values() if int(v["side"])==2 and int(v["filled_quantity_lots"])==1]
        assert len(opening)==len(exits)==1
        venue_exit=venue("/crossex/orders/"+exits[0]["client_order_id"])
        assert venue_exit["text"]==exits[0]["client_order_id"] and venue_exit["state"]=="FILLED"
        assert venue_exit["side"]=="SELL" and venue_exit["reduce_only"]=="true" and venue_exit["time_in_force"]=="IOC"
        assert Decimal(venue_exit["executed_qty"])==Decimal("0.0001")
        private(a.root/"venue-fill-proof.json",json.dumps({"passed":True,"closing_client_id":venue_exit["text"],"state":venue_exit["state"],"side":venue_exit["side"],"reduce_only":True,"time_in_force":venue_exit["time_in_force"],"executed_qty":venue_exit["executed_qty"]},indent=2))
        assert all(int(v["quantity_lots"])==1 for v in latest.values())
        old_fills,old_recording=archived(run,"fill");new_fills,new_recording=archived(active,"fill")
        assert len(old_fills)==len(new_fills)==1 and int(old_recording["gap_count"])>0
        elapsed=round(time.monotonic()-began,3);assert elapsed<=120
        result={"passed":True,"checks":checks,"host":"43.130.234.54","account":"gate","account_id":1,"instrument":SYMBOL,"binary_sha256":a.expected_sha256,"artifact_sha256":manifest["strategy_file_sha256"],"maximum_run_seconds":120,"handler_budget_seconds":1,"stop_timeout_seconds":10,"checkpoint_interval_seconds":60,"max_exposure_quote":"100","max_price_deviation_bps":50,"minimum_order_base":"0.0001","elapsed_seconds":elapsed,"positioned_stop_elapsed_seconds":round(stop_seconds,6),"opening_fill_count":1,"closing_fill_count":1,"independent_fill_count":2,"real_fill_recovery_tested":True,"positioned_flatten_tested":True,"original_trade_fee_digest_unchanged":True,"old_recording":old_recording,"new_recording":new_recording,"final_target_orders":0,"final_target_positions":[],"emergency_close_used":False}
        private(a.root/"results.json",json.dumps(result,indent=2));successful=True;print(json.dumps(result),flush=True)
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
            orders,positions=facts();remaining=[]
            if registry.exists():
                for pid,stamp in rows:
                    stat=Path(f"/proc/{pid}/stat")
                    if stat.exists() and int(stat.read_text().rsplit(")",1)[1].split()[19])==int(stamp):remaining.append(pid)
            if not orders and not positions and not remaining or time.monotonic()>=deadline:break
            time.sleep(.5)
        if positions and not remaining:
            # A failed acceptance stays failed. This is a separately identified,
            # reduce-only safety close after all owned runtimes have exited.
            emergency_used=True
            for attempt in range(3):
                quantity=position_quantity(facts()[1])
                if quantity==0:break
                book=venue("/futures/usdt/order_book",(("contract","BTC_USDT"),("limit",1)),False)
                body=protective_close_request(quantity,book["bids"][0]["p"],book["asks"][0]["p"],"titan-safe-"+secrets.token_hex(8))
                private(a.root/("safety-intent-"+str(attempt)+".json"),json.dumps({"acceptance_failed":True,"purpose":"reduce-only cleanup after owned workers exited","request":body}))
                encoded=json.dumps(body,separators=(",",":")).encode();stamp=str(int(time.time()));path="/api/v4/crossex/orders";signed="\n".join(("POST",path,"",hashlib.sha512(encoded).hexdigest(),stamp));headers={"Content-Type":"application/json","KEY":key,"Timestamp":stamp,"SIGN":hmac.new(secret.encode(),signed.encode(),hashlib.sha512).hexdigest()}
                try:
                    with request.urlopen(request.Request("https://api.gateio.ws"+path,data=encoded,headers=headers,method="POST"),timeout=5) as response:json.load(response)
                except (error.URLError,TimeoutError,ConnectionError):pass
                # Never repeat an unknown write. Resolve its exact client ID first.
                stamp=str(int(time.time()));lookup=path+"/"+body["text"];signed="\n".join(("GET",lookup,"",hashlib.sha512(b"").hexdigest(),stamp));headers={"KEY":key,"Timestamp":stamp,"SIGN":hmac.new(secret.encode(),signed.encode(),hashlib.sha512).hexdigest()}
                with request.urlopen(request.Request("https://api.gateio.ws"+lookup,headers=headers),timeout=5) as response:outcome=json.load(response)
                assert outcome["text"]==body["text"] and outcome["state"] in ("FILLED","CANCELED","EXPIRED","REJECTED"),"unknown safety close; preserve evidence"
                time.sleep(.3)
            orders,positions=facts()
        clean=not orders and not positions and not remaining
        if reusing:(a.root/"postflight.json").unlink()
        private(a.root/"postflight.json",json.dumps({"passed":clean,"acceptance_passed":successful,"emergency_close_used":emergency_used,"target_order_count":len(orders),"target_positions":[{k:v.get(k) for k in ("symbol","position_side","position_qty")} for v in positions],"remaining_owned_worker_pids":remaining},indent=2))
        assert clean,"target exposure or owned worker remains; preserve private evidence"
if __name__=="__main__":main()
