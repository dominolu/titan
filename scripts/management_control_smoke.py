#!/usr/bin/env python3
"""M3 build-server acceptance: real A/N/worker/connector, loopback simulated venue only."""
import argparse, base64, hashlib, shutil, http.cookiejar, json, os, select, secrets, signal, socket, sqlite3, struct, subprocess, threading, time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, error, parse
SYMBOL="GATE_FUTURE_BTC_USDT"
def port():
    with socket.socket() as s:s.bind(("127.0.0.1",0));return s.getsockname()[1]
def inline(v):
    if isinstance(v,dict):return "{"+",".join(json.dumps(k)+"="+inline(x) for k,x in v.items())+"}"
    if isinstance(v,list):return "["+",".join(map(inline,v))+"]"
    return json.dumps(v)
def toml(v):return "\n".join(json.dumps(k)+"="+inline(x) for k,x in v.items())+"\n"
def unit(c,s=0):return {"coefficient":c,"scale":s}
class Venue(ThreadingHTTPServer):
    daemon_threads=True
    def __init__(self):
        super().__init__(("127.0.0.1",0),Handler);self.lock=threading.RLock();self.orders={};self.private=[];self.public=[];self.calls=[];self.failed=[];self.ws_frames=[];self.alive=True
    def broadcast(self,channel,payload):
        self.ws_frames.append({"sent":channel,"connections":len(self.private),"symbol":payload.get("symbol"),"state":payload.get("state")})
        with self.lock:
            for stream in self.private[:]:
                try:stream.send_ws({"event":"subscribe","channel":channel,"time_ms":int(time.time()*1000),"payload":payload,"result":{"code":"100000"}})
                except (OSError,ValueError):self.private.remove(stream)
    def prices(self):
        while self.alive:
            with self.lock:
                for stream in self.public[:]:
                    try:stream.send_ws({"event":"update","channel":"ticker","result":{"s":SYMBOL,"ts":int(time.time()*1000),"bp":"999","bs":"10","ap":"1001","as":"10"}})
                    except (OSError,ValueError):self.public.remove(stream)
            time.sleep(.1)
class Handler(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def log_message(self,*_):pass
    def reply(self,value,status=200):
        data=json.dumps(value).encode();self.send_response(status);self.send_header("Content-Type","application/json");self.send_header("Content-Length",str(len(data)));self.end_headers();self.wfile.write(data)
    def send_ws(self,value,opcode=1):
        data=json.dumps(value).encode() if opcode==1 else value
        header=bytes([0x80|opcode,len(data)]) if len(data)<126 else bytes([0x80|opcode,126])+struct.pack("!H",len(data))
        with self.frame_lock:self.wfile.write(header+data);self.wfile.flush()
    def websocket(self):
        self.frame_lock=threading.Lock();self.send_response(101);self.send_header("Upgrade","websocket");self.send_header("Connection","Upgrade");self.send_header("Sec-WebSocket-Accept",base64.b64encode(hashlib.sha1((self.headers["Sec-WebSocket-Key"]+"258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode());self.end_headers()
        private=self.path=="/private";self.server.ws_frames.append({"path":self.path,"private":private})
        while self.server.alive:
            if not select.select([self.connection],[],[],.2)[0]:continue
            raw=self.rfile.read(2)
            if len(raw)!=2:break
            opcode=raw[0]&15;length=raw[1]&127
            if length==126:length=struct.unpack("!H",self.rfile.read(2))[0]
            elif length==127:length=struct.unpack("!Q",self.rfile.read(8))[0]
            assert length<65536
            mask=self.rfile.read(4) if raw[1]&128 else b"";data=self.rfile.read(length)
            if mask:data=bytes(b^mask[i%4] for i,b in enumerate(data))
            if opcode==8:break
            if opcode==9:self.send_ws(data,10);continue
            if opcode!=1:continue
            value=json.loads(data);self.server.ws_frames.append({"received":value.get("event"),"channel":value.get("channel")})
            if value.get("event")=="login":self.send_ws({"event":"login","result":{"code":"100000"}})
            elif value.get("event")=="subscribe":
                self.send_ws({"event":"subscribe","channel":value["channel"],"result":{"code":"100000"}})
                with self.server.lock:
                    members=self.server.private if private else self.server.public
                    if self not in members:members.append(self)
    def do_GET(self):
        if self.headers.get("Upgrade","").lower()=="websocket":
            try:self.websocket()
            except (OSError,ValueError,struct.error):pass
            return
        url=parse.urlparse(self.path);path=url.path.removeprefix("/api/v4/crossex/");query=dict(parse.parse_qsl(url.query));self.server.calls.append(("GET",path))
        if path=="rule/symbols":return self.reply([{"symbol":SYMBOL,"business_type":"FUTURE","state":"live","tick_size":"1","lot_size":"0.001","min_size":"0.001","min_notional":"0.1","max_limit_size":"100","max_market_size":"50","contract_size":"1"}])
        if path=="accounts":return self.reply({"account_mode":"CROSS_EXCHANGE","position_mode":"SINGLE","margin_balance":"100","available_margin":"100","update_time":str(int(time.time()*1000)),"assets":[{"coin":"USDT","exchange_type":"GATE","balance":"100","available_balance":"100","upnl":"0","equity":"100"}]})
        if path in ("positions","account_book"):return self.reply([])
        if path=="open_orders":return self.reply([v for v in self.server.orders.values() if v["state"]=="OPEN"])
        if path.startswith("orders/"):
            key=parse.unquote(path.split("/")[-1]);order=self.server.orders.get(key) or next((o for o in self.server.orders.values() if o["text"]==key),None)
            return self.reply(order if order else {"label":"ORDER_NOT_FOUND","message":"not found"},200 if order else 404)
        self.server.failed.append(("GET",self.path));return self.reply({"label":"UNSUPPORTED","message":self.path},400)
    def do_POST(self):
        path=parse.urlparse(self.path).path.removeprefix("/api/v4/crossex/");body=json.loads(self.rfile.read(int(self.headers.get("Content-Length",0))) or b"{}");self.server.calls.append(("POST",path))
        if path=="orders":
            now=str(int(time.time()*1000));key=str(len(self.server.orders)+1)
            order={**body,"order_id":key,"state":"OPEN","executed_qty":"0","executed_avg_price":"0","create_time":now,"update_time":now,"position_side":"NONE"}
            self.server.orders[key]=order;self.server.broadcast("order",order);return self.reply(order)
        self.server.failed.append(("POST",path));return self.reply({},400)
    def do_DELETE(self):
        path=parse.urlparse(self.path).path.removeprefix("/api/v4/crossex/");self.server.calls.append(("DELETE",path))
        if path.startswith("orders/"):
            order=self.server.orders[path.split("/")[-1]];order["state"]="CANCELED";order["update_time"]=str(int(time.time()*1000));self.server.broadcast("order",order);return self.reply(order)
        self.server.failed.append(("DELETE",path));return self.reply({},400)

def main():
    parser=argparse.ArgumentParser();parser.add_argument("--repo",type=Path,required=True);parser.add_argument("--binary",type=Path,required=True);parser.add_argument("--output",type=Path,required=True);parser.add_argument("--browser",action="store_true");parser.add_argument("--prepare-only",action="store_true");parser.add_argument("--parameters-file",type=Path);args=parser.parse_args()
    assert "192.168.3.88" in subprocess.check_output(["hostname","-I"]).decode(),"build server only"
    args.repo=args.repo.resolve();args.binary=args.binary.resolve();args.output.mkdir(parents=True);root=args.output.resolve();root.chmod(0o700)
    venue=Venue();threading.Thread(target=venue.serve_forever,daemon=True).start();threading.Thread(target=venue.prices,daemon=True).start()
    vp=venue.server_port;mp,bp,np=port(),port(),port();origin=f"http://127.0.0.1:{mp}"
    password,token=secrets.token_hex(24),secrets.token_hex(32);env=dict(os.environ,RUST_LOG="warn,titan_strategy_runtime=debug,connector::gatecrossex::stream=debug",TITAN_CONSOLE_NODE_TOKEN=token,TITAN_STRATEGY_PYTHON="/home/dominolu/dev/titan/.venv/bin/python",TITAN_STRATEGY_SDK=str(args.repo/"python/titan-strategy-sdk"))
    hashed=subprocess.check_output([str(args.binary),"console","password-hash"],input=password.encode(),env=env).decode().strip()
    key=root/"signing.key";seed=secrets.token_bytes(32);key.write_bytes(seed);key.chmod(0o600)
    der=root/"key.der";der.write_bytes(bytes.fromhex("302e020100300506032b657004220420")+seed);der.chmod(0o600)
    public=subprocess.check_output(["openssl","pkey","-inform","DER","-in",str(der),"-pubout","-outform","DER"])[-32:].hex()
    parameters=json.loads((args.repo/"strategies/grid/parameters.json").read_text());parameters.update(quote_currency_no=1,lower_price_ticks=800,upper_price_ticks=1200,grid_levels=200,order_qty_lots=1,allocated_capital_tick_lots=100000,max_notional_tick_lots=100000,leverage=1,quote_units_per_tick_lot=100000,stop_action="cancel",max_slippage_ticks=2)
    if args.parameters_file:parameters=json.loads(args.parameters_file.read_text())
    (root/"parameters.json").write_text(json.dumps(parameters));secret=root/"secrets";secret.mkdir();(secret/"gate.toml").write_text('api_key="synthetic"\nsecret="synthetic"\n');secret.chmod(0o700);(secret/"gate.toml").chmod(0o600)
    cfg=toml({"rest_url":f"http://127.0.0.1:{vp}/api/v4","public_ws_url":f"ws://127.0.0.1:{vp}/public","private_ws_url":f"ws://127.0.0.1:{vp}/private","market_quantity_multipliers":{SYMBOL:1.0}})
    instrument={"native_symbol":SYMBOL,"asset_id":1,"price_tick":unit(1),"quantity_lot":unit(1,3)}
    duration=lambda secs,nanos=0:{"secs":secs,"nanos":nanos}
    runtime={"schema_version":1,"managed_by_console":True,"account_service":{"secret_root":str(secret)},"strategy_service":{"trusted_ed25519_keys":{"acceptance":public}},"market_sources":[{"source_key":"gate-market","connector_type":"gate-crossex","connector_config":cfg,"instruments":[instrument],"enabled":True,"definition_version":1}],"accounts":[{"account_key":"gate","account_id":1,"connector_type":"gate-crossex-account","credential_ref":"secret://file/gate.toml","connector_config":cfg,"ownership":"ObserveAll","shutdown_order_policy":"LeaveOpen","instruments":[{**instrument,"contract_multiplier":unit(1)}],"currencies":[{"native_currency":"USDT","currency_id":1,"amount_unit":unit(1,8)}],"enabled":True,"definition_version":1}],"strategies":[{"strategy_key":"grid","strategy_id":77,"package":{"loader_type":"native-v13","uri":"file://unused.titan","expected_digest":[1]*32},"markets":[{"local_market_no":0,"local_asset_no":0,"source_key":"gate-market","asset_id":1,"data_mode":"tick"}],"accounts":[{"local_account_no":0,"account_key":"gate","tradable_assets":[{"local_asset_no":0,"asset_id":1}]}],"risk_scope":"acceptance","runtime":{"async_lane_capacity":256,"critical_reserve":32,"reliable_pending_capacity":32,"worker_policy":"park","timer_capacity":16,"max_commands_per_callback":64,"checkpoint_interval":duration(1),"timer_interval":duration(0,100000000),"max_handler_duration":duration(1),"startup_timeout":duration(10),"stop_timeout":duration(10)},"recovery":"fresh","enabled":True,"definition_version":1}]}
    (root/"runtime.toml").write_text(toml(runtime))
    binding={"account":"gate","account_identity":"synthetic-gate-1","instrument":SYMBOL,"runtime_config":str(root/"runtime.toml"),"strategy_key":"grid","max_exposure_quote":"100","max_price_deviation_bps":50,"max_duration_seconds":120}
    remote=lambda p,live:{"endpoint":f"http://127.0.0.1:{p}","token_env":"TITAN_CONSOLE_NODE_TOKEN","environments":["live" if live else "backtest"],"live_control":live}
    build={"id":"build","listen":f"127.0.0.1:{bp}","token_env":"TITAN_CONSOLE_NODE_TOKEN","work_root":str(root/"build"),"titan_program":str(args.binary),"environments":["backtest"],"signing_key":str(key),"signing_key_id":"acceptance","templates":{"grid":{"source":str(args.repo/"strategies/grid/strategy.py"),"parameters":str(root/"parameters.json")}}}
    node={"id":"live","listen":f"127.0.0.1:{np}","token_env":"TITAN_CONSOLE_NODE_TOKEN","work_root":str(root/"live"),"titan_program":str(args.binary),"environments":["live"],"live_control":True,"bindings":{"gate-btc":binding},"artifact_sources":{"build":remote(bp,False)}}
    users=[{"username":name,"password_hash":hashed,"role":role,"nodes":["build","live"],"accounts":["gate"],"environments":["backtest","live"]} for name,role in (("operator","operator"),("reader","reader"))]
    manager={"listen":f"127.0.0.1:{mp}","public_origin":origin,"database":str(root/"manager.db"),"web_root":str(args.repo/"web/console/dist"),"secure_cookie":False,"nodes":{"build":remote(bp,False),"live":remote(np,True)},"users":users}
    for name,value in (("build",{"node":build}),("live",{"node":node}),("manager",{"manager":manager})):(root/(name+".toml")).write_text(toml({"schema_version":1,**value}))
    processes=[]
    def start(name,kind):
        p=subprocess.Popen([str(args.binary),"console",kind,"--config",str(root/(name+".toml"))],env=env,stdin=subprocess.DEVNULL,stdout=(root/(name+".log")).open("ab"),stderr=subprocess.STDOUT);processes.append(p);return p
    def stop(p):
        if p.poll() is None:
            p.send_signal(signal.SIGINT)
            try:p.wait(timeout=8)
            except subprocess.TimeoutExpired:p.kill();p.wait()
    opener=request.build_opener(request.HTTPCookieProcessor(http.cookiejar.CookieJar()));csrf=None
    def call(path,data=None):
        headers={"Origin":origin,"Content-Type":"application/json"}
        if csrf:headers["X-CSRF-Token"]=csrf
        with opener.open(request.Request(origin+path,data=None if data is None else json.dumps(data).encode(),headers=headers),timeout=12) as response:return json.load(response)
    def wait(callback,timeout=40):
        until=time.monotonic()+timeout
        while time.monotonic()<until:
            try:
                value=callback()
                if value:return value
            except (error.URLError,TimeoutError,ConnectionError):pass
            time.sleep(.15)
        raise RuntimeError("M3 acceptance timeout; inspect isolated logs")
    def submit(action,node="live",key=None):return call("/api/v1/operations",{"idempotency_key":key or secrets.token_hex(16),"node_id":node,"action":action})
    def completed(op,success=True):
        result=wait(lambda:(v if v["state"] in ("succeeded","failed","cancelled") else None) if (v:=call("/api/v1/operations/"+op["id"])) else None,180)
        assert (result["state"]=="succeeded")==success,result;return result
    def resources(kind):return [r for r in call("/api/v1/snapshot")["resources"] if r["kind"]==kind]
    def resource(op,kind):return next(r for r in op["result"]["resources"] if r["kind"]==kind)
    checks=[]
    def passed(name):checks.append(name);print(name+": passed",flush=True)
    try:
        buildp=start("build","node");nodep=start("live","node");managerp=start("manager","manager")
        session=wait(lambda:call("/api/v1/session",{"username":"operator","password":password}));csrf=session["csrf_token"]
        wait(lambda:resources("template") and resources("binding"))
        described=completed(submit({"kind":"describe","template_id":"grid","parameters":parameters},"build"));version=call("/api/v1/parameter-versions",{"node_id":"build","template_id":"grid","parameters":parameters,"expected_revision":None,"freeze_operation_id":described["id"]})
        built=completed(submit({"kind":"build","version_id":version["id"]},"build"));artifact=resource(built,"artifact");passed("real-signed-grid-build")
        if args.prepare_only:
            package=root/"artifact-source";operation=package/"operations"/built["id"];operation.mkdir(parents=True);(package/"published").mkdir()
            for name in ("strategy.titan","receipt.json"):shutil.copy2(root/"build/operations"/built["id"]/name,operation/name)
            shutil.copy2(root/"build/published"/(artifact["id"]+".json"),package/"published"/(artifact["id"]+".json"))
            (package/"manifest.json").write_text(json.dumps({"artifact":artifact,"public_key":public,"parameters":parameters,"blob_sha256":hashlib.file_digest((operation/"strategy.titan").open("rb"),"sha256").hexdigest()},indent=2))
            assert not venue.calls
            passed("prepare-only-no-account-or-trading-connection")
            return
        if args.browser:
            subprocess.run(["node",str(args.repo/"scripts/management_control_browser.mjs"),origin,str(root/"browser"),artifact["id"]],env=dict(env,TITAN_BROWSER_PASSWORD=password),check=True,timeout=180)
            wait(lambda:resources("run") and all(r["body"]["release_proof"]["safe"] for r in resources("run")))
            passed("actual-browser-deploy-start-pause-resume-stop-reader")
        deployment=resource(completed(submit({"kind":"deploy","artifact_id":artifact["id"],"binding_id":"gate-btc"})),"deployment");passed("cross-node-deploy-signature-abi-digest")
        deployed=root/"live/deployments"/deployment["body"]["operation_id"]/"strategy.titan";good_deployed=hashlib.file_digest(deployed.open("rb"),"sha256").hexdigest()
        blob=root/"build/operations"/built["id"]/"strategy.titan";original=blob.read_bytes();blob.write_bytes(original[:-1]+bytes([original[-1]^1]))
        try:completed(submit({"kind":"deploy","artifact_id":artifact["id"],"binding_id":"gate-btc"}),False)
        finally:blob.write_bytes(original)
        assert hashlib.file_digest(deployed.open("rb"),"sha256").hexdigest()==good_deployed;passed("tampered-transfer-preserves-good-deployment")
        original_runtime=(root/"runtime.toml").read_text();(root/"runtime.toml").write_text(original_runtime+"\n# drift after deployment\n")
        try:completed(submit({"kind":"start","deployment_id":deployment["id"]}),False)
        finally:(root/"runtime.toml").write_text(original_runtime)
        with sqlite3.connect(root/"manager.db") as db:assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]==0
        passed("config-drift-rejected-before-activation-and-reservation-released")
        direct=subprocess.run([str(args.binary),"core-run","--config",str(root/"runtime.toml"),"--json"],capture_output=True,timeout=10);assert direct.returncode!=0 and b"authenticated console" in direct.stdout+direct.stderr;passed("direct-cli-bypass-rejected")
        started=completed(submit({"kind":"start","deployment_id":deployment["id"]},key="start-once"));run=resource(started,"run");body=run["body"];generation=body["generation"]
        duplicate=submit({"kind":"start","deployment_id":deployment["id"]},key="start-once");assert duplicate["id"]==started["id"];passed("start-idempotency")
        start_receipt=root/"live/operations"/started["id"]/"receipt.json";saved_start=start_receipt.read_bytes();lost_start=json.loads(saved_start);lost_start.update(state="unknown",result=None);start_receipt.write_text(json.dumps(lost_start))
        try:
            with request.urlopen(request.Request(f"http://127.0.0.1:{np}/v1/operations/{started['id']}",headers={"Authorization":"Bearer "+token}),timeout=10) as response:assert json.load(response)["state"]=="succeeded"
        finally:start_receipt.write_bytes(saved_start)
        passed("lost-start-ack-reconciles-same-worker")
        try:submit({"kind":"start","deployment_id":deployment["id"]});raise AssertionError("duplicate account owner accepted")
        except error.HTTPError as failure:assert failure.code==409
        wait(lambda:len([o for o in venue.orders.values() if o["state"]=="OPEN"])==2);passed("actual-runtime-quotes-within-envelope")
        paused=completed(submit({"kind":"pause","run_id":run["id"],"generation":generation}));wait(lambda:all(o["state"]!="OPEN" for o in venue.orders.values()));passed("pause-consumes-cancel-confirmations")
        receipt_path=root/"live/operations"/paused["id"]/"receipt.json";saved=receipt_path.read_bytes();lost=json.loads(saved);lost.update(state="unknown",result=None,error="simulated adapter acknowledgement loss");receipt_path.write_text(json.dumps(lost))
        try:
            with request.urlopen(request.Request(f"http://127.0.0.1:{np}/v1/operations/{paused['id']}",headers={"Authorization":"Bearer "+token}),timeout=10) as response:assert json.load(response)["state"]=="succeeded"
        finally:receipt_path.write_bytes(saved)
        passed("lost-control-ack-reconciles-same-engine-operation")
        before=len(venue.orders);time.sleep(.6);assert len(venue.orders)==before
        stop(managerp);stop(nodep);nodep=start("live","node");managerp=start("manager","manager")
        current=wait(lambda:next((r for r in resources("run") if r["id"]==run["id"] and r["body"]["available"]),None));assert current["body"]["observation"]["pid"]==body["observation"]["pid"];passed("A-N-restart-preserves-paused-worker")
        completed(submit({"kind":"resume","run_id":run["id"],"generation":generation}));wait(lambda:len(venue.orders)>before);passed("resume-same-generation")
        completed(submit({"kind":"pause","run_id":run["id"],"generation":generation}));wait(lambda:all(o["state"]!="OPEN" for o in venue.orders.values()));time.sleep(.5)
        pid=int(body["observation"]["pid"]);os.kill(pid,signal.SIGKILL)
        old=wait(lambda:next((r for r in resources("run") if r["id"]==run["id"] and r["body"]["checkpoint_id"] and r["body"]["process_state"]=="UNKNOWN"),None));passed("crash-retains-checkpoint-and-occupancy")
        bad=completed(submit({"kind":"recover","run_id":old["id"],"generation":generation,"checkpoint_id":"0"*64}),False);assert "checkpoint" in bad["error"];passed("wrong-checkpoint-rejected")
        # A failed recovery keeps reservation until the proven old owner is explicitly retried.
        recovered=completed(submit({"kind":"recover","run_id":old["id"],"generation":generation,"checkpoint_id":old["body"]["checkpoint_id"]}));new=resource(recovered,"run");assert int(new["body"]["generation"])>int(generation) and new["body"]["expires_at_ns"]==old["body"]["expires_at_ns"];passed("pinned-recovery-new-generation-original-budget")
        recovered_spec=json.loads((root/"live/managed"/recovered["id"]/"managed.json").read_text());controls=Path(recovered_spec["claim"]["socket"]).parent/"controls";controls.mkdir(exist_ok=True)
        for index in range(1024-len(list(controls.iterdir()))):(controls/("capacity-"+str(index)+".json")).write_text("{}")
        completed(submit({"kind":"stop","run_id":new["id"],"generation":new["body"]["generation"]}));passed("stop-reserved-receipt-capacity");wait(lambda:next((r for r in resources("run") if r["id"]==new["id"] and r["body"]["release_proof"]["safe"]),None));assert all(o["state"]!="OPEN" for o in venue.orders.values());passed("stop-engine-proof-and-flat-release")
        with sqlite3.connect(root/"manager.db") as db:assert db.execute("SELECT COUNT(*) FROM occupancy").fetchone()[0]==0
        assert not venue.failed,venue.failed
        (root/"results.json").write_text(json.dumps({"passed":True,"source":"real runtime with loopback simulated venue; no real account","checks":checks,"venue_order_count":len(venue.orders),"remaining_orders":0},indent=2))
    finally:
        (root/"venue-debug.json").write_text(json.dumps({"ws":venue.ws_frames,"calls":venue.calls,"failed":venue.failed},indent=2))
        for p in reversed(processes):stop(p)
        if (root/"live/runs.sqlite3").exists():
            with sqlite3.connect(root/"live/runs.sqlite3") as db:
                for pid,start_time in db.execute("SELECT pid,process_start_time FROM runs WHERE strategy_id='management-runtime'"):
                    path=Path(f"/proc/{pid}/stat")
                    if path.exists() and int(path.read_text().rsplit(")",1)[1].split()[19])==int(start_time):os.kill(pid,signal.SIGTERM)
        venue.alive=False;venue.shutdown()
if __name__=="__main__":main()
