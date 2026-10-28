#!/usr/bin/env python3
"""Real PostgreSQL/S3 process and transport faults. Isolated build-server fixtures only."""
import argparse,hashlib,http.client,json,os,re,secrets,signal,socket,socketserver,sqlite3,subprocess,threading,time,uuid
from http.server import BaseHTTPRequestHandler,ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlsplit
from persistence_acceptance import storage,switch_objects,switch_history,backend_paused

class Proxy(ThreadingHTTPServer):
    daemon_threads=True
    def __init__(self,target):
        super().__init__(("127.0.0.1",0),Handler);self.target=urlsplit(target);self.drop_put=0;self.cut_get=0;self.ignore_conditional=False;self.deny_put=False;self.hits=[]
class Handler(BaseHTTPRequestHandler):
    protocol_version="HTTP/1.1"
    def log_message(self,*args):pass
    def do_GET(self):self.forward()
    def do_PUT(self):self.forward()
    def do_DELETE(self):self.forward()
    def do_HEAD(self):self.forward()
    def forward(self):
        conn=http.client.HTTPConnection(self.server.target.hostname,self.server.target.port,timeout=30)
        try:
            length=int(self.headers.get("Content-Length","0"));assert length<32*1024*1024
            # AWS file uploads in this fixture have an explicit content length.
            assert not self.headers.get("Transfer-Encoding"),"unexpected chunked test request"
            body=self.rfile.read(length);headers=dict(self.headers)
            if self.server.deny_put and self.command=="PUT":
                self.send_response(403);self.send_header("Content-Length","0");self.end_headers();return
            conn.request(self.command,self.path,body=body,headers=headers);response=conn.getresponse();data=response.read()
            is_object="/objects/" in self.path
            if self.command=="PUT" and is_object and self.server.drop_put:
                self.server.drop_put-=1;self.server.hits.append("put_ack_lost_after_server_commit");self.connection.shutdown(socket.SHUT_RDWR);self.close_connection=True;return
            self.send_response(200 if self.server.ignore_conditional and response.status==412 else response.status)
            for key,value in response.getheaders():
                if key.lower() not in ("connection","transfer-encoding","content-length","server","date"):self.send_header(key,value)
            self.send_header("Content-Length",str(len(data)));self.end_headers()
            if self.command=="GET" and is_object and self.server.cut_get:
                self.server.cut_get-=1;self.server.hits.append("get_body_interrupted");self.wfile.write(data[:max(1,len(data)//2)]);self.wfile.flush();self.connection.shutdown(socket.SHUT_RDWR);self.close_connection=True;return
            if self.command!="HEAD":self.wfile.write(data)
        except (BrokenPipeError,ConnectionResetError):self.close_connection=True
        finally:conn.close()

class PgProxy(socketserver.ThreadingTCPServer):
    allow_reuse_address=True;daemon_threads=True
    def __init__(self,port):
        self.target_port=port;self.drop_commit=threading.Event();self.hit=threading.Event();self.query_tags=[];self.backend_commits=0
        super().__init__(("127.0.0.1",0),PgHandler)
class PgHandler(socketserver.BaseRequestHandler):
    def handle(self):
        upstream=socket.create_connection(("127.0.0.1",self.server.target_port),timeout=10);client=self.request
        upstream.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1);client.setsockopt(socket.IPPROTO_TCP,socket.TCP_NODELAY,1)
        def exact(sock,count):
            data=bytearray()
            while len(data)<count:
                part=sock.recv(count-len(data))
                if not part:raise EOFError()
                data.extend(part)
            return bytes(data)
        def frame(sock):
            header=exact(sock,5);length=int.from_bytes(header[1:],"big");assert 4<=length<=8*1024*1024
            return header,exact(sock,length-4)
        def frontend():
            try:
                length=exact(client,4);size=int.from_bytes(length,"big");assert 8<=size<=65536
                upstream.sendall(length+exact(client,size-4))
                while True:
                    header,body=frame(client)
                    if header[:1]==b"Q":self.server.query_tags.append(body.rstrip(b"\0").split(None,1)[0][:20].decode("ascii",errors="replace"))
                    if header[:1]==b"Q" and body.rstrip(b"\0").strip().rstrip(b";").upper()==b"COMMIT":self.server.drop_commit.set()
                    upstream.sendall(header+body)
            except (OSError,EOFError,AssertionError):
                try:upstream.shutdown(socket.SHUT_RDWR)
                except OSError:pass
        thread=threading.Thread(target=frontend,daemon=True);thread.start()
        try:
            while True:
                header,body=frame(upstream)
                if header[:1]==b"C" and body.startswith(b"COMMIT"):self.server.backend_commits+=1
                if header[:1]==b"C" and body.startswith(b"COMMIT") and self.server.drop_commit.is_set():
                    self.server.hit.set();client.shutdown(socket.SHUT_RDWR);break
                client.sendall(header+body)
        except (OSError,EOFError,AssertionError):pass
        finally:
            upstream.close();client.close();thread.join(timeout=2)

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument("--binary",type=Path,required=True);parser.add_argument("--profile",type=Path,required=True);parser.add_argument("--services",type=Path,required=True);parser.add_argument("--output",type=Path,required=True);parser.add_argument("--contract",type=Path,required=True)
    args=parser.parse_args();assert "192.168.3.88" in subprocess.check_output(["hostname","-I"]).decode().split();args.output.mkdir();args.output.chmod(0o700)
    output=args.output.resolve();base=json.loads(args.profile.read_text());env=dict(os.environ);checks=[]
    def passed(name,**detail):checks.append({"name":name,"passed":True,**detail});print(json.dumps(checks[-1]),flush=True)
    def raw(root,*command,extra=None):
        p=subprocess.run([str(args.binary),"storage",*map(str,command),"--storage-root",str(root)],env=dict(env,**(extra or {})),capture_output=True,text=True,timeout=240)
        return p
    proxy=Proxy(base["s3"]["endpoint"]);thread=threading.Thread(target=proxy.serve_forever,daemon=True);thread.start()
    try:
        profile=json.loads(json.dumps(base));profile["postgres"]=None;profile["s3"].update(endpoint=f"http://127.0.0.1:{proxy.server_port}",prefix="fault-"+uuid.uuid4().hex,timeout_ms=2000)
        root=output/"storage";profile_path=output/"profile.json";profile_path.write_text(json.dumps(profile));profile_path.chmod(0o600)
        storage(args.binary,root,"initialize","--profile",profile_path,env=env);switch_objects(args.binary,root,"s3",env)
        bundle=output/"bundle";bundle.mkdir();content=b'{"original":"financial semantics remain unchanged"}';(bundle/"result.json").write_bytes(content)
        manifest={"schema_version":1,"run_id":"transport-fault","strategy_id":"same-native-artifact","files":[{"path":"result.json","bytes":len(content),"sha256":hashlib.sha256(content).hexdigest()}]};(bundle/"manifest.input.json").write_text(json.dumps(manifest))
        proxy.drop_put=2
        first=raw(root,"register-bundle","--bundle",bundle,"--manifest",bundle/"manifest.input.json")
        assert "put_ack_lost_after_server_commit" in proxy.hits
        proxy.drop_put=0
        receipt=storage(args.binary,root,"register-bundle","--bundle",bundle,"--manifest",bundle/"manifest.input.json",env=env)
        reference=receipt.get("manifest",receipt.get("reference",receipt))
        # Read the durable catalog, independent of CLI output presentation.
        with sqlite3.connect(root/"catalog.db") as db:
            sha,size=db.execute("SELECT manifest_sha256,manifest_bytes FROM object_catalog WHERE logical_id='transport-fault'").fetchone()
        reference=json.dumps({"sha256":sha,"bytes":str(size)})
        passed("lost_s3_put_ack_retries_same_original_object",first_exit=first.returncode,hits=proxy.hits.count("put_ack_lost_after_server_commit"))
        proxy.cut_get=4;interrupted=raw(root,"materialize-bundle","--reference",reference,"--output",output/"interrupted")
        assert interrupted.returncode!=0 and not (output/"interrupted/manifest.json").exists();proxy.cut_get=0
        storage(args.binary,root,"materialize-bundle","--reference",reference,"--output",output/"materialized",env=env)
        assert (output/"materialized/result.json").read_bytes()==content
        assert (output/"materialized/manifest.json").read_bytes()==(bundle/"manifest.input.json").read_bytes()
        passed("interrupted_s3_body_has_no_success_manifest_and_can_retry")
        bad=json.loads(json.dumps(profile));bad["s3"]["prefix"]="bad-creds-"+uuid.uuid4().hex;bad["s3"]["secret_key_env"]="TITAN_M3_REJECTED_CANARY";badpath=output/"bad-profile.json";badpath.write_text(json.dumps(bad));badpath.chmod(0o600)
        canary=secrets.token_hex(32);rejected=raw(output/"bad-storage","initialize","--profile",badpath,extra={"TITAN_M3_REJECTED_CANARY":canary});assert rejected.returncode!=0 and canary not in rejected.stdout+rejected.stderr
        passed("real_s3_invalid_credentials_rejected_and_secret_redacted")
        unsupported=json.loads(json.dumps(profile));unsupported["s3"]["prefix"]="unsupported-"+uuid.uuid4().hex;unsupported_path=output/"unsupported.json";unsupported_path.write_text(json.dumps(unsupported));unsupported_path.chmod(0o600)
        proxy.ignore_conditional=True;rejected=raw(output/"unsupported-storage","initialize","--profile",unsupported_path);proxy.ignore_conditional=False
        assert rejected.returncode!=0 and "s3_conditional_writes_unsupported" in rejected.stdout+rejected.stderr
        passed("incorrect_conditional_success_response_rejected_at_capability_probe")
        # Leave an orphan exactly as SIGKILL would; cleanup must retain active-session leases (Rust contract).
        stage=root/".s3-stage"/("session-"+str(uuid.uuid4()));stage.mkdir(parents=True);(stage/".tmp-interrupted").write_bytes(b"partial")
        cleanup=storage(args.binary,root,"clean-staging",env=env);assert not stage.exists();passed("orphan_staging_cleanup_is_explicit_and_content_preserving",receipt=cleanup)
    finally:proxy.shutdown();proxy.server_close();thread.join(timeout=5)
    # Target PostgreSQL fault transactions are scoped by source UUID. Never stop unrelated services.
    state=json.loads(args.services.read_text());owned=str(Path(state["root"]).resolve())
    def docker(*command):return subprocess.check_output(["docker",*command],stderr=subprocess.STDOUT).decode()
    assert docker("inspect","--format",'{{index .Config.Labels "com.titan.persistence-test"}}',state["postgres"]).strip()==owned
    def sql(statement):return subprocess.check_output(["docker","exec","-i",state["postgres"],"psql","-U","titan","-d","titan","-v","ON_ERROR_STOP=1","-Atc",statement],stderr=subprocess.STDOUT).decode().strip()
    contract=json.loads((args.contract/"results.json").read_text());source=contract["source_id"];history=args.contract/"history.db";source_root=args.contract/"storage"
    # Reset only this test source's projection to prove process death cannot split cursor and batch.
    sql(f"DELETE FROM titan_fact_records WHERE source_id='{source}';UPDATE titan_projection_sources SET committed_row=0,observed_upper=0 WHERE source_id='{source}';")
    fn="titan_sleep_"+uuid.uuid4().hex
    sql(f"CREATE FUNCTION {fn}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.source_id='{source}' THEN PERFORM pg_sleep(20); END IF;RETURN NEW;END $$;CREATE TRIGGER {fn} BEFORE UPDATE ON titan_projection_sources FOR EACH ROW EXECUTE FUNCTION {fn}();")
    proc=subprocess.Popen([str(args.binary),"storage","project","--storage-root",str(source_root),"--database",str(history)],env=env,stdout=(output/"killed-export.log").open("wb"),stderr=subprocess.STDOUT)
    try:
        deadline=time.monotonic()+8
        while time.monotonic()<deadline:
            if int(sql("SELECT count(*) FROM pg_stat_activity WHERE application_name='titan-persistence' AND wait_event='PgSleep'"))>0:break
            if proc.poll() is not None:raise AssertionError("exporter exited before kill injection")
            time.sleep(.02)
        else:raise AssertionError("did not reach in-transaction checkpoint boundary")
        proc.kill();proc.wait(timeout=5);time.sleep(.1)
    finally:
        if proc.poll() is None:proc.kill();proc.wait()
        sql(f"DROP TRIGGER {fn} ON titan_projection_sources;DROP FUNCTION {fn}();")
    assert sql(f"SELECT committed_row FROM titan_projection_sources WHERE source_id='{source}'")=="0"
    assert sql(f"SELECT count(*) FROM titan_fact_records WHERE source_id='{source}'")=="0"
    pg_proxy=PgProxy(int(state["ports"]["postgres"]));pg_thread=threading.Thread(target=pg_proxy.serve_forever,daemon=True);pg_thread.start()
    dsn_name=base["postgres"]["dsn_env"];proxied=re.sub(r"\bport=\d+","port="+str(pg_proxy.server_address[1]),env[dsn_name],count=1);assert proxied!=env[dsn_name]
    try:
        lost_ack=raw(source_root,"project","--database",history,extra={dsn_name:proxied})
        if lost_ack.returncode==0 or not pg_proxy.hit.is_set():
            print(json.dumps({"injection_failed":True,"exit":lost_ack.returncode,"hit":pg_proxy.hit.is_set(),"query_tags":pg_proxy.query_tags,"backend_commits":pg_proxy.backend_commits,"error":(lost_ack.stdout+lost_ack.stderr)[-2000:]}),flush=True)
        assert lost_ack.returncode!=0 and pg_proxy.hit.is_set()
        durable_before_retry=sql(f"SELECT count(*),max(row_id) FROM titan_fact_records WHERE source_id='{source}'")
        assert int(durable_before_retry.split("|")[0])>0
    finally:pg_proxy.shutdown();pg_proxy.server_close();pg_thread.join(timeout=5)
    storage(args.binary,source_root,"project","--database",history,env=env)
    assert sql(f"SELECT count(*),max(row_id) FROM titan_fact_records WHERE source_id='{source}'")==durable_before_retry
    passed("sigkill_exporter_before_pg_commit_rolls_back_rows_and_cursor")
    passed("pg_commit_ack_dropped_after_durable_command_retries_without_duplicates")
    # An acknowledged commit whose CLI receipt was lost is safe to repeat.
    storage(args.binary,source_root,"project","--database",history,env=env)
    before=sql(f"SELECT count(*),max(row_id) FROM titan_fact_records WHERE source_id='{source}'")
    storage(args.binary,source_root,"project","--database",history,env=env)
    assert sql(f"SELECT count(*),max(row_id) FROM titan_fact_records WHERE source_id='{source}'")==before
    passed("acknowledged_pg_batch_retry_does_not_repeat_facts")
    with backend_paused("postgres"):
        started=time.monotonic();offline=raw(source_root,"project","--database",history);elapsed=time.monotonic()-started
        assert offline.returncode!=0 and elapsed<7
        assert "postgres_client_deadline_exceeded" in offline.stdout+offline.stderr
    storage(args.binary,source_root,"project","--database",history,env=env)
    passed("postgres_network_blackhole_has_client_deadline_and_resumes",seconds=elapsed)

    switch_history(args.binary,source_root,history,"postgres",env)
    with sqlite3.connect(history) as db:upper=db.execute("SELECT last_row FROM archive_sequence WHERE id=1").fetchone()[0]
    storage(args.binary,source_root,"prune-history","--database",history,"--through-row",upper,env=env)
    storage(args.binary,source_root,"project","--database",history,env=env)
    exported=storage(args.binary,source_root,"export-facts","--database",history,"--account","contract-account","--run-id","contract","--output",output/"pg-export.parquet",env=env)
    inspected=subprocess.run([str(args.binary),"storage","inspect-parquet","--input",str(output/"pg-export.parquet")],env=env,capture_output=True,text=True,check=True)
    assert json.loads(inspected.stdout)["records"]==exported["records"]
    switch_history(args.binary,source_root,history,"sqlite",env)
    passed("pruned_history_exports_via_postgres_and_returns_to_sqlite",records=exported["records"])
    (output/"results.json").write_text(json.dumps({"passed":True,"checks":checks,"no_real_accounts":True,"power_loss_scope":"process SIGKILL and transaction/transport faults; no physical power loss"},indent=2))
if __name__=="__main__":main()
