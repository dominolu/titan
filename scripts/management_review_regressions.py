"""Build-server regressions for management Stop admission, catalog failure, and dead task slots."""
import argparse, json, os, secrets, signal, socket, sqlite3, subprocess, threading, time
from http.cookiejar import CookieJar
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, error

parser=argparse.ArgumentParser()
parser.add_argument('--repo',type=Path,required=True);parser.add_argument('--binary',type=Path,required=True);parser.add_argument('--output',type=Path,required=True)
args=parser.parse_args()
ROOT=args.repo.resolve();BIN=args.binary.resolve();WORK=args.output.resolve()
WORK.mkdir(parents=True)
assert '192.168.3.88' in subprocess.check_output(['hostname', '-I']).decode()
def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0)); return s.getsockname()[1]
def inline(v):
    if isinstance(v, dict): return '{'+','.join(json.dumps(k)+'='+inline(x) for k,x in v.items())+'}'
    if isinstance(v, list): return '['+','.join(map(inline,v))+']'
    return json.dumps(v)
def toml(v): return '\n'.join(json.dumps(k)+'='+inline(x) for k,x in v.items())+'\n'
def wait(fn,timeout=10):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            result=fn()
            if result: return result
        except (OSError, error.URLError): pass
        time.sleep(.1)
    raise RuntimeError('wait timed out')
def http(url, data=None, headers=None, opener=None):
    req=request.Request(url, data=None if data is None else json.dumps(data).encode(), headers={'Content-Type':'application/json', **(headers or {})})
    try:
        with (opener or request.build_opener()).open(req, timeout=15) as response: return response.status, json.load(response)
    except error.HTTPError as response: return response.code, json.load(response)
class Node(BaseHTTPRequestHandler):
    fail=False; posts=[]; receipts={}
    def log_message(self,*args): pass
    def reply(self, code, value):
        data=json.dumps(value).encode(); self.send_response(code); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(data))); self.end_headers(); self.wfile.write(data)
    def do_GET(self):
        if self.path == '/v1/catalog':
            if Node.fail: return self.reply(503, {'code':'CATALOG_FAILED','message':'synthetic catalog failure','request_id':'probe'})
            return self.reply(200, resources)
        id=self.path.rsplit('/',1)[-1]
        if id.startswith('unknown-'): return self.reply(503, {'code':'RECEIPT_UNCERTAIN','message':'preserved unknown','request_id':'probe'})
        if id in Node.receipts:return self.reply(200,Node.receipts[id])
        return self.reply(404,{'code':'NOT_FOUND','message':'never admitted','request_id':'probe'})
    def do_POST(self):
        Node.posts.append(self.path)
        value=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        receipt={'operation_id':value['operation_id'],'input_digest':value['input_digest'],'state':'succeeded','result':{'source':'loopback_test_node'},'error':None}
        Node.receipts[value['operation_id']]=receipt
        return self.reply(200,receipt)

env={**os.environ,'RUST_LOG':'warn','TITAN_REVIEW_NODE_TOKEN':secrets.token_hex(32)}
resources=[{'id':'live-run-probe','node_id':'live','kind':'run','revision':'','frozen':False,'digest':'','body':{'account':'gate','account_identity':'canonical','instrument':'BTC_USDT','generation':'1','environment':'live','worker_id':'probe'}}]
processes=[]; logs=[]; results={}
try:
    node=ThreadingHTTPServer(('127.0.0.1',0), Node); threading.Thread(target=node.serve_forever,daemon=True).start()
    mp=port(); origin=f'http://127.0.0.1:{mp}'; password=secrets.token_hex(20)
    hashed=subprocess.check_output([str(BIN),'console','password-hash'],input=password.encode(),env=env).decode().strip()
    manager={'listen':f'127.0.0.1:{mp}','public_origin':origin,'secure_cookie':False,'database':str(WORK/'manager.db'),'web_root':str(ROOT/'web/console'),'nodes':{'live':{'endpoint':f'http://127.0.0.1:{node.server_port}','token_env':'TITAN_REVIEW_NODE_TOKEN','environments':['live'],'live_control':True}},'users':[{'username':'review','password_hash':hashed,'role':'operator','nodes':['live'],'environments':['live'],'accounts':['gate']}]}
    cfg=WORK/'manager.toml'; cfg.write_text(toml({'schema_version':1,'manager':manager}))
    log=(WORK/'manager.log').open('w'); logs.append(log); processes.append(subprocess.Popen([str(BIN),'console','manager','--config',str(cfg)],env=env,stdout=log,stderr=log,start_new_session=True))
    cookie=request.build_opener(request.HTTPCookieProcessor(CookieJar()))
    status,session=wait(lambda: http(origin+'/api/v1/session',{'username':'review','password':password},{'Origin':origin},cookie),timeout=40)
    assert status==200
    headers={'Origin':origin,'X-CSRF-Token':session['csrf_token']}
    wait(lambda: http(origin+'/api/v1/resources/live-run-probe',opener=cookie)[0]==200)
    now=str(time.time_ns())
    with sqlite3.connect(WORK/'manager.db') as db:
        for i in range(256):
            action={'kind':'describe','template_id':'probe','parameters':{}}
            op={'id':f'unknown-{i}','actor':'review','node_id':'live','input':action,'account':None,'input_digest':'probe','state':'unknown','cancel_requested':False,'created_at_ns':now,'updated_at_ns':now,'result':None,'error':'synthetic lost receipt','revision':'0'}
            req={'api_version':1,'operation_id':op['id'],'input_digest':'probe','action':action,'resources':[]}
            db.execute('INSERT INTO operations VALUES (?,?,?,?,?,?,?)',(op['id'],'review',op['id'],'probe','live',json.dumps(op),json.dumps(req)))
    stop={'idempotency_key':'review-stop-capacity','node_id':'live','action':{'kind':'stop','run_id':'live-run-probe','generation':'1'}}
    status,body=http(origin+'/api/v1/operations',stop,headers,cookie)
    assert status==202,(status,body)
    accepted_id=body['id']
    wait(lambda:http(origin+'/api/v1/operations/'+accepted_id,opener=cookie)[1]['state']=='succeeded')
    results['stop_when_256_unknown_operations']={'http_status':status,'state':'succeeded','passed':True}
    Node.fail=True; before=len(Node.posts)
    stop['idempotency_key']='review-stop-catalog'
    status,body=http(origin+'/api/v1/operations',stop,headers,cookie); assert status==202, (status,body)
    wait(lambda:http(origin+'/api/v1/operations/'+body['id'],opener=cookie)[1]['state']=='succeeded')
    assert len(Node.posts)==before+1
    results['stop_with_failed_catalog']={'accepted_status':status,'state':'succeeded','node_post_count':len(Node.posts)-before,'passed':True}
    # Simulate a node worker dying after spawn, before it can persist a final receipt.
    np=port(); work=WORK/'node'; wrapper=WORK/'crash-worker.sh'; wrapper.write_text('#!/bin/sh\nexit 99\n'); wrapper.chmod(0o700)
    nc={'id':'build','listen':f'127.0.0.1:{np}','token_env':'TITAN_REVIEW_NODE_TOKEN','work_root':str(work),'titan_program':str(wrapper),'environments':['backtest']}
    path=WORK/'node.toml'; path.write_text(toml({'schema_version':1,'node':nc}))
    log=(WORK/'node.log').open('w'); logs.append(log); processes.append(subprocess.Popen([str(BIN),'console','node','--config',str(path)],env=env,stdout=log,stderr=log,start_new_session=True))
    nh={'Authorization':'Bearer '+env['TITAN_REVIEW_NODE_TOKEN']}; endpoint=f'http://127.0.0.1:{np}'
    wait(lambda:http(endpoint+'/v1/catalog',headers=nh)[0]==200,timeout=40)
    def node_request(id):return {'api_version':1,'operation_id':id,'input_digest':'probe','action':{'kind':'describe','template_id':'probe','parameters':{}},'resources':[]}
    status,body=http(endpoint+'/v1/operations',node_request('crashed-task'),nh); assert status==200,(status,body)
    time.sleep(.3)
    _,first=http(endpoint+'/v1/operations/crashed-task',headers=nh)
    status,second=http(endpoint+'/v1/operations',node_request('next-task'),nh)
    with sqlite3.connect(work/'runs.sqlite3') as db:
        columns=[row[1] for row in db.execute('PRAGMA table_info(runs)')]
        row=db.execute("SELECT * FROM runs WHERE id='crashed-task'").fetchone(); record=dict(zip(columns,row))
    assert not Path('/proc/'+str(record['pid'])).exists()
    assert status==200,(status,second)
    status,retry=http(endpoint+'/v1/operations',node_request('crashed-task'),nh)
    assert status==200 and retry['state']=='unknown'
    results['dead_task_releases_only_process_capacity']={'first_receipt_state':first['state'],'registry_state':record['state'],'process_alive':False,'next_task_http_status':status,'old_admission_still_unknown':True,'passed':True}
finally:
    for p in processes:
        if p.poll() is None: os.killpg(p.pid,signal.SIGTERM)
    for p in processes:
        try:p.wait(timeout=5)
        except subprocess.TimeoutExpired: os.killpg(p.pid,signal.SIGKILL);p.wait()
    for log in logs:log.close()
    (WORK/'results.json').write_text(json.dumps(results,indent=2))
print(json.dumps(results,indent=2))
