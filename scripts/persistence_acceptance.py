"""Shared real-driver acceptance helpers, used only by explicit M3 server gates."""
import hashlib,json,os,sqlite3,subprocess,uuid,time
from pathlib import Path

def enabled():return os.environ.get("TITAN_PERSISTENCE_STAGE")=="M3"
def storage(binary,root,*args,env=None,timeout=300):
    for attempt in range(4):
        p=subprocess.run([str(binary),"storage",*map(str,args),"--storage-root",str(root)],env=env,capture_output=True,text=True,timeout=timeout)
        if p.returncode==0:break
        if "database is locked" not in p.stdout+p.stderr or attempt==3:
            raise RuntimeError("storage command failed: "+str(args[0])+" "+(p.stdout+p.stderr)[-2000:])
        time.sleep(.1*2**attempt)  # Retry the exact maintenance receipt; never repeat an engine task.
    value=json.loads(p.stdout);return value.get("result",value)
def initialize(binary,root,env):
    if not enabled():return
    profile=json.loads(Path(os.environ["TITAN_M3_PROFILE"]).read_text());profile["s3"]["prefix"]="acceptance-"+uuid.uuid4().hex;profile["s3"]["timeout_ms"]=30000
    root.parent.mkdir(parents=True,exist_ok=True);path=root.parent/"storage-profile.json";path.write_text(json.dumps(profile));path.chmod(0o600)
    storage(binary,root,"initialize","--profile",path,env=env)
def switch_history(binary,root,path,target,env):
    plan=storage(binary,root,"prepare-history","--database",path,"--target",target,env=env)
    plan=storage(binary,root,"activate","--plan",plan["id"],env=env)
    assert plan["phase"]=="committed";return plan
def switch_objects(binary,root,target,env):
    # Bulk copy and read-back can span many bounded S3 requests.
    plan=storage(binary,root,"prepare-objects","--target",target,env=env,timeout=1800)
    plan=storage(binary,root,"activate","--plan",plan["id"],env=env,timeout=1800)
    assert plan["phase"]=="committed";return plan
def source_upper(path):
    with sqlite3.connect("file:"+str(path)+"?mode=ro",uri=True) as db:return db.execute("SELECT last_row FROM archive_sequence WHERE id=1").fetchone()[0]
def digest(path):return hashlib.file_digest(Path(path).open("rb"),"sha256").hexdigest()

from contextlib import contextmanager
@contextmanager
def backend_paused(kind):
    if not enabled():yield;return
    state=json.loads(Path(os.environ["TITAN_M3_SERVICES"]).read_text());name=state[kind]
    label=subprocess.check_output(["docker","inspect","--format",'{{index .Config.Labels "com.titan.persistence-test"}}',name]).decode().strip()
    assert label==str(Path(state["root"]).resolve()),"refuse an unrelated backend"
    subprocess.run(["docker","pause",name],check=True,capture_output=True)
    try:yield
    finally:subprocess.run(["docker","unpause",name],check=True,capture_output=True)
