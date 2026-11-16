"""Shared real-driver acceptance helpers, used only by explicit M3 server gates.

The external backends (S3 object store, PostgreSQL history projection) were retired as
over-engineered for a backtest/live platform: results are written to local files and SQLite only.
The M3 gates that depended on switching between those backends are gone with them, so the switch
helpers now fail loudly instead of silently doing nothing.
"""
import hashlib,json,os,sqlite3,subprocess,time
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
    """Creates the local storage root. Retained: the live gates still start from it."""
    if not enabled():return
    root.parent.mkdir(parents=True,exist_ok=True)
    storage(binary,root,"initialize",env=env)

def _retired(name):
    raise RuntimeError(
        name+" belongs to the retired external-backend acceptance path "
        "(S3/PostgreSQL were removed); run the gates with TITAN_PERSISTENCE_STAGE=M1")

def switch_history(*args,**kwargs):_retired("switch_history")
def switch_objects(*args,**kwargs):_retired("switch_objects")

def source_upper(path):
    with sqlite3.connect("file:"+str(path)+"?mode=ro",uri=True) as db:return db.execute("SELECT last_row FROM archive_sequence WHERE id=1").fetchone()[0]

def digest(path):return hashlib.file_digest(Path(path).open("rb"),"sha256").hexdigest()

from contextlib import contextmanager
@contextmanager
def backend_paused(kind):
    _retired("backend_paused")
    yield
