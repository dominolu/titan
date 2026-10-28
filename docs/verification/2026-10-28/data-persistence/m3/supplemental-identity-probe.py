import json,os,sqlite3,subprocess,uuid
from pathlib import Path
base=Path('/home/dominolu/dev/titan-persistence-m3-20261027-59973648');service=base/'evidence/services-02';root=base/'evidence/lifecycle-external-01/storage';history=root.parent/'history.db';out=base/'evidence/m3-final-05/supplemental-identity.json'
env=dict(os.environ,**json.loads((service/'client-env.json').read_text()));source=json.loads((root.parent/'results.json').read_text())['source_id'];checks=[]
def command(*args):
 return subprocess.run(['/tmp/titan-deplugin-target/debug/titan','storage',*map(str,args),'--storage-root',str(root)],env=env,capture_output=True,text=True,timeout=60)
with sqlite3.connect(root/'control.db') as db:
 old_pg=db.execute('SELECT target_id FROM storage_targets WHERE name=?',('postgres',)).fetchone()[0]
 old_s3=db.execute('SELECT target_id FROM storage_targets WHERE name=?',('s3',)).fetchone()[0]
 old_route=db.execute('SELECT target_id,revision FROM history_routes WHERE source_id=?',(source,)).fetchone()
 stale=db.execute('SELECT id FROM storage_plans WHERE domain=? AND source_id=? AND phase<>? AND from_revision<? LIMIT 1',('history',source,'committed',old_route[1])).fetchone()[0]
try:
 with sqlite3.connect(root/'control.db') as db:
  fake=str(uuid.uuid4());db.execute('UPDATE storage_targets SET target_id=? WHERE name=?',(fake,'postgres'));db.execute('UPDATE history_routes SET target_id=? WHERE source_id=?',(fake,source))
 p=command('project','--database',history);assert p.returncode!=0 and 'postgres_target_identity_changed' in p.stdout+p.stderr;checks.append({'name':'postgres_expected_identity_mismatch_rejected','passed':True})
finally:
 with sqlite3.connect(root/'control.db') as db:
  db.execute('UPDATE storage_targets SET target_id=? WHERE name=?',(old_pg,'postgres'));db.execute('UPDATE history_routes SET target_id=? WHERE source_id=?',(old_route[0],source))
try:
 with sqlite3.connect(root/'control.db') as db:db.execute('UPDATE storage_targets SET target_id=? WHERE name=?',(str(uuid.uuid4()),'s3'))
 p=command('prepare-objects','--target','s3');assert p.returncode!=0 and 's3_target_identity_changed' in p.stdout+p.stderr;checks.append({'name':'s3_expected_identity_mismatch_rejected','passed':True})
finally:
 with sqlite3.connect(root/'control.db') as db:db.execute('UPDATE storage_targets SET target_id=? WHERE name=?',(old_s3,'s3'))
p=command('resume','--plan',stale);assert p.returncode!=0 and 'storage_plan_revision_changed' in p.stdout+p.stderr;checks.append({'name':'obsolete_plan_revision_rejected','passed':True})
out.write_text(json.dumps({'passed':True,'scope':'isolated lifecycle fixture; injected expected identity mismatch, restored afterward; no shared destination metadata changed','checks':checks},indent=2));print(out.read_text())
