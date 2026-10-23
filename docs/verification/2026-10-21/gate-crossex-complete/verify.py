import os,subprocess,time,json,sys,pathlib
root=pathlib.Path('/home/dominolu/dev/titan-gatecrossex-complete-20261021-39f1267b')
logs=pathlib.Path('/tmp/titan-crossex-complete-final');logs.mkdir(exist_ok=True)
os.chdir(root)
env=os.environ.copy();env.update(CARGO_TARGET_DIR='/home/dominolu/dev/titan-compiler-20261021-39f1267b/target',TITAN_STRATEGY_PYTHON='/home/dominolu/dev/titan-compiler-20261021-39f1267b/.venv/bin/python',TITAN_STRATEGY_SDK=str(root/'python/titan-strategy-sdk'))
checks=[
 ('fmt',['cargo','fmt','--all','--','--check']),
 ('clippy',['cargo','clippy','--locked','--workspace','--all-targets','--all-features','--','-D','warnings']),
 ('feature',['cargo','check','--locked','-p','connector','--no-default-features','--features','gatecrossex','--lib']),
 ('workspace',['cargo','test','--locked','--workspace','--all-targets','--all-features']),
 ('doctest',['cargo','test','--locked','--workspace','--all-features','--doc']),
 ('public-stream',['cargo','run','--locked','-p','connector','--example','gate_crossex_market_stream_probe']),
 ('public-query',['cargo','run','--locked','-p','connector','--example','gate_crossex_public_api_probe']),
]
results=[]
for name,command in checks:
 start=time.monotonic()
 with (logs/(name+'.log')).open('w') as out:
  out.write('$ '+' '.join(command)+'\n');out.flush()
  code=subprocess.run(command,env=env,stdout=out,stderr=subprocess.STDOUT).returncode
 result=dict(check=name,exit_code=code,seconds=round(time.monotonic()-start,2));results.append(result)
 print(json.dumps(result),flush=True)
 (logs/'results.json').write_text(json.dumps(results,indent=2)+'\n')
 if code:
  print((logs/(name+'.log')).read_text()[-12000:],flush=True);sys.exit(code)
