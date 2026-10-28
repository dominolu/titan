#!/usr/bin/env python3
"""Own only labelled, ephemeral M3 PostgreSQL/S3 services on the build server."""
import argparse,json,os,secrets,subprocess,time,urllib.request
from pathlib import Path

PG_IMAGE="postgres:15-alpine"
S3_IMAGE="alpine/minio@sha256:cf23643a6cf9ce159c57643ceb88279e431262282428c9e0bf3a7ef1a97e84b4"

def command(*args):
    p=subprocess.run(args,capture_output=True,text=True,timeout=180)
    if p.returncode:raise RuntimeError("test service command failed: "+args[0]+" "+p.stderr[-1500:])
    return p.stdout.strip()
def save(path,value):
    path.write_text(json.dumps(value,indent=2)+"\n");path.chmod(0o600)
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action",choices=("start","stop","status"));parser.add_argument("--root",type=Path,required=True)
    args=parser.parse_args();assert "192.168.3.88" in command("hostname","-I").split()
    root=args.root.resolve();state=root/"services.json"
    if args.action=="start":
        root.mkdir(parents=True,exist_ok=False);root.chmod(0o700)
        token=secrets.token_hex(6);label="com.titan.persistence-test="+str(root)
        pg_name="titan-m3-pg-"+token;s3_name="titan-m3-s3-"+token
        pg_password=secrets.token_hex(24);s3_key="titan"+secrets.token_hex(8);s3_secret=secrets.token_hex(32)
        pg_env=root/"postgres.env";pg_env.write_text("POSTGRES_USER=titan\nPOSTGRES_DB=titan\nPOSTGRES_PASSWORD="+pg_password+"\n");pg_env.chmod(0o600)
        s3_env=root/"minio.env";s3_env.write_text("MINIO_ROOT_USER="+s3_key+"\nMINIO_ROOT_PASSWORD="+s3_secret+"\n");s3_env.chmod(0o600)
        volumes=["titan-m3-pg-data-"+token,"titan-m3-s3-data-"+token]
        for volume in volumes:command("docker","volume","create","--label",label,volume)
        command("docker","run","--rm","--user","0","--label",label,"--entrypoint","/bin/sh","-v",volumes[1]+":/data",S3_IMAGE,"-c","chown minio:minio /data && chmod 700 /data")
        created=[]
        try:
            created.append(command("docker","run","-d","--name",pg_name,"--label",label,"--restart","no","--env-file",str(pg_env),"-p","127.0.0.1::5432","-v",volumes[0]+":/var/lib/postgresql/data",PG_IMAGE))
            created.append(command("docker","run","-d","--name",s3_name,"--label",label,"--restart","no","--env-file",str(s3_env),"-p","127.0.0.1::9000","-v",volumes[1]+":/data",S3_IMAGE,"server","/data","--address",":9000"))
            pg_port=json.loads(command("docker","inspect","--format",'{{json .NetworkSettings.Ports}}',pg_name))["5432/tcp"][0]["HostPort"]
            s3_port=json.loads(command("docker","inspect","--format",'{{json .NetworkSettings.Ports}}',s3_name))["9000/tcp"][0]["HostPort"]
            env={"TITAN_M3_PG_DSN":f"host=127.0.0.1 port={pg_port} user=titan password={pg_password} dbname=titan sslmode=disable","TITAN_M3_S3_KEY":s3_key,"TITAN_M3_S3_SECRET":s3_secret}
            save(root/"client-env.json",env)
            profile={"schema_version":1,"postgres":{"dsn_env":"TITAN_M3_PG_DSN","allow_insecure_loopback":True,"root_certificate":None,"statement_timeout_ms":2000,"connect_timeout_ms":2000},"s3":{"endpoint":"http://127.0.0.1:"+s3_port,"region":"us-east-1","bucket":"titan-m3-"+token,"prefix":"titan-node","access_key_env":"TITAN_M3_S3_KEY","secret_key_env":"TITAN_M3_S3_SECRET","session_token_env":None,"allow_insecure_loopback":True,"timeout_ms":30000}}
            save(root/"profile.json",profile)
            save(state,{"root":str(root),"postgres":pg_name,"s3":s3_name,"container_ids":created,"volumes":volumes,"profile":str(root/"profile.json"),"environment":str(root/"client-env.json"),"postgres_image":command("docker","image","inspect","--format","{{.Id}}",PG_IMAGE),"s3_image":command("docker","image","inspect","--format","{{.Id}}",S3_IMAGE),"ports":{"postgres":pg_port,"s3":s3_port}})
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                p=subprocess.run(["docker","exec",pg_name,"pg_isready","-U","titan","-d","titan"],capture_output=True)
                if p.returncode==0:break
                time.sleep(.2)
            else:raise RuntimeError("isolated postgres did not become ready")
            deadline=time.monotonic()+60
            while time.monotonic()<deadline:
                try:
                    with urllib.request.urlopen(profile["s3"]["endpoint"]+"/minio/health/ready",timeout=2) as response:
                        if response.status==200:break
                except Exception:time.sleep(.2)
            else:raise RuntimeError("isolated S3 did not become ready")
            print(json.dumps({"started":True,"root":str(root),"services":[pg_name,s3_name],"ports":{"postgres":pg_port,"s3":s3_port}}))
        except BaseException:
            for container in created:subprocess.run(["docker","rm","-f",container],capture_output=True)
            for volume in volumes:subprocess.run(["docker","volume","rm",volume],capture_output=True)
            raise
    else:
        data=json.loads(state.read_text());assert data["root"]==str(root)
        results=[]
        for name in (data["postgres"],data["s3"]):
            actual=command("docker","inspect","--format",'{{index .Config.Labels "com.titan.persistence-test"}}',name)
            assert actual==str(root),"refuse to touch an unrelated container"
            if args.action=="stop":command("docker","rm","-f",name)
            else:results.append({"name":name,"state":command("docker","inspect","--format","{{.State.Status}}",name)})
        if args.action=="stop":
            for name in data["volumes"]:
                assert command("docker","volume","inspect","--format",'{{index .Labels "com.titan.persistence-test"}}',name)==str(root)
                command("docker","volume","rm",name)
        print(json.dumps({"action":args.action,"root":str(root),"services":results,"passed":True}))
if __name__=="__main__":main()
