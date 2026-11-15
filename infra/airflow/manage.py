#!/usr/bin/env python3
"""WSL helper for the additive Airflow lab. Does not modify the HDFS stack."""
import argparse
import base64
from datetime import datetime,timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import uuid
import zipfile

HERE=Path(__file__).resolve().parent
PROJECT=HERE.parents[1]
DAG='toniot_hdfs_batch'
APPROVED='etl-full-20261024T171623Z-c82816ee'
ADAPTERS=['scripts/airflow_etl_task.py','infra/airflow/dags/toniot_hdfs_batch.py']


def read(p):return json.loads(Path(p).read_text())
def save(p,obj):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_suffix(p.suffix+'.tmp');tmp.write_text(json.dumps(obj,ensure_ascii=False,indent=2));tmp.replace(p)
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def command(args,capture=False,timeout=None,check=True):
    if not capture:print('RUN',' '.join(map(str,args)),flush=True)
    return subprocess.run(list(map(str,args)),capture_output=capture,text=True,check=check,timeout=timeout)
def dc(*args,**kw):
    return command(['docker','compose','--env-file',HERE/'.env','-f',HERE/'compose.yaml',*args],**kw)
def key(run_id):return 'airflow-'+hashlib.sha256(run_id.encode()).hexdigest()[:24]


def configure(approved):
    if not hasattr(os,'getuid') or os.getuid()==0:raise ValueError('Run as your normal WSL user, not sudo')
    if '/' in approved or '\\' in approved or not approved.startswith('etl-full-'):raise ValueError('Invalid report basename')
    report=PROJECT/'reports/hdfs'/approved
    summary=read(report/'summary.json');plan=read(report/'plan.json')
    if summary.get('status')!='passed' or summary.get('scope')!='full':raise ValueError('Full ETL report must be passed')
    sys.path.insert(0,str(PROJECT/'scripts'))
    from hdfs_etl_common import validate_plan,code_hash
    validate_plan(plan)
    if len(plan['inputs'])!=28 or summary['hdfs_snapshot']!=plan['plan_id']:raise ValueError('Full snapshot identity mismatch')
    if code_hash(PROJECT)!=plan['code_hash']:raise ValueError('ETL source changed since approved full run')
    for p in ADAPTERS:
        if not (PROJECT/p).is_file():raise ValueError('Missing '+p)
    command(['docker','version'],capture=True,timeout=30)
    hdc=['docker','compose','-f',PROJECT/'infra/hdfs/compose.yaml']
    networks=[]
    for service in ('namenode','datanode'):
        cid=command(hdc+['ps','-q',service],capture=True,timeout=30).stdout.strip()
        if not cid:raise ValueError(service+' is not running; start existing HDFS stack first')
        obj=json.loads(command(['docker','inspect',cid],capture=True,timeout=30).stdout)[0]
        if obj['State'].get('Health',{}).get('Status')!='healthy':raise ValueError(service+' is not healthy')
        networks.append(set(obj['NetworkSettings']['Networks']))
    common=networks[0]&networks[1]
    if len(common)!=1:raise ValueError('Ambiguous HDFS network: '+str(common))
    probe='it5427-airflow-probe-'+uuid.uuid4().hex[:8]
    try:
        command(hdc+['run','-d','--no-deps','--name',probe,'client','sleep','120'],capture=True,timeout=60)
        obj=json.loads(command(['docker','inspect',probe],capture=True,timeout=30).stdout)[0]
        mounts={m['Destination']:m for m in obj['Mounts']}
        for name in ('/project','/reports'):
            if mounts[name]['Type']!='bind':raise ValueError('Expected existing bind mount '+name)
        image=obj['Config']['Image']
        runtime={'version':1,'approved_report':approved,'approved_plan_sha256':sha(report/'plan.json'),
                 'approved_snapshot':summary['hdfs_snapshot'],'spark_image':image,'spark_image_id':obj['Image'],
                 'hdfs_network':common.pop(),'project_mount_source':mounts['/project']['Source'],
                 'reports_mount_source':mounts['/reports']['Source'],
                 'adapter_hashes':{p:sha(PROJECT/p) for p in ADAPTERS}}
    finally:
        command(['docker','rm','-f',probe],capture=True,timeout=30,check=False)
    if shutil.disk_usage(PROJECT).free<8*1024**3:raise ValueError('Need at least 8 GiB free for Airflow images/logs')
    for name in ('config','logs','state'):(HERE/name).mkdir(exist_ok=True)
    save(HERE/'config/runtime.json',runtime)
    if not (HERE/'.env').exists():
        values={'AIRFLOW_UID':str(os.getuid()),'DOCKER_GID':str(os.stat('/var/run/docker.sock').st_gid),
                'POSTGRES_PASSWORD':secrets.token_hex(24),'FERNET_KEY':base64.urlsafe_b64encode(os.urandom(32)).decode(),
                'JWT_SECRET':secrets.token_hex(32),'API_SECRET':secrets.token_hex(32)}
        with (HERE/'.env').open('x') as f:f.write(''.join(k+'='+v+'\n' for k,v in values.items()))
        (HERE/'.env').chmod(0o600)
    if not (HERE/'state/passwords.json').exists():
        save(HERE/'state/passwords.json',{'admin':secrets.token_urlsafe(18)})
        (HERE/'state/passwords.json').chmod(0o600)
    dc('config','--quiet')
    print('CONFIGURED. Approved snapshot:',runtime['approved_snapshot'])
    print('Next: python3 infra/airflow/manage.py up')


def trigger(fail_once):
    runtime=read(HERE/'config/runtime.json')
    image=json.loads(command(['docker','image','inspect',runtime['spark_image']],capture=True,timeout=30).stdout)[0]
    if image['Id']!=runtime['spark_image_id']:raise ValueError('Spark image changed since configure')
    run_id='it5427_'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'_'+uuid.uuid4().hex[:6]
    dc('exec','-T','scheduler','airflow','dags','unpause',DAG)
    dc('exec','-T','scheduler','airflow','dags','trigger',DAG,'--run-id',run_id,
       '--conf',json.dumps({'fail_once':fail_once}))
    save(HERE/'state/last_run.json',{'run_id':run_id,'fail_once':fail_once})
    print('RUN_ID:',run_id)
    print('UI: http://localhost:8080')
    print('Report folder:',PROJECT/'reports/hdfs'/key(run_id))


def report(run_id):
    run_id=run_id or read(HERE/'state/last_run.json')['run_id']
    folder=PROJECT/'reports/hdfs'/key(run_id);folder.mkdir(parents=True,exist_ok=True)
    state=dc('exec','-T','scheduler','airflow','tasks','states-for-dag-run',DAG,run_id,
             '--output','json',capture=True,check=False,timeout=60)
    (folder/'airflow_task_states.txt').write_text(state.stdout+'\n'+state.stderr)
    # Only reports/logs; no environment secrets, passwords, data or Parquet.
    archive=folder.with_suffix('.zip')
    with zipfile.ZipFile(archive,'w',zipfile.ZIP_DEFLATED) as z:
        for p in sorted(folder.rglob('*')):
            if p.is_file() and p.suffix in ('.json','.txt','.log'):
                data=p.read_bytes()
                if len(data)>5*1024**2:data=b'[tail only]\n'+data[-5*1024**2:]
                z.writestr('reports/'+p.relative_to(folder).as_posix(),data)
        logs=HERE/'logs'/('dag_id='+DAG)/('run_id='+run_id)
        for p in sorted(logs.rglob('*.log')):
            data=p.read_bytes()
            if len(data)>5*1024**2:data=b'[tail only]\n'+data[-5*1024**2:]
            z.writestr('airflow_logs/'+p.relative_to(logs).as_posix(),data)
    if (folder/'summary.json').exists():print(json.dumps(read(folder/'summary.json'),ensure_ascii=False,indent=2))
    else:print('No passed summary yet. Report can still be sent for diagnosis.')
    print('SEND_THIS_FILE',archive)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',choices=['configure','up','status','login','trigger','report','stop'])
    p.add_argument('--approved-report',default=APPROVED)
    p.add_argument('--fail-once',action='store_true')
    p.add_argument('--run-id')
    a=p.parse_args()
    if a.action=='configure':configure(a.approved_report)
    elif a.action=='up':
        if not (HERE/'config/runtime.json').exists():raise ValueError('Run configure first')
        dc('build','api-server')
        dc('up','-d','--wait','--wait-timeout','180','postgres')
        dc('run','--rm','init')
        dc('up','-d','--wait','--wait-timeout','300','api-server','scheduler','dag-processor')
        dc('exec','-T','scheduler','python','-c','import docker; print("Docker API:", docker.from_env().ping())')
        dc('exec','-T','scheduler','airflow','dags','list-import-errors','--output','json')
        print('Airflow started: http://localhost:8080 ; run manage.py login for credentials.')
    elif a.action=='status':
        dc('ps');dc('exec','-T','scheduler','airflow','dags','list-import-errors','--output','json')
        dc('exec','-T','scheduler','airflow','dags','list','--output','table')
    elif a.action=='login':print('URL: http://localhost:8080\nUsername: admin\nPassword:',read(HERE/'state/passwords.json')['admin'])
    elif a.action=='trigger':trigger(a.fail_once)
    elif a.action=='report':report(a.run_id)
    elif a.action=='stop':dc('stop')

if __name__=='__main__':
    try:main()
    except (Exception,KeyboardInterrupt) as exc:
        print('FAILED:',str(exc),file=sys.stderr);sys.exit(1)
