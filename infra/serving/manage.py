#!/usr/bin/env python3
"""Standalone serving stack. Does not modify ETL/Airflow files or volumes."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys
import uuid
import zipfile

HERE=Path(__file__).resolve().parent
PROJECT=HERE.parents[1]
sys.path.insert(0,str(HERE/'app'))
from core import VERSIONS, inside, manifest, read, sha


def write(path,value):
    path.parent.mkdir(parents=True,exist_ok=True)
    path.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')


def dc(*args, capture=False):
    cmd=['docker','compose','--env-file',str(HERE/'.env'),'-f',str(HERE/'compose.yaml'),*args]
    return subprocess.run(cmd,cwd=PROJECT,check=True,text=True,capture_output=capture)


def configure(experiments):
    if not (PROJECT/'toniot').is_dir(): raise ValueError('Copy infra/serving into your project first')
    folders=[inside(PROJECT,p) for p in experiments] if experiments else []
    skipped=[]
    if not folders:
        base=PROJECT/'experiments'
        for p in sorted(base.rglob('summary.json')) if base.exists() else []:
            if any(s.startswith('.') for s in p.relative_to(base).parts): continue
            meta=read(p)
            if meta.get('script_version') not in VERSIONS: continue
            folders.append(p.parent)
    items=[];seen=set()
    for folder in folders:
        item=manifest(PROJECT,folder)
        if item['experiment_id'] in seen:
            skipped.append(str(folder));continue
        seen.add(item['experiment_id']);items.append(item)
    if not items:
        raise ValueError('No supported scored experiment under experiments/. Use configure --experiment <relative-folder>; need summary.json, scores.parquet, models.json, input_checksums.json.')
    documents=[]
    candidates=[PROJECT/'reports/quality_audit.json',PROJECT/'reports/catalog.json']
    # Only root summaries/comparisons; never .env, runtime credentials or task logs.
    for pattern in ('etl-full-*','airflow-*'):
        for folder in sorted((PROJECT/'reports/hdfs').glob(pattern)):
            p=folder/'summary.json'
            if p.is_file() and read(p).get('status')=='passed':
                candidates.append(p)
                if (folder/'comparison.json').is_file(): candidates.append(folder/'comparison.json')
    for p in candidates:
        if p.is_file():
            p=inside(PROJECT,p)
            documents.append({'path':p.relative_to(PROJECT).as_posix(),'sha256':sha(p)})
    write(HERE/'config.json',{'version':1,'experiments':items,'documents':documents})
    env=HERE/'.env'
    if not env.exists():
        env.write_text('POSTGRES_PASSWORD='+secrets.token_hex(24)+'\nSERVING_UID='+str(os.getuid())+'\nSERVING_GID='+str(os.getgid())+'\n')
        env.chmod(0o600)
    (PROJECT/'reports/serving').mkdir(parents=True,exist_ok=True)
    dc('config','--quiet')
    for item in items: print('SELECTED',item['path'],item['experiment_id'][:12])
    print('CONFIGURED',len(items),'experiments;',len(documents),'reports; duplicate bundles skipped:',len(skipped))


def job(command):
    if not (HERE/'config.json').is_file(): raise ValueError('Run configure first')
    run_id=command+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:6]
    folder=PROJECT/'reports/serving'/run_id;folder.mkdir(parents=True)
    write(HERE/'last_run.json',{'run_id':run_id})
    cmd=['run','--rm','--no-deps','-T','loader','python','-u','/app/loader.py',command,'--report','/reports/'+run_id+'/summary.json']
    args=['docker','compose','--env-file',str(HERE/'.env'),'-f',str(HERE/'compose.yaml'),*cmd]
    with (folder/'run.log').open('w') as log:
        proc=subprocess.Popen(args,cwd=PROJECT,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,bufsize=1)
        for line in proc.stdout:
            print(line,end='',flush=True);log.write(line);log.flush()
        status=proc.wait()
    print('REPORT',folder)
    if status: raise RuntimeError('Serving command failed; run manage.py report to export diagnostics')


def report():
    run=read(HERE/'last_run.json')['run_id']
    folder=PROJECT/'reports/serving'/run
    output=folder.with_suffix('.zip')
    with zipfile.ZipFile(output,'w',zipfile.ZIP_DEFLATED) as z:
        for p in folder.glob('*'):
            if p.suffix in ('.json','.log'): z.write(p,p.name)
        z.write(HERE/'config.json','config.json')
        state=dc('ps',capture=True)
        z.writestr('containers.txt',state.stdout)
    print('SEND_THIS_FILE:',output)


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('command',choices=['configure','up','load','verify','dashboard','status','report','stop'])
    p.add_argument('--experiment',action='append',default=[],help='Optional relative experiment folder; repeat to select multiple')
    a=p.parse_args()
    if a.command=='configure': configure(a.experiment)
    elif a.command=='up':
        if not (HERE/'config.json').is_file(): raise ValueError('Run configure first')
        dc('build','loader');dc('up','-d','--wait','--wait-timeout','180','postgres')
        print('PostgreSQL ready. Next: python3 infra/serving/manage.py load')
    elif a.command=='load':job('load')
    elif a.command=='verify':job('verify-replay')
    elif a.command=='dashboard':
        dc('up','-d','--no-build','--wait','--wait-timeout','120','dashboard')
        print('DASHBOARD: http://localhost:8501')
    elif a.command=='status':dc('ps')
    elif a.command=='report':report()
    elif a.command=='stop':dc('stop')

if __name__=='__main__':
    try:main()
    except Exception as exc:
        print('FAILED:',exc,file=sys.stderr);sys.exit(1)
