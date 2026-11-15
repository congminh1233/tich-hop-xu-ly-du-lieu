"""Airflow adapter: replay an approved full ETL plan with retry-safe reports."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import traceback
import uuid
from types import SimpleNamespace
from urllib.parse import urlsplit

PROJECT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(PROJECT));sys.path.insert(0,str(PROJECT/'scripts'))
PHASES=('plan','upload','reference','build','compare','finalize')
OUTPUTS={'upload':'bronze_transfer.json','reference':'reference.json',
         'build':'build.json','compare':'comparison.json'}


def read(p):return json.loads(Path(p).read_text(encoding='utf-8'))
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def run_key(run_id):return 'airflow-'+hashlib.sha256(run_id.encode()).hexdigest()[:24]
def write(p,value):
    p=Path(p);p.parent.mkdir(parents=True,exist_ok=True)
    tmp=p.with_name(p.name+'.tmp-'+uuid.uuid4().hex)
    tmp.write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')
    os.replace(tmp,p)


def validate_approved(project,config):
    from hdfs_etl_common import validate_plan,code_hash
    name=config['approved_report']
    if not re.fullmatch(r'etl-full-[A-Za-z0-9-]+',name):raise ValueError('Invalid approved report name')
    folder=project/'reports/hdfs'/name
    summary=read(folder/'summary.json');plan=validate_plan(read(folder/'plan.json'))
    if sha(folder/'plan.json')!=config['approved_plan_sha256']:raise ValueError('Approved plan changed')
    if summary.get('status')!='passed' or summary.get('scope')!='full':raise ValueError('Full reference not passed')
    if summary['hdfs_snapshot']!=plan['plan_id'] or summary['code_hash']!=plan['code_hash']:
        raise ValueError('Reference summary/plan mismatch')
    if code_hash(project)!=plan['code_hash']:raise ValueError('ETL code changed since successful full run')
    if plan['scope']!='full' or len(plan['inputs'])!=28:raise ValueError('Expected approved full 28-file plan')
    for rel,expected in config['adapter_hashes'].items():
        if sha(project/rel)!=expected:raise ValueError('Airflow adapter changed; run configure again')
    return plan,summary


def promote(attempt,root,name):write(root/name,read(attempt/name))


def finish(root,plan,summary,run_id,fsck_text):
    comparison=read(root/'comparison.json');output=read(root/'build.json')
    if comparison.get('status')!='passed' or not comparison.get('source_accounting_equal'):
        raise ValueError('Comparison did not pass')
    if output.get('status')!='committed' or output['snapshot_id']!=plan['plan_id']:
        raise ValueError('Build output identity mismatch')
    expected=plan['root'].rstrip('/')+'/snapshots/'+plan['plan_id']
    if output['uri']!=expected or summary['hdfs_snapshot']!=plan['plan_id']:
        raise ValueError('Unexpected HDFS snapshot')
    if not re.search(r'Status:\s+HEALTHY\b',fsck_text):raise ValueError('HDFS fsck is not HEALTHY')
    return {'status':'passed','airflow_run_id':run_id,'approved_snapshot':summary['hdfs_snapshot'],
            'hdfs_snapshot':output['snapshot_id'],'hdfs_uri':expected,'same_snapshot_id':True,
            'hdfs_fsck':'HEALTHY','source_accounting_equal':True,'gold_tables':{
                k:v for k,v in comparison['tables'].items() if k.startswith('gold/')},
            'scope':'Airflow replay of approved 28-file batch; same immutable snapshot; not new-data backfill',
            'retry_probe_triggered':(root/'retry_probe.marker').exists()}


def execute(a,project=PROJECT,reports=Path('/reports')):
    config=read(project/'infra/airflow/config/runtime.json')
    plan,summary=validate_approved(project,config)
    root=reports/run_key(a.run_id);root.mkdir(parents=True,exist_ok=True)
    # Clearing any task invalidates a prior passed summary until finalize runs again.
    (root/'summary.json').unlink(missing_ok=True)
    attempt=root/'attempts'/(a.phase+'-'+str(a.try_number)+'-'+uuid.uuid4().hex[:8]);attempt.mkdir(parents=True)
    started=time.monotonic()
    write(attempt/'task.json',{'phase':a.phase,'try_number':a.try_number,'airflow_run_id':a.run_id})
    try:
        if a.phase=='plan':
            if (root/'plan.json').exists() and read(root/'plan.json')!=plan:raise ValueError('Run plan conflicts')
            write(root/'plan.json',plan)
            write(root/'run.json',{'run_id':a.run_id,'approved_report':config['approved_report']})
        else:
            if read(root/'plan.json')!=plan:raise ValueError('Plan dependency is missing or changed')
            write(attempt/'plan.json',plan)
            if a.phase=='upload' and a.fail_once=='true':
                try:
                    with (root/'retry_probe.marker').open('x') as f:f.write('Intentional failure before upload; no input modified.\n')
                except FileExistsError:pass
                else:raise RuntimeError('INTENTIONAL_RETRY_PROBE: first upload attempt fails before any data write')
            if a.phase=='finalize':
                output=read(root/'build.json')
                expected=plan['root'].rstrip('/')+'/snapshots/'+plan['plan_id']
                if output['uri']!=expected:raise ValueError('Unexpected fsck destination')
                r=subprocess.run(['hdfs','fsck',urlsplit(expected).path,'-files','-blocks'],
                                 capture_output=True,text=True,timeout=120)
                fsck=r.stdout+'\n'+r.stderr
                (attempt/'fsck.log').write_text(fsck)
                if r.returncode:raise RuntimeError('HDFS fsck command failed; see attempt/fsck.log')
                write(root/'summary.json',finish(root,plan,summary,a.run_id,fsck))
            else:
                if a.phase=='compare':
                    for name in ('build.json','reference.json'):write(attempt/name,read(root/name))
                from run_hdfs_etl import internal
                internal(SimpleNamespace(step=a.phase,report=str(attempt),scope='full',snapshot=plan['baseline_snapshot']))
                promote(attempt,root,OUTPUTS[a.phase])
        write(attempt/'result.json',{'status':'passed','elapsed_seconds':time.monotonic()-started})
        print('TASK_PASSED',a.phase,'REPORT',root,flush=True)
    except Exception as exc:
        write(attempt/'failure.json',{'status':'failed','error':str(exc),'traceback':traceback.format_exc(),
                                      'elapsed_seconds':time.monotonic()-started})
        raise


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--phase',choices=PHASES,required=True)
    p.add_argument('--run-id',required=True)
    p.add_argument('--try-number',type=int,default=1)
    p.add_argument('--fail-once',choices=['true','false'],default='false')
    a=p.parse_args();execute(a)

if __name__=='__main__':main()
