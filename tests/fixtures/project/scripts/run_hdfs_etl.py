#!/usr/bin/env python3
"""WSL launcher: smoke/full HDFS ETL, frozen source selection, reference comparison."""
import argparse
import faulthandler
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import uuid
import zipfile
from urllib.parse import urlsplit
from datetime import datetime,timezone

PROJECT=Path(__file__).resolve().parent.parent
sys.path.insert(0,str(PROJECT))
from hdfs_etl_common import make_plan,validate_plan,code_hash,read,save,log,transfer_one,local_reference,ROOT


def internal(a):
    report=Path(a.report)
    if a.step=='plan':
        plan=make_plan(PROJECT,a.snapshot,a.scope)
        save(report/'plan.json',plan)
        # Conservative lab estimate, not a disk-capacity guarantee for arbitrary data.
        reserve=max(10*1024**3,2*plan['total_upload_bytes']+8*plan['records_bytes'])
        free=shutil.disk_usage(PROJECT).free
        save(report/'storage_preflight.json',{'project_free_bytes':free,'estimated_required_bytes':reserve,
              'upload_bytes':plan['total_upload_bytes'],'records_bytes':plan['records_bytes'],
              'note':'Docker virtual disk may be on another drive; monitor actual Windows disk too.'})
        log(f'PLAN {a.scope}: {len(plan["inputs"])} files; upload={plan["total_upload_bytes"]/1024**3:.2f} GiB; '
            f'estimated reserve={reserve/1024**3:.2f} GiB; project free={free/1024**3:.2f} GiB')
        if free<reserve:raise ValueError('Insufficient project-drive free space for conservative ETL estimate')
        return
    plan=validate_plan(read(report/'plan.json'))
    if code_hash(PROJECT)!=plan['code_hash']:raise ValueError('Source code changed after plan')
    if a.step=='upload':
        sys.path.insert(0,str(PROJECT/'infra/hdfs/jobs'))
        from transfer_gold import Hdfs
        class LoggedHdfs(Hdfs):
            def run(self,*args,**kwargs):
                try:return super().run(*args,**kwargs)
                except subprocess.CalledProcessError as exc:
                    log('HDFS command error: '+str(exc.stderr or exc.stdout or exc))
                    raise
        fs=LoggedHdfs();fs.run('-ls','/')
        disk=fs.run('-df','/').stdout
        # Hadoop `dfs -df` without -h: Filesystem Size Used Available Use%.
        remaining=int(disk.strip().splitlines()[-1].split()[-2])
        required=max(5*1024**3,plan['total_upload_bytes']+8*plan['records_bytes'])
        save(report/'hdfs_storage_preflight.json',{'hdfs_available_bytes':remaining,
             'estimated_required_bytes':required,'raw_df_output':disk})
        if remaining<required:raise ValueError('Insufficient HDFS capacity before Bronze upload')
        results=[]
        for item in plan['inputs']:
            action=transfer_one(fs,PROJECT,item)
            results.append({'file_name':item['manifest']['file_name'],'action':action})
            log(action+' '+item['manifest']['file_name'])
        save(report/'bronze_transfer.json',{'status':'passed','files':results})
    elif a.step=='reference':
        uri=local_reference(PROJECT,plan,report)
        save(report/'reference.json',{'uri':uri,'scope':plan['scope']})
    elif a.step in ('build','compare'):
        from hdfs_etl_core import session,build,HadoopStore
        spark=session();spark.sparkContext.setLogLevel('WARN')
        try:
            if a.step=='build':
                fs=HadoopStore(spark,ROOT)
                if fs.fs.getStatus().getRemaining()<max(5*1024**3,8*plan['records_bytes']):
                    raise ValueError('HDFS filesystem has insufficient free space')
                dest,meta=build(plan,spark)
                save(report/'build.json',{'status':'committed','uri':dest,'snapshot_id':meta['snapshot_id'],
                                        'tables':meta['tables'],'source_accounting':meta['source_accounting'],
                                        'elapsed_seconds':meta['elapsed_seconds']})
            else:
                from hdfs_etl_compare import compare
                output=read(report/'build.json');reference=read(report/'reference.json')['uri']
                meta=HadoopStore(spark,output['uri']).read(output['uri']+'/snapshot.json')
                if meta['plan']!=plan or meta['status']!='committed':raise ValueError('Committed metadata changed')
                result=compare(spark,reference,output['uri'],meta)
                save(report/'comparison.json',result)
        finally:spark.stop()


def run_command(cmd,logfile,timeout,container=None):
    log('RUN '+' '.join(cmd))
    with Path(logfile).open('x',encoding='utf-8') as stream:
        proc=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,text=True,errors='replace')
        def drain():
            for line in proc.stdout:
                stream.write(line);stream.flush();print(line,end='',flush=True)
        reader=threading.Thread(target=drain,daemon=True);reader.start()
        try:code=proc.wait(timeout=timeout)
        except (subprocess.TimeoutExpired,KeyboardInterrupt):
            if container:
                try:subprocess.run(['docker','stop','--timeout','15',container],stdout=stream,stderr=stream,timeout=25)
                except subprocess.TimeoutExpired:pass
            proc.kill();proc.wait();code=124
        reader.join(5)
    if code:raise RuntimeError(f'Command failed ({code}); see {logfile}')


def host(a):
    if not hasattr(os,'getuid') or os.getuid()==0:raise SystemExit('Run in WSL as normal user, not sudo')
    compose=PROJECT/'infra/hdfs/compose.yaml'
    if not compose.is_file():raise SystemExit('Copy all four new files into project/scripts first')
    scope=a.scope if a.action=='plan' else a.action
    if scope=='full' and a.action!='plan':
        prior=list((PROJECT/'reports/hdfs').glob('etl-smoke-*/summary.json'))
        valid=[read(p) for p in prior]
        if not any(d.get('status')=='passed' and d.get('baseline_snapshot')==a.snapshot
                   and d.get('code_hash')==code_hash(PROJECT) for d in valid):
            raise SystemExit('Run smoke successfully with this code and snapshot before full')
    run_id='etl-'+scope+'-'+datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]
    folder=PROJECT/'reports/hdfs'/run_id;folder.mkdir(parents=True)
    dc=['docker','compose','-f',str(compose)]
    counter=0
    def command(args,timeout=60,container=None):
        nonlocal counter
        counter+=1;path=folder/f'{counter:02d}.log'
        run_command(args,path,timeout,container)
        return path
    success=False
    try:
        command(['docker','version'])
        command(dc+['config','--quiet'])
        for service in ('namenode','datanode'):
            command(dc+['exec','-T',service,'python','/opt/it5427/health.py',service])
        for step in ('plan',) if a.action=='plan' else ('plan','upload','reference','build','compare'):
            name='it5427-'+run_id+'-'+step
            limit=7200 if scope=='full' else 1800
            args=dc+['run','--rm','--no-deps','--name',name,
                     '-e','PYTHONPATH=/project:/project/scripts',
                     'client','timeout','--signal=TERM','--kill-after=20s',str(limit)+'s']
            if step in ('reference','build','compare'):
                args+=['spark-submit','--master','local[2]','--driver-memory','2g',
                       '--conf','spark.sql.adaptive.enabled=false']
            else:args+=['python','-u']
            if step=='reference':args+=['--conf','spark.hadoop.fs.defaultFS=file:///']
            args+=['/project/scripts/run_hdfs_etl.py','--step',step,'--scope',scope,'--snapshot',a.snapshot,
                   '--report','/reports/'+run_id]
            command(args,limit+60,name)
        if a.action=='plan':
            log('PLAN ONLY: no uploads or ETL performed')
            success=True
        else:
            plan=read(folder/'plan.json');build_result=read(folder/'build.json')
            fsck=command(dc+['exec','-T','namenode','hdfs','fsck',urlsplit(build_result['uri']).path,'-files','-blocks'],120)
            if not re.search(r'Status:\s+HEALTHY\b',fsck.read_text()):raise ValueError('fsck did not confirm HEALTHY')
            comparison=read(folder/'comparison.json')
            if comparison.get('status')!='passed':raise ValueError('Comparison did not pass')
            summary={'status':'passed','scope':scope,'run_id':run_id,'baseline_snapshot':a.snapshot,
                     'code_hash':plan['code_hash'],'hdfs_snapshot':build_result['snapshot_id'],
                     'hdfs_uri':build_result['uri'],'hdfs_fsck':'HEALTHY',
                     'input_file_count':len(plan['inputs']),'gold_tables':{
                         k:v for k,v in comparison['tables'].items() if k.startswith('gold/')},
                     'source_accounting_equal':True,'master':'local[2]',
                     'scope_note':'Local CSV ingest reused; ETL Bronze Parquet -> Silver -> Gold runs on HDFS; no distributed-worker speedup claim'}
            save(folder/'summary.json',summary);success=True
            log('PASSED HDFS ETL '+scope)
    except Exception as exc:
        save(folder/'failure.json',{'status':'failed','error':str(exc),'run_id':run_id})
        log('FAILED '+str(exc))
    finally:
        # Only top-level reports/logs; exclude local_reference Parquet and source data.
        archive=folder.with_suffix('.zip')
        with zipfile.ZipFile(archive,'x',zipfile.ZIP_DEFLATED) as z:
            for p in sorted(folder.iterdir()):
                if p.is_file() and p.suffix in ('.json','.log','.txt'):z.write(p,p.relative_to(folder.parent))
        log('SEND_THIS_FILE '+str(archive))
    return 0 if success else 1


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('action',nargs='?',choices=['smoke','full','plan'],default='smoke')
    p.add_argument('--scope',choices=['smoke','full'],default='smoke')
    p.add_argument('--snapshot',default='2f14f1cbb8e6c322fa283c26')
    p.add_argument('--step',choices=['plan','upload','reference','build','compare'])
    p.add_argument('--report')
    a=p.parse_args()
    if a.step:
        if not a.report:p.error('--report required with --step')
        faulthandler.enable();faulthandler.dump_traceback_later(300,repeat=True)
        try:internal(a)
        finally:faulthandler.cancel_dump_traceback_later()
        return 0
    return host(a)


if __name__=='__main__':sys.exit(main())
