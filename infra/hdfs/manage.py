#!/usr/bin/env python3
"""WSL-side launcher; uses only Python stdlib and Docker Compose."""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import uuid
from datetime import datetime,timezone
from pathlib import Path

HERE=Path(__file__).resolve().parent
PROJECT=HERE.parent.parent
sys.path.insert(0,str(HERE/'jobs'))
from transfer_gold import discover,validate_id
DEFAULT_SNAPSHOT='2f14f1cbb8e6c322fa283c26'
IMAGE='it5427-hdfs-spark:3.3.4-3.5.7-v1'


def stamp():return datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')+'-'+uuid.uuid4().hex[:8]


def save(path,obj):
 path.parent.mkdir(parents=True,exist_ok=True)
 with path.open('x',encoding='utf-8') as f:json.dump(obj,f,ensure_ascii=False,indent=2)


class Runner:
 def __init__(self,folder):
  self.folder=folder;folder.mkdir(parents=True,exist_ok=False)
  self.env=os.environ.copy();self.env['LOCAL_UID']=str(os.getuid());self.env['LOCAL_GID']=str(os.getgid())
  self.compose=['docker','compose','-f',str(HERE/'compose.yaml')]
  self.number=0
 def run(self,args,quiet=False):
  self.number+=1;log=self.folder/(f'{self.number:02d}.log')
  print('RUN:', ' '.join(args),flush=True)
  chunks=[]
  with log.open('x',encoding='utf-8') as out:
   with subprocess.Popen(args,cwd=PROJECT,env=self.env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,
                         text=True,encoding='utf-8',errors='replace') as proc:
    for line in proc.stdout:
     out.write(line);out.flush();chunks.append(line)
     if not quiet:print(line,end='',flush=True)
    code=proc.wait()
  if code:raise RuntimeError(f'Command failed ({code}); see {log}')
  return ''.join(chunks)
 def dc(self,*args,quiet=False):return self.run(self.compose+list(args),quiet)


def preflight(r,snapshot):
 if os.getuid()==0:raise ValueError('Run as your normal WSL user, not sudo/root')
 if not shutil.which('docker'):raise ValueError('Docker CLI is missing')
 r.run(['docker','version']);r.run(['docker','compose','version'])
 r.dc('config','--quiet')
 root,files,tables=discover(PROJECT,snapshot)
 total=sum(p.stat().st_size for p in files)
 if total>512*1024**2:raise ValueError('Gold subset exceeds 512 MiB starter cap; review scope before uploading')
 disk=shutil.disk_usage(PROJECT)
 if disk.free<10*1024**3:raise ValueError('Less than 10 GiB free on project filesystem; resolve space before building')
 memory={}
 for line in Path('/proc/meminfo').read_text().splitlines():
  k,v=line.split(':',1)
  if k in ('MemTotal','MemAvailable','SwapTotal'):memory[k+'_bytes']=int(v.strip().split()[0])*1024
 if memory.get('MemAvailable_bytes',0)<6*1024**3:
  raise ValueError('Less than 6 GiB WSL memory available; close other heavy jobs before continuing')
 result=dict(status='passed',snapshot_id=snapshot,project=str(PROJECT),selected_files=len(files),selected_bytes=total,
  tables=list(tables),project_disk_free_bytes=disk.free,memory=memory,uid=os.getuid(),gid=os.getgid(),
  note='Disk free applies to project filesystem. Docker data may be on another drive; inspect its filesystem after startup.')
 save(r.folder/'preflight.json',result);print(json.dumps(result,indent=2));return result


def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('action',choices=['preflight','up','verify','status','stop'])
 p.add_argument('--snapshot',default=DEFAULT_SNAPSHOT)
 a=p.parse_args();validate_id(a.snapshot)
 if not hasattr(os,'getuid'):raise ValueError('Run this launcher inside WSL')
 report_root=PROJECT/'reports'/'hdfs';run_id=a.action+'-'+stamp()
 r=Runner(report_root/run_id)
 try:
  if a.action=='preflight':preflight(r,a.snapshot)
  elif a.action=='up':
   preflight(r,a.snapshot)
   r.dc('build','namenode')
   r.dc('up','-d','--wait','--wait-timeout','240','namenode','datanode')
   r.dc('ps')
   r.dc('exec','-T','namenode','hdfs','dfsadmin','-report')
   r.dc('exec','-T','datanode','df','-h','/data/data')
   image=r.run(['docker','image','inspect',IMAGE,'--format','{{.Id}}'],quiet=True).strip()
   save(r.folder/'up.json',{'status':'healthy','image_id':image,'ui':'http://localhost:9870','run_id':run_id})
  elif a.action=='verify':
   r.dc('exec','-T','namenode','python','/opt/it5427/health.py','namenode')
   r.dc('exec','-T','datanode','python','/opt/it5427/health.py','datanode')
   base='/reports/'+run_id
   r.dc('run','--rm','--no-deps','client','python','/opt/it5427/transfer_gold.py',
        '--snapshot',a.snapshot,'--report',base+'/import.json')
   r.dc('run','--rm','--no-deps','client','timeout','--signal=TERM','--kill-after=20s','600s',
        'spark-submit','--master','local[2]','--driver-memory','2g',
        '--conf','spark.sql.adaptive.enabled=false',
        '/project/scripts/spark_hdfs_demo_v2.py','--snapshot',a.snapshot,'--run-id',run_id,'--report',base+'/spark_demo.json')
   fsck=r.dc('exec','-T','namenode','hdfs','fsck','/it5427/snapshots/'+a.snapshot,'-files','-blocks')
   if not re.search(r'\bHEALTHY\b',fsck):raise ValueError('fsck did not confirm HEALTHY; review log')
   imported=json.loads((r.folder/'import.json').read_text());demo=json.loads((r.folder/'spark_demo.json').read_text())
   if imported['status'] not in ('imported_verified','already_present_verified') or demo['status']!='passed':
    raise ValueError('Unexpected validation report status')
   image=r.run(['docker','image','inspect',IMAGE,'--format','{{.Id}}'],quiet=True).strip()
   save(r.folder/'summary.json',dict(status='passed',run_id=run_id,snapshot_id=a.snapshot,image_id=image,
    import_status=imported['status'],verified_files=imported['verified_files'],input_counts=demo['input_counts'],
    hdfs_fsck='HEALTHY',spark_validation=demo['validation'],runtime=demo['runtime'],scope=demo['scope']))
  elif a.action=='status':
   r.dc('ps');r.dc('exec','-T','namenode','hdfs','dfsadmin','-report')
  elif a.action=='stop':r.dc('stop','datanode','namenode')
 except Exception as e:
  save(r.folder/'failure.json',{'status':'failed','message':str(e),'action':a.action,'run_id':run_id})
  print('FAILED:',e,file=sys.stderr)
  print('Logs:',r.folder,file=sys.stderr);return 1
 print('DONE. Reports:',r.folder)
 return 0

if __name__=='__main__':sys.exit(main())
