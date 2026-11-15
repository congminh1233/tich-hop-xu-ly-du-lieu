#!/usr/bin/env python3
"""Copy a bounded, immutable Gold subset into HDFS; verify every file byte."""
import argparse
import hashlib
import json
import re
import subprocess
import tempfile
import uuid
from pathlib import Path
from datetime import datetime,timezone

VERSION='hdfs-gold-import-1.0'
REQUIRED=('host_window_features','sensor_window_features')
OPTIONAL=('network_overview',)


def validate_id(value):
 if not re.fullmatch(r'[a-f0-9]{24}',value):raise ValueError('Snapshot must be its 24-character lowercase hex ID')
 return value


def sha_file(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
 return h.hexdigest()


def discover(project,snapshot):
 validate_id(snapshot);project=Path(project).resolve();root=project/'lake'/'snapshots'/snapshot
 if root.is_symlink() or root.resolve()!=root:raise ValueError('Snapshot path must not contain symlinks')
 meta_path=root/'snapshot.json'
 meta=json.loads(meta_path.read_text(encoding='utf-8'))
 if meta.get('snapshot_id')!=snapshot or meta.get('status')!='committed':raise ValueError('Snapshot not committed or identity mismatch')
 files=[meta_path];tables={}
 for name in REQUIRED+OPTIONAL:
  folder=root/'gold'/name
  if not folder.exists() and name in OPTIONAL:continue
  parts=sorted(folder.rglob('*.parquet'))
  if not parts:raise ValueError(f'Missing/empty Gold table: {folder}')
  tables[name]=[p.relative_to(root).as_posix() for p in parts]
  files+=parts
 for p in files:
  if p.resolve()!=p or not p.is_file():raise ValueError(f'Unexpected symlink/non-file: {p}')
 return root,files,tables


def build_manifest(project,snapshot,max_bytes=512*1024**2):
 import pyarrow.parquet as pq
 root,files,tables=discover(project,snapshot)
 total=sum(p.stat().st_size for p in files)
 if total>max_bytes:raise ValueError(f'Gold subset {total} bytes exceeds safety cap {max_bytes}')
 records=[];rows={name:0 for name in tables}
 for p in files:
  rel=p.relative_to(root).as_posix()
  count=pq.ParquetFile(p).metadata.num_rows if p.suffix=='.parquet' else None
  records.append(dict(path=rel,bytes=p.stat().st_size,sha256=sha_file(p),parquet_rows=count))
  if count is not None:rows[p.relative_to(root).parts[1]]+=count
 return root,dict(version=VERSION,snapshot_id=snapshot,scope='Gold subset only, plus original snapshot metadata',
                  files=records,total_bytes=total,table_rows=rows)


class Hdfs:
 def run(self,*args,capture=True):
  return subprocess.run(['hdfs','dfs',*map(str,args)],check=True,text=True,capture_output=capture)
 def exists(self,path):
  r=subprocess.run(['hdfs','dfs','-test','-e',path],capture_output=True,text=True)
  if r.returncode not in (0,1):raise RuntimeError(r.stderr)
  return r.returncode==0
 def read_json(self,path):return json.loads(self.run('-cat',path).stdout)
 def hash(self,path):
  h=hashlib.sha256()
  with tempfile.TemporaryFile() as errors:
   proc=subprocess.Popen(['hdfs','dfs','-cat',path],stdout=subprocess.PIPE,stderr=errors)
   try:
    for chunk in iter(lambda:proc.stdout.read(1048576),b''):h.update(chunk)
    code=proc.wait()
   finally:
    proc.stdout.close()
    if proc.poll() is None:proc.kill();proc.wait()
   if code:
    errors.seek(0);raise RuntimeError(errors.read().decode(errors='replace'))
  return h.hexdigest()


def verify(fs,dest,manifest):
 for row in manifest['files']:
  if fs.hash(dest+'/'+row['path'])!=row['sha256']:raise ValueError('HDFS checksum mismatch: '+row['path'])


def transfer(fs,root,manifest):
 sid=manifest['snapshot_id'];dest='/it5427/snapshots/'+sid
 fs.run('-ls','/') # Ensure daemon access before interpreting a missing target.
 fs.run('-mkdir','-p','/it5427/snapshots','/it5427/_staging','/it5427/_locks')
 lock='/it5427/_locks/'+sid
 fs.run('-mkdir',lock) # Exclusive among cooperating import jobs; stale locks are not auto-deleted.
 try:
  if fs.exists(dest):
   if fs.read_json(dest+'/import_manifest.json')!=manifest:raise ValueError('Existing HDFS snapshot has different manifest; refusing overwrite')
   verify(fs,dest,manifest)
   return dict(status='already_present_verified',hdfs_path='hdfs://namenode:9000'+dest)
  stage='/it5427/_staging/'+sid+'-'+uuid.uuid4().hex
  fs.run('-mkdir',stage)
  parents=sorted({str(Path(row['path']).parent) for row in manifest['files'] if str(Path(row['path']).parent)!='.'})
  for parent in parents:fs.run('-mkdir','-p',stage+'/'+parent)
  for row in manifest['files']:
   path=root/row['path']
   if sha_file(path)!=row['sha256']:raise ValueError('Local file changed during import: '+str(path))
   print('Upload',row['path'],flush=True)
   fs.run('-put',path,stage+'/'+row['path'])
  verify(fs,stage,manifest)
  with tempfile.TemporaryDirectory(prefix='it5427-manifest-') as temp:
   local=Path(temp)/'import_manifest.json';local.write_text(json.dumps(manifest,sort_keys=True,indent=2),encoding='utf-8')
   fs.run('-put',local,stage+'/import_manifest.json')
  if fs.exists(dest):raise ValueError('Destination appeared during import; staging retained')
  fs.run('-mv',stage,dest)
  return dict(status='imported_verified',hdfs_path='hdfs://namenode:9000'+dest)
 finally:
  # Only remove this job's empty coordination directory; never remove snapshot/staging data.
  fs.run('-rmdir',lock)


def main():
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--snapshot',required=True)
 p.add_argument('--project',default='/project');p.add_argument('--report',required=True)
 a=p.parse_args();report=Path(a.report)
 if report.exists():raise ValueError('Report already exists')
 root,manifest=build_manifest(a.project,a.snapshot)
 print(json.dumps({'selected_bytes':manifest['total_bytes'],'table_rows':manifest['table_rows']}),flush=True)
 result=transfer(Hdfs(),root,manifest)
 result.update(version=VERSION,snapshot_id=a.snapshot,manifest=manifest,
  verified_files=len(manifest['files']),created_at_utc=datetime.now(timezone.utc).isoformat())
 report.parent.mkdir(parents=True,exist_ok=True)
 with report.open('x',encoding='utf-8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
 print('DONE',report,flush=True)

if __name__=='__main__':main()
