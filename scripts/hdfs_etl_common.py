"""Frozen plan + verified Bronze transport. Original toniot code is unchanged."""
import hashlib
import json
from pathlib import Path
import re
import shutil
import sys
import tempfile
import uuid
from datetime import datetime, timezone

ROOT = 'hdfs://namenode:9000/it5427/etl_v1'
SMOKE = ['Network_dataset_1.csv', 'windows7_dataset.csv', 'windows10_dataset.csv']
FULL = [f'Network_dataset_{i}.csv' for i in range(1,24)] + [
    'windows7_dataset.csv','windows10_dataset.csv','IoT_Weather.csv','IoT_Modbus.csv','IoT_Garage_Door.csv']
NEW_SCRIPTS = ['hdfs_etl_common.py','hdfs_etl_core.py','hdfs_etl_compare.py','run_hdfs_etl.py']


def log(message):
    print(datetime.now(timezone.utc).isoformat(), message, flush=True)


def digest(value):
    return hashlib.sha256(json.dumps(value,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()


def sha(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for b in iter(lambda:f.read(4*1024**2),b''):h.update(b)
    return h.hexdigest()


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def save(path,value):
    path=Path(path);path.parent.mkdir(parents=True,exist_ok=True)
    with path.open('x',encoding='utf-8') as f:
        json.dump(value,f,ensure_ascii=False,indent=2,allow_nan=False)


def code_hash(project):
    project=Path(project)
    paths=list((project/'toniot').glob('*.py'))+[project/'scripts'/p for p in NEW_SCRIPTS]
    return digest({p.relative_to(project).as_posix():sha(p) for p in paths})


def validate_plan(plan):
    payload={k:v for k,v in plan.items() if k!='plan_id'}
    if digest(payload)[:24]!=plan.get('plan_id'):
        raise ValueError('Plan checksum mismatch')
    if plan['root']!=ROOT:
        raise ValueError('Unexpected HDFS destination')
    ids=[]
    for item in plan['inputs']:
        m=item['manifest'];sid=m['source_file_id'];source=m['source_id']
        if not re.fullmatch('[a-f0-9]{64}',sid) or not re.fullmatch('[a-z0-9_]+',source):
            raise ValueError('Invalid source identifier')
        expected=f'lake/bronze/{source}/{sid}'
        if item['local_folder']!=expected:
            raise ValueError('Unexpected Bronze source path')
        for name in (m['file_name'], *item['files']):
            if '/' in name or '\\' in name or name in ('.','..'):
                raise ValueError('Invalid Bronze filename')
        if set(item['files'])!={m['file_name'],'records.parquet','manifest.json'}:
            raise ValueError('Bronze file set differs')
        if m['status']!='committed':raise ValueError('Uncommitted Bronze')
        ids.append(sid)
    if not ids or len(ids)!=len(set(ids)):raise ValueError('Empty/duplicate input selection')
    return plan


def make_plan(project,snapshot,scope):
    import pyarrow.parquet as pq
    if not re.fullmatch('[a-f0-9]{24}',snapshot):raise ValueError('Invalid baseline snapshot')
    project=Path(project).resolve()
    meta=read(project/'lake/snapshots'/snapshot/'snapshot.json')
    if meta.get('status')!='committed' or meta.get('snapshot_id')!=snapshot:
        raise ValueError('Baseline snapshot is not committed')
    names=SMOKE if scope=='smoke' else FULL
    wanted=set(meta['spec']['input_manifest_ids']);found={}
    for path in sorted((project/'lake/bronze').glob('*/*/manifest.json')):
        m=read(path)
        if m['source_file_id'] in wanted and m['file_name'] in names:
            if m['file_name'] in found:raise ValueError('Ambiguous selected filename')
            found[m['file_name']]=(path.parent,m)
    if set(found)!=set(names):raise ValueError('Missing frozen inputs: '+str(sorted(set(names)-set(found))))
    inputs=[]
    for name in names:
        folder,m=found[name]
        log('PLAN hash '+name)
        files={}
        for f in (name,'records.parquet','manifest.json'):
            p=folder/f
            if p.resolve()!=p or not p.is_file():raise ValueError('Missing or symlink Bronze file: '+str(p))
            files[f]={'bytes':p.stat().st_size,'sha256':sha(p)}
        if files[name]['sha256']!=m['sha256']:raise ValueError('Raw CSV checksum mismatch')
        if pq.ParquetFile(folder/'records.parquet').metadata.num_rows!=m['input_rows']:
            raise ValueError('Bronze Parquet row count mismatch')
        inputs.append({'local_folder':folder.relative_to(project).as_posix(),'manifest':m,'files':files})
    plan={'version':'hdfs-etl-plan-1','baseline_snapshot':snapshot,'scope':scope,'root':ROOT,
          'inputs':inputs,'entities':meta['spec']['entities'],'time_rules':meta['spec']['time_rules'],
          'window_seconds':meta['spec']['window_seconds'],'code_hash':code_hash(project),
          'baseline_metadata_sha256':sha(project/'lake/snapshots'/snapshot/'snapshot.json'),
          'expected_files':names,'total_upload_bytes':sum(f['bytes'] for i in inputs for f in i['files'].values()),
          'records_bytes':sum(i['files']['records.parquet']['bytes'] for i in inputs),
          'ingest_scope':'Reuse existing local CSV ingestion; verified Bronze CSV + indexed Parquet copied to HDFS'}
    plan['plan_id']=digest(plan)[:24]
    return validate_plan(plan)


def verify_remote(fs,path,files):
    for name,info in files.items():
        if fs.hash(path+'/'+name)!=info['sha256']:
            raise ValueError('Remote checksum mismatch: '+path+'/'+name)


def transfer_one(fs,project,item,root=ROOT):
    """Exclusive per-source lock; immutable destination, full checksums on retry."""
    m=item['manifest'];sid=m['source_file_id']
    dest=root+'/bronze/'+m['source_id']+'/'+sid
    receipt={'source_file_id':sid,'files':item['files']}
    fs.run('-mkdir','-p',root+'/_locks',root+'/_staging',root+'/bronze/'+m['source_id'])
    lock=root+'/_locks/bronze-'+sid
    fs.run('-mkdir',lock)  # Must acquire before entering finally: never remove another writer's lock.
    try:
        if fs.exists(dest):
            if fs.read_json(dest+'/transfer.json')!=receipt:
                raise ValueError('Existing Bronze differs; refusing overwrite')
            verify_remote(fs,dest,item['files'])
            return 'already_present_verified'
        stage=root+'/_staging/bronze-'+sid+'-'+uuid.uuid4().hex
        fs.run('-mkdir',stage)
        for name,info in item['files'].items():
            src=Path(project)/item['local_folder']/name
            if sha(src)!=info['sha256']:raise ValueError('Source changed since plan: '+str(src))
            log('UPLOAD '+m['file_name']+' / '+name)
            fs.run('-put',src,stage+'/'+name)
        verify_remote(fs,stage,item['files'])
        with tempfile.TemporaryDirectory() as temp:
            local=Path(temp)/'transfer.json';save(local,receipt)
            fs.run('-put',local,stage+'/transfer.json')
        if fs.exists(dest):raise ValueError('Destination appeared; refusing overwrite')
        fs.run('-mv',stage,dest)
        return 'imported_verified'
    finally:
        fs.run('-rmdir',lock)


def local_reference(project,plan,report):
    """Original local implementation; smoke uses symlinked Bronze, no input copies."""
    project=Path(project);report=Path(report)
    if plan['scope']=='full':
        path=project/'lake/snapshots'/plan['baseline_snapshot']
        if sha(path/'snapshot.json')!=plan['baseline_metadata_sha256']:
            raise ValueError('Baseline metadata changed')
        return path.as_uri()
    from toniot.spark_job import build
    lake=report/'reference_lake';lake.mkdir()
    for item in plan['inputs']:
        m=item['manifest'];link=lake/'bronze'/m['source_id']/m['source_file_id']
        link.parent.mkdir(parents=True,exist_ok=True)
        link.symlink_to(project/item['local_folder'],target_is_directory=True)
    entity=report/'reference_entities.json';rules=report/'reference_time_rules.json'
    save(entity,plan['entities']);save(rules,plan['time_rules'])
    log('LOCAL_REFERENCE uses original toniot.spark_job.build')
    meta=build(str(lake),str(entity),str(rules),plan['window_seconds'],'local[2]',
               [i['manifest']['source_file_id'] for i in plan['inputs']],plan['expected_files'],True)
    return (lake/'snapshots'/meta['snapshot_id']).as_uri()
