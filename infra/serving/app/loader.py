import argparse
import json
import os
from pathlib import Path
import time
import uuid
import psycopg
from psycopg.types.json import Jsonb
import pyarrow.parquet as pq
from core import COLUMNS, REQUIRED, OPTIONAL, inside, manifest, normalize, read, sha, add_digest


def connect():
    return psycopg.connect(host=os.environ['PGHOST'], dbname=os.environ['PGDATABASE'],
                          user=os.environ['PGUSER'], password=os.environ['PGPASSWORD'], connect_timeout=10)


def initialize(conn):
    conn.execute(Path(__file__).with_name('schema.sql').read_text())


def check_item(project, item):
    current = manifest(project,item['path'])
    if current != item: raise ValueError('Experiment changed after configure: '+item['path']+'; run configure again')
    return inside(project,item['path'])


def load_one(conn, project, item):
    folder = check_item(project,item)
    meta = read(folder/'summary.json')
    pf = pq.ParquetFile(folder/'scores.parquet')
    if pf.metadata.num_rows != meta.get('scored_rows'): raise ValueError('Parquet count differs from summary.scored_rows')
    eid = item['experiment_id']
    with conn.transaction():
        conn.execute('SELECT pg_advisory_xact_lock(hashtextextended(%s,0))',(eid,))
        existing = conn.execute('SELECT row_count FROM serving.experiments WHERE experiment_id=%s',(eid,)).fetchone()
        if existing:
            return {'experiment_id':eid,'path':item['path'],'action':'reused','rows':existing[0]}
        conn.execute('INSERT INTO serving.experiments(experiment_id,source_path,script_version,snapshot_id,manifest,summary,row_count,score_digest) VALUES (%s,%s,%s,%s,%s,%s,0,\'pending\')',
                     (eid,item['path'],meta['script_version'],meta['snapshot_id'],Jsonb(item),Jsonb(meta)))
        count = digest = 0
        with conn.cursor().copy('COPY serving.scores ('+','.join(COLUMNS)+') FROM STDIN') as cp:
            for batch in pf.iter_batches(batch_size=4096):
                for raw in batch.to_pylist():
                    row = normalize(raw,meta,eid)
                    digest = add_digest(digest,row)
                    cp.write_row(row)
                    count += 1
        if count != meta['scored_rows']: raise ValueError('Loaded row count mismatch')
        # Rehash after reading: a file edited during import cannot be committed silently.
        check_item(project,item)
        conn.execute('UPDATE serving.experiments SET row_count=%s,score_digest=%s WHERE experiment_id=%s',
                     (count,f'{digest:064x}',eid))
    return {'experiment_id':eid,'path':item['path'],'action':'inserted','rows':count}


def verify_one(conn, project, item):
    check_item(project,item)
    eid=item['experiment_id']
    meta=conn.execute('SELECT row_count,score_digest,manifest FROM serving.experiments WHERE experiment_id=%s',(eid,)).fetchone()
    if not meta or meta[2]!=item: raise ValueError('Missing/conflicting experiment metadata: '+eid)
    count=digest=alerts=0
    with conn.cursor(name='verify_'+uuid.uuid4().hex) as cur:
        cur.execute('SELECT '+','.join(COLUMNS)+' FROM serving.scores WHERE experiment_id=%s',(eid,))
        for row in cur:
            count+=1; alerts+=row[9]; digest=add_digest(digest,row)
    if count != meta[0] or f'{digest:064x}'!=meta[1]: raise ValueError('Stored score digest/count mismatch: '+eid)
    return {'experiment_id':eid,'path':item['path'],'rows':count,'prediction_1_rows':alerts,
            'digest_matches':True,'status':'passed'}


def load_documents(conn, project, entries):
    result=[]
    for entry in entries:
        p=inside(project,entry['path'])
        if sha(p)!=entry['sha256']: raise ValueError('Report changed; run configure again: '+entry['path'])
        body=read(p)
        did=entry['sha256']
        conn.execute('INSERT INTO serving.documents(document_id,source_path,body) VALUES (%s,%s,%s) ON CONFLICT(document_id) DO NOTHING',
                     (did,entry['path'],Jsonb(body)))
        result.append({'path':entry['path'],'document_id':did})
    return result


def execute(command, project, config, out):
    started=time.monotonic()
    result={'status':'running','command':command,'experiments':[],
            'scope':'Import existing experiment outputs; no training, HDFS ETL or new-data backfill'}
    try:
        cfg=read(config)
        if cfg.get('version')!=1 or not cfg.get('experiments'): raise ValueError('No configured experiments')
        with connect() as conn:
            initialize(conn)
            conn.commit()
            if command=='verify-replay':
                result['before']=[verify_one(conn,project,item) for item in cfg['experiments']]
                conn.commit()
            if command in ('load','verify-replay'):
                for item in cfg['experiments']:
                    result['experiments'].append(load_one(conn,project,item))
                if command=='verify-replay' and any(x['action']!='reused' for x in result['experiments']):
                    raise ValueError('Replay unexpectedly inserted an experiment')
                result['documents']=load_documents(conn,project,cfg.get('documents',[]))
                conn.commit()
            result['verification']=[verify_one(conn,project,item) for item in cfg['experiments']]
        result['status']='passed'
    except Exception as exc:
        result.update(status='failed',error=f'{type(exc).__name__}: {exc}')
        raise
    finally:
        result['elapsed_seconds']=round(time.monotonic()-started,3)
        out=Path(out);out.parent.mkdir(parents=True,exist_ok=True)
        out.write_text(json.dumps(result,ensure_ascii=False,indent=2,allow_nan=False))
        print(json.dumps(result,ensure_ascii=False),flush=True)
    return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('command',choices=['load','verify','verify-replay'])
    p.add_argument('--project',default='/project');p.add_argument('--config',default='/project/infra/serving/config.json')
    p.add_argument('--report',required=True);a=p.parse_args()
    execute(a.command,Path(a.project),Path(a.config),Path(a.report))
