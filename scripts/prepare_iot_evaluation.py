#!/usr/bin/env python3
"""Prepare IoT labels and fixed time splits from an existing local snapshot."""
import argparse
import json
import shutil
import sys
import time
import uuid
from pathlib import Path
from collections import Counter
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F
import _iot_common as c

VERSION='iot-evaluation-prep-1.0'


def reject(df,message):
 rows=df.limit(3).collect()
 if rows:raise ValueError(message+': '+str([r.asDict() for r in rows]))


def aggregate_labels(labels,seconds):
 x=labels.withColumn('window_start',(F.floor(F.col('event_time_utc').cast('double')/seconds)*seconds).cast('long'))
 valid=F.coalesce(F.col('label').isin(0,1),F.lit(False))
 y=x.groupBy(F.col('source_id').alias('sensor_stream_id'),'window_start').agg(
  F.count('*').alias('event_count'),
  F.sum(F.when(F.col('label')==0,1).otherwise(0)).alias('normal_events'),
  F.sum(F.when(F.col('label')==1,1).otherwise(0)).alias('attack_events'),
  F.sum(F.when(~valid,1).otherwise(0)).alias('unknown_events'),
  F.sort_array(F.collect_set(F.when(F.col('label')==1,F.col('attack_type')))).alias('attack_types'))
 return y.withColumn('label_observed',F.when(F.col('attack_events')>0,1)
      .when(F.col('unknown_events')==0,0).otherwise(F.lit(None).cast('int'))),x


def reconcile(features,labels):
 ff=c.index(features,'features');ll=c.index(labels,'labels')
 if ff.keys()!=ll.keys():
  raise ValueError(f'Gold/label window keys differ: Gold-only={len(ff.keys()-ll.keys())}, label-only={len(ll.keys()-ff.keys())}')
 for k in ff:
  if ff[k]['event_count']!=ll[k]['event_count']:raise ValueError(f'Gold/label event count mismatch: {k}')
  if sum(ll[k][n] for n in ('normal_events','attack_events','unknown_events'))!=ll[k]['event_count']:
   raise ValueError(f'Label accounting failed: {k}')


def prepare(spark,snapshot,output,max_gold_rows=200000):
 start=time.perf_counter();snapshot=Path(snapshot).resolve();output=Path(output).resolve()
 lake=snapshot.parent.parent
 if output.exists():raise ValueError(f'Output exists: {output}; choose a new name')
 if output==lake or lake in output.parents or output in lake.parents:raise ValueError('Output must be outside source lake')
 meta_path=snapshot/'snapshot.json';meta=json.loads(meta_path.read_text())
 if meta.get('status')!='committed' or meta.get('snapshot_id')!=snapshot.name:raise ValueError('Invalid snapshot identity/status')
 seconds=int(meta['spec']['window_seconds'])
 if seconds not in (30,60,120):raise ValueError('Unsupported window size')
 cached=None;stage=None
 try:
  print('[1/5] Validate long Gold and create label-free device features',flush=True)
  gold=spark.read.parquet(str(snapshot/'gold/sensor_window_features'))
  missing=set(c.GOLD_COLUMNS)-set(gold.columns)
  if missing:raise ValueError(f'Missing Gold columns: {missing}')
  if any('label' in n.lower() or 'attack' in n.lower() for n in gold.columns):raise ValueError('Unexpected target columns in Gold')
  if gold.count()>max_gold_rows:raise ValueError('Gold exceeds safety row limit')
  gg=[r.asDict(recursive=True) for r in gold.select(*c.GOLD_COLUMNS).collect()]
  features,splits,policies,audit=c.make_features(gg,seconds,snapshot.name)
  streams=sorted(policies)
  print('[2/5] Scan only IoT record-label columns; check event identity',flush=True)
  path=snapshot/'evaluation/record_labels'
  lab=spark.read.parquet(str(path))
  names=['source_id','event_id','source_file_id','source_row_ref','event_time_utc','label','attack_type']
  if set(names)-set(lab.columns):raise ValueError('Missing record_labels columns')
  cached=lab.select(*names).filter(F.col('source_id').isin(streams)).persist(StorageLevel.DISK_ONLY)
  reject(cached.filter(F.col('event_id').isNull()|F.col('source_file_id').isNull()|F.col('source_row_ref').isNull()|F.col('event_time_utc').isNull()),'Null IoT event provenance/time')
  reject(cached.groupBy('event_id').count().filter(F.col('count')!=1),'Duplicate IoT event_id; refusing label multiplication')
  agg,links=aggregate_labels(cached,seconds)
  labels=[r.asDict(recursive=True) for r in agg.collect()]
  print('[3/5] Reconcile every Gold window with record labels',flush=True)
  reconcile(features,labels)
  fi=c.index(features,'features');sp=c.index(splits,'splits')
  distribution=Counter((r['sensor_stream_id'],sp[c.key(r)]['split'],r['label_observed'],fi[c.key(r)]['eligible']) for r in labels)
  summary=dict(status='prepared',script_version=VERSION,snapshot_id=snapshot.name,window_seconds=seconds,
    feature_policy=c.POLICY_VERSION,gold_metric_rows=len(gg),device_windows=len(features),
    eligible_windows=sum(r['eligible'] for r in features),excluded_windows=sum(not r['eligible'] for r in features),
    source_audit=audit,split_policy={'method':'60/20/20 sorted distinct windows PER DEVICE; before exclusions; no label use','devices':policies},
    label_policy='1 if any attack; 0 if all events known normal; otherwise null. One label per source event before metrics unpivot.',
    label_distribution=[dict(sensor_stream_id=s,split=part,label_observed=y,eligible=ok,windows=n) for (s,part,y,ok),n in sorted(distribution.items(),key=str)],
    checks={'gold_label_key_difference':0,'gold_label_count_mismatch_windows':0,'duplicate_iot_event_ids':0},
    snapshot_sha256=c.sha256(meta_path),script_sha256=c.sha256(__file__),common_sha256=c.sha256(c.__file__),
    versions={'spark':spark.version,'pyarrow':pa.__version__},
    time_rules=meta['spec'].get('time_rules'),created_at_utc=c.datetime.now(c.timezone.utc).isoformat(),
    warnings=['Original lake and Gold are not modified.',
      'record_labels supplies source-event provenance; this preparation does not independently reconstruct Silver lineage.',
      'Pressure excluded from baseline features pending semantic/unit review; original pressure Gold retained.',
      'State sets contain no frequencies, duration, transition count or joint event ordering.',
      'A nonempty state set does not prove every event has a valid state; per-event completeness unavailable in current Gold.',
      'Per-device split boundaries differ; do not use them as a shared cross-device chronological evaluation.',
      'Timestamps inherit snapshot encoding rules; no second timezone offset is applied.',
      'Quarantined source events are not assigned a fabricated timestamp or normal window.'])
  print('[4/5] Write separate experiment and event-to-window links',flush=True)
  stage=output.parent/('.'+output.name+'.staging-'+uuid.uuid4().hex);stage.mkdir(parents=True)
  for name,rows in [('features',features),('splits',splits),('window_labels',labels)]:
   (stage/name).mkdir();pq.write_table(pa.Table.from_pylist(rows),stage/name/'part-00000.parquet')
  # Small existing Gold retained as an immutable experiment copy for audit.
  gold.write.mode('error').parquet(str(stage/'gold_long'))
  (links.select(F.col('source_id').alias('sensor_stream_id'),'window_start','event_id','source_file_id','source_row_ref')
       .write.mode('error').parquet(str(stage/'record_label_links')))
  c.write_json(stage/'source_snapshot.json',meta)
  c.write_json(stage/'input_inventory.json',{name:[{'path':str(p.relative_to(snapshot)),'bytes':p.stat().st_size}
    for p in sorted((snapshot/name).rglob('*.parquet'))] for name in ['gold/sensor_window_features','evaluation/record_labels']})
  summary['elapsed_seconds']=round(time.perf_counter()-start,3);c.write_json(stage/'summary.json',summary)
  if output.exists():raise ValueError('Output appeared during run')
  stage.rename(output);stage=None
  print('[5/5] DONE:',output/'summary.json',flush=True)
  print(json.dumps({'windows':len(features),'eligible':summary['eligible_windows'],'source_audit':audit},ensure_ascii=False))
  return summary
 finally:
  if cached is not None:
   try:cached.unpersist()
   except Exception:pass
  if stage is not None and stage.exists():shutil.rmtree(stage)


def main():
 p=argparse.ArgumentParser(description=__doc__)
 p.add_argument('--lake',default='lake');p.add_argument('--snapshot',required=True)
 p.add_argument('--output',required=True);p.add_argument('--master',default='local[2]')
 p.add_argument('--shuffle-partitions',type=int,default=32);p.add_argument('--max-gold-rows',type=int,default=200000)
 args=p.parse_args()
 if args.shuffle_partitions<1:raise ValueError('shuffle-partitions must be positive')
 spark=(SparkSession.builder.appName('IT5427 IoT evaluation preparation').master(args.master)
  .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions',str(args.shuffle_partitions))
  .config('spark.sql.files.maxPartitionBytes',str(32*1024*1024))
  .config('spark.sql.parquet.columnarReaderBatchSize','1024')
  .config('spark.sql.inMemoryColumnarStorage.batchSize','1000')
  .config('spark.sql.adaptive.enabled','true').config('spark.sql.adaptive.coalescePartitions.enabled','false')
  .config('spark.sql.autoBroadcastJoinThreshold','-1').getOrCreate())
 spark.sparkContext.setLogLevel('WARN')
 try:prepare(spark,Path(args.lake)/'snapshots'/args.snapshot,args.output,args.max_gold_rows)
 finally:
  try:spark.stop()
  except Exception as exc:print('WARN: Spark cleanup failed:',type(exc).__name__,file=sys.stderr)

if __name__=='__main__':main()
