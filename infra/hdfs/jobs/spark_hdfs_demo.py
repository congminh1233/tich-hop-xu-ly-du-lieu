#!/usr/bin/env python3
"""Read real Gold from HDFS, aggregate, write HDFS output and verify round trip."""
import argparse
import json
import uuid
from pathlib import Path
from datetime import datetime,timezone
from pyspark.sql import SparkSession,functions as F
from transfer_gold import validate_id,Hdfs


def require_unique(df,keys,name):
 if df.filter(' OR '.join('`'+k+'` IS NULL' for k in keys)).limit(1).count():raise ValueError(name+': null key')
 if df.groupBy(*keys).count().filter('count > 1').limit(1).count():raise ValueError(name+': duplicate key')


def run(snapshot,run_id,report):
 validate_id(snapshot)
 if not run_id or any(x not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for x in run_id):raise ValueError('Invalid run ID')
 report=Path(report)
 if report.exists():raise ValueError('Report exists')
 cli=Hdfs();base='hdfs://namenode:9000/it5427/snapshots/'+snapshot
 manifest=cli.read_json(base+'/import_manifest.json')
 if manifest['snapshot_id']!=snapshot:raise ValueError('Imported snapshot identity mismatch')
 final='/it5427/experiments/hdfs_demo/'+run_id
 if cli.exists(final):raise ValueError('HDFS demo output already exists')
 stage='/it5427/_staging/demo-'+run_id+'-'+uuid.uuid4().hex
 spark=(SparkSession.builder.appName('IT5427 HDFS Gold integration demo')
  .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','8')
  .config('spark.sql.parquet.columnarReaderBatchSize','1024').config('spark.ui.enabled','false').getOrCreate())
 spark.sparkContext.setLogLevel('WARN')
 try:
  if not spark.sparkContext._jsc.hadoopConfiguration().get('fs.defaultFS').startswith('hdfs://'):
   raise ValueError('HDFS configuration was not loaded')
  dfs={};counts={}
  for table,expected in manifest['table_rows'].items():
   df=spark.read.parquet(base+'/gold/'+table);counts[table]=df.count();dfs[table]=df
   if counts[table]!=expected:raise ValueError('Row count differs from copied Parquet metadata: '+table)
  host=dfs['host_window_features'];sensor=dfs['sensor_window_features']
  require_unique(host,['entity_id','window_start'],'host')
  require_unique(sensor,['sensor_stream_id','metric_name','window_start'],'sensor')
  if host.filter(~F.col('snapshot_id').eqNullSafe(snapshot)).limit(1).count():raise ValueError('Host snapshot mismatch')
  host_summary=host.groupBy('entity_id','has_network','has_os').agg(F.count('*').alias('windows'))
  sensor_summary=sensor.groupBy('sensor_stream_id','metric_name').agg(
   F.count('*').alias('windows'),F.sum('observed_count').alias('source_events_for_this_metric'),
   F.sum('numeric_count').alias('numeric_values'),F.min('min').alias('minimum_value'),F.max('max').alias('maximum_value'))
  outputs={'host_coverage':host_summary,'sensor_metric_summary':sensor_summary};results={}
  for name,df in outputs.items():
   path='hdfs://namenode:9000'+stage+'/'+name
   df.write.mode('error').parquet(path)
   back=spark.read.parquet(path)
   if df.exceptAll(back.select(df.columns)).limit(1).count() or back.select(df.columns).exceptAll(df).limit(1).count():
    raise ValueError('HDFS write/read values mismatch: '+name)
   results[name]={'rows':back.count(),'preview':[r.asDict() for r in back.orderBy(*back.columns[:2]).collect()]}
  result=dict(status='passed',version='hdfs-spark-demo-1.0',snapshot_id=snapshot,run_id=run_id,
   input_uri=base,output_uri='hdfs://namenode:9000'+final,input_counts=counts,outputs=results,
   validation={'input_counts_match':True,'unique_keys':True,'output_roundtrip_equal':True},
   runtime={'spark':spark.version,'hadoop_client':spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
            'java':spark._jvm.java.lang.System.getProperty('java.version'),'master':spark.sparkContext.master,
            'application_id':spark.sparkContext.applicationId},
   scope='HDFS storage and Spark local[2] compute in Docker; original Bronze/Silver ETL is still local; no cluster speedup claim.',
   created_at_utc=datetime.now(timezone.utc).isoformat())
  cli.run('-mkdir','-p','/it5427/experiments/hdfs_demo')
  if cli.exists(final):raise ValueError('Output appeared; staging retained')
  cli.run('-mv',stage,final)
  report.parent.mkdir(parents=True,exist_ok=True)
  with report.open('x',encoding='utf-8') as f:json.dump(result,f,ensure_ascii=False,indent=2)
  print(json.dumps(result,ensure_ascii=False,indent=2))
 finally:spark.stop()

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--snapshot',required=True)
 p.add_argument('--run-id',required=True);p.add_argument('--report',required=True)
 a=p.parse_args();run(a.snapshot,a.run_id,a.report)
