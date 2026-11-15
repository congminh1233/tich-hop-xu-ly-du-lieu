import sys,json,tempfile
from pathlib import Path
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
sys.path.insert(0,str(Path(__file__).resolve().parent))
import pyarrow as pa,pyarrow.parquet as pq
from pyspark.sql import SparkSession
import prepare_iot_evaluation as p
import run_iot_baseline as b
import _iot_common as c
from test_iot import fixture_gold
scratch=tempfile.TemporaryDirectory(prefix='iot-e2e-');root=Path(scratch.name);snap=root/'lake/snapshots/synthetic'
(snap/'gold/sensor_window_features').mkdir(parents=True,exist_ok=False)
(snap/'evaluation/record_labels').mkdir(parents=True)
gold=fixture_gold()
pq.write_table(pa.Table.from_pylist(gold),snap/'gold/sensor_window_features/part.parquet')
labels=[]
for source in c.METRICS:
 for i in range(200):
  for j in range(2):
   lab=1 if i%7==0 and j==1 else 0
   if i==150:lab=None if j==1 else 0
   labels.append(dict(source_id=source,event_id=f'{source}:{i}:{j}',source_file_id=source,
    source_row_ref=i*2+j,event_time_utc=datetime.fromtimestamp(1554000000+i*60+j,timezone.utc),
    label=lab,attack_type='attack' if lab else 'normal'))
pq.write_table(pa.Table.from_pylist(labels),snap/'evaluation/record_labels/part.parquet')
c.write_json(snap/'snapshot.json',dict(status='committed',snapshot_id='synthetic',spec={'window_seconds':60}))
spark=(SparkSession.builder.master('local[2]').appName('IoT synthetic test').config('spark.ui.enabled','false')
 .config('spark.sql.session.timeZone','UTC').config('spark.sql.shuffle.partitions','4').getOrCreate())
spark.sparkContext.setLogLevel('ERROR')
try:
 out=root/'prepared';summary=p.prepare(spark,snap,out)
 assert summary['device_windows']==600 and summary['eligible_windows']==600
 meta,rows,excluded=b.load_input(out)
 assert sum(r['label_observed'] is None for r in rows)==3
 result=b.run(out,root/'baseline')
 assert result['scored_rows']==1200 # activity constant -> skipped; values and combined remain
 assert all(m['status']=='fitted' for m in result['models'].values())
 labs=pq.read_table(out/'window_labels/part-00000.parquet').to_pylist()
 assert all(r['event_count']==2 for r in labs) # not multiplied by 2/3/4 metrics
 assert sum(r['label_observed']==1 for r in labs)==3*29
 try:p.reconcile([{**r,'event_count':r['event_count']+1} for r in rows],labs)
 except ValueError:pass
 else:raise AssertionError('Count mismatch not rejected')
 df=spark.read.parquet(str(snap/'evaluation/record_labels'))
 try:p.reject(df.unionByName(df.limit(1)).groupBy('event_id').count().filter('count > 1'),'duplicate')
 except ValueError:pass
 else:raise AssertionError('Duplicate identity not rejected')
 try:p.prepare(spark,snap,out)
 except ValueError:pass
 else:raise AssertionError('Existing output not rejected')
 c.write_json(root/'e2e_validation.json',dict(status='passed',synthetic_only=True,device_windows=600,
   source_events=1200,scored_rows=result['scored_rows'],unknown_label_windows=3,
   checks=['event count not multiplied by metric rows','unknown-label propagation','attack OR rule',
    'event-count mismatch rejected','duplicate event identity rejected','existing output protected',
    'full Spark preparation then PyArrow baseline'],spark=spark.version))
 print('E2E PASS')
finally:
 spark.stop()
 scratch.cleanup()
