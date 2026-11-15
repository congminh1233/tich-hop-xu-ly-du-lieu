"""Storage-aware ETL orchestration; transformations imported from original toniot."""
import json
import time
import uuid
from hdfs_etl_common import log, digest


class HadoopStore:
    def __init__(self,spark,root):
        self.j=spark._jvm
        self.conf=spark.sparkContext._jsc.hadoopConfiguration()
        self.fs=self.j.org.apache.hadoop.fs.FileSystem.get(self.j.java.net.URI(root),self.conf)
    def path(self,p):return self.j.org.apache.hadoop.fs.Path(p)
    def exists(self,p):return self.fs.exists(self.path(p))
    def mkdir(self,p):
        if not self.fs.mkdirs(self.path(p)):raise IOError('mkdir failed: '+p)
    def read(self,p):
        stream=self.fs.open(self.path(p))
        try:return json.loads(self.j.org.apache.commons.io.IOUtils.toString(stream,'UTF-8'))
        finally:stream.close()
    def text(self,p,text):
        stream=self.fs.create(self.path(p),False)
        try:stream.write(bytearray(text.encode('utf-8')))
        finally:stream.close()
    def write(self,p,obj):self.text(p,json.dumps(obj,ensure_ascii=False,indent=2,allow_nan=False))
    def delete(self,p,recursive=False):
        if not self.fs.delete(self.path(p),recursive):raise IOError('delete failed: '+p)
    def rename(self,src,dst):
        if self.exists(dst):raise ValueError('Destination exists: '+dst)
        if not self.fs.rename(self.path(src),self.path(dst)):raise IOError('rename failed')


def session():
    from pyspark.sql import SparkSession
    return (SparkSession.builder.appName('IT5427 Bronze Silver Gold on HDFS').master('local[2]')
            .config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','false')
            .config('spark.sql.legacy.timeParserPolicy','CORRECTED')
            .config('spark.sql.shuffle.partitions','8').config('spark.sql.adaptive.enabled','false')
            .config('spark.ui.enabled','false').getOrCreate())


def build(plan,spark,root=None):
    from pyspark.sql import functions as F
    from toniot.spark_job import normalize,union,host_edges,aggregate_network,aggregate_hosts,add_window,col
    root=(root or plan['root']).rstrip('/')
    fs=HadoopStore(spark,root)
    sid=plan['plan_id'];dest=root+'/snapshots/'+sid
    fs.mkdir(root+'/_locks');fs.mkdir(root+'/_staging');fs.mkdir(root+'/snapshots')
    lock=root+'/_locks/build-'+sid
    fs.text(lock,sid)  # exclusive create(overwrite=False); only its owner releases it
    try:
        if fs.exists(dest):
            meta=fs.read(dest+'/snapshot.json')
            if meta.get('status')!='committed' or meta.get('plan')!=plan:
                raise ValueError('Existing snapshot metadata conflicts')
            log('REUSE committed snapshot; table validation follows')
            return dest,meta
        stage=root+'/_staging/build-'+sid+'-'+uuid.uuid4().hex
        fs.mkdir(stage)
        started=time.monotonic();tables={};labels=[];quarantines=[];accounting=[];inventory={}
        def write(df,relative,partition=False):
            path=stage+'/'+relative
            log('WRITE '+relative)
            writer=df.write.mode('error')
            if partition:writer=writer.partitionBy('event_date')
            writer.parquet(path)
            back=spark.read.schema(df.schema).parquet(path)
            inventory[relative]={'rows':back.count(),'schema':df.schema.jsonValue()}
            log('DONE '+relative+' rows='+str(inventory[relative]['rows']))
            return back
        for index,item in enumerate(plan['inputs'],1):
            m=item['manifest'];source_root=root+'/bronze/'+m['source_id']+'/'+m['source_file_id']
            if fs.read(source_root+'/manifest.json')!=m:
                raise ValueError('HDFS Bronze manifest differs from plan')
            log(f'SOURCE {index}/{len(plan["inputs"])} '+m['file_name'])
            df=spark.read.parquet(source_root+'/records.parquet')
            name,silver,target,quarantine,valid=normalize(df,m,plan['time_rules'],plan['entities'])
            accepted,rejected=valid.count(),quarantine.count()
            if accepted+rejected!=m['input_rows']:raise ValueError('Source accounting mismatch')
            accounting.append({'source_file_id':m['source_file_id'],'file_name':m['file_name'],
                               'input_rows':m['input_rows'],'accepted_source_rows':accepted,
                               'quarantined_source_rows':rejected})
            # Break the large per-file union lineage before building combined tables.
            prefix='_parts/'+m['source_file_id']
            tables.setdefault(name,[]).append(write(silver,prefix+'/'+name))
            labels.append(write(target,prefix+'/labels'))
            quarantines.append(write(quarantine,prefix+'/quarantine'))
        tables={name:write(union(frames),'silver/'+name,True) for name,frames in tables.items()}
        write(union(labels),'evaluation/record_labels')
        write(union(quarantines),'quality/quarantine')
        net=tables.get('network_flow');host=tables.get('host_metric')
        nw=hw=None
        if net is not None:
            edges=host_edges(net,plan['entities'],plan['window_seconds'])
            nw=aggregate_network(edges)
            write(edges.select('entity_id','window_start','event_id','source_file_id','source_row_ref'),
                  'lineage/network_host_edges')
            write(add_window(net,plan['window_seconds']).groupBy('window_start').agg(F.count('*').alias('flow_count')),
                  'gold/network_overview')
        if host is not None:
            hw=aggregate_hosts(host,plan['window_seconds'])
            write(add_window(host,plan['window_seconds']).select('entity_id','window_start','event_id',
                  'source_file_id','source_row_ref').distinct(),'lineage/os_host_edges')
        gold=nw.join(hw,['entity_id','window_start'],'full') if nw is not None and hw is not None else nw if nw is not None else hw
        coverage=[]
        if gold is not None:
            for c in ('n_network','n_os','n_cpu_valid'):
                if c not in gold.columns:gold=gold.withColumn(c,F.lit(None).cast('long'))
            gold=(gold.withColumn('has_network',F.coalesce(col('n_network')>0,F.lit(False)))
                  .withColumn('has_os',F.coalesce(col('n_os')>0,F.lit(False)))
                  .withColumn('has_cpu',F.coalesce(col('n_cpu_valid')>0,F.lit(False)))
                  .withColumn('window_seconds',F.lit(plan['window_seconds']))
                  .withColumn('snapshot_id',F.lit(sid))
                  .withColumn('mapping_version',F.lit(plan['entities']['version'])))
            fs.text(stage+'/explain_host_windows.txt',gold._jdf.queryExecution().toString())
            gold=write(gold,'gold/host_window_features')
            coverage=[r.asDict() for r in gold.groupBy('entity_id','has_network','has_os','has_cpu').count().collect()]
        sensor=tables.get('sensor_observation')
        if sensor is not None:
            write(add_window(sensor,plan['window_seconds']).groupBy('sensor_stream_id','metric_name','window_start').agg(
                F.count('*').alias('observed_count'),F.count('numeric_value').alias('numeric_count'),
                F.avg('numeric_value').alias('mean'),F.min('numeric_value').alias('min'),F.max('numeric_value').alias('max'),
                F.stddev_samp('numeric_value').alias('stddev'),F.collect_set('categorical_value').alias('states_seen'),
                F.collect_set('boolean_value').alias('boolean_values_seen')),'gold/sensor_window_features')
        # Only delete temporary files owned by this completed build, never input data.
        fs.delete(stage+'/_parts',True)
        inventory={k:v for k,v in inventory.items() if not k.startswith('_parts/')}
        meta={'status':'committed','snapshot_id':sid,'plan':plan,'source_accounting':accounting,
              'host_coverage':coverage,'tables':inventory,'elapsed_seconds':time.monotonic()-started,
              'execution_master':spark.sparkContext.master,'aqe_enabled':spark.conf.get('spark.sql.adaptive.enabled'),
              'scope':'CSV ingest remains local; Silver/Gold ETL reads and writes HDFS; Spark local[2] in Docker'}
        fs.write(stage+'/snapshot.json',meta)
        fs.rename(stage,dest)
        log('COMMITTED '+dest)
        return dest,meta
    finally:
        fs.delete(lock)
