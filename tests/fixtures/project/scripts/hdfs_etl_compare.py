"""Distributed per-key Gold comparison; no exceptAll or full-table collect."""
from functools import reduce
from hdfs_etl_common import log
from hdfs_etl_core import HadoopStore

GOLD_KEYS={'host_window_features':['entity_id','window_start'],
           'sensor_window_features':['sensor_stream_id','metric_name','window_start'],
           'network_overview':['window_start']}


def compare_table(left,right,keys,expected_left_snapshot=None,expected_right_snapshot=None,atol=1e-8,rtol=1e-9):
    from pyspark.sql import functions as F,types as T
    def c(name):return F.col('`'+name.replace('`','``')+'`')
    for df,sid in ((left,expected_left_snapshot),(right,expected_right_snapshot)):
        if df.filter(reduce(lambda a,b:a|b,[c(k).isNull() for k in keys])).limit(1).count():
            raise ValueError('Null Gold key')
        if df.groupBy(*keys).count().filter('count > 1').limit(1).count():
            raise ValueError('Duplicate Gold key')
        if 'snapshot_id' in df.columns and sid is not None:
            if df.filter(~c('snapshot_id').eqNullSafe(sid)).limit(1).count():
                raise ValueError('Incorrect snapshot lineage ID')
    # Different backend/code produces a different snapshot ID: validate each, then exclude from values.
    left=left.drop('snapshot_id');right=right.drop('snapshot_id')
    lt={f.name:f.dataType.simpleString() for f in left.schema.fields}
    rt={f.name:f.dataType.simpleString() for f in right.schema.fields}
    if lt!=rt:raise ValueError('Gold schema names/types differ')
    def normalized(df):
        return df.select(*[(F.sort_array(c(f.name)) if isinstance(f.dataType,T.ArrayType) else c(f.name)).alias(f.name)
                           for f in df.schema.fields])
    l=normalized(left).withColumn('__left_present',F.lit(True)).alias('l')
    r=normalized(right).withColumn('__right_present',F.lit(True)).alias('r')
    joined=l.join(r,keys,'full_outer')
    mismatches=[]
    for name in sorted(set(left.columns)-set(keys)):
        a=F.col('l.`'+name+'`');b=F.col('r.`'+name+'`')
        equal=a.eqNullSafe(b)
        if lt[name] in ('double','float'):
            close=(F.abs(a-b)<=F.lit(atol)+F.lit(rtol)*F.greatest(F.abs(a),F.abs(b)))
            # Exclude infinite/NaN operands from tolerance; exact equality handles their equal cases.
            finite=(F.abs(a)<float('inf')) & (F.abs(b)<float('inf')) & ~F.isnan(a) & ~F.isnan(b)
            equal=equal|F.coalesce(finite & close,F.lit(False))
        mismatches.append(~equal)
    mismatch=reduce(lambda a,b:a|b,mismatches,F.lit(False))
    absent=F.col('__left_present').isNull()|F.col('__right_present').isNull()
    result=joined.agg(F.count('*').alias('joined_keys'),
                      F.sum(F.when(absent,1).otherwise(0)).alias('missing_keys'),
                      F.sum(F.when(~absent & mismatch,1).otherwise(0)).alias('different_rows')).first().asDict()
    result={k:int(v or 0) for k,v in result.items()}
    result.update(atol=atol,rtol=rtol,arrays_compared_as_sorted_sets=True)
    if result['missing_keys'] or result['different_rows']:
        raise ValueError('Gold comparison failed: '+str(result))
    return result


def compare(spark,reference,dest,meta):
    from pyspark.sql.types import StructType
    ref_fs=HadoopStore(spark,reference);out_fs=HadoopStore(spark,dest)
    baseline=ref_fs.read(reference+'/snapshot.json')
    key=lambda r:r['source_file_id']
    if sorted(baseline['source_accounting'],key=key)!=sorted(meta['source_accounting'],key=key):
        raise ValueError('Source accepted/quarantined accounting differs from local reference')
    checks={}
    for relative,info in meta['tables'].items():
        if not ref_fs.exists(reference+'/'+relative):raise ValueError('Missing reference table: '+relative)
        schema=StructType.fromJson(info['schema'])
        log('COMPARE '+relative)
        left=spark.read.schema(schema).parquet(reference+'/'+relative)
        right=spark.read.schema(schema).parquet(dest+'/'+relative)
        a,b=left.count(),right.count()
        if a!=b or b!=info['rows']:raise ValueError('Table count mismatch: '+relative)
        checks[relative]={'reference_rows':a,'hdfs_rows':b}
        if relative.startswith('gold/'):
            # Infer schemas for Gold to detect unexpected column/type changes.
            left=spark.read.parquet(reference+'/'+relative);right=spark.read.parquet(dest+'/'+relative)
            checks[relative].update(compare_table(left,right,GOLD_KEYS[relative.split('/')[1]],
                                                baseline['snapshot_id'],meta['snapshot_id']))
    expected_gold={'gold/'+name for name in GOLD_KEYS if ref_fs.exists(reference+'/gold/'+name)}
    if expected_gold!={p for p in meta['tables'] if p.startswith('gold/')}:
        raise ValueError('Gold table set differs')
    return {'status':'passed','source_accounting_equal':True,'tables':checks,
            'gold_comparison':'full outer join on validated unique keys; all values, float tolerance, sorted arrays',
            'other_tables_comparison':'row counts + source-level accepted/quarantined accounting; not full cell equality',
            'snapshot_id_policy':'Each host Gold snapshot_id validated against its own metadata; excluded from value comparison'}
