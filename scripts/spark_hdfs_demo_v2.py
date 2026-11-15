#!/usr/bin/env python3
"""HDFS Gold demo: exact, bounded multiset validation of small aggregate outputs."""
import argparse
from collections import Counter
from datetime import datetime, timezone, date
from decimal import Decimal
import hashlib
import json
import math
from pathlib import Path
import sys
import time
import uuid

MAX_SUMMARY_ROWS = 10000


def progress(message):
    print(datetime.now(timezone.utc).isoformat(), message, flush=True)


def step(name, fn):
    progress('BEGIN '+name)
    start = time.monotonic()
    result = fn()
    progress('END '+name+f' seconds={time.monotonic()-start:.2f}')
    return result


def canonical(value):
    # Typed scalar keys; no string concatenation or lossy hashes. Spark equates NaNs.
    if value is None:
        return ('null',)
    if isinstance(value, float) and math.isnan(value):
        return ('float_nan',)
    if isinstance(value, Decimal) and value.is_nan():
        return ('decimal_nan',)
    if isinstance(value, (bool, int, float, str, bytes, Decimal, date, datetime)):
        return (type(value).__name__, value)
    raise TypeError('Unsupported summary value type: '+type(value).__name__)


def assert_same_rows(expected, actual):
    left = Counter(tuple(canonical(v) for v in row) for row in expected)
    right = Counter(tuple(canonical(v) for v in row) for row in actual)
    if left != right:
        raise ValueError('Roundtrip values/multiplicities differ: '
                         f'missing={sum((left-right).values())}, extra={sum((right-left).values())}')


def bounded_rows(df, maximum=MAX_SUMMARY_ROWS):
    if maximum < 1:
        raise ValueError('Invalid summary row limit')
    rows = df.limit(maximum+1).collect()
    if len(rows) > maximum:
        raise ValueError(f'Summary exceeds {maximum} rows; refusing driver comparison. '
                         'Use a distributed validator for larger outputs.')
    return rows


def check_schema(expected, actual):
    # Parquet may relax nullable; names, order and scalar data types must still match.
    def fields(schema):
        return [(f.name, f.dataType.simpleString()) for f in schema.fields]
    if fields(expected) != fields(actual):
        raise ValueError('Roundtrip column names/order/types differ')


def write_and_check(df, path, name):
    before = step(name+'.collect_expected_summary', lambda: bounded_rows(df))
    step(name+'.write_parquet', lambda: df.write.mode('error').parquet(path))
    back = step(name+'.read_parquet', lambda: df.sparkSession.read.parquet(path))
    check_schema(df.schema, back.schema)
    after = step(name+'.collect_readback_summary', lambda: bounded_rows(back))
    step(name+'.compare_all_rows', lambda: assert_same_rows(before, after))
    return {'rows': len(after), 'preview': [r.asDict() for r in after[:100]],
            'preview_limit': 100, 'preview_truncated': len(after)>100}


def require_unique(df, keys, name):
    if step(name+'.null_keys', lambda: df.filter(' OR '.join('`'+k+'` IS NULL' for k in keys)).limit(1).count()):
        raise ValueError(name+': null key')
    if step(name+'.duplicate_keys', lambda: df.groupBy(*keys).count().filter('count > 1').limit(1).count()):
        raise ValueError(name+': duplicate key')


def run(snapshot, run_id, report):
    from pyspark.sql import SparkSession, functions as F
    # Helper remains in existing image; this script is read from /project/scripts.
    sys.path.insert(0, '/opt/it5427')
    from transfer_gold import validate_id, Hdfs
    validate_id(snapshot)
    if not run_id or any(c not in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in run_id):
        raise ValueError('Invalid run ID')
    report = Path(report)
    if report.exists():
        raise ValueError('Report exists')
    cli = Hdfs()
    base = 'hdfs://namenode:9000/it5427/snapshots/'+snapshot
    manifest = step('manifest', lambda: cli.read_json(base+'/import_manifest.json'))
    if manifest['snapshot_id'] != snapshot:
        raise ValueError('Imported snapshot identity mismatch')
    final = '/it5427/experiments/hdfs_demo/'+run_id
    if cli.exists(final):
        raise ValueError('HDFS demo output already exists')
    stage = '/it5427/_staging/demo-'+run_id+'-'+uuid.uuid4().hex
    spark = (SparkSession.builder.appName('IT5427 HDFS Gold integration demo v2')
             .config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.shuffle.partitions', '8')
             .config('spark.sql.parquet.columnarReaderBatchSize', '1024')
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    progress('CONFIG AQE='+spark.conf.get('spark.sql.adaptive.enabled'))
    try:
        if not (spark.sparkContext._jsc.hadoopConfiguration().get('fs.defaultFS') or '').startswith('hdfs://'):
            raise ValueError('HDFS configuration was not loaded')
        dfs, counts = {}, {}
        for table, expected in manifest['table_rows'].items():
            if table not in ('host_window_features','sensor_window_features','network_overview'):
                raise ValueError('Unexpected table: '+table)
            df = step(table+'.read', lambda: spark.read.parquet(base+'/gold/'+table))
            counts[table] = step(table+'.count', df.count)
            if counts[table] != expected:
                raise ValueError('Input count mismatch: '+table)
            dfs[table] = df
        host, sensor = dfs['host_window_features'], dfs['sensor_window_features']
        require_unique(host, ['entity_id','window_start'], 'host')
        require_unique(sensor, ['sensor_stream_id','metric_name','window_start'], 'sensor')
        if step('host.snapshot_id', lambda: host.filter(~F.col('snapshot_id').eqNullSafe(snapshot)).limit(1).count()):
            raise ValueError('Host snapshot mismatch')
        outputs = {
            'host_coverage': host.groupBy('entity_id','has_network','has_os').agg(F.count('*').alias('windows')),
            'sensor_metric_summary': sensor.groupBy('sensor_stream_id','metric_name').agg(
                F.count('*').alias('windows'), F.sum('observed_count').alias('source_events_for_this_metric'),
                F.sum('numeric_count').alias('numeric_values'), F.min('min').alias('minimum_value'),
                F.max('max').alias('maximum_value')),
        }
        results = {}
        for name, df in outputs.items():
            results[name] = write_and_check(df, 'hdfs://namenode:9000'+stage+'/'+name, name)
        result = dict(status='passed', version='hdfs-spark-demo-2.0', snapshot_id=snapshot, run_id=run_id,
                      input_uri=base, output_uri='hdfs://namenode:9000'+final, input_counts=counts, outputs=results,
                      validation={'input_counts_match':True,'unique_keys':True,'output_roundtrip_equal':True,
                                  'output_schema_equal_ignoring_nullability':True,
                                  'roundtrip_method':'bounded_exact_multiset',
                                  'max_summary_rows':MAX_SUMMARY_ROWS},
                      runtime={'spark':spark.version,'hadoop_client':spark._jvm.org.apache.hadoop.util.VersionInfo.getVersion(),
                               'java':spark._jvm.java.lang.System.getProperty('java.version'),
                               'master':spark.sparkContext.master,'application_id':spark.sparkContext.applicationId,
                               'aqe_enabled':spark.conf.get('spark.sql.adaptive.enabled'),
                               'script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest()},
                      scope='Gold HDFS storage + Spark local[2]; bounded driver comparison only for small output summaries; original ETL still local.',
                      created_at_utc=datetime.now(timezone.utc).isoformat())
        step('create_output_parent', lambda: cli.run('-mkdir','-p','/it5427/experiments/hdfs_demo'))
        if cli.exists(final):
            raise ValueError('Output appeared; staging retained')
        step('commit_output', lambda: cli.run('-mv', stage, final))
        report.parent.mkdir(parents=True, exist_ok=True)
        with report.open('x', encoding='utf-8') as stream:
            json.dump(result, stream, ensure_ascii=False, indent=2)
        progress('PASSED '+str(report))
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
    finally:
        spark.stop()


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--snapshot', required=True)
    p.add_argument('--run-id', required=True)
    p.add_argument('--report', required=True)
    a = p.parse_args()
    run(a.snapshot, a.run_id, a.report)
