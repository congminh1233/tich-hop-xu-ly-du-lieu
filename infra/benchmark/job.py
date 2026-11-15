"""Measured Silver -> Gold workload; every trial writes to a fresh HDFS path."""
import argparse
from datetime import datetime, timezone
import os
from pathlib import Path
import re
import sys
import time
import traceback

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
sys.path.insert(0, str(PROJECT)); sys.path.insert(0, str(PROJECT / 'scripts'))
from common import GOLD, SILVER, VERSION, digest, read, sha, write


def log(message): print(datetime.now(timezone.utc).isoformat(), message, flush=True)


def checked_config(path):
    cfg = read(path)
    if cfg['config_id'] != digest({k: v for k, v in cfg.items() if k != 'config_id'}):
        raise ValueError('Benchmark config checksum changed')
    if not re.fullmatch(r'bench-(smoke|matrix)-[0-9TZ]+-[a-f0-9]{8}', cfg['run_id']):
        raise ValueError('Invalid benchmark run ID')
    expected = 'hdfs://namenode:9000/it5427/benchmarks_v1/' + cfg['run_id']
    if cfg['output_root'] != expected: raise ValueError('Unsafe benchmark output root')
    for rel, checksum in cfg['files'].items():
        if sha(PROJECT / rel) != checksum: raise ValueError('File changed after benchmark setup: ' + rel)
    from hdfs_etl_common import validate_plan, code_hash
    validate_plan(cfg['plan'])
    if code_hash(PROJECT) != cfg['plan']['code_hash']: raise ValueError('ETL code changed')
    return cfg


def session(cores, partitions):
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.appName('IT5427 Silver Gold benchmark').master(f'local[{cores}]')
             .config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'false')
             .config('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
             .config('spark.sql.adaptive.enabled', 'false').config('spark.sql.shuffle.partitions', str(partitions))
             .config('spark.sql.files.maxPartitionBytes', str(128 * 1024**2))
             .config('spark.ui.enabled', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('WARN')
    return spark


def sampled(df, percent):
    from pyspark.sql import functions as F
    if percent == 100: return df
    # All metric rows sharing an event_id stay together. Same rows at all CPU settings.
    return df.filter(F.pmod(F.xxhash64('event_id'), F.lit(100)) < F.lit(percent))


def gold_frames(spark, inputs, plan):
    from pyspark.sql import functions as F
    from toniot.spark_job import host_edges, aggregate_network, aggregate_hosts, add_window, col
    net, host, sensor = [spark.read.parquet(inputs + '/' + name) for name in SILVER]
    seconds = plan['window_seconds']
    nw = aggregate_network(host_edges(net, plan['entities'], seconds))
    hw = aggregate_hosts(host, seconds)
    gold = nw.join(hw, ['entity_id', 'window_start'], 'full')
    gold = (gold.withColumn('has_network', F.coalesce(col('n_network') > 0, F.lit(False)))
            .withColumn('has_os', F.coalesce(col('n_os') > 0, F.lit(False)))
            .withColumn('has_cpu', F.coalesce(col('n_cpu_valid') > 0, F.lit(False)))
            .withColumn('window_seconds', F.lit(seconds)).withColumn('snapshot_id', F.lit(plan['plan_id']))
            .withColumn('mapping_version', F.lit(plan['entities']['version'])))
    return {
        'network_overview': add_window(net, seconds).groupBy('window_start').agg(F.count('*').alias('flow_count')),
        'host_window_features': gold,
        'sensor_window_features': add_window(sensor, seconds).groupBy('sensor_stream_id', 'metric_name', 'window_start').agg(
            F.count('*').alias('observed_count'), F.count('numeric_value').alias('numeric_count'),
            F.avg('numeric_value').alias('mean'), F.min('numeric_value').alias('min'), F.max('numeric_value').alias('max'),
            F.stddev_samp('numeric_value').alias('stddev'), F.collect_set('categorical_value').alias('states_seen'),
            F.collect_set('boolean_value').alias('boolean_values_seen'))}


def write_gold(spark, inputs, output, plan):
    frames = gold_frames(spark, inputs, plan)
    for name in GOLD:
        log('WRITE ' + name)
        frames[name].write.mode('errorifexists').parquet(output + '/' + name)
    return frames


def compare_gold(spark, left, right, plan):
    from hdfs_etl_compare import compare_table, GOLD_KEYS
    checks = {}
    for name in GOLD:
        a = spark.read.parquet(left + '/' + name); b = spark.read.parquet(right + '/' + name)
        checks[name] = compare_table(a, b, GOLD_KEYS[name], plan['plan_id'], plan['plan_id'])
    return checks


def prepare(spark, cfg, report):
    from hdfs_etl_core import HadoopStore
    from pyspark.sql import functions as F
    root = cfg['output_root']; source = cfg['source_uri']; fs = HadoopStore(spark, root)
    if fs.exists(root): raise ValueError('Benchmark path already exists; create a new run instead of overwriting')
    meta = fs.read(source + '/snapshot.json')
    if meta.get('status') != 'committed' or meta.get('plan') != cfg['plan']:
        raise ValueError('HDFS source snapshot differs from approved plan')
    source_bytes = sum(fs.fs.getContentSummary(fs.path(source + '/silver/' + name)).getLength() for name in SILVER)
    required = max(5 * 1024**3, int(source_bytes * sum(cfg['percents']) / 100 * 2) + 2 * 1024**3)
    available = fs.fs.getStatus().getRemaining()
    write(report / 'storage.json', {'source_silver_bytes': source_bytes, 'hdfs_remaining_bytes': available,
                                    'project_disk_free_at_start': cfg.get('project_disk_free_at_start'),
                                    'estimated_reserve_bytes': required})
    if available < required: raise ValueError('Not enough HDFS disk space for prepared subsets and outputs')
    if cfg.get('project_disk_free_at_start', required) < required:
        raise ValueError('Project drive has insufficient estimated free space; Docker disk may share this drive')
    fs.mkdir(root); fs.write(root + '/owner.json', {'config_id': cfg['config_id'], 'version': VERSION})
    checks = {}
    for name in SILVER:
        df = spark.read.parquet(source + '/silver/' + name)
        rows = df.count()
        if rows != meta['tables']['silver/' + name]['rows']: raise ValueError('Source Silver row count mismatch')
        if df.filter(F.col('event_id').isNull()).limit(1).count(): raise ValueError('Null sampling event_id')
        checks[name] = rows
    prepared = {'status': 'preparing', 'config_id': cfg['config_id'], 'source_rows': checks,
                'source_snapshot_metadata_digest': digest(meta), 'subsets': {}}
    for percent in cfg['percents']:
        log('PREPARE subset ' + str(percent) + '% (excluded from timed trials)')
        inputs = root + f'/inputs/p{percent:02d}'; counts = {}; sizes = {}
        for name in SILVER:
            path = inputs + '/' + name
            df = sampled(spark.read.parquet(source + '/silver/' + name), percent)
            df.repartition(cfg['partitions']).write.mode('errorifexists').parquet(path)
            counts[name] = spark.read.parquet(path).count()
            if counts[name] <= 0: raise ValueError('Empty prepared source: ' + name)
            if percent == 100 and counts[name] != checks[name]: raise ValueError('100% copy row count mismatch')
            sizes[name] = int(fs.fs.getContentSummary(fs.path(path)).getLength())
        reference = root + f'/references/p{percent:02d}'
        write_gold(spark, inputs, reference, cfg['plan'])
        subset = {'input_uri': inputs, 'reference_uri': reference, 'rows': counts, 'bytes': sizes}
        if percent == 100:
            subset['approved_gold_comparison'] = compare_gold(spark, source + '/gold', reference, cfg['plan'])
        prepared['subsets'][str(percent)] = subset
        write(report / 'prepared.json', prepared)
    prepared['status'] = 'passed'; fs.write(root + '/prepared.json', prepared)
    write(report / 'prepared.json', prepared)
    log('PREPARE_PASSED')


def trial(spark, cfg, spec, report):
    from hdfs_etl_core import HadoopStore
    root = cfg['output_root']; fs = HadoopStore(spark, root)
    prepared = read(report / 'prepared.json')
    if prepared.get('status') != 'passed' or prepared['config_id'] != cfg['config_id']:
        raise ValueError('Missing or incompatible prepared inputs')
    if fs.read(root + '/prepared.json') != prepared: raise ValueError('Prepared receipt changed')
    subset = prepared['subsets'][str(spec['percent'])]
    output = root + '/trials/' + spec['trial_id']
    if fs.exists(output): raise ValueError('Trial output already exists; refusing result reuse')
    if spark.sparkContext.master != f"local[{spec['cores']}]": raise ValueError('Spark master mismatch')
    # Small JVM/Spark warmup only; no target data cached in Spark.
    spark.range(10000).selectExpr('id % 11 AS k').groupBy('k').count().collect()
    started = time.perf_counter()
    frames = write_gold(spark, subset['input_uri'], output, cfg['plan'])
    elapsed = time.perf_counter() - started
    plans = {name: frame._jdf.queryExecution().toString() for name, frame in frames.items()}
    validation_start = time.perf_counter()
    comparisons = compare_gold(spark, subset['reference_uri'], output, cfg['plan'])
    counts = {name: spark.read.parquet(output + '/' + name).count() for name in GOLD}
    validation_seconds = time.perf_counter() - validation_start
    for name, plan in plans.items():
        (report / (spec['trial_id'] + '-' + name + '-explain.txt')).write_text(plan)
    result = {'status': 'passed', 'config_id': cfg['config_id'], **spec,
              'input_rows': sum(subset['rows'].values()), 'input_rows_by_table': subset['rows'],
              'input_bytes': sum(subset['bytes'].values()), 'output_uri': output, 'output_rows': counts,
              'output_bytes': int(fs.fs.getContentSummary(fs.path(output)).getLength()),
              'workload_seconds': elapsed, 'validation_seconds': validation_seconds,
              'comparison': comparisons, 'spark_version': spark.version, 'master': spark.sparkContext.master,
              'spark_conf': {k: v for k, v in spark.sparkContext.getConf().getAll()
                             if k in ('spark.master','spark.driver.memory','spark.sql.shuffle.partitions',
                                      'spark.sql.adaptive.enabled','spark.sql.files.maxPartitionBytes')},
              'finished_utc': datetime.now(timezone.utc).isoformat()}
    write(report / (spec['trial_id'] + '.json'), result)
    log(f"TRIAL_PASSED {spec['trial_id']} workload_seconds={elapsed:.3f}")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['prepare', 'trial']); p.add_argument('--config', required=True)
    p.add_argument('--trial-id'); a = p.parse_args(); report = Path(a.config).parent
    spark = None
    try:
        cfg = checked_config(a.config)
        spec = next((x for x in cfg['trials'] if x['trial_id'] == a.trial_id), None)
        if a.action == 'trial' and spec is None: raise ValueError('Unknown trial')
        spark = session(spec['cores'] if spec else 2, cfg['partitions'])
        if a.action == 'prepare': prepare(spark, cfg, report)
        else: trial(spark, cfg, spec, report)
    except Exception as exc:
        write(report / ((a.trial_id or 'prepare') + '-failure.json'),
              {'status': 'failed', 'error': str(exc), 'traceback': traceback.format_exc()})
        raise
    finally:
        if spark is not None: spark.stop()


if __name__ == '__main__': main()
