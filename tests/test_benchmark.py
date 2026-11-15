"""Tests execute real Spark 3.5.7 with small synthetic inputs, local filesystem."""
import copy
from datetime import datetime
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

PACKAGE = Path(__file__).resolve().parents[1]
FIXTURE = PACKAGE / 'tests/fixtures/project'
sys.path[:0] = [str(PACKAGE / 'infra/benchmark'), str(FIXTURE), str(FIXTURE / 'scripts')]
import common as c
import job


class SummaryTests(unittest.TestCase):
    def config(self): return {'profile': 'matrix', 'config_id': 'cfg', 'trials': c.trials('matrix')}
    def results(self, cfg):
        return [{'status': 'passed', 'config_id': 'cfg', **spec, 'input_rows': spec['percent'] * 100,
                 'workload_seconds': 12 / spec['cores'], 'validation_seconds': 1.0, 'output_bytes': 200}
                for spec in cfg['trials']]
    def test_schedule_balanced_reproducible(self):
        schedule = c.trials('matrix'); self.assertEqual(schedule, c.trials('matrix'))
        self.assertEqual(len(schedule), 27); self.assertEqual(len({s['trial_id'] for s in schedule}), 27)
        for p in (25, 50, 100):
            for core in (1, 2, 4): self.assertEqual(sum(x['percent'] == p and x['cores'] == core for x in schedule), 3)
    def test_median_speedup(self):
        cfg = self.config(); result = c.summarize(cfg, self.results(cfg))
        self.assertEqual(result['status'], 'passed')
        for group in result['groups']: self.assertEqual(group['speedup_vs_local1'], group['cores'])
    def test_incomplete_and_duplicates(self):
        cfg = self.config(); rows = self.results(cfg)
        self.assertEqual(c.summarize(cfg, rows[:-1])['status'], 'incomplete')
        with self.assertRaisesRegex(ValueError, 'duplicate'): c.summarize(cfg, rows + rows[:1])
    def test_changed_inputs_rejected(self):
        cfg = self.config(); rows = self.results(cfg); rows[0]['input_rows'] += 1
        with self.assertRaisesRegex(ValueError, 'Input row count changed'): c.summarize(cfg, rows)
    def test_failed_trial_rejected(self):
        cfg = self.config(); rows = self.results(cfg); rows[0]['status'] = 'failed'
        with self.assertRaisesRegex(ValueError, 'failed trial'): c.summarize(cfg, rows)
    def test_smoke_has_no_speedup_claim(self):
        cfg = {'profile': 'smoke', 'config_id': 'cfg', 'trials': c.trials('smoke')}
        result = c.summarize(cfg, self.results(cfg))
        self.assertIsNone(result['groups'][0]['speedup_vs_local1'])


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
        cls.temp = tempfile.TemporaryDirectory(); cls.root = Path(cls.temp.name)
        cls.spark = job.session(2, 2)
        cls.plan = {'plan_id': 'f' * 24, 'window_seconds': 60,
                    'entities': {'version': 'synthetic-v1', 'hosts': [{'entity_id': 'h1', 'ip': '10.0.0.1'},
                                                                   {'entity_id': 'h2', 'ip': '10.0.0.2'}]}}
        cls.source = (cls.root / 'source').as_uri(); cls.inputs = cls.source + '/silver'
        ts = datetime(2019, 4, 2, 0, 0, 1)
        net = cls.spark.createDataFrame([
            ('n1', ts, '10.0.0.1', '10.0.0.2', 80, 100, 20, []),
            ('n2', ts, '10.0.0.1', '8.8.8.8', 53, 30, 10, []),
            ('n3', ts, '8.8.8.8', '1.1.1.1', 53, 5, 5, [])],
            'event_id string,event_time_utc timestamp,src_ip string,dst_ip string,dst_port long,src_bytes long,dst_bytes long,quality_flags array<string>')
        host = cls.spark.createDataFrame([
            ('o1', ts, 'h1', 'host_cpu_pct', 40.0), ('o2', ts, 'h1', 'host_cpu_pct', 60.0),
            ('o1', ts, 'h1', 'memory_available_bytes', 1000.0)],
            'event_id string,event_time_utc timestamp,entity_id string,metric_name string,numeric_value double')
        sensor = cls.spark.createDataFrame([
            ('s1', ts, 'weather', 'temperature', 20.0, None, None),
            ('s2', ts, 'weather', 'temperature', 28.0, None, None),
            ('s3', ts, 'garage', 'door_state', None, 'open', True)],
            'event_id string,event_time_utc timestamp,sensor_stream_id string,metric_name string,numeric_value double,categorical_value string,boolean_value boolean')
        for name, df in zip(c.SILVER, (net, host, sensor)): df.write.parquet(cls.inputs + '/' + name)
        job.write_gold(cls.spark, cls.inputs, cls.source + '/gold', cls.plan)
        from hdfs_etl_core import HadoopStore
        fs = HadoopStore(cls.spark, cls.source)
        fs.write(cls.source + '/snapshot.json', {'status': 'committed', 'plan': cls.plan,
                 'tables': {'silver/' + n: {'rows': 3} for n in c.SILVER}})

    @classmethod
    def tearDownClass(cls): cls.spark.stop(); cls.temp.cleanup()

    def test_known_aggregate_values_and_host_join(self):
        gold = self.spark.read.parquet(self.source + '/gold/host_window_features')
        h1 = gold.filter("entity_id = 'h1'").first()
        self.assertEqual(h1.n_network, 2); self.assertEqual(h1.n_os, 2)
        self.assertEqual(h1.host_cpu_mean, 50.0); self.assertEqual(h1.flow_start_sent_bytes, 130)
        self.assertTrue(h1.has_network and h1.has_os)
        h2 = gold.filter("entity_id = 'h2'").first()
        self.assertEqual(h2.flow_start_received_bytes, 100); self.assertFalse(h2.has_os)
        self.assertEqual(self.spark.read.parquet(self.source + '/gold/network_overview').first().flow_count, 3)
        sensor = self.spark.read.parquet(self.source + '/gold/sensor_window_features')
        self.assertEqual(sensor.filter("sensor_stream_id='weather'").first()['mean'], 24.0)

    def test_sampling_nested_deterministic_and_event_coherent(self):
        from pyspark.sql import functions as F
        df = self.spark.range(1000).select(F.concat(F.lit('e'), F.col('id')).alias('event_id'))
        df = df.unionByName(df)
        selected = {}
        for percent in (25, 50, 100):
            picked = job.sampled(df, percent)
            self.assertFalse(picked.groupBy('event_id').count().filter('count != 2').count())
            selected[percent] = {r.event_id for r in picked.select('event_id').distinct().collect()}
        self.assertTrue(selected[25] < selected[50] < selected[100])
        self.assertEqual(selected[25], {r.event_id for r in job.sampled(df, 25).collect()})

    def test_prepare_trial_validate_and_reuse_rejected(self):
        out = (self.root / 'benchmark').as_uri(); report = self.root / 'reports'; report.mkdir()
        cfg = {'output_root': out, 'source_uri': self.source, 'config_id': 'test', 'plan': self.plan,
               'partitions': 2, 'percents': [100]}
        job.prepare(self.spark, cfg, report)
        prepared = c.read(report / 'prepared.json')
        self.assertEqual(prepared['status'], 'passed')
        self.assertIn('approved_gold_comparison', prepared['subsets']['100'])
        spec = {'trial_id': 'p100-c2-r1', 'percent': 100, 'cores': 2, 'repeat': 1}
        job.trial(self.spark, cfg, spec, report)
        result = c.read(report / 'p100-c2-r1.json')
        self.assertEqual(result['input_rows'], 9); self.assertGreater(result['workload_seconds'], 0)
        self.assertEqual(result['output_rows']['host_window_features'], 2)
        self.assertGreater(result['validation_seconds'], 0)
        with self.assertRaisesRegex(ValueError, 'refusing result reuse'): job.trial(self.spark, cfg, spec, report)
        with self.assertRaisesRegex(ValueError, 'already exists'): job.prepare(self.spark, cfg, report)

    def test_changed_output_is_rejected(self):
        from pyspark.sql import functions as F
        dest = (self.root / 'corrupt').as_uri()
        job.write_gold(self.spark, self.inputs, dest, self.plan)
        # Independent in-memory replacement avoids overwriting a file being read.
        bad = self.spark.createDataFrame([(1554163200, 999)], 'window_start long, flow_count long')
        bad.write.mode('overwrite').parquet(dest + '/network_overview')
        with self.assertRaisesRegex(ValueError, 'Gold comparison failed'):
            job.compare_gold(self.spark, self.source + '/gold', dest, self.plan)


if __name__ == '__main__': unittest.main(verbosity=2)
