import sys
from pathlib import Path
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
import spark_hdfs_demo_v2 as job
from install_hdfs_roundtrip_fix import patch


class ComparisonTests(unittest.TestCase):
    def test_order_does_not_matter(self):
        job.assert_same_rows([(1,'a'),(2,'b')], [(2,'b'),(1,'a')])

    def test_duplicate_multiplicity_matters(self):
        with self.assertRaises(ValueError):
            job.assert_same_rows([(1,),(1,),(2,)], [(1,),(2,),(2,)])

    def test_null_nan_infinity_and_signed_zero(self):
        job.assert_same_rows([(None,float('nan'),float('inf'),-0.0)],
                             [(None,float('nan'),float('inf'),0.0)])

    def test_changed_value_detected(self):
        with self.assertRaises(ValueError):
            job.assert_same_rows([('host',1)], [('host',2)])

    def test_limit_rejects_truncation(self):
        class Fake:
            def limit(self,n):
                self.n=n
                return self
            def collect(self):
                return [(i,) for i in range(self.n)]
        with self.assertRaises(ValueError):
            job.bounded_rows(Fake(), 3)

    def test_installer_preserves_guards_and_is_idempotent(self):
        old="args=['600s','spark.sql.adaptive.enabled=false','/opt/it5427/spark_hdfs_demo.py']"
        result=patch(old)
        self.assertIn('/project/scripts/spark_hdfs_demo_v2.py',result)
        self.assertIn('600s',result)
        self.assertEqual(patch(result),result)

    def test_installer_refuses_unknown_launcher(self):
        with self.assertRaises(ValueError):
            patch('print(42)')


class SparkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        from pyspark.sql import SparkSession
        cls.spark=(SparkSession.builder.master('local[2]').appName('IT5427 roundtrip regression')
                   .config('spark.sql.adaptive.enabled','false').config('spark.sql.shuffle.partitions','2')
                   .config('spark.ui.enabled','false').getOrCreate())
        cls.spark.sparkContext.setLogLevel('WARN')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_spark_parquet_roundtrip_and_corruption(self):
        from pyspark.sql import functions as F
        spark=self.spark
        with tempfile.TemporaryDirectory() as folder:
            source=spark.createDataFrame([('w7',True,True),('w7',True,True),('w10',True,False)],
                                         ['entity_id','has_network','has_os'])
            aggregate=source.groupBy('entity_id','has_network','has_os').agg(F.count('*').alias('windows'))
            result=job.write_and_check(aggregate, Path(folder).as_uri()+'/host', 'host')
            self.assertEqual(result['rows'],2)
            scalar=spark.createDataFrame([('x',None),('x',None),('y',float('nan')),('z',-0.0)],'name string, value double')
            result=job.write_and_check(scalar, Path(folder).as_uri()+'/scalar', 'scalar')
            self.assertEqual(result['rows'],4)
            with self.assertRaises(ValueError):
                job.assert_same_rows(job.bounded_rows(scalar),job.bounded_rows(scalar.dropDuplicates()))
            with self.assertRaises(ValueError):
                job.check_schema(scalar.schema,scalar.select('value','name').schema)

    def test_sensor_aggregate(self):
        from pyspark.sql import functions as F
        with tempfile.TemporaryDirectory() as folder:
            df=self.spark.createDataFrame([('weather','temperature',3,3,-5.0,12.0),
                                           ('weather','temperature',2,2,0.0,15.0),
                                           ('garage','door',4,0,None,None)],
                                          'sensor_stream_id string, metric_name string, observed_count long, numeric_count long, min double, max double')
            aggregate=df.groupBy('sensor_stream_id','metric_name').agg(F.count('*').alias('windows'),
                F.sum('observed_count').alias('source_events_for_this_metric'),
                F.sum('numeric_count').alias('numeric_values'),F.min('min').alias('minimum_value'),
                F.max('max').alias('maximum_value'))
            result=job.write_and_check(aggregate, Path(folder).as_uri()+'/sensor', 'sensor')
            self.assertEqual(result['rows'],2)
            weather=next(r for r in result['preview'] if r['sensor_stream_id']=='weather')
            self.assertEqual(weather['source_events_for_this_metric'],5)
            self.assertEqual(weather['minimum_value'],-5.0)
            self.assertEqual(weather['maximum_value'],15.0)


if __name__=='__main__':
    unittest.main()
