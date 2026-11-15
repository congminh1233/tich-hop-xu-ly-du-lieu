"""Synthetic invariants. Run from project root; requires original toniot + Spark 3.5.7."""
import csv
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
import sys

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
from hdfs_etl_common import digest, make_plan, validate_plan, transfer_one, sha, read, save, local_reference
from hdfs_etl_core import build as remote_build, session, HadoopStore
from hdfs_etl_compare import compare, compare_table
from toniot.ingest import ingest_file
from toniot.spark_job import build as original_build
import toniot

PROJECT=Path(toniot.__file__).resolve().parent.parent
SCRIPTS=Path(__file__).resolve().parents[1]/'scripts'

class FakeHdfs:
    """Only filesystem behavior used by Bronze transport, including exclusive mkdir."""
    def __init__(self,root): self.root=Path(root)
    def path(self,p): return self.root/str(p).replace('hdfs://namenode:9000/','').lstrip('/')
    def exists(self,p):return self.path(p).exists()
    def hash(self,p):return sha(self.path(p))
    def read_json(self,p):return read(self.path(p))
    def run(self,*args):
        op,*args=map(str,args)
        if op=='-mkdir':
            recursive=args[0]=='-p'
            for p in args[1:] if recursive else args:self.path(p).mkdir(parents=recursive,exist_ok=recursive)
        elif op=='-put':shutil.copyfile(args[0],self.path(args[1]))
        elif op=='-mv':self.path(args[0]).rename(self.path(args[1]))
        elif op=='-rmdir':self.path(args[0]).rmdir()
        else:raise AssertionError(op)

class HdfsEtlTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp=tempfile.TemporaryDirectory();cls.root=Path(cls.tmp.name)
        cls.project=cls.root/'project';cls.project.mkdir();cls.lake=cls.project/'lake'
        shutil.copytree(PROJECT/'toniot',cls.project/'toniot')
        shutil.copytree(SCRIPTS,cls.project/'scripts')
        def fixture(name,header,rows):
            p=cls.root/name
            with p.open('w',newline='') as f:
                w=csv.writer(f);w.writerow(header);w.writerows(rows)
            ingest_file(p,cls.lake,'synthetic_test_only')
        t=1556137080
        nh='ts,src_ip,dst_ip,src_port,dst_port,proto,duration,src_bytes,dst_bytes,src_pkts,dst_pkts,conn_state,label,type'.split(',')
        good=[t,'192.168.1.193','192.168.1.195',5000,80,'tcp',1,100,20,2,1,'SF',0,'normal']
        bad=good[:];bad[1]='999.1.1.1'
        fixture('Network_dataset_1.csv',nh,[good,good,bad])
        wh=['ts','Processor(_Total) pct_ Processor Time','Memory Available Bytes','label','type']
        fixture('windows7_dataset.csv',wh,[[t+5,10,1000,0,'normal'],[t+15,30,800,1,'backdoor'],[t+120,'',700,0,'normal']])
        fixture('windows10_dataset.csv',wh,[[t+5,60,2000,0,'normal']])
        fixture('IoT_Weather.csv',['date','time','temperature','pressure','humidity','label','type'],
                [['25-Apr-19','0:18:00',20,1000,50,0,'normal'],['25-Apr-19','0:18:01',22,'bad',55,1,'scanning']])
        fixture('IoT_Modbus.csv',['date','time','FC1_Read_Input_Register','FC2_Read_Discrete_Value','FC3_Read_Holding_Register','FC4_Read_Coil','label','type'],
                [['','',1,2,3,4,0,'normal']])
        fixture('IoT_Garage_Door.csv',['date','time','door_state','sphone_signal','label','type'],
                [['25-Apr-19','0:18:00','open',' true ',0,'normal'],['25-Apr-19','0:18:01','closed','0',0,'normal'],['','','','',0,'normal']])
        entities=PROJECT/'config/entities.json';rules=PROJECT/'config/time_rules.verified.json'
        cls.meta=original_build(cls.lake,entities,rules,expected_files=[p.name for p in cls.root.glob('*.csv')],strict=True)
        cls.plan=make_plan(cls.project,cls.meta['snapshot_id'],'smoke')
        # For E2E test cover all 6 source families, not just the 3-file production smoke.
        inputs=[]
        for p in sorted((cls.lake/'bronze').glob('*/*/manifest.json')):
            m=read(p);files={name:{'sha256':sha(p.parent/name),'bytes':(p.parent/name).stat().st_size}
                              for name in (m['file_name'],'records.parquet','manifest.json')}
            inputs.append({'local_folder':p.parent.relative_to(cls.project).as_posix(),'manifest':m,'files':files})
        cls.allplan=dict(cls.plan);cls.allplan['inputs']=inputs
        cls.allplan['plan_id']=digest({k:v for k,v in cls.allplan.items() if k!='plan_id'})[:24]
        validate_plan(cls.allplan)
        cls.spark=session();cls.spark.sparkContext.setLogLevel('ERROR')
        cls.spark.conf.set('spark.sql.shuffle.partitions','2')
        cls.dest,cls.output=remote_build(cls.allplan,cls.spark,root=cls.lake.as_uri())

    @classmethod
    def tearDownClass(cls):
        if hasattr(cls,'spark'):cls.spark.stop()
        cls.tmp.cleanup()

    def test_01_exact_gold_and_accounting(self):
        reference=(self.lake/'snapshots'/self.meta['snapshot_id']).as_uri()
        result=compare(self.spark,reference,self.dest,self.output)
        self.assertEqual(result['status'],'passed')
        self.assertEqual(len([p for p in result['tables'] if p.startswith('gold/')]),3)
        self.assertEqual(sum(r['quarantined_source_rows'] for r in self.output['source_accounting']),3)
        report=os.environ.get('IT5427_TEST_REPORT')
        if report:Path(report).write_text(json.dumps(result,indent=2))

    def test_02_idempotent_build_and_lock(self):
        dest,meta=remote_build(self.allplan,self.spark,root=self.lake.as_uri())
        self.assertEqual(dest,self.dest);self.assertEqual(meta,self.output)
        fs=HadoopStore(self.spark,self.dest)
        lock=self.lake.as_uri()+'/_locks/build-'+self.allplan['plan_id']
        fs.text(lock,'other_owner')
        with self.assertRaises(Exception):remote_build(self.allplan,self.spark,root=self.lake.as_uri())
        self.assertTrue(fs.exists(lock));fs.delete(lock)
        different=dict(self.allplan);different['window_seconds']=120
        with self.assertRaisesRegex(ValueError,'conflicts'):remote_build(different,self.spark,root=self.lake.as_uri())

    def test_03_frozen_plan(self):
        self.assertEqual(len(self.plan['inputs']),3)
        self.assertEqual(self.plan['time_rules'],self.meta['spec']['time_rules'])
        bad=dict(self.plan);bad['window_seconds']=120
        with self.assertRaisesRegex(ValueError,'checksum'):validate_plan(bad)
        with self.assertRaisesRegex(ValueError,'Missing frozen inputs'):make_plan(self.project,self.meta['snapshot_id'],'full')

    def test_04_bronze_retry_and_corruption(self):
        with tempfile.TemporaryDirectory() as td:
            fs=FakeHdfs(td);item=self.plan['inputs'][0]
            self.assertEqual(transfer_one(fs,self.project,item),'imported_verified')
            self.assertEqual(transfer_one(fs,self.project,item),'already_present_verified')
            m=item['manifest'];root=self.plan['root'];dest=root+'/bronze/'+m['source_id']+'/'+m['source_file_id']
            fs.path(dest+'/records.parquet').write_text('corrupted')
            with self.assertRaisesRegex(ValueError,'checksum'):transfer_one(fs,self.project,item)
            lock=root+'/_locks/bronze-'+m['source_file_id'];fs.path(lock).mkdir()
            with self.assertRaises(FileExistsError):transfer_one(fs,self.project,item)
            self.assertTrue(fs.exists(lock))

    def test_05_comparator_detects_errors(self):
        df=self.spark.createDataFrame([(1,1.0,['b','a']),(2,None,[])], 'id long, value double, states array<string>')
        reordered=self.spark.createDataFrame([(1,1.0+1e-10,['a','b']),(2,None,[])],df.schema)
        self.assertEqual(compare_table(df,reordered,['id'])['different_rows'],0)
        from pyspark.sql import functions as F
        cases=[(df.union(df),'Duplicate'),(df.limit(1),'comparison failed'),
               (df.withColumn('value',F.lit(100.0)),'comparison failed'),
               (df.withColumn('id',F.lit(None).cast('long')),'Null')]
        for other,message in cases:
            with self.subTest(message=message):
                with self.assertRaisesRegex(ValueError,message):compare_table(df,other,['id'])

    def test_06_failure_does_not_commit(self):
        bad=json.loads(json.dumps(self.allplan))
        bad['inputs'][0]['manifest']['input_rows']+=1
        bad['plan_id']=digest({k:v for k,v in bad.items() if k!='plan_id'})[:24]
        fs=HadoopStore(self.spark,self.lake.as_uri())
        with self.assertRaisesRegex(ValueError,'manifest differs'):
            remote_build(bad,self.spark,root=self.lake.as_uri())
        self.assertFalse(fs.exists(self.lake.as_uri()+'/snapshots/'+bad['plan_id']))
        self.assertFalse(fs.exists(self.lake.as_uri()+'/_locks/build-'+bad['plan_id']))

    def test_07_smoke_reference_via_symlinks(self):
        # Original builder owns/stops its Spark session; explicitly restart for comparison.
        self.spark.stop()
        report=self.root/'smoke_reference';report.mkdir()
        reference=local_reference(self.project,self.plan,report)
        self.__class__.spark=session();self.spark.sparkContext.setLogLevel('ERROR')
        self.spark.conf.set('spark.sql.shuffle.partitions','2')
        dest,meta=remote_build(self.plan,self.spark,root=self.lake.as_uri())
        self.assertEqual(compare(self.spark,reference,dest,meta)['status'],'passed')

if __name__=='__main__':unittest.main(verbosity=2)
