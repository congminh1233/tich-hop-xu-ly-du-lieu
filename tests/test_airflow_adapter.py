"""Tests use approved report fixtures; no TON_IoT CSV and no Docker daemon needed."""
import copy
import importlib.util
import json
import os
from pathlib import Path
import runpy
import shutil
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

PACKAGE=Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('adapter',PACKAGE/'scripts/airflow_etl_task.py')
adapter=importlib.util.module_from_spec(spec);spec.loader.exec_module(adapter)
FIXTURES=Path(os.environ.get('IT5427_FIXTURE_PROJECT',str(PACKAGE/'tests/fixtures/project')))

class AdapterTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.root=Path(self.temp.name)
        self.project=self.root/'project';shutil.copytree(FIXTURES,self.project)
        self.reports=self.root/'reports';self.calls=[]
        self.config=adapter.read(self.project/'infra/airflow/config/runtime.json')
        self.approved=self.project/'reports/hdfs'/self.config['approved_report']
        sys.path.insert(0,str(self.project/'scripts'))
    def tearDown(self):
        sys.path.remove(str(self.project/'scripts'));self.temp.cleanup()
    def args(self,phase,fail='false',attempt=1):return SimpleNamespace(phase=phase,run_id='test/run:1',try_number=attempt,fail_once=fail)
    def fake_internal(self,a):
        self.calls.append(a.step)
        name=adapter.OUTPUTS[a.step]
        adapter.write(Path(a.report)/name,adapter.read(self.approved/name))
    def run_phase(self,phase,fail='false',attempt=1):
        with patch.dict(sys.modules,{'run_hdfs_etl':SimpleNamespace(internal=self.fake_internal)}):
            adapter.execute(self.args(phase,fail,attempt),self.project,self.reports)
    def test_retry_probe_and_report_idempotency(self):
        self.run_phase('plan')
        with self.assertRaisesRegex(RuntimeError,'INTENTIONAL_RETRY_PROBE'):self.run_phase('upload','true')
        self.assertEqual(self.calls,[])
        self.run_phase('upload','true',2)
        self.run_phase('upload','true',3)
        self.assertEqual(self.calls,['upload','upload'])
        root=self.reports/adapter.run_key('test/run:1')
        self.assertEqual(len(list((root/'attempts').glob('upload-*'))),3)
        self.assertEqual(adapter.read(root/'bronze_transfer.json')['status'],'passed')
    def test_dependency_missing_stops_before_etl(self):
        with self.assertRaises(FileNotFoundError):self.run_phase('build')
        self.assertEqual(self.calls,[])
    def test_source_changes_rejected(self):
        with (self.project/'scripts/hdfs_etl_core.py').open('a') as f:f.write('\n# change\n')
        with self.assertRaisesRegex(ValueError,'ETL code changed'):self.run_phase('plan')
    def test_frozen_plan_change_rejected(self):
        p=self.approved/'plan.json';v=adapter.read(p);v['window_seconds']=120;adapter.write(p,v)
        with self.assertRaisesRegex(ValueError,'checksum'):self.run_phase('plan')
    def test_finalize_and_failure_invalidates_summary(self):
        for phase in ('plan','upload','reference','build','compare'):self.run_phase(phase)
        with patch.object(adapter.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout='Status: HEALTHY',stderr='')):
            self.run_phase('finalize')
        root=self.reports/adapter.run_key('test/run:1')
        self.assertTrue(adapter.read(root/'summary.json')['same_snapshot_id'])
        with patch.object(adapter.subprocess,'run',return_value=SimpleNamespace(returncode=1,stdout='Status: CORRUPT',stderr='')):
            with self.assertRaisesRegex(RuntimeError,'fsck command failed'):self.run_phase('finalize',attempt=2)
        self.assertFalse((root/'summary.json').exists())
    def test_bad_comparison_cannot_pass_finalize(self):
        for phase in ('plan','upload','reference','build','compare'):self.run_phase(phase)
        root=self.reports/adapter.run_key('test/run:1')
        adapter.write(root/'comparison.json',{'status':'failed'})
        with patch.object(adapter.subprocess,'run',return_value=SimpleNamespace(returncode=0,stdout='Status: HEALTHY',stderr='')):
            with self.assertRaisesRegex(ValueError,'Comparison did not pass'):self.run_phase('finalize')
    def test_real_airflow_dag_parse_and_render(self):
        # Load the real DAG using Airflow 3.1.0 and Docker provider 4.4.3, replacing only runtime config path.
        actual_exists=Path.exists;actual_read=Path.read_text
        target='/opt/airflow/config/runtime.json'
        with patch.object(Path,'exists',lambda p: True if str(p)==target else actual_exists(p)), \
             patch.object(Path,'read_text',lambda p,*a,**kw:json.dumps(self.config) if str(p)==target else actual_read(p,*a,**kw)):
            dag=runpy.run_path(str(PACKAGE/'infra/airflow/dags/toniot_hdfs_batch.py'))['dag']
        self.assertEqual(dag.task_ids,list(adapter.PHASES))
        self.assertEqual(dag.max_active_runs,1);self.assertEqual(dag.max_active_tasks,1)
        for index,phase in enumerate(adapter.PHASES):
            task=dag.get_task(phase)
            self.assertEqual(task.upstream_task_ids,set() if index==0 else {adapter.PHASES[index-1]})
            self.assertEqual(task.retries,1);self.assertFalse(task.mount_tmp_dir)
            task.render_template_fields({'run_id':'manual_test','ti':SimpleNamespace(try_number=2),'params':{'fail_once':True}})
            self.assertIn('manual_test',task.command);self.assertIn('true',task.command)
            self.assertEqual('spark.hadoop.fs.defaultFS=file:///' in task.command,phase=='reference')
        from airflow.serialization.serialized_objects import SerializedDAG
        encoded=SerializedDAG.to_dict(dag)
        self.assertIn('dag',encoded)

if __name__=='__main__':unittest.main(verbosity=2)
