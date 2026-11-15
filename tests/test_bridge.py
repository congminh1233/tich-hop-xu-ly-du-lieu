"""Real Airflow parse; synthetic serving results, no live Docker/PostgreSQL."""
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

PACKAGE = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PACKAGE / 'scripts'))
import airflow_serving_bridge as b


class BridgeTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.base = Path(self.temp.name)
        self.project = self.base / 'project'
        shutil.copytree(PACKAGE / 'tests/fixtures/project', self.project)
        self.reports = self.project / 'reports/hdfs'
        self.af = b.read(self.project / 'infra/airflow/config/runtime.json')
        self.approved = self.reports / self.af['approved_report']
        self.plan = b.read(self.approved / 'plan.json')
        self.run_id = 'it5427_serving_test'; self.root = self.reports / b.key(self.run_id)
        self.root.mkdir()
        for name in ('plan.json', 'build.json', 'comparison.json'): shutil.copy2(self.approved / name, self.root / name)
        b.write(self.root / 'run.json', {'run_id': self.run_id})
        b.write(self.root / 'summary.json', {'status': 'passed', 'airflow_run_id': self.run_id,
                'hdfs_snapshot': self.plan['plan_id'], 'hdfs_fsck': 'HEALTHY'})
        self.folder = self.project / 'experiments/synthetic'
        b.write(self.folder / 'summary.json', {'status': 'completed', 'snapshot_id': self.plan['baseline_snapshot']})
        self.app = self.base / 'app'; self.app.mkdir()
        for name in ('loader.py', 'core.py', 'schema.sql'): (self.app / name).write_text('synthetic image fixture')
        self.cfg = {'version': 1, 'airflow': self.af, 'source_snapshot': self.plan['baseline_snapshot'],
                    'hdfs_snapshot': self.plan['plan_id'], 'app_hashes': {p.name: b.sha(p) for p in self.app.iterdir()},
                    'file_hashes': {}, 'spark_tag': 'test-spark:pinned', 'serving_tag': 'test-serving:pinned',
                    'serving_network': 'test-serving-net', 'serving_user': '1000:1000'}
        self.set_experiment()
        (self.project / 'infra/serving/.env').write_text('POSTGRES_PASSWORD=' + 'a' * 48 + '\n')
        self.calls = []
        self.old_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear(); os.environ.update(self.old_env)
        sys.path[:] = [p for p in sys.path if not p.startswith(str(self.base))]
        self.temp.cleanup()

    def save_cfg(self):
        self.cfg.pop('bridge_id', None); self.cfg['bridge_id'] = b.digest(self.cfg)[:24]
        b.write(self.project / b.CONFIG, self.cfg)

    def set_experiment(self):
        identity = {'adapter_version': 1, 'files': {'summary.json': b.sha(self.folder / 'summary.json')}}
        self.item = {'path': 'experiments/synthetic', 'experiment_id': b.digest(identity), **identity}
        b.write(self.project / 'infra/serving/config.json', {'version': 1, 'experiments': [self.item]})
        self.cfg['experiment_ids'] = [self.item['experiment_id']]; self.save_cfg()

    def runner(self, command, project, config, out):
        self.calls.append(command)
        docs = b.read(config)['documents']; self.assertEqual(len(docs), 2)
        for doc in docs: self.assertEqual(b.sha(project / doc['path']), doc['sha256'])
        value = {'status': 'passed', 'experiments': [{'experiment_id': self.item['experiment_id'], 'action': 'reused'}],
                 'verification': [{'experiment_id': self.item['experiment_id'], 'status': 'passed',
                                   'digest_matches': True, 'rows': 12}]}
        b.write(out, value); return value

    def phase(self, phase, fail='false', attempt=1):
        a = SimpleNamespace(phase=phase, run_id=self.run_id, bridge_id=self.cfg['bridge_id'],
                            try_number=attempt, fail_once=fail)
        b.execute(a, self.project, self.reports, self.app, self.runner)

    def states(self): return [{'task_id': p, 'state': 'success'} for p in b.PHASES]

    def test_load_verify_completion(self):
        self.phase('load'); self.phase('verify')
        result = b.completion(self.root, self.run_id, self.states())
        self.assertTrue(result['replay_all_reused']); self.assertEqual(self.calls, ['load', 'verify-replay'])

    def test_retry_before_any_database_call(self):
        with self.assertRaisesRegex(RuntimeError, 'INTENTIONAL_SERVING_RETRY'): self.phase('load', 'true')
        self.assertEqual(self.calls, [])
        self.phase('load', 'true', 2); self.phase('verify')
        self.assertTrue(b.read(self.root / 'serving/summary.json')['retry_probe_triggered'])

    def test_missing_load_receipt(self):
        with self.assertRaises(FileNotFoundError): self.phase('verify')
        self.assertEqual(self.calls, [])

    def test_wrong_source_snapshot_even_with_new_hashes(self):
        b.write(self.folder / 'summary.json', {'status': 'completed', 'snapshot_id': 'other'})
        self.set_experiment()
        with self.assertRaisesRegex(ValueError, 'source snapshot'): self.phase('load')

    def test_changed_experiment_bytes(self):
        with (self.folder / 'summary.json').open('a') as f: f.write(' ')
        with self.assertRaisesRegex(ValueError, 'checksum changed'): self.phase('load')

    def test_failed_gold_check(self):
        p = self.root / 'comparison.json'; obj = b.read(p)
        obj['tables']['gold/host_window_features']['different_rows'] = 1; b.write(p, obj)
        with self.assertRaisesRegex(ValueError, 'Gold equality'): self.phase('load')

    def test_wrong_run_finalize(self):
        p = self.root / 'summary.json'; obj = b.read(p); obj['airflow_run_id'] = 'other'; b.write(p, obj)
        with self.assertRaisesRegex(ValueError, 'finalize'): self.phase('load')

    def test_changed_etl_code(self):
        with (self.project / 'scripts/hdfs_etl_core.py').open('a') as f: f.write('\n# changed\n')
        with self.assertRaisesRegex(ValueError, 'ETL code changed'): self.phase('load')

    def test_changed_image_code(self):
        (self.app / 'core.py').write_text('different')
        with self.assertRaisesRegex(ValueError, 'image code'): self.phase('load')

    def test_etl_changes_between_publish_and_verify(self):
        self.phase('load')
        with (self.root / 'summary.json').open('a') as f: f.write(' ')
        with self.assertRaisesRegex(ValueError, 'stale'): self.phase('verify')

    def test_failed_rerun_removes_passed_summary(self):
        self.phase('load'); self.phase('verify')
        (self.app / 'core.py').write_text('different')
        with self.assertRaises(ValueError): self.phase('load')
        self.assertFalse((self.root / 'serving/summary.json').exists())

    def test_report_rejects_stale_etl(self):
        self.phase('load'); self.phase('verify')
        with (self.root / 'summary.json').open('a') as f: f.write(' ')
        with self.assertRaisesRegex(ValueError, 'stale'): b.completion(self.root, self.run_id, self.states())

    def test_report_requires_all_eight_success(self):
        self.phase('load'); self.phase('verify')
        states = self.states(); states[-1]['state'] = 'running'
        with self.assertRaisesRegex(ValueError, 'not all succeeded'): b.completion(self.root, self.run_id, states)

    def test_config_change_detected(self):
        self.cfg['source_snapshot'] = 'tampered'; b.write(self.project / b.CONFIG, self.cfg)
        with self.assertRaisesRegex(ValueError, 'configuration identity'): self.phase('load')

    def test_changed_tracked_file(self):
        rel = 'infra/serving/config.json'; self.cfg['file_hashes'][rel] = b.sha(self.project / rel); self.save_cfg()
        with (self.project / rel).open('a') as f: f.write(' ')
        with self.assertRaisesRegex(ValueError, 'Configured file changed'): self.phase('load')

    def test_real_airflow_dag_parse_render_serialization(self):
        module = runpy.run_path(str(PACKAGE / 'infra/airflow_serving/toniot_batch_serving.py'))
        dag = module['make_dag'](self.cfg)
        self.assertEqual(dag.task_ids, list(b.PHASES)); self.assertEqual(dag.max_active_tasks, 1)
        self.assertEqual(dag.max_active_runs, 1)
        for i, phase in enumerate(b.PHASES):
            task = dag.get_task(phase)
            self.assertEqual(task.upstream_task_ids, set() if i == 0 else {b.PHASES[i-1]})
            self.assertEqual(task.retries, 1)
            self.assertNotIn('PGPASSWORD', task.environment)
            task.render_template_fields({'run_id': self.run_id, 'ti': SimpleNamespace(try_number=2),
                                         'params': {'fail_serving_once': True}})
            self.assertIn(self.run_id, task.command)
            self.assertEqual(task.image, self.cfg['spark_tag' if i < 6 else 'serving_tag'])
            self.assertEqual(task.network_mode, self.af['hdfs_network'] if i < 6 else 'test-serving-net')
            self.assertEqual('spark.hadoop.fs.defaultFS=file:///' in task.command, phase == 'reference')
        from airflow.serialization.serialized_objects import SerializedDAG
        self.assertIn('dag', SerializedDAG.to_dict(dag))


class ManagerTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('bridge_manager', PACKAGE / 'infra/airflow_serving/manage.py')
        cls.m = importlib.util.module_from_spec(spec); spec.loader.exec_module(cls.m)

    def test_cli_json_with_log_prefix(self):
        self.assertEqual(self.m.array_output('warning\n[{"run_id":"r"}]\n', 'run_id'), [{'run_id': 'r'}])
        self.assertEqual(self.m.array_output('[]\n', 'run_id'), [])

    def test_pin_local_tag(self):
        image_id = 'sha256:' + 'a' * 64
        with patch.object(self.m, 'command', side_effect=[SimpleNamespace(returncode=1), SimpleNamespace(returncode=0)]) as call:
            tag = self.m.pin_image(image_id, 'serving')
        self.assertEqual(tag, 'it5427-bridge-serving:' + 'a' * 24)
        self.assertEqual(call.call_args.args[0], ['docker', 'tag', image_id, tag])

    def test_conflicting_local_tag_rejected(self):
        with patch.object(self.m, 'command', return_value=SimpleNamespace(returncode=0, stdout='[{"Id":"other"}]')):
            with self.assertRaisesRegex(ValueError, 'conflicts'): self.m.pin_image('sha256:' + 'a' * 64, 'spark')


if __name__ == '__main__': unittest.main(verbosity=2)
