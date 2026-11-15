"""Approved fixed batch -> existing experiment publication -> replay verification."""
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
from airflow.sdk import DAG, Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount


def make_dag(cfg):
    af = cfg['airflow']
    with DAG('toniot_batch_serving', start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
             schedule=None, catchup=False, max_active_runs=1, max_active_tasks=1,
             default_args={'owner': 'it5427', 'retries': 1, 'retry_delay': timedelta(seconds=30)},
             params={'fail_serving_once': Param(False, type='boolean', description='Fail first publish before DB write')},
             tags=['IT5427', 'HDFS', 'PostgreSQL'],
             description='Replay approved batch, publish existing scores and verify idempotency') as dag:
        common = dict(api_version='auto', docker_url='unix://var/run/docker.sock',
            mount_tmp_dir=False, auto_remove='force', force_pull=False, do_xcom_push=False,
            mounts=[Mount('/project', af['project_mount_source'], type='bind', read_only=True),
                    Mount('/reports', af['reports_mount_source'], type='bind')],
            environment={'PYTHONPATH': '/project:/project/scripts', 'PYTHONUNBUFFERED': '1',
                         'PYTHONDONTWRITEBYTECODE': '1', 'HOME': '/tmp'},
            log_opts_max_size='10m', log_opts_max_file='3')
        previous = None
        for phase in ('plan', 'upload', 'reference', 'build', 'compare', 'finalize'):
            command = ['timeout', '--signal=TERM', '--kill-after=20s', '7200s']
            if phase in ('reference', 'build', 'compare'):
                command += ['spark-submit', '--master', 'local[2]', '--driver-memory', '2g',
                            '--conf', 'spark.sql.adaptive.enabled=false']
                if phase == 'reference': command += ['--conf', 'spark.hadoop.fs.defaultFS=file:///']
            else: command += ['python', '-u']
            command += ['/project/scripts/airflow_etl_task.py', '--phase', phase,
                        '--run-id', '{{ run_id }}', '--try-number', '{{ ti.try_number }}', '--fail-once', 'false']
            task = DockerOperator(task_id=phase, image=cfg['spark_tag'], command=command,
                network_mode=af['hdfs_network'], mem_limit='4g', cpus=2.0,
                execution_timeout=timedelta(seconds=7300), **common)
            if previous is not None: previous >> task
            previous = task
        for task_id, phase in (('publish_postgres', 'load'), ('verify_postgres', 'verify')):
            command = ['timeout', '--signal=TERM', '--kill-after=20s', '900s', 'python', '-u',
                '/project/scripts/airflow_serving_bridge.py', '--phase', phase,
                '--run-id', '{{ run_id }}', '--try-number', '{{ ti.try_number }}',
                '--bridge-id', cfg['bridge_id'], '--fail-once',
                "{{ 'true' if params.fail_serving_once else 'false' }}"]
            task = DockerOperator(task_id=task_id, image=cfg['serving_tag'], command=command,
                network_mode=cfg['serving_network'], user=cfg['serving_user'], mem_limit='1536m', cpus=1.0,
                execution_timeout=timedelta(seconds=930), **common)
            previous >> task
            previous = task
    return dag


CONFIG = Path('/opt/airflow/config/serving_bridge.json')
if CONFIG.exists():
    dag = make_dag(json.loads(CONFIG.read_text()))
