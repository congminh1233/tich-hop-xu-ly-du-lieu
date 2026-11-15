"""Manual replay of the approved full TON_IoT batch. No raw data in XCom."""
from datetime import datetime,timedelta,timezone
import json
from pathlib import Path
from airflow.sdk import DAG,Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

CONFIG=Path('/opt/airflow/config/runtime.json')
# This is a local configuration file; no DB/network I/O during DAG parsing.
if CONFIG.exists():
    cfg=json.loads(CONFIG.read_text())
    with DAG('toniot_hdfs_batch',start_date=datetime(2026,1,1,tzinfo=timezone.utc),schedule=None,
             catchup=False,max_active_runs=1,max_active_tasks=1,
             default_args={'owner':'it5427','retries':1,'retry_delay':timedelta(seconds=30)},
             params={'fail_once':Param(False,type='boolean',description='Fail first upload attempt to demonstrate retry')},
             tags=['IT5427','TON_IoT','HDFS'],
             description='Replay approved 28-file ETL; verify identical immutable HDFS snapshot') as dag:
        previous=None
        for phase in ('plan','upload','reference','build','compare','finalize'):
            command=['timeout','--signal=TERM','--kill-after=20s','7200s']
            if phase in ('reference','build','compare'):
                command+=['spark-submit','--master','local[2]','--driver-memory','2g',
                          '--conf','spark.sql.adaptive.enabled=false']
                if phase=='reference':command+=['--conf','spark.hadoop.fs.defaultFS=file:///']
            else:command+=['python','-u']
            command+=['/project/scripts/airflow_etl_task.py','--phase',phase,
                      '--run-id','{{ run_id }}','--try-number','{{ ti.try_number }}',
                      '--fail-once',"{{ 'true' if params.fail_once else 'false' }}"]
            task=DockerOperator(task_id=phase,image=cfg['spark_image'],api_version='auto',
                docker_url='unix://var/run/docker.sock',command=command,
                network_mode=cfg['hdfs_network'],mount_tmp_dir=False,
                mounts=[Mount('/project',cfg['project_mount_source'],type='bind',read_only=True),
                        Mount('/reports',cfg['reports_mount_source'],type='bind')],
                environment={'PYTHONPATH':'/project:/project/scripts','PYTHONUNBUFFERED':'1'},
                mem_limit='4g',cpus=2.0,auto_remove='force',force_pull=False,
                do_xcom_push=False,execution_timeout=timedelta(seconds=7300),
                log_opts_max_size='10m',log_opts_max_file='3')
            if previous is not None:previous >> task
            previous=task
