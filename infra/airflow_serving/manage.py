#!/usr/bin/env python3
"""Additive helper for the approved TON_IoT batch + serving DAG; run in WSL."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
sys.path.insert(0, str(PROJECT / 'scripts'))
from airflow_serving_bridge import CONFIG, DAG_ID, completion, digest, key, load_config, read, sha, validate_inputs, write


def command(args, capture=False, check=True, timeout=60):
    if not capture: print('RUN:', ' '.join(map(str, args)), flush=True)
    return subprocess.run(list(map(str, args)), capture_output=capture, text=True, check=check, timeout=timeout)


def dc(stack, *args, **kw):
    folder = PROJECT / 'infra' / stack
    cmd = ['docker', 'compose']
    if (folder / '.env').exists(): cmd += ['--env-file', folder / '.env']
    return command(cmd + ['-f', folder / 'compose.yaml', *args], **kw)


def inspect(ref, image=False):
    return json.loads(command(['docker', *(['image'] if image else []), 'inspect', ref], capture=True).stdout)[0]


def array_output(text, field):
    decoder = json.JSONDecoder()
    for i, char in enumerate(text):
        if char != '[': continue
        try: value, _ = decoder.raw_decode(text[i:])
        except ValueError: continue
        if isinstance(value, list) and all(isinstance(x, dict) and field in x for x in value): return value
    raise ValueError('Could not parse Airflow JSON; see command output')


def healthy(stack, service):
    cid = dc(stack, 'ps', '-q', service, capture=True).stdout.strip()
    if not cid: raise ValueError(stack + '/' + service + ' is not running')
    obj = inspect(cid)
    if obj['State'].get('Health', {}).get('Status') != 'healthy':
        raise ValueError(stack + '/' + service + ' is not healthy')
    return obj


def ensure_idle():
    # The old DAG remains available. Avoid starting two lab pipelines together.
    for dag_id in ('toniot_hdfs_batch', DAG_ID):
        for state in ('running', 'queued'):
            result = dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'list-runs',
                        dag_id, '--state', state, '--output', 'json', capture=True, check=False)
            if result.returncode:
                text = result.stdout + result.stderr
                if dag_id == DAG_ID and ('not found' in text.lower() or 'does not exist' in text.lower()): continue
                raise ValueError('Cannot check active DAG runs: ' + text[-1800:])
            if array_output(result.stdout, 'run_id'):
                raise ValueError(dag_id + ' has an active run; wait for it to finish')


def pin_image(image_id, role):
    if not re.fullmatch(r'sha256:[0-9a-f]{64}', image_id): raise ValueError('Invalid Docker image ID')
    tag = 'it5427-bridge-' + role + ':' + image_id.split(':')[1][:24]
    found = command(['docker', 'image', 'inspect', tag], capture=True, check=False)
    if found.returncode == 0:
        if json.loads(found.stdout)[0]['Id'] != image_id: raise ValueError('Pinned image tag conflicts: ' + tag)
    else:
        command(['docker', 'tag', image_id, tag])
    return tag


def configure():
    if not hasattr(os, 'getuid') or os.getuid() == 0: raise ValueError('Run as your normal WSL user, without sudo')
    healthy('airflow', 'scheduler'); healthy('airflow', 'dag-processor'); ensure_idle()
    af = read(PROJECT / 'infra/airflow/config/runtime.json')
    from airflow_etl_task import validate_approved
    plan, _ = validate_approved(PROJECT, af)
    for service in ('namenode', 'datanode'): healthy('hdfs', service)
    pg = healthy('serving', 'postgres')
    networks = [name for name, info in pg['NetworkSettings']['Networks'].items() if 'postgres' in (info.get('Aliases') or [])]
    if len(networks) != 1: raise ValueError('Cannot determine the serving PostgreSQL network')
    spark = inspect(af['spark_image'], image=True)
    if spark['Id'] != af['spark_image_id']: raise ValueError('Original Spark image changed after Airflow configure')
    serving = read(PROJECT / 'infra/serving/config.json')
    probe = 'it5427-serving-bridge-probe-' + uuid.uuid4().hex[:8]
    try:
        dc('serving', 'run', '-d', '--no-deps', '--name', probe, 'loader', 'python', '-c',
           'import time; time.sleep(120)', capture=True)
        obj = inspect(probe)
        mounts = {m['Destination']: m for m in obj['Mounts']}
        if mounts['/project']['Source'] != af['project_mount_source']:
            raise ValueError('Airflow and serving mount different project folders')
        if obj['Config']['User'] != str(os.getuid()) + ':' + str(os.getgid()):
            raise ValueError('Serving UID:GID differs from current WSL user')
        code = "import hashlib,json,pathlib; print(json.dumps({n:hashlib.sha256((pathlib.Path('/app')/n).read_bytes()).hexdigest() for n in ('loader.py','core.py','schema.sql')}))"
        hashes = json.loads(command(['docker', 'exec', probe, 'python', '-c', code], capture=True).stdout)
    finally:
        command(['docker', 'rm', '-f', probe], capture=True, check=False)
    for name, checksum in hashes.items():
        if sha(PROJECT / 'infra/serving/app' / name) != checksum:
            raise ValueError('Serving image differs from app source: ' + name + '; rebuild the serving image first')
    target = PROJECT / 'infra/airflow/dags/toniot_batch_serving.py'
    config = PROJECT / CONFIG
    source = HERE / 'toniot_batch_serving.py'
    if target.exists() and target.read_bytes() != source.read_bytes() and not config.exists():
        raise ValueError('Refusing to overwrite an unrelated DAG: ' + str(target))
    old = {p: p.read_bytes() if p.exists() else None for p in (target, config)}
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_suffix('.py.tmp'); temp.write_bytes(source.read_bytes()); temp.replace(target)
        tracked = ['infra/airflow/config/runtime.json', 'infra/serving/config.json',
                   'scripts/airflow_serving_bridge.py', 'infra/airflow/dags/toniot_batch_serving.py',
                   *af['adapter_hashes'], *['infra/serving/app/' + n for n in hashes]]
        cfg = {'version': 1, 'airflow': af, 'source_snapshot': plan['baseline_snapshot'],
               'hdfs_snapshot': plan['plan_id'], 'spark_image_id': spark['Id'],
               'spark_tag': pin_image(spark['Id'], 'spark'), 'serving_image_id': obj['Image'],
               'serving_tag': pin_image(obj['Image'], 'serving'), 'serving_network': networks[0],
               'serving_user': obj['Config']['User'], 'app_hashes': hashes,
               'experiment_ids': [x['experiment_id'] for x in serving['experiments']],
               'file_hashes': {p: sha(PROJECT / p) for p in tracked}}
        cfg['bridge_id'] = digest(cfg)[:24]
        validate_inputs(PROJECT, cfg); write(config, cfg)
    except Exception:
        for path, data in old.items():
            if data is None: path.unlink(missing_ok=True)
            else: path.write_bytes(data)
        raise
    print('CONFIGURED:', DAG_ID, 'bridge_id:', cfg['bridge_id'])
    print('Wait about 30 seconds for DAG discovery, then run status and trigger.')


def trigger(fail_once):
    cfg = load_config(PROJECT); validate_inputs(PROJECT, cfg); ensure_idle()
    healthy('serving', 'postgres')
    for service in ('namenode', 'datanode'): healthy('hdfs', service)
    for role in ('spark', 'serving'):
        if inspect(cfg[role + '_tag'], image=True)['Id'] != cfg[role + '_image_id']:
            raise ValueError('Pinned image changed: ' + role)
    result = dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'list', '--output', 'json', capture=True)
    if DAG_ID not in {x['dag_id'] for x in array_output(result.stdout, 'dag_id')}:
        raise ValueError('New DAG is not discovered yet; run status and check import errors')
    run_id = 'it5427_serving_' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '_' + uuid.uuid4().hex[:6]
    dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'unpause', DAG_ID)
    dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'trigger', DAG_ID, '--run-id', run_id,
       '--conf', json.dumps({'fail_serving_once': fail_once}))
    write(HERE / 'state/last_run.json', {'run_id': run_id, 'fail_serving_once': fail_once})
    print('RUN_ID:', run_id, '\nUI: http://localhost:8080')
    print('Reports:', PROJECT / 'reports/hdfs' / key(run_id))


def report(run_id):
    run_id = run_id or read(HERE / 'state/last_run.json')['run_id']
    if not re.fullmatch(r'it5427_serving_[A-Za-z0-9_]+', run_id): raise ValueError('Invalid bridge run ID')
    folder = PROJECT / 'reports/hdfs' / key(run_id); folder.mkdir(parents=True, exist_ok=True)
    result = dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'tasks', 'states-for-dag-run',
                DAG_ID, run_id, '--output', 'json', capture=True, check=False)
    (folder / 'airflow_task_states.txt').write_text(result.stdout + '\n' + result.stderr)
    try:
        if result.returncode: raise ValueError('Airflow task states command failed')
        states = array_output(result.stdout, 'task_id')
        summary = completion(folder, run_id, states)
        write(folder / 'bridge_export_status.json', {'status': 'passed', 'run_id': run_id, 'all_eight_tasks_success': True})
        print(json.dumps(summary, ensure_ascii=False, indent=2))
    except Exception as exc:
        write(folder / 'bridge_export_status.json', {'status': 'incomplete', 'run_id': run_id, 'reason': str(exc)})
        print('NOT COMPLETE:', exc, '\nReport still exported for diagnosis.')
    archive = folder.with_name(folder.name + '-serving.zip')
    logs = PROJECT / 'infra/airflow/logs' / ('dag_id=' + DAG_ID) / ('run_id=' + run_id)
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:
        for base, prefix in ((folder, 'reports'), (logs, 'airflow_logs')):
            for path in sorted(base.rglob('*')):
                if path.is_file() and path.suffix in ('.json', '.txt', '.log'):
                    data = path.read_bytes()
                    if len(data) > 5 * 1024**2: data = b'[tail only]\n' + data[-5 * 1024**2:]
                    z.writestr(prefix + '/' + path.relative_to(base).as_posix(), data)
    print('SEND_THIS_FILE:', archive)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['configure', 'status', 'trigger', 'report'])
    p.add_argument('--fail-serving-once', action='store_true'); p.add_argument('--run-id')
    a = p.parse_args()
    if a.action == 'configure': configure()
    elif a.action == 'trigger': trigger(a.fail_serving_once)
    elif a.action == 'report': report(a.run_id)
    else:
        dc('airflow', 'ps')
        dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'list-import-errors', '--output', 'json')
        dc('airflow', 'exec', '-T', 'scheduler', 'airflow', 'dags', 'list', '--output', 'table')


if __name__ == '__main__':
    try: main()
    except (Exception, KeyboardInterrupt) as exc:
        print('FAILED:', str(exc), file=sys.stderr); sys.exit(1)
