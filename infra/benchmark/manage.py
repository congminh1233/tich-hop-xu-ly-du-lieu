#!/usr/bin/env python3
"""Run isolated Spark Silver-to-Gold benchmarks using the existing Docker image."""
import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import time
import uuid
import zipfile

HERE = Path(__file__).resolve().parent
PROJECT = HERE.parents[1]
from common import VERSION, csv_write, digest, read, sha, summarize, trials, write


def command(args, check=True):
    return subprocess.run(list(map(str, args)), capture_output=True, text=True, check=check, timeout=60)


def image_info(image): return json.loads(command(['docker', 'image', 'inspect', image]).stdout)[0]


def check(profile='smoke'):
    if not hasattr(os, 'getuid') or os.getuid() == 0: raise ValueError('Use your normal WSL account, not sudo')
    af = read(PROJECT / 'infra/airflow/config/runtime.json')
    sys.path.insert(0, str(PROJECT / 'scripts'))
    from airflow_etl_task import validate_approved
    plan, _ = validate_approved(PROJECT, af)
    image = image_info(af['spark_image'])
    if image['Id'] != af['spark_image_id']: raise ValueError('Spark Docker image differs from approved runtime')
    info = json.loads(command(['docker', 'info', '--format', '{{json .}}']).stdout)
    if info['NCPU'] < (4 if profile == 'matrix' else 2): raise ValueError('Docker has too few CPUs for this profile')
    if info['MemTotal'] < 8 * 1024**3: raise ValueError('Need at least 8 GiB available to Docker for this profile')
    for service in ('namenode', 'datanode'):
        cid = command(['docker', 'compose', '-f', PROJECT / 'infra/hdfs/compose.yaml', 'ps', '-q', service]).stdout.strip()
        if not cid: raise ValueError('HDFS ' + service + ' is stopped')
        obj = json.loads(command(['docker', 'inspect', cid]).stdout)[0]
        if obj['State'].get('Health', {}).get('Status') != 'healthy': raise ValueError(service + ' is not healthy')
        if af['hdfs_network'] not in obj['NetworkSettings']['Networks']: raise ValueError('HDFS network changed')
    containers = command(['docker', 'ps', '--format', '{{.Names}}']).stdout.splitlines()
    if any(name.startswith('it5427-bench-') for name in containers):
        raise ValueError('Another benchmark container is running; wait before starting a new run')
    busy = [name for name in containers if name.startswith(('it5427-airflow-', 'it5427-serving-'))]
    if busy:
        raise ValueError('Stop the idle Airflow and serving stacks before benchmarking to reduce resource contention: ' + ', '.join(busy))
    facts = {'status': 'passed', 'platform': platform.platform(), 'host_cpu_count': os.cpu_count(),
             'docker_cpus': info['NCPU'], 'docker_memory_bytes': info['MemTotal'],
             'project_disk_free_bytes': shutil.disk_usage(PROJECT).free, 'spark_image_id': image['Id'],
             'running_containers': containers, 'container_memory_limit': '6g', 'driver_memory': '4g',
             'container_cpu_limit': 4 if profile == 'matrix' else 2}
    print(json.dumps(facts, indent=2)); return af, plan, facts


def run_container(cfg, report, action, spec=None):
    af = cfg['runtime']; tid = spec['trial_id'] if spec else 'prepare'
    name = 'it5427-bench-' + uuid.uuid4().hex[:10]
    cores = spec['cores'] if spec else 2
    limit = cfg['cpu_limit']
    args = ['docker', 'run', '--rm', '--pull', 'never', '--name', name,
            '--network', af['hdfs_network'], '--memory', '6g', '--cpus', str(limit),
            '--mount', 'type=bind,src=' + af['project_mount_source'] + ',dst=/project,readonly',
            '--mount', 'type=bind,src=' + af['reports_mount_source'] + ',dst=/reports',
            '-e', 'PYTHONPATH=/project:/project/scripts:/project/infra/benchmark',
            '-e', 'PYTHONDONTWRITEBYTECODE=1', '-e', 'PYTHONUNBUFFERED=1',
            af['spark_image_id'], 'timeout', '--signal=TERM', '--kill-after=20s', '7200s',
            'spark-submit', '--master', f'local[{cores}]', '--driver-memory', '4g',
            '--conf', 'spark.sql.adaptive.enabled=false', '/project/infra/benchmark/job.py', action,
            '--config', '/reports/' + cfg['run_id'] + '/config.json']
    if spec: args += ['--trial-id', tid]
    print('START', tid, 'cores', cores, 'log:', report / (tid + '.log'), flush=True)
    write(report / (tid + '-launch.json'), {'container': name, 'command': args, 'started_utc': datetime.now(timezone.utc).isoformat()})
    start = time.monotonic(); last_message = start
    with (report / (tid + '.log')).open('x') as log:
        process = subprocess.Popen(args, stdout=log, stderr=subprocess.STDOUT, text=True)
        try:
            while process.poll() is None:
                time.sleep(1)
                elapsed = time.monotonic() - start
                if time.monotonic() - last_message >= 30:
                    print(f'RUNNING {tid}: {elapsed:.0f}s; see {tid}.log', flush=True); last_message = time.monotonic()
                if elapsed > 7260: raise TimeoutError('Container exceeded benchmark time limit')
            if process.returncode: raise RuntimeError(f'{tid} exited with code {process.returncode}; see log')
        except BaseException:
            command(['docker', 'stop', '--timeout', '20', name], check=False)
            if process.poll() is None: process.terminate()
            raise
    write(report / (tid + '-wall.json'), {'process_wall_seconds': time.monotonic() - start})
    print('DONE', tid, flush=True)


def export(report):
    archive = report.with_suffix('.zip')
    with zipfile.ZipFile(archive, 'w', zipfile.ZIP_DEFLATED) as z:
        for path in sorted(report.iterdir()):
            if path.is_file() and path.suffix in ('.json', '.csv', '.txt', '.log'):
                content = path.read_bytes()
                if len(content) > 5 * 1024**2: content = b'[tail only]\n' + content[-5 * 1024**2:]
                z.writestr(report.name + '/' + path.name, content)
    print('SEND_THIS_FILE:', archive)


def refresh_summary(report):
    cfg = read(report / 'config.json'); results = []
    for spec in cfg['trials']:
        path = report / (spec['trial_id'] + '.json')
        if path.exists(): results.append(read(path))
    result = summarize(cfg, results); write(report / 'summary.json', result)
    csv_write(report / 'trials.csv', result['samples']); csv_write(report / 'summary.csv', result['groups'])
    print('SUMMARY:', result['status'], result['completed_trials'], '/', result['expected_trials'])
    return result


def run(profile):
    af, plan, facts = check(profile)
    rid = 'bench-' + profile + '-' + datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ') + '-' + uuid.uuid4().hex[:8]
    report = PROJECT / 'reports/hdfs' / rid; report.mkdir(parents=True)
    schedule = trials(profile)
    cfg = {'version': VERSION, 'profile': profile, 'run_id': rid, 'plan': plan, 'runtime': af,
           'source_uri': plan['root'].rstrip('/') + '/snapshots/' + plan['plan_id'],
           'output_root': 'hdfs://namenode:9000/it5427/benchmarks_v1/' + rid,
           'percents': sorted({r['percent'] for r in schedule}), 'partitions': 8,
           'project_disk_free_at_start': facts['project_disk_free_bytes'],
           'cpu_limit': 4 if profile == 'matrix' else 2, 'trials': schedule,
           'files': {str(p.relative_to(PROJECT)): sha(p) for p in sorted(HERE.glob('*.py'))}}
    cfg['config_id'] = digest(cfg)
    write(report / 'config.json', cfg); write(report / 'environment.json', facts)
    write(HERE / 'state/last_run.json', {'run_id': rid})
    print('RUN_ID:', rid, '\nFresh HDFS output:', cfg['output_root'], flush=True)
    try:
        run_container(cfg, report, 'prepare')
        for spec in schedule:
            run_container(cfg, report, 'trial', spec)
            refresh_summary(report)
        print('BENCHMARK_PASSED', profile)
    except (Exception, KeyboardInterrupt) as exc:
        write(report / 'launcher_failure.json', {'status': 'failed', 'error': str(exc) or 'Interrupted by user'})
        raise
    finally:
        refresh_summary(report); export(report)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('action', choices=['check', 'smoke', 'matrix', 'report']); p.add_argument('--run-id')
    a = p.parse_args()
    if a.action == 'check': check()
    elif a.action in ('smoke', 'matrix'): run(a.action)
    else:
        rid = a.run_id or read(HERE / 'state/last_run.json')['run_id']
        if not re.fullmatch(r'bench-(smoke|matrix)-[0-9TZ]+-[a-f0-9]{8}', rid): raise ValueError('Invalid run ID')
        report = PROJECT / 'reports/hdfs' / rid; refresh_summary(report); export(report)


if __name__ == '__main__':
    try: main()
    except (Exception, KeyboardInterrupt) as exc:
        print('FAILED:', str(exc), file=sys.stderr); sys.exit(1)
