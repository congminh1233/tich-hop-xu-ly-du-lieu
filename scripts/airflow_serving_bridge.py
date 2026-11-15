"""Publish existing experiments only after this Airflow run passes the approved ETL."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import traceback
import uuid

PROJECT = Path(__file__).resolve().parents[1]
CONFIG = 'infra/airflow/config/serving_bridge.json'
DAG_ID = 'toniot_batch_serving'
PHASES = ('plan', 'upload', 'reference', 'build', 'compare', 'finalize',
          'publish_postgres', 'verify_postgres')
EVIDENCE = ('run.json', 'plan.json', 'summary.json', 'build.json', 'comparison.json')


def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'))


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, ensure_ascii=False,
        separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''): h.update(block)
    return h.hexdigest()


def write(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    temp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    temp.replace(path)


def inside(root, rel):
    root = Path(root).resolve(); path = (root / rel).resolve()
    if not path.is_relative_to(root): raise ValueError('Path outside project: ' + str(rel))
    return path


def key(run_id):
    return 'airflow-' + hashlib.sha256(run_id.encode()).hexdigest()[:24]


def load_config(project, expected=None):
    cfg = read(project / CONFIG)
    identity = digest({k: v for k, v in cfg.items() if k != 'bridge_id'})[:24]
    if cfg.get('version') != 1 or cfg.get('bridge_id') != identity:
        raise ValueError('Invalid bridge configuration identity')
    if expected is not None and identity != expected:
        raise ValueError('Configuration changed after DAG was scheduled')
    for rel, checksum in cfg['file_hashes'].items():
        if sha(inside(project, rel)) != checksum: raise ValueError('Configured file changed: ' + rel)
    return cfg


def validate_inputs(project, cfg):
    sys.path.insert(0, str(project / 'scripts'))
    from airflow_etl_task import validate_approved
    plan, _ = validate_approved(project, cfg['airflow'])
    if (plan['baseline_snapshot'] != cfg['source_snapshot'] or
            plan['plan_id'] != cfg['hdfs_snapshot']):
        raise ValueError('Approved snapshot changed')
    serving = read(project / 'infra/serving/config.json')
    items = serving.get('experiments', [])
    ids = [i['experiment_id'] for i in items]
    if serving.get('version') != 1 or not items or len(set(ids)) != len(ids):
        raise ValueError('Invalid experiment configuration')
    if sorted(ids) != sorted(cfg['experiment_ids']): raise ValueError('Experiment set changed')
    for item in items:
        folder = inside(project, item['path'])
        for name, checksum in item['files'].items():
            if sha(inside(folder, name)) != checksum:
                raise ValueError('Experiment checksum changed: ' + item['path'] + '/' + name)
        identity = digest({'adapter_version': item['adapter_version'], 'files': item['files']})
        if identity != item['experiment_id']: raise ValueError('Experiment identity mismatch')
        summary = read(folder / 'summary.json')
        if summary.get('status') not in ('completed', 'completed_no_fitted_models'):
            raise ValueError('Experiment is incomplete')
        if summary.get('snapshot_id') != plan['baseline_snapshot']:
            raise ValueError('Experiment source snapshot differs from approved baseline')
    return plan, serving


def validate_etl(root, run_id, plan):
    if read(root / 'run.json').get('run_id') != run_id: raise ValueError('ETL run identity mismatch')
    if read(root / 'plan.json') != plan: raise ValueError('ETL plan mismatch')
    summary = read(root / 'summary.json'); build = read(root / 'build.json')
    comparison = read(root / 'comparison.json')
    if (summary.get('status') != 'passed' or summary.get('airflow_run_id') != run_id or
            summary.get('hdfs_snapshot') != plan['plan_id'] or summary.get('hdfs_fsck') != 'HEALTHY'):
        raise ValueError('This run has not passed finalize')
    if (build.get('status') != 'committed' or build.get('snapshot_id') != plan['plan_id'] or
            build.get('uri') != plan['root'].rstrip('/') + '/snapshots/' + plan['plan_id']):
        raise ValueError('Committed HDFS snapshot mismatch')
    if comparison.get('status') != 'passed' or comparison.get('source_accounting_equal') is not True:
        raise ValueError('ETL comparison failed')
    for name in ('host_window_features', 'sensor_window_features', 'network_overview'):
        result = comparison['tables']['gold/' + name]
        if (type(result.get('reference_rows')) is not int or type(result.get('hdfs_rows')) is not int or
                result.get('missing_keys') != 0 or result.get('different_rows') != 0 or
                result.get('reference_rows') != result.get('hdfs_rows')):
            raise ValueError('Gold equality check failed: ' + name)
    return {name: sha(root / name) for name in EVIDENCE}


def set_database_env(project):
    values = {}
    for line in (project / 'infra/serving/.env').read_text().splitlines():
        if line.strip() and not line.lstrip().startswith('#') and '=' in line:
            k, v = line.split('=', 1); values[k.strip()] = v.strip()
    password = values.get('POSTGRES_PASSWORD', '')
    if not re.fullmatch('[0-9a-fA-F]{48}', password):
        raise ValueError('Expected generated serving .env password; do not print the secret')
    os.environ.update(PGHOST='postgres', PGDATABASE='toniot_analytics', PGUSER='toniot', PGPASSWORD=password)


def validate_result(result, ids, replay=False):
    checks = result.get('verification', [])
    if (result.get('status') != 'passed' or
            sorted(x['experiment_id'] for x in checks) != sorted(ids) or
            any(x.get('status') != 'passed' or x.get('digest_matches') is not True for x in checks)):
        raise ValueError('PostgreSQL verification failed')
    if replay:
        actions = result.get('experiments', [])
        if (sorted(x['experiment_id'] for x in actions) != sorted(ids) or
                any(x.get('action') != 'reused' for x in actions)):
            raise ValueError('Replay did not reuse every experiment')


def execute(a, project=PROJECT, reports=Path('/reports'), app=Path('/app'), runner=None):
    root = reports / key(a.run_id); out = root / 'serving'
    out.mkdir(parents=True, exist_ok=True)
    (out / 'summary.json').unlink(missing_ok=True)
    if a.phase == 'load': (out / 'load.json').unlink(missing_ok=True)
    attempt = out / 'attempts' / (a.phase + '-' + str(a.try_number) + '-' + uuid.uuid4().hex[:8])
    write(attempt / 'task.json', vars(a))
    try:
        cfg = load_config(project, a.bridge_id)
        plan, serving = validate_inputs(project, cfg)
        evidence = validate_etl(root, a.run_id, plan)
        for name, checksum in cfg['app_hashes'].items():
            if sha(app / name) != checksum: raise ValueError('Serving image code changed: ' + name)
        if a.phase == 'load' and a.fail_once == 'true':
            try:
                with (out / 'retry_probe.marker').open('x') as f: f.write('Fail before database connection.\n')
            except FileExistsError: pass
            else: raise RuntimeError('INTENTIONAL_SERVING_RETRY: first attempt fails before database connection')
        if a.phase == 'verify':
            previous = read(out / 'load.json')
            if (previous.get('run_id') != a.run_id or previous.get('bridge_id') != cfg['bridge_id'] or
                    previous.get('etl_hashes') != evidence):
                raise ValueError('Load receipt missing, stale or from another run')
            validate_result(previous['result'], cfg['experiment_ids'])
        # Add this run's ETL evidence without editing the existing serving config.
        task_cfg = dict(serving); task_cfg['documents'] = list(serving.get('documents', []))
        for name in ('summary.json', 'comparison.json'):
            task_cfg['documents'].append({'path': 'reports/hdfs/' + key(a.run_id) + '/' + name,
                                          'sha256': evidence[name]})
        write(attempt / 'serving_config.json', task_cfg)
        set_database_env(project)
        if runner is None:
            sys.path.insert(0, str(app))
            from loader import execute as runner
        result = runner('load' if a.phase == 'load' else 'verify-replay', project,
                        attempt / 'serving_config.json', attempt / 'result.json')
        validate_result(result, cfg['experiment_ids'], replay=a.phase == 'verify')
        if validate_etl(root, a.run_id, plan) != evidence: raise ValueError('ETL evidence changed during publish')
        receipt = {'run_id': a.run_id, 'bridge_id': cfg['bridge_id'], 'etl_hashes': evidence, 'result': result}
        write(out / (a.phase + '.json'), receipt)
        if a.phase == 'verify':
            write(out / 'summary.json', {'status': 'passed', 'dag_id': DAG_ID, 'run_id': a.run_id,
                'bridge_id': cfg['bridge_id'], 'source_snapshot': plan['baseline_snapshot'],
                'hdfs_snapshot': plan['plan_id'], 'postgres_status': 'passed', 'replay_all_reused': True,
                'experiments': result['verification'], 'retry_probe_triggered': (out / 'retry_probe.marker').exists(),
                'scope': 'Replay approved batch and publish existing experiments; no new scoring or arbitrary backfill'})
        print('TASK_PASSED', a.phase, 'REPORT', out, flush=True)
    except Exception as exc:
        write(attempt / 'failure.json', {'status': 'failed', 'error': str(exc), 'traceback': traceback.format_exc()})
        raise


def completion(root, run_id, states):
    """Never present an old serving summary as a fresh successful DAG run."""
    if len(states) != len(PHASES) or {x.get('task_id') for x in states} != set(PHASES):
        raise ValueError('Expected all eight task states')
    if any(x.get('state') != 'success' for x in states): raise ValueError('Eight tasks have not all succeeded')
    result = read(root / 'serving/summary.json'); receipt = read(root / 'serving/verify.json')
    if result.get('status') != 'passed' or result.get('run_id') != run_id or receipt.get('run_id') != run_id:
        raise ValueError('Serving summary does not match this run')
    current = {name: sha(root / name) for name in EVIDENCE}
    if receipt.get('etl_hashes') != current: raise ValueError('Serving summary is stale after an ETL rerun')
    return result


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--phase', choices=['load', 'verify'], required=True)
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--bridge-id', required=True)
    parser.add_argument('--try-number', type=int, default=1)
    parser.add_argument('--fail-once', choices=['true', 'false'], default='false')
    execute(parser.parse_args())
