"""Standard-library helpers shared by launcher and Spark benchmark."""
import csv
import hashlib
import json
from pathlib import Path
import random
import statistics
import uuid

VERSION = 'silver-gold-benchmark-1'
SILVER = ('network_flow', 'host_metric', 'sensor_observation')
GOLD = ('network_overview', 'host_window_features', 'sensor_window_features')


def read(path): return json.loads(Path(path).read_text(encoding='utf-8'))


def write(path, obj):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    tmp.write_text(json.dumps(obj, ensure_ascii=False, indent=2, allow_nan=False), encoding='utf-8')
    tmp.replace(path)


def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def digest(obj):
    return hashlib.sha256(json.dumps(obj, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def trials(profile):
    if profile == 'smoke': return [{'trial_id': 'p01-c2-r1', 'percent': 1, 'cores': 2, 'repeat': 1}]
    if profile != 'matrix': raise ValueError('Unknown profile')
    result = []; rng = random.Random(5427)
    for repeat in (1, 2, 3):
        block = [{'trial_id': f'p{p:02d}-c{c}-r{repeat}', 'percent': p, 'cores': c, 'repeat': repeat}
                 for p in (25, 50, 100) for c in (1, 2, 4)]
        rng.shuffle(block); result.extend(block)
    return result


def summarize(cfg, results):
    expected = {x['trial_id']: x for x in cfg['trials']}
    seen = set(); buckets = {}; samples = []
    for row in results:
        tid = row['trial_id']
        if tid not in expected or tid in seen: raise ValueError('Unexpected or duplicate trial')
        seen.add(tid)
        if row.get('status') != 'passed': raise ValueError('Cannot summarize a failed trial')
        if row.get('config_id') != cfg['config_id']: raise ValueError('Trial configuration differs')
        spec = expected[tid]
        if any(row.get(k) != spec[k] for k in ('percent', 'cores', 'repeat')): raise ValueError('Trial identity differs')
        if row['workload_seconds'] <= 0: raise ValueError('Invalid trial time')
        group = (row['percent'], row['cores']); buckets.setdefault(group, []).append(row)
        samples.append({k: row[k] for k in ('trial_id', 'percent', 'cores', 'repeat', 'input_rows',
                                          'workload_seconds', 'validation_seconds', 'output_bytes')})
    medians = {k: statistics.median(r['workload_seconds'] for r in rows) for k, rows in buckets.items()}
    summary = []
    for (percent, cores), rows in sorted(buckets.items()):
        if len({r['input_rows'] for r in rows}) != 1: raise ValueError('Input row count changed between trials')
        times = [r['workload_seconds'] for r in rows]; median = medians[(percent, cores)]
        baseline = medians.get((percent, 1))
        summary.append({'percent': percent, 'cores': cores, 'runs': len(rows), 'input_rows': rows[0]['input_rows'],
                        'median_seconds': median, 'min_seconds': min(times), 'max_seconds': max(times),
                        'logical_rows_per_second': rows[0]['input_rows'] / median,
                        'speedup_vs_local1': baseline / median if baseline else None})
    return {'status': 'passed' if seen == set(expected) else 'incomplete', 'version': VERSION,
            'profile': cfg['profile'], 'completed_trials': len(seen), 'expected_trials': len(expected),
            'scope': 'Silver Parquet read + window aggregation + host join + three Gold Parquet writes',
            'not_measured': 'CSV ingest, Bronze-to-Silver normalization, model training, Airflow overhead',
            'cache_policy': 'Fresh JVM per trial; no Spark persist/cache; OS/HDFS caches not flushed',
            'groups': summary, 'samples': samples}


def csv_write(path, rows):
    if not rows: return
    with Path(path).open('w', newline='', encoding='utf-8') as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0])); w.writeheader(); w.writerows(rows)
