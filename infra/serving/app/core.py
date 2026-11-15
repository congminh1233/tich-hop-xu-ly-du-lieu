"""Versioned adapters for the three TON_IoT experiment writers; no model fitting."""
import hashlib
import json
import math
from pathlib import Path

VERSIONS = {'host-baseline-1.0': 'host', 'host-comparison-1.0': 'host', 'iot-baseline-1.0': 'iot'}
REQUIRED = ('summary.json', 'scores.parquet', 'models.json', 'input_checksums.json')
OPTIONAL = ('metrics.csv', 'metrics.json', 'reference_metrics.json', 'integration_deltas.csv', 'integration_audit.json')
COLUMNS = ('experiment_id','subject_kind','subject_id','window_start','family','feature_group',
           'split','score','threshold','prediction','label_observed','payload')

def canonical(obj):
    return json.dumps(obj, sort_keys=True, ensure_ascii=False, separators=(',', ':'), allow_nan=False)

def read(path):
    return json.loads(Path(path).read_text(encoding='utf-8'), parse_constant=lambda x: (_ for _ in ()).throw(ValueError('Nonfinite JSON: '+x)))

def sha(path):
    h = hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda: f.read(1024*1024), b''): h.update(block)
    return h.hexdigest()

def inside(root, rel):
    root = Path(root).resolve()
    p = (root / rel).resolve()
    if not p.is_relative_to(root): raise ValueError('Path outside project: '+str(rel))
    return p

def manifest(root, folder):
    folder = inside(root, folder)
    summary = read(folder/'summary.json')
    if summary.get('script_version') not in VERSIONS: raise ValueError('Unsupported script_version: '+str(folder))
    if summary.get('status') not in ('completed','completed_no_fitted_models'): raise ValueError('Incomplete experiment: '+str(folder))
    if not summary.get('snapshot_id'): raise ValueError('Missing snapshot_id')
    for name in REQUIRED:
        if not (folder/name).is_file(): raise ValueError(f'Missing {folder/name}')
        inside(root, folder/name)
    files = {n: sha(inside(root, folder/n)) for n in REQUIRED+OPTIONAL if (folder/n).is_file()}
    identity = {'adapter_version':1,'files':files}
    return {'path':folder.relative_to(Path(root).resolve()).as_posix(),
            'experiment_id':hashlib.sha256(canonical(identity).encode()).hexdigest(), **identity}

def normalize(row, meta, eid):
    kind = VERSIONS[meta['script_version']]
    subject = row.get('entity_id' if kind=='host' else 'sensor_stream_id')
    if not isinstance(subject,str) or not subject: raise ValueError('Missing subject')
    if row.get('snapshot_id') != meta['snapshot_id']: raise ValueError('Row/summary snapshot mismatch')
    epoch = row.get('window_start')
    if type(epoch) is not int or epoch % 60: raise ValueError('window_start must be integer Unix seconds aligned to 60s')
    if row.get('split') not in ('train','validation','test'): raise ValueError('Invalid split')
    if meta['script_version']=='host-comparison-1.0':
        family, group = row.get('family'), row.get('group')
    else:
        family, group = meta['script_version'], row.get('model')
    if not all(isinstance(v,str) and v for v in (family,group)): raise ValueError('Missing family/group')
    score, threshold = row.get('score'), row.get('threshold')
    if any(isinstance(v,bool) or not isinstance(v,(float,int)) or not math.isfinite(v) for v in (score,threshold)):
        raise ValueError('Nonfinite or missing score/threshold')
    prediction = row.get('prediction')
    if prediction not in (0,1) or prediction != int(score > threshold): raise ValueError('Prediction/threshold mismatch')
    label = row.get('label_observed')
    if label not in (None,0,1): raise ValueError('Invalid label_observed')
    # Preserve all original labels/explanations; never infer a missing label or feature.
    canonical(row)
    return (eid,kind,subject,epoch,family,group,row['split'],float(score),float(threshold),int(prediction),label,canonical(row))

def add_digest(total, row):
    return (total + int(hashlib.sha256(canonical(list(row)).encode()).hexdigest(),16)) % (1<<256)
