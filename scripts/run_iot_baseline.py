#!/usr/bin/env python3
"""Train-normal IoT baseline: measurement, activity and combined scores."""
import argparse
import json
import math
import shutil
import statistics
import time
import uuid
from collections import Counter,defaultdict
from pathlib import Path
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import _iot_common as c

VERSION='iot-baseline-1.0'
GROUPS=('values','activity','combined')
ALPHA=.05


def load_input(root,max_rows=200000):
 root=Path(root);meta=json.loads((root/'summary.json').read_text())
 if meta.get('status')!='prepared' or meta.get('feature_policy')!=c.POLICY_VERSION:raise ValueError('Unsupported/unprepared IoT input')
 tables={}
 for name in ('features','splits','window_labels'):
  d=ds.dataset(str(root/name),format='parquet')
  if d.count_rows()>max_rows:raise ValueError('Input exceeds Gold safety limit')
  tables[name]=c.index(d.to_table().to_pylist(),name)
 ff,ss,ll=(tables[n] for n in ('features','splits','window_labels'))
 if ff.keys()!=ss.keys() or ff.keys()!=ll.keys():raise ValueError('Input table keys differ')
 if len(ff)!=meta['device_windows']:raise ValueError('Window count disagrees with summary')
 rows=[];excluded=[]
 for k,f in sorted(ff.items()):
  s,t=k
  if f['snapshot_id']!=meta['snapshot_id'] or f['window_seconds']!=meta['window_seconds'] or t%meta['window_seconds']:
   raise ValueError('Snapshot/window mismatch')
  p=meta['split_policy']['devices'][s]
  expected='train' if t<p['validation_start_epoch'] else 'validation' if t<p['test_start_epoch'] else 'test'
  if ss[k]['split']!=expected:raise ValueError('Chronological split mismatch')
  label=ll[k];a=label['attack_events'];u=label['unknown_events'];n=label['normal_events'];count=label['event_count']
  if min(a,u,n)<0 or a+u+n!=count or count!=f['event_count']:raise ValueError('Event accounting mismatch')
  observed=1 if a else 0 if u==0 else None
  if label['label_observed']!=observed:raise ValueError('Incorrect window target')
  # Persist missing-feature status; never silently turn missing data into normal.
  if not f['eligible']:
   excluded.append(dict(sensor_stream_id=s,window_start=t,split=expected,reasons=f['exclusion_reasons'],label_observed=observed));continue
  for name in ['event_count']+c.VALUE_FEATURES.get(s,[]):
   value=f[name]
   if value is None or not math.isfinite(value):raise ValueError(f'Eligible row has invalid feature {name}')
  if f['event_count']<=0:raise ValueError('Nonpositive event count')
  if s=='iot_garage_door' and not f['state_pattern']:raise ValueError('Missing categorical pattern')
  rows.append({**f,'split':expected,'label_observed':observed})
 if len(rows)!=meta['eligible_windows']:raise ValueError('Eligible count mismatch')
 return meta,rows,excluded


def fit_numeric(values,log=False):
 x=[math.log1p(v) if log else float(v) for v in values]
 median=statistics.median(x);mad=statistics.median(abs(v-median) for v in x)
 iqr=c.quantile(x,.75)-c.quantile(x,.25)
 if min(x)==max(x):scale=None;method='constant_excluded'
 elif mad>0:scale=1.4826*mad;method='MAD'
 elif iqr>0:scale=iqr/1.349;method='IQR_fallback'
 else:scale=statistics.pstdev(x);method='std_fallback'
 return dict(center=median,scale=scale,scale_method=method,transform='log1p' if log else 'identity')


def numeric_score(value,param):
 if param['scale'] is None:return None
 value=math.log1p(value) if param['transform']=='log1p' else value
 return abs(value-param['center'])/param['scale']


def score(row,model,group):
 values=[]
 if group in ('values','combined'):
  if row['sensor_stream_id']=='iot_garage_door':
   counts=model['category_counts'];n=model['fit_count'];k=len(counts)
   # Laplace smoothing with one bucket reserved for any unseen whole pattern.
   value=-math.log((counts.get(row['state_pattern'],0)+1)/(n+k+1))
   values.append(('joint_state_set_rarity',value))
  else:
   for name in c.VALUE_FEATURES[row['sensor_stream_id']]:
    val=numeric_score(row[name],model['numeric'][name])
    if val is not None:values.append((name,val))
 if group in ('activity','combined'):
  val=numeric_score(row['event_count'],model['numeric']['event_count'])
  if val is not None:values.append(('event_count',val))
 if not values:return None,None
 top,val=max(values,key=lambda x:x[1]);return val,top


def fit_models(rows):
 models={};roles=[]
 for source in sorted({r['sensor_stream_id'] for r in rows}):
  normals=sorted([r for r in rows if r['sensor_stream_id']==source and r['split']=='train' and r['label_observed']==0],key=lambda r:r['window_start'])
  nc=math.ceil(len(normals)*.2);fit=normals[:-nc] if nc else [];cal=normals[-nc:] if nc else []
  model=dict(normal_train_count=len(normals),fit_count=len(fit),calibration_count=len(cal),groups={},numeric={},category_counts={})
  models[source]=model
  if len(fit)<50 or len(cal)<20:
   model['status']='skipped_insufficient_normal_train';continue
  model.update(status='fitted',fit_first_utc=c.utc(fit[0]['window_start']),fit_last_utc=c.utc(fit[-1]['window_start']),
               calibration_first_utc=c.utc(cal[0]['window_start']),calibration_last_utc=c.utc(cal[-1]['window_start']))
  model['numeric']={name:fit_numeric([r[name] for r in fit],log=name=='event_count') for name in ['event_count']+c.VALUE_FEATURES.get(source,[])}
  if source=='iot_garage_door':model['category_counts']=dict(Counter(r['state_pattern'] for r in fit))
  for group in GROUPS:
   scores=[score(r,model,group)[0] for r in cal]
   if all(s is None for s in scores):
    model['groups'][group]={'status':'skipped_no_variable_numeric_features'};continue
   rank=math.ceil((len(scores)+1)*(1-ALPHA))
   if rank>len(scores):raise ValueError('Not enough calibration normals')
   threshold=sorted(scores)[rank-1]
   model['groups'][group]=dict(status='fitted',threshold=threshold,comparison='strict >',rank=rank,
     calibration_alerts=sum(s>threshold for s in scores),calibration_count=len(scores))
  for role,rr in [('fit_normal',fit),('calibration_normal',cal)]:
   roles.extend(dict(sensor_stream_id=source,window_start=r['window_start'],role=role) for r in rr)
 return models,roles


def score_rows(rows,models,roles):
 ri={c.key(r):r['role'] for r in roles};scored=[]
 for r in rows:
  model=models[r['sensor_stream_id']]
  for group,g in model['groups'].items():
   if g['status']!='fitted':continue
   value,top=score(r,model,group)
   scored.append(dict(sensor_stream_id=r['sensor_stream_id'],window_start=r['window_start'],window_start_utc=c.utc(r['window_start']),
    snapshot_id=r['snapshot_id'],split=r['split'],model=group,score=value,threshold=g['threshold'],
    prediction=int(value>g['threshold']),top_feature=top,label_observed=r['label_observed'],
    training_role=ri.get(c.key(r),'unused_train' if r['split']=='train' else 'evaluation')))
 return scored


def evaluate(scored):
 buckets=defaultdict(list)
 for r in scored:
  if r['split'] in ('validation','test'):buckets[r['sensor_stream_id'],r['model'],r['split']].append(r)
 metrics=[]
 for (s,model,part),rr in sorted(buckets.items()):
  known=[r for r in rr if r['label_observed'] in (0,1)]
  metrics.append(dict(sensor_stream_id=s,model=model,split=part,scored_windows=len(rr),unknown_label_windows=len(rr)-len(known),
    **c.classification_metrics([(r['label_observed'],r['prediction']) for r in known])))
 return metrics


def run(input_path,output_path):
 start=time.perf_counter();root=Path(input_path).resolve();output=Path(output_path).resolve()
 if output.exists():raise ValueError(f'Output exists: {output}; choose a new name')
 if root==output or root in output.parents or output in root.parents:raise ValueError('Input and output must be separate')
 print('[1/3] Check prepared IoT cohort',flush=True)
 meta,rows,excluded=load_input(root)
 print('[2/3] Fit earlier normal train; calibrate on later normal train only',flush=True)
 models,roles=fit_models(rows);scored=score_rows(rows,models,roles);metrics=evaluate(scored)
 # Same eligible labelled cohort; reference predictors prevent over-reading F1.
 references=[]
 for source in sorted({r['sensor_stream_id'] for r in rows}):
  for part in ('validation','test'):
   known=[r for r in rows if r['sensor_stream_id']==source and r['split']==part and r['label_observed'] in (0,1)]
   for name,pred in [('always_normal',0),('always_attack',1)]:
    references.append(dict(sensor_stream_id=source,model=name,split=part,**c.classification_metrics([(r['label_observed'],pred) for r in known])))
 summary=dict(status='completed' if scored else 'completed_no_fitted_models',script_version=VERSION,
  snapshot_id=meta['snapshot_id'],feature_policy=c.POLICY_VERSION,eligible_windows=len(rows),excluded_windows=len(excluded),scored_rows=len(scored),
  config={'alpha':ALPHA,'calibration_fraction':.2,'fit':'earliest 80% of eligible normal train windows per device',
   'calibration':'latest 20% of eligible normal train only','numeric_score':'two-sided absolute standardized deviation; activity log1p',
   'garage_score':'negative log Laplace-smoothed frequency of unordered joint state-set pattern',
   'combined_score':'max raw component score; empirically calibrated separately; not a probability',
   'threshold':'normal calibration order statistic ceil((m+1)*.95); strict >','model_selection':'none'},
  split_policy=meta['split_policy'],label_distribution=meta['label_distribution'],models=models,metrics=metrics,reference_metrics=references,
  script_sha256=c.sha256(__file__),common_sha256=c.sha256(c.__file__),pyarrow_version=pa.__version__,
  created_at_utc=c.datetime.now(c.timezone.utc).isoformat(),
  warnings=['Source labels identify recorded attack events; not proof of sensor compromise.',
   'Numeric constant features are excluded; their future changes are not covered by that component.',
   'Pressure not scored. No state duration, transition count or missing-state fraction is inferred from sets.',
   'Alpha is an empirical calibration setting, not a guaranteed future false-alarm rate under temporal drift.',
   'Unknown targets are scored but excluded from metrics and normal training.',
   'If normal train support is inadequate the source is explicitly skipped; validation/test never fills the gap.',
   'This is a fixed baseline, not a claim that a different sensor is comparable in physical units.',
   'No IoT test labels were supplied in the review bundle; numerical detection results must come from this local run.'])
 stage=output.parent/('.'+output.name+'.staging-'+uuid.uuid4().hex);stage.mkdir(parents=True)
 try:
  c.write_json(stage/'models.json',models)
  c.write_json(stage/'input_checksums.json',{str(p.relative_to(root)):c.sha256(p) for p in [root/'summary.json']+
    [p for n in ('features','window_labels','splits') for p in sorted((root/n).rglob('*.parquet'))]})
  mfields=['sensor_stream_id','model','split','scored_windows','unknown_label_windows','n','normal','attack','tp','tn','fp','fn','precision','recall','f1','false_positive_rate','balanced_accuracy']
  c.write_csv(stage/'metrics.csv',metrics,mfields)
  c.write_json(stage/'reference_metrics.json',references)
  c.write_csv(stage/'training_roles.csv',roles,['sensor_stream_id','window_start','role'])
  c.write_csv(stage/'excluded_windows.csv',excluded,['sensor_stream_id','window_start','split','reasons','label_observed'])
  schema=pa.schema([('sensor_stream_id',pa.string()),('window_start',pa.int64()),('window_start_utc',pa.string()),
    ('snapshot_id',pa.string()),('split',pa.string()),('model',pa.string()),('score',pa.float64()),('threshold',pa.float64()),
    ('prediction',pa.int8()),('top_feature',pa.string()),('label_observed',pa.int8()),('training_role',pa.string())])
  pq.write_table(pa.Table.from_pylist(scored,schema=schema),stage/'scores.parquet')
  summary['elapsed_seconds']=round(time.perf_counter()-start,3);c.write_json(stage/'summary.json',summary)
  if output.exists():raise ValueError('Output appeared during run')
  stage.rename(output)
 finally:
  if stage.exists():shutil.rmtree(stage)
 print('[3/3] DONE:',output/'summary.json')
 for source,m in models.items():print(source,m['status'],'normal_train=',m['normal_train_count'])
 for m in metrics:
  if m['split']=='test':print(json.dumps(m))
 return summary

if __name__=='__main__':
 p=argparse.ArgumentParser(description=__doc__);p.add_argument('--input',required=True);p.add_argument('--output',required=True)
 a=p.parse_args();run(a.input,a.output)
