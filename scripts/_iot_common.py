"""Shared, dependency-free rules for the three TON_IoT sensor streams."""
import csv
import hashlib
import json
import math
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

METRICS = {
 'iot_weather': ['temperature', 'humidity', 'pressure'],
 'iot_modbus': ['FC1_Read_Input_Register', 'FC2_Read_Discrete_Value', 'FC3_Read_Holding_Register', 'FC4_Read_Coil'],
 'iot_garage_door': ['door_state', 'sphone_signal'],
}
VALUE_METRICS = {'iot_weather': ['temperature', 'humidity'], 'iot_modbus': METRICS['iot_modbus']}
VALUE_FEATURES = {s: [m+'__'+stat for m in names for stat in ('mean', 'range')] for s,names in VALUE_METRICS.items()}
NUMERIC_FEATURES = ['event_count'] + [f for names in VALUE_FEATURES.values() for f in names]
KEYS = ['sensor_stream_id', 'window_start']
GOLD_COLUMNS = KEYS + ['metric_name', 'observed_count', 'numeric_count', 'mean', 'min', 'max', 'stddev', 'states_seen', 'boolean_values_seen']
POLICY_VERSION = 'iot-feature-policy-1.0'


def utc(t):
 return datetime.fromtimestamp(t, timezone.utc).isoformat()


def sha256(path):
 h=hashlib.sha256()
 with Path(path).open('rb') as f:
  for chunk in iter(lambda:f.read(1048576),b''): h.update(chunk)
 return h.hexdigest()


def write_json(path,value):
 Path(path).write_text(json.dumps(value,ensure_ascii=False,indent=2,allow_nan=False),encoding='utf-8')


def write_csv(path,rows,fields):
 with Path(path).open('w',encoding='utf-8-sig',newline='') as f:
  w=csv.DictWriter(f,fieldnames=fields);w.writeheader();w.writerows(rows)


def key(r):
 return r['sensor_stream_id'],r['window_start']


def index(rows,context):
 out={}
 for r in rows:
  k=key(r)
  if k in out: raise ValueError(f'Duplicate {context} key: {k}')
  out[k]=r
 return out


def quantile(values,q):
 x=sorted(values);pos=(len(x)-1)*q;a,b=math.floor(pos),math.ceil(pos)
 return x[a]+(x[b]-x[a])*(pos-a)


def make_features(gold,seconds,snapshot_id):
 """One row per stream/window; no labels and no invented state frequencies."""
 groups=defaultdict(dict)
 for r in gold:
  s,t=key(r);m=r['metric_name']
  if s not in METRICS or m not in METRICS[s]:raise ValueError(f'Unexpected stream/metric: {s}/{m}')
  if not isinstance(t,int) or t % seconds:raise ValueError('Invalid/misaligned timestamp')
  if m in groups[s,t]:raise ValueError(f'Duplicate Gold key: {s}/{m}/{t}')
  if not isinstance(r['observed_count'],int) or r['observed_count']<1:raise ValueError('Invalid observed count')
  if not isinstance(r['numeric_count'],int) or not 0<=r['numeric_count']<=r['observed_count']:raise ValueError('Invalid numeric count')
  groups[s,t][m]=r
 features=[]
 for (s,t),mm in sorted(groups.items()):
  if set(mm)!=set(METRICS[s]):raise ValueError(f'Missing metric rows: {s}/{t}')
  counts={r['observed_count'] for r in mm.values()}
  if len(counts)!=1:raise ValueError(f'Metric event counts differ: {s}/{t}')
  n=counts.pop(); reasons=[]
  f={name:None for name in NUMERIC_FEATURES}
  f.update(sensor_stream_id=s,window_start=t,event_count=float(n),snapshot_id=snapshot_id,
           window_seconds=seconds,state_pattern=None,feature_policy=POLICY_VERSION)
  if s in VALUE_METRICS:
   for m in VALUE_METRICS[s]:
    r=mm[m]; values=[r[k] for k in ('mean','min','max')]
    if r['numeric_count']!=n:reasons.append(m+':missing_numeric')
    if any(v is None or not math.isfinite(v) for v in values):
     reasons.append(m+':invalid_summary');continue
    mean,lo,hi=values
    if lo>hi or mean<lo-1e-8*max(1,abs(lo)) or mean>hi+1e-8*max(1,abs(hi)):
     raise ValueError(f'Inconsistent numeric summary: {s}/{m}/{t}')
    if m=='humidity' and (lo<0 or hi>100):reasons.append(m+':outside_0_100')
    if s=='iot_modbus' and (lo<0 or hi>65535):reasons.append(m+':outside_register_domain')
    f[m+'__mean']=float(mean);f[m+'__range']=float(hi-lo)
  else:
   door=mm['door_state']['states_seen'];phone=mm['sphone_signal']['boolean_values_seen']
   if not door:reasons.append('door_state:no_observed_state')
   if not phone:reasons.append('sphone_signal:no_observed_state')
   if not set(door).issubset({'open','closed'}):reasons.append('door_state:unknown_category')
   if any(type(x) is not bool for x in phone):reasons.append('sphone_signal:invalid_boolean')
   # Order is unknown; store only the unordered joint pattern of two sets.
   f['state_pattern']=json.dumps({'door':sorted(set(door)),'phone':sorted(set(phone))},sort_keys=True,separators=(',',':'))
  f['eligible']=not reasons;f['exclusion_reasons']=';'.join(reasons)
  features.append(f)
 splits=[];policies={}
 for s in sorted({r['sensor_stream_id'] for r in features}):
  tt=sorted(r['window_start'] for r in features if r['sensor_stream_id']==s)
  a,b=int(len(tt)*.6),int(len(tt)*.8)
  if not 0<a<b<len(tt):raise ValueError(f'Too few windows for chronological split: {s}')
  v,test=tt[a],tt[b]
  policies[s]=dict(distinct_windows=len(tt),validation_start_epoch=v,test_start_epoch=test,
                  validation_start_utc=utc(v),test_start_utc=utc(test),first_utc=utc(tt[0]),last_utc=utc(tt[-1]))
  for t in tt:splits.append(dict(sensor_stream_id=s,window_start=t,split='train' if t<v else 'validation' if t<test else 'test'))
 audit=[]
 for s in sorted(policies):
  ff=[r for r in features if r['sensor_stream_id']==s]
  audit.append(dict(sensor_stream_id=s,metric_rows=sum(len(mm) for (ss,_),mm in groups.items() if ss==s),
      windows=len(ff),eligible_windows=sum(r['eligible'] for r in ff),
      source_events=sum(int(r['event_count']) for r in ff),
      excluded_windows=sum(not r['eligible'] for r in ff),
      reasons=dict(Counter(r['exclusion_reasons'] for r in ff if not r['eligible']))))
 return features,splits,policies,audit


def classification_metrics(pairs):
 tp=sum(y==p==1 for y,p in pairs);tn=sum(y==p==0 for y,p in pairs)
 fp=sum(y==0 and p==1 for y,p in pairs);fn=sum(y==1 and p==0 for y,p in pairs)
 div=lambda a,b:a/b if b else None
 recall=div(tp,tp+fn);specificity=div(tn,tn+fp)
 return dict(n=len(pairs),normal=tn+fp,attack=tp+fn,tp=tp,tn=tn,fp=fp,fn=fn,
             precision=div(tp,tp+fp),recall=recall,f1=div(2*tp,2*tp+fp+fn),false_positive_rate=div(fp,fp+tn),
             balanced_accuracy=(recall+specificity)/2 if recall is not None and specificity is not None else None)
