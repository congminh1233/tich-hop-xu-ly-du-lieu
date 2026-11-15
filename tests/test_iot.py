import copy
import json
import sys
import unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'scripts'))
import _iot_common as c
import run_iot_baseline as b


def fixture_gold(n=200):
 rows=[]
 for source,metrics in c.METRICS.items():
  for i in range(n):
   for j,m in enumerate(metrics):
    numeric=source!='iot_garage_door';lo=float(i%13+j);hi=lo+1
    rows.append(dict(sensor_stream_id=source,metric_name=m,window_start=1554000000+i*60,
     observed_count=2,numeric_count=2 if numeric else 0,mean=(lo+hi)/2 if numeric else None,
     min=lo if numeric else None,max=hi if numeric else None,stddev=.707 if numeric else None,
     states_seen=['closed'] if m=='door_state' else [],boolean_values_seen=[False] if m=='sphone_signal' else []))
 return rows


def fixture_rows():
 ff,ss,_,_=c.make_features(fixture_gold(),60,'synthetic')
 si=c.index(ss,'split')
 return [{**r,'split':si[c.key(r)]['split'],'label_observed':int(i%7==0)} for i,r in enumerate(ff)]


class IoTTests(unittest.TestCase):
 def test_pressure_never_enters_features(self):
  gold=fixture_gold();f,_,_,_=c.make_features(gold,60,'s')
  for r in gold:
   if r['metric_name']=='pressure':r.update(mean=-999999,min=-1e10,max=-1,stddev=99999)
  other,_,_,_=c.make_features(gold,60,'s')
  self.assertEqual(f,other)
  self.assertFalse(any('pressure' in name for name in c.NUMERIC_FEATURES))

 def test_missing_states_do_not_change_split(self):
  gold=fixture_gold();_,before,_,_=c.make_features(gold,60,'s')
  for r in gold:
   if r['sensor_stream_id']=='iot_garage_door' and r['window_start']==1554000000:
    r['states_seen']=[];r['boolean_values_seen']=[]
  ff,after,_,_=c.make_features(gold,60,'s')
  self.assertEqual(before,after);self.assertEqual(sum(not r['eligible'] for r in ff),1)

 def test_duplicate_or_count_mismatch_fails(self):
  gold=fixture_gold()
  with self.assertRaises(ValueError):c.make_features(gold+[gold[0]],60,'s')
  gold[0]['observed_count']=3
  with self.assertRaises(ValueError):c.make_features(gold,60,'s')

 def test_future_labels_and_values_do_not_affect_fit(self):
  rows=fixture_rows();models,roles=b.fit_models(rows);changed=copy.deepcopy(rows)
  self.assertTrue(all(m['status']=='fitted' for m in models.values()))
  for r in changed:
   if r['split']!='train':
    r['label_observed']=1-r['label_observed'];r['event_count']=1e10;r['state_pattern']='unseen'
    for n in c.VALUE_FEATURES.get(r['sensor_stream_id'],[]):r[n]=1e9
  other,other_roles=b.fit_models(changed)
  self.assertEqual(models,other);self.assertEqual(roles,other_roles)

 def test_insufficient_normals_skip_not_use_future(self):
  rows=fixture_rows()
  for r in rows:
   if r['split']=='train':r['label_observed']=None
  models,roles=b.fit_models(rows)
  self.assertTrue(all(m['status']=='skipped_insufficient_normal_train' for m in models.values()))
  self.assertEqual(roles,[]);self.assertEqual(b.score_rows(rows,models,roles),[])

 def test_unknown_metric_and_single_class(self):
  scored=[dict(sensor_stream_id='s',model='values',split='test',label_observed=None,prediction=1),
          dict(sensor_stream_id='s',model='values',split='test',label_observed=1,prediction=1)]
  m=b.evaluate(scored)[0]
  self.assertEqual(m['unknown_label_windows'],1);self.assertEqual(m['n'],1)
  self.assertIsNone(m['false_positive_rate']);self.assertIsNone(m['balanced_accuracy'])

if __name__=='__main__':unittest.main()
