import copy
import sys
import unittest
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[1]/'scripts'))
import compare_host_models as c


def synthetic_rows():
    result=[]
    for i in range(100):
        row={name: float(1+(i%17)) for name in c.base.FEATURES}
        row.update(entity_id='host', window_start=i*60, split='train' if i<80 else 'validation',
                   label_observed=i%2,label_network=i%2,label_os=i%2,snapshot_id='synthetic')
        result.append(row)
    return result


class ComparisonTests(unittest.TestCase):
    def test_future_values_and_labels_cannot_change_fit(self):
        rows=synthetic_rows()
        m1,d1=c.fit_models(rows)
        changed=copy.deepcopy(rows)
        for r in changed:
            if r['split']!='train':
                r['label_observed']=1-r['label_observed']
                for f in c.base.FEATURES:
                    r[f]=99999.0
        m2,d2=c.fit_models(changed)
        self.assertEqual(d1,d2)
        for key in m1:
            x=c.matrix(rows,c.base.GROUPS[key[2]])
            np.testing.assert_array_equal(m1[key].predict_proba(x),m2[key].predict_proba(x))

    def test_allowlist_ignores_labels_time_and_extra_columns(self):
        rows=synthetic_rows()
        x=c.matrix(rows,c.base.FEATURES)
        for r in rows:
            r.update(label_observed=99,window_start=-99,attack_events=1e100)
        np.testing.assert_array_equal(x,c.matrix(rows,c.base.FEATURES))

    def test_metrics_single_class_and_threshold_tie(self):
        row=synthetic_rows()[-1]
        row.update(label_observed=1,label_network=1,label_os=1)
        scored=[c.score_record(row,'logistic','combined',.5,.5)]
        self.assertEqual(scored[0]['prediction'],0)
        metric=c.evaluate(scored)[0]
        self.assertIsNone(metric['false_positive_rate'])
        self.assertIsNone(metric['balanced_accuracy'])
        self.assertIsNone(metric['roc_auc'])
        self.assertIsNone(metric['average_precision'])
        self.assertEqual(metric['fn'],1)

    def test_all_feature_groups_score_identical_cohort(self):
        rows=synthetic_rows()
        models,_=c.fit_models(rows)
        scores=c.predict_models(rows,models)
        for family in c.FAMILIES:
            sets=[{(r['entity_id'],r['window_start']) for r in scores if r['family']==family and r['group']==g}
                  for g in c.base.GROUPS]
            self.assertTrue(all(s==sets[0] for s in sets))
            self.assertEqual(len(sets[0]),100)


if __name__=='__main__':
    unittest.main()
