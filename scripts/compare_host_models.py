#!/usr/bin/env python3
"""Fixed exploratory comparison on existing small Gold tables; no Spark needed."""
import argparse
import json
import platform
import shutil
import time
import uuid
import warnings
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sklearn
from sklearn.exceptions import ConvergenceWarning
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier, export_text
from threadpoolctl import threadpool_limits

import _host_eval_common as base

VERSION = 'host-comparison-1.0'
FAMILIES = ('logistic', 'tree')
POLICY = {
    'target': 'label_observed', 'fit': 'known-label train only; both classes',
    'features': base.GROUPS, 'transforms': 'log1p network and memory_available_min; identity otherwise',
    'logistic': {'C': 1.0, 'class_weight': 'balanced', 'solver': 'lbfgs', 'max_iter': 2000,
                 'scaling': 'StandardScaler fitted on train only'},
    'tree': {'max_depth': 4, 'min_samples_leaf': 20, 'class_weight': 'balanced', 'random_state': 5427},
    'threshold': 0.5, 'comparison': 'strict >', 'model_selection': 'none; report all fixed comparisons',
    'test_status': 'exploratory: earlier baseline test and distributions already inspected',
}


def matrix(rows, names):
    x = np.asarray([[r[n] for n in names] for r in rows], dtype=np.float64)
    for j, name in enumerate(names):
        if name in base.LOG_FEATURES:
            x[:, j] = np.log1p(x[:, j])
    return x


def fit_models(rows):
    """Future rows are never used by either fit or scaling."""
    models, descriptions = {}, {}
    for host in sorted({r['entity_id'] for r in rows}):
        train = [r for r in rows if r['entity_id'] == host and r['split'] == 'train'
                 and r['label_observed'] in (0, 1)]
        y = np.array([r['label_observed'] for r in train])
        if set(y) != {0, 1}:
            raise ValueError(f'{host}: train must contain both classes')
        for group, names in base.GROUPS.items():
            x = matrix(train, names)
            for family in FAMILIES:
                if family == 'logistic':
                    estimator = make_pipeline(StandardScaler(), LogisticRegression(
                        C=1.0, class_weight='balanced', solver='lbfgs', max_iter=2000))
                else:
                    estimator = DecisionTreeClassifier(max_depth=4, min_samples_leaf=20,
                                                       class_weight='balanced', random_state=5427)
                with warnings.catch_warnings(), threadpool_limits(limits=1):
                    warnings.simplefilter('error', ConvergenceWarning)
                    estimator.fit(x, y)
                k = (host, family, group)
                models[k] = estimator
                d = {'host': host, 'family': family, 'group': group, 'features': names,
                     'train_rows': len(train), 'train_normal': int(sum(y == 0)), 'train_attack': int(sum(y == 1)),
                     'train_first_utc': base.utc(min(r['window_start'] for r in train)),
                     'train_last_utc': base.utc(max(r['window_start'] for r in train)),
                     'threshold': 0.5}
                if family == 'logistic':
                    scaler, clf = estimator.steps[0][1], estimator.steps[1][1]
                    d.update(mean=scaler.mean_.tolist(), scale=scaler.scale_.tolist(),
                             coefficients=clf.coef_[0].tolist(), intercept=float(clf.intercept_[0]),
                             iterations=clf.n_iter_.tolist())
                else:
                    tree = estimator.tree_
                    d.update(feature_importance=estimator.feature_importances_.tolist(),
                             rules_transformed=export_text(estimator, feature_names=names, decimals=6),
                             children_left=tree.children_left.tolist(), children_right=tree.children_right.tolist(),
                             feature_index=tree.feature.tolist(), thresholds=tree.threshold.tolist(),
                             values=tree.value.tolist(), classes=estimator.classes_.tolist())
                descriptions['|'.join(k)] = d
    return models, descriptions


def predict_models(rows, models):
    result = []
    for (host, family, group), estimator in models.items():
        subset = [r for r in rows if r['entity_id'] == host]
        with threadpool_limits(limits=1):
            scores = estimator.predict_proba(matrix(subset, base.GROUPS[group]))[:, 1]
        for r, score in zip(subset, scores):
            result.append(score_record(r, family, group, float(score), .5))
    return result


def score_record(row, family, group, score, threshold):
    return {**{n: row[n] for n in ('entity_id', 'window_start', 'split', 'label_observed',
                                  'label_network', 'label_os', 'snapshot_id')},
            'family': family, 'group': group, 'score': score, 'threshold': threshold,
            'prediction': int(score > threshold)}


def evaluate(scored):
    buckets = defaultdict(list)
    for r in scored:
        if r['split'] in ('validation', 'test'):
            buckets[r['entity_id'], r['family'], r['group'], r['split']].append(r)
    results = []
    for (host, family, group, part), rr in sorted(buckets.items()):
        for target in ('label_observed', 'label_network', 'label_os', 'agreement_only'):
            label = 'label_network' if target == 'agreement_only' else target
            known = [r for r in rr if r[label] in (0, 1) and (target != 'agreement_only'
                     or r['label_network'] == r['label_os'])]
            y = [r[label] for r in known]
            scores = [r['score'] for r in known]
            metrics = base.metrics([(r[label], r['prediction']) for r in known])
            both = set(y) == {0, 1}
            results.append(dict(entity_id=host, family=family, group=group, split=part, target=target,
                excluded_from_metric=len(rr)-len(known), **metrics,
                roc_auc=float(roc_auc_score(y, scores)) if both else None,
                average_precision=float(average_precision_score(y, scores)) if both else None))
    return results


def profiles(rows):
    result = []
    for host in sorted({r['entity_id'] for r in rows}):
        normals = [r for r in rows if r['entity_id'] == host and r['split'] == 'train' and r['label_observed'] == 0]
        for part in ('train', 'validation', 'test'):
            for label in (0, 1):
                rr = [r for r in rows if r['entity_id'] == host and r['split'] == part and r['label_observed'] == label]
                if not rr:
                    continue
                for f in base.FEATURES:
                    values = [r[f] for r in rr]
                    lo, hi = (base.quantile([r[f] for r in normals], q) for q in (.05, .95))
                    result.append(dict(entity_id=host, split=part, label_observed=label, feature=f, n=len(rr),
                        minimum=min(values), q05=base.quantile(values, .05), q25=base.quantile(values, .25),
                        median=base.quantile(values, .5), q75=base.quantile(values, .75),
                        q95=base.quantile(values, .95), maximum=max(values),
                        zero_fraction=sum(v == 0 for v in values)/len(values),
                        train_normal_q05=lo, train_normal_q95=hi,
                        outside_train_normal_central90=sum(v < lo or v > hi for v in values)/len(values)))
    return result


def integration_deltas(scored, metrics):
    lookup = {(r['entity_id'], r['family'], r['group'], r['split']): r for r in metrics if r['target'] == 'label_observed'}
    idx = defaultdict(dict)
    for r in scored:
        if r['label_observed'] is not None:
            idx[r['entity_id'], r['family'], r['group'], r['split']][r['window_start']] = r
    result = []
    for host in sorted({r['entity_id'] for r in scored}):
        for family in ('mad_v1',) + FAMILIES:
            for part in ('validation', 'test'):
                a = lookup[host, family, 'combined', part]
                for other in ('network', 'windows'):
                    b = lookup[host, family, other, part]
                    aa, bb = idx[host, family, 'combined', part], idx[host, family, other, part]
                    assert aa.keys() == bb.keys()
                    d = dict(entity_id=host, family=family, split=part, comparison='combined_minus_'+other,
                        n=len(aa), changed_predictions=sum(aa[k]['prediction'] != bb[k]['prediction'] for k in aa),
                        corrected_errors=sum(bb[k]['prediction'] != aa[k]['label_observed'] and aa[k]['prediction'] == aa[k]['label_observed'] for k in aa),
                        introduced_errors=sum(bb[k]['prediction'] == aa[k]['label_observed'] and aa[k]['prediction'] != aa[k]['label_observed'] for k in aa))
                    for metric in ('f1', 'balanced_accuracy', 'false_positive_rate', 'recall', 'roc_auc'):
                        d['delta_'+metric] = a[metric]-b[metric] if a[metric] is not None and b[metric] is not None else None
                    result.append(d)
    return result


def integration_audit(root):
    tables = {name: ds.dataset(str(root/name), format='parquet').to_table().to_pylist()
              for name in ('features', 'splits', 'window_labels')}
    features = base.index_rows(tables['features'], 'features')
    labels = base.index_rows(tables['window_labels'], 'window_labels')
    details = []
    for host in sorted({r['entity_id'] for r in tables['splits']}):
        rr = [r for r in tables['splits'] if r['entity_id'] == host]
        shared = sum(r['shared_window'] for r in rr)
        disagreements = sum(r['label_network'] is not None and r['label_os'] is not None
                            and r['label_network'] != r['label_os']
                            for r in labels.values() if r['entity_id'] == host)
        details.append(dict(entity_id=host, union_windows=len(rr), shared_windows=shared,
            network_only=sum(r['has_network'] and not r['has_os'] for r in rr),
            os_only=sum(r['has_os'] and not r['has_network'] for r in rr),
            shared_fraction_of_union=shared/len(rr), source_label_disagreements=disagreements,
            disagreement_fraction_of_shared=disagreements/shared if shared else None))
    mismatches = {source: sum((r[column] or 0) != labels[k][count]
                 for k, r in features.items()) for source,column,count in
                 [('network','n_network','network_events'),('os','n_os','os_events')]}
    if any(mismatches.values()):
        raise ValueError(f'Gold event counts disagree with evaluation lineage counts: {mismatches}')
    return dict(hosts=details, count_mismatch_windows=mismatches,
        interpretation='Coverage is descriptive, not entity-link accuracy. Non-overlap may reflect source collection periods. Label disagreement is preserved; it is not automatically bad data.',
        not_verified=['raw CSV correctness', 'clock offset validity', 'IP-to-host mapping truth',
                      'attack ground truth independence', 'Silver lineage reconstruction'])


def run(input_path, output_path, max_rows=200000):
    start = time.perf_counter()
    root, output = Path(input_path).resolve(), Path(output_path).resolve()
    if output.exists():
        raise ValueError(f'Output exists; choose a new directory: {output}')
    if root == output or root in output.parents or output in root.parents:
        raise ValueError('Use separate input/output directories')
    print('[1/4] Validate Gold and fixed chronological cohort', flush=True)
    meta, rows, exclusions = base.load_input(root, max_rows)
    audit = integration_audit(root)
    print('[2/4] Fit train-only fixed supervised models and reproduce MAD v1', flush=True)
    models, descriptions = fit_models(rows)
    mad, roles = base.build_models(rows)
    print('[3/4] Score and compare all fixed models; no selection or tuning', flush=True)
    scored = predict_models(rows, models)
    for r in base.score_rows(rows, mad, roles):
        scored.append(score_record(r, 'mad_v1', r['model'], r['score'], r['threshold']))
    for r in rows:
        for family, score in (('always_normal', 0.0), ('always_attack', 1.0)):
            scored.append(score_record(r, family, 'reference', score, .5))
    metrics = evaluate(scored)
    deltas = integration_deltas(scored, metrics)
    source_files = [root/'summary.json'] + [p for name in ('features', 'splits', 'window_labels')
                                               for p in sorted((root/name).rglob('*.parquet'))]
    summary = dict(status='completed', script_version=VERSION, snapshot_id=meta['snapshot_id'],
        created_at_utc=base.datetime.now(base.timezone.utc).isoformat(), config=POLICY,
        versions={'python': platform.python_version(), 'numpy': np.__version__, 'pyarrow': pa.__version__, 'scikit_learn': sklearn.__version__},
        eligible_windows=len(rows), excluded_shared_windows=len(exclusions), scored_rows=len(scored),
        cohort_counts=[dict(entity_id=h, split=s, label_observed=y, rows=n) for (h,s,y),n in
                       sorted(Counter((r['entity_id'],r['split'],r['label_observed']) for r in rows).items(), key=str)],
        script_sha256=base.sha256(__file__), common_module_sha256=base.sha256(base.__file__),
        metrics=metrics, integration_deltas=deltas,
        warnings=['Test was already inspected during earlier baseline diagnosis: exploratory results only.',
                  'Windows 10 validation has no normals in this bundle; FPR and balanced accuracy there are undefined.',
                  'Source-label OR means observed attack in either source, not proven host compromise.',
                  'Complete shared cases only: this experiment does not assess missing-source periods.',
                  'Supervised fits use attack labels; improvements over MAD are not pure integration gains.',
                  'Balanced class weights make probability outputs uncalibrated risk scores.',
                  'Entity mapping and time alignment are inherited, not independently verified by this experiment.'])
    stage = output.parent / ('.'+output.name+'.staging-'+uuid.uuid4().hex)
    stage.mkdir(parents=True)
    try:
        base.write_json(stage/'models.json', dict(version=VERSION, config=POLICY, supervised=descriptions, mad_v1=mad))
        base.write_json(stage/'integration_audit.json', audit)
        base.write_json(stage/'input_checksums.json', {str(p.relative_to(root)):base.sha256(p) for p in source_files})
        base.write_csv(stage/'metrics.csv', metrics)
        base.write_csv(stage/'integration_deltas.csv', deltas)
        base.write_csv(stage/'feature_profiles.csv', profiles(rows))
        base.write_csv(stage/'excluded_windows.csv', exclusions, ['entity_id','window_start','split','label_observed','reasons'])
        base.write_csv(stage/'training_roles.csv', [dict(entity_id=r['entity_id'],window_start=r['window_start'],
            split=r['split'],label_observed=r['label_observed'],role='fit_supervised' if r['split']=='train' and r['label_observed'] in (0,1) else 'not_fit') for r in rows])
        pq.write_table(pa.Table.from_pylist(scored), stage/'scores.parquet')
        summary['elapsed_seconds'] = round(time.perf_counter()-start,3)
        base.write_json(stage/'summary.json', summary)
        if output.exists():
            raise ValueError('Output appeared during run; refusing overwrite')
        stage.rename(output)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    print('[4/4] DONE:', output/'summary.json')
    for r in metrics:
        if r['target']=='label_observed' and r['split']=='test':
            print(json.dumps({k:r[k] for k in ('entity_id','family','group','tp','fp','tn','fn','f1','balanced_accuracy')}))
    return summary


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--input', required=True)
    p.add_argument('--output', required=True)
    p.add_argument('--max-rows', type=int, default=200000)
    args = p.parse_args()
    run(args.input, args.output, args.max_rows)
