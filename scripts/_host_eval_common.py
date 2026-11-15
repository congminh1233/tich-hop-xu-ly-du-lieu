#!/usr/bin/env python3
"""Small-Gold host baseline: train-only normal fit/calibration, fixed time splits.

Python 3.10+, pyarrow 19.0.1. No Spark JVM needed for this ~19k-row experiment.
"""
import argparse
import csv
import hashlib
import json
import math
import shutil
import statistics
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

VERSION = "host-baseline-1.0"
# Explicit feature allowlist: no labels, timestamps or coverage flags are scored.
NETWORK = ["n_network", "unique_remote_ips", "unique_dst_ports",
           "flow_start_sent_bytes", "flow_start_received_bytes"]
WINDOWS = ["host_cpu_mean", "host_cpu_max", "memory_available_min", "memory_committed_mean"]
FEATURES = NETWORK + WINDOWS
GROUPS = {"network": NETWORK, "windows": WINDOWS, "combined": FEATURES}
LOG_FEATURES = set(NETWORK + ["memory_available_min"])


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat()


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def quantile(values, q):
    """Linear interpolation between adjacent order statistics."""
    x = sorted(values)
    if not x or not 0 <= q <= 1:
        raise ValueError("Invalid quantile input")
    pos = (len(x) - 1) * q
    lo, hi = math.floor(pos), math.ceil(pos)
    return x[lo] + (x[hi] - x[lo]) * (pos - lo)


def fit_feature(values, name):
    x = [math.log1p(float(v)) if name in LOG_FEATURES else float(v) for v in values]
    center = statistics.median(x)
    mad = statistics.median([abs(v - center) for v in x])
    iqr = quantile(x, 0.75) - quantile(x, 0.25)
    if min(x) == max(x):
        scale, method = None, "constant_feature_excluded"
    elif mad > 0:
        scale, method = 1.4826 * mad, "scaled_MAD"
    elif iqr > 0:
        scale, method = iqr / 1.349, "IQR_fallback"
    else:
        scale, method = statistics.pstdev(x), "std_fallback_for_zero_MAD_and_IQR"
    if scale is not None and (not math.isfinite(scale) or scale <= 0):
        raise ValueError(f"Invalid fitted scale: {name}")
    return {"feature": name, "transform": "log1p" if name in LOG_FEATURES else "identity",
            "direction": "lower" if name == "memory_available_min" else "upper",
            "center": center, "scale": scale, "scale_method": method,
            "fit_count": len(x), "fit_min_transformed": min(x), "fit_max_transformed": max(x)}


def score_feature(value, params):
    if params["scale"] is None:
        return None
    value = math.log1p(float(value)) if params["transform"] == "log1p" else float(value)
    deviation = (value - params["center"]) / params["scale"]
    if params["direction"] == "lower":
        deviation = -deviation
    score = max(0.0, deviation)
    if not math.isfinite(score):
        raise ValueError("Nonfinite anomaly score")
    return score


def score_group(row, feature_params, group):
    scores = [(name, score_feature(row[name], feature_params[name])) for name in GROUPS[group]]
    scores = [(name, value) for name, value in scores if value is not None]
    if not scores:
        raise ValueError(f"No nonconstant fitted feature in group {group}")
    top, score = max(scores, key=lambda item: item[1])
    return score, top, scores


def calibrate_threshold(scores, alpha):
    """Fixed upper-tail rank; predict only for score strictly above threshold."""
    if not scores or not 0 < alpha < 1:
        raise ValueError("Invalid calibration parameters")
    rank = math.ceil((len(scores) + 1) * (1 - alpha))
    if rank > len(scores):
        raise ValueError("Too few calibration normals for alpha; do not use test to fix it")
    return sorted(scores)[rank - 1], rank


def metrics(pairs):
    tp = sum(y == 1 and p == 1 for y, p in pairs)
    tn = sum(y == 0 and p == 0 for y, p in pairs)
    fp = sum(y == 0 and p == 1 for y, p in pairs)
    fn = sum(y == 1 and p == 0 for y, p in pairs)
    def div(a, b):
        return a / b if b else None
    recall, specificity = div(tp, tp + fn), div(tn, tn + fp)
    return {"n": len(pairs), "normal": tn + fp, "attack": tp + fn,
            "tp": tp, "tn": tn, "fp": fp, "fn": fn,
            "precision": div(tp, tp + fp), "recall": recall,
            "f1": div(2 * tp, 2 * tp + fp + fn), "false_positive_rate": div(fp, fp + tn),
            "specificity": specificity, "accuracy": div(tp + tn, len(pairs)),
            "balanced_accuracy": (recall + specificity) / 2 if recall is not None and specificity is not None else None,
            "has_both_classes": bool((tp + fn) and (tn + fp))}


def key(row):
    return row["entity_id"], row["window_start"]


def index_rows(rows, table):
    result = {}
    for row in rows:
        k = key(row)
        if not isinstance(k[0], str) or not k[0] or not isinstance(k[1], int):
            raise ValueError(f"Invalid key in {table}: {k}")
        if k in result:
            raise ValueError(f"Duplicate key in {table}: {k}")
        result[k] = row
    return result


def read_rows(root, table, columns, max_rows):
    dataset = ds.dataset(str(root / table), format="parquet")
    missing = set(columns) - set(dataset.schema.names)
    if missing:
        raise ValueError(f"Missing columns in {table}: {sorted(missing)}")
    if dataset.count_rows() > max_rows:
        raise ValueError(f"{table} exceeds --max-rows; this experiment is for small Gold tables")
    return dataset.to_table(columns=columns).to_pylist()


def invalid_features(row):
    reasons = []
    for name in FEATURES:
        value = row[name]
        if value is None or isinstance(value, bool) or not isinstance(value, (int, float)):
            reasons.append(name + ":missing_or_nonnumeric")
        elif not math.isfinite(value):
            reasons.append(name + ":nonfinite")
        elif value < 0:
            reasons.append(name + ":negative")
        elif name in ("host_cpu_mean", "host_cpu_max", "memory_committed_mean") and value > 100:
            reasons.append(name + ":above_100_percent")
    return reasons


def load_input(root, max_rows):
    meta = json.loads((root / "summary.json").read_text(encoding="utf-8"))
    if meta.get("status") != "prepared":
        raise ValueError("Input summary must have status=prepared")
    feature_rows = read_rows(root, "features", ["entity_id", "window_start", "snapshot_id", "window_seconds"] + FEATURES, max_rows)
    split_rows = read_rows(root, "splits", ["entity_id", "window_start", "split", "shared_window", "has_network", "has_os"], max_rows)
    label_rows = read_rows(root, "window_labels", ["entity_id", "window_start", "label_network", "label_os", "label_observed"], max_rows)
    features, splits, labels = [index_rows(r, t) for r, t in (
        (feature_rows, "features"), (split_rows, "splits"), (label_rows, "window_labels"))]
    if not (features.keys() == splits.keys() == labels.keys()):
        raise ValueError("Feature, split and label keys must match exactly")
    if len(features) != meta["host_windows"]:
        raise ValueError("Input row count disagrees with preparation summary")
    boundaries = meta["split_policy"]
    v_start, t_start = boundaries["validation_start_epoch"], boundaries["test_start_epoch"]
    if not v_start < t_start:
        raise ValueError("Invalid chronological boundaries")
    rows, exclusions = [], []
    shared_count = 0
    for k, f in sorted(features.items()):
        s, lab = splits[k], labels[k]
        if f["snapshot_id"] != meta["snapshot_id"] or f["window_seconds"] != meta["window_seconds"]:
            raise ValueError("Mixed snapshot/window sizes")
        if k[1] % meta["window_seconds"]:
            raise ValueError("Misaligned window")
        expected = "train" if k[1] < v_start else "validation" if k[1] < t_start else "test"
        if s["split"] != expected:
            raise ValueError(f"Time split mismatch: {k}")
        if s["shared_window"] != (s["has_network"] and s["has_os"]):
            raise ValueError(f"Inconsistent coverage: {k}")
        if not s["shared_window"]:
            continue
        shared_count += 1
        a, b, observed = lab["label_network"], lab["label_os"], lab["label_observed"]
        if any(x is not None and x not in (0, 1) for x in (a, b, observed)):
            raise ValueError(f"Invalid normalized labels: {k}")
        combined = 1 if 1 in (a, b) else 0 if a == b == 0 else None
        if observed != combined:
            raise ValueError(f"Label aggregation mismatch: {k}")
        invalid = invalid_features(f)
        if invalid:
            exclusions.append({"entity_id": k[0], "window_start": k[1], "split": s["split"],
                               "label_observed": observed, "reasons": ";".join(invalid)})
            continue
        rows.append({**f, "split": s["split"], "label_network": a, "label_os": b,
                     "label_observed": observed})
    if shared_count != meta["shared_windows"]:
        raise ValueError("Shared coverage disagrees with preparation summary")
    if not rows:
        raise ValueError("No complete shared windows")
    return meta, rows, exclusions


def build_models(rows, calibration_fraction=0.2, alpha=0.05):
    if not 0 < calibration_fraction < 1 or not 0 < alpha < 1:
        raise ValueError("Invalid fractions")
    models, roles = {}, []
    for host in sorted({r["entity_id"] for r in rows}):
        normals = sorted([r for r in rows if r["entity_id"] == host and r["split"] == "train"
                          and r["label_network"] == r["label_os"] == 0], key=lambda r: r["window_start"])
        n_cal = math.ceil(len(normals) * calibration_fraction)
        fit, calibration = normals[:-n_cal], normals[-n_cal:]
        if len(fit) < 50 or len(calibration) < 20:
            raise ValueError(f"{host}: need >=50 fit normals and >=20 calibration normals; found {len(fit)}, {len(calibration)}")
        if fit[-1]["window_start"] >= calibration[0]["window_start"]:
            raise ValueError("Fit/calibration ordering failed")
        params = {name: fit_feature([r[name] for r in fit], name) for name in FEATURES}
        groups = {}
        for group in GROUPS:
            scores = [score_group(row, params, group)[0] for row in calibration]
            threshold, rank = calibrate_threshold(scores, alpha)
            groups[group] = {"threshold": threshold, "comparison": "strictly_greater_than",
                             "calibration_rank_1_based": rank, "calibration_count": len(scores),
                             "calibration_alerts": sum(s > threshold for s in scores),
                             "calibration_alert_rate": sum(s > threshold for s in scores) / len(scores),
                             "active_features": [n for n in GROUPS[group] if params[n]["scale"] is not None]}
        models[host] = {"normal_train_count": len(normals), "fit_count": len(fit), "calibration_count": len(calibration),
                        "fit_first_utc": utc(fit[0]["window_start"]), "fit_last_utc": utc(fit[-1]["window_start"]),
                        "calibration_first_utc": utc(calibration[0]["window_start"]),
                        "calibration_last_utc": utc(calibration[-1]["window_start"]),
                        "features": params, "groups": groups}
        for role, subset in (("fit_normal", fit), ("calibration_normal", calibration)):
            roles.extend({"entity_id": host, "window_start": r["window_start"], "role": role} for r in subset)
    return models, roles


def score_rows(rows, models, roles):
    role_index = {key(r): r["role"] for r in roles}
    output = []
    for row in rows:
        model = models[row["entity_id"]]
        for group in GROUPS:
            score, top, feature_scores = score_group(row, model["features"], group)
            threshold = model["groups"][group]["threshold"]
            active = sorted(feature_scores, key=lambda x: x[1], reverse=True)[:3]
            output.append({"entity_id": row["entity_id"], "window_start": row["window_start"],
                           "window_start_utc": utc(row["window_start"]), "split": row["split"],
                           "training_role": role_index.get(key(row), "unused_train" if row["split"] == "train" else "evaluation"),
                           "model": group, "score": score, "threshold": threshold,
                           "prediction": int(score > threshold), "top_feature": top,
                           "top_feature_value": float(row[top]),
                           "top_feature_scores_json": json.dumps(dict(active)),
                           "label_network": row["label_network"], "label_os": row["label_os"],
                           "label_observed": row["label_observed"], "snapshot_id": row["snapshot_id"]})
    return output


def evaluate(scored):
    result, strata = [], []
    for host in sorted({r["entity_id"] for r in scored}):
        for group in GROUPS:
            for part in ("validation", "test"):
                subset = [r for r in scored if r["entity_id"] == host and r["model"] == group and r["split"] == part]
                for target in ("label_observed", "label_network", "label_os", "agreement_only"):
                    if target == "agreement_only":
                        known = [r for r in subset if r["label_network"] is not None and r["label_network"] == r["label_os"]]
                        pairs = [(r["label_network"], r["prediction"]) for r in known]
                    else:
                        known = [r for r in subset if r[target] is not None]
                        pairs = [(r[target], r["prediction"]) for r in known]
                    result.append({"entity_id": host, "model": group, "split": part, "target": target,
                                   "excluded_from_this_metric": len(subset) - len(known), **metrics(pairs)})
                for a, b in ((0, 0), (0, 1), (1, 0), (1, 1), (None, None)):
                    selected = [r for r in subset if r["label_network"] == a and r["label_os"] == b]
                    if selected:
                        alerts = sum(r["prediction"] for r in selected)
                        strata.append({"entity_id": host, "model": group, "split": part,
                                       "label_network": a, "label_os": b, "windows": len(selected),
                                       "alerts": alerts, "alert_rate": alerts / len(selected)})
    return result, strata


def write_csv(path, rows, fields=None):
    fields = fields or list(rows[0])
    with Path(path).open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_json(path, value):
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf-8")


def run(input_path, output_path, calibration_fraction=0.2, alpha=0.05, max_rows=200000):
    started = time.perf_counter()
    root, output = Path(input_path).resolve(), Path(output_path).resolve()
    if output.exists():
        raise ValueError(f"Output exists; choose a new directory: {output}")
    if output == root or root in output.parents or output in root.parents:
        raise ValueError("Input and output must be separate sibling directories")
    print("[1/4] Reading prepared Gold tables and checking shared cohort...", flush=True)
    meta, rows, exclusions = load_input(root, max_rows)
    print("[2/4] Fitting on earlier normal train windows; calibrating on later normal train windows...", flush=True)
    models, roles = build_models(rows, calibration_fraction, alpha)
    print("[3/4] Scoring three models; evaluating fixed validation/test splits...", flush=True)
    scored = score_rows(rows, models, roles)
    metric_rows, strata = evaluate(scored)
    input_files = [root / "summary.json"] + [p for t in ("features", "splits", "window_labels")
                  for p in sorted((root / t).rglob("*.parquet"))]
    provenance = {str(p.relative_to(root)): sha256(p) for p in input_files}
    warning_list = [
        "Targets are source-derived window labels, not proof of host compromise.",
        "This is a one-class baseline using labels to select normal training windows.",
        "Alpha is a fixed empirical calibration target; temporal dependence/drift prevent a guaranteed future false-positive rate.",
        "Validation and test did not select transforms, parameters, alpha or thresholds.",
        "Agreement-only metrics exclude disputed labels and change the evaluation population; report as sensitivity analysis only.",
        "Upper-tail traffic/CPU/committed-memory and lower-tail available-memory scores cover only these anomaly directions.",
        "Complete-case shared windows may not represent missing-source or missing-feature periods.",
    ]
    for r in metric_rows:
        if r["target"] == "label_observed" and r["model"] == "combined" and not r["has_both_classes"]:
            warning_list.append(f"{r['entity_id']} {r['split']}: one or both target classes absent; inspect supports and null metrics.")
    cohorts = Counter((r["entity_id"], r["split"]) for r in rows)
    excluded_counts = Counter((r["entity_id"], r["split"]) for r in exclusions)
    summary = {
        "status": "completed", "script_version": VERSION, "snapshot_id": meta["snapshot_id"],
        "input_experiment": str(root), "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "script_sha256": sha256(__file__), "pyarrow_version": pa.__version__,
        "config": {"alpha": alpha, "normal_train_calibration_fraction": calibration_fraction,
                   "cohort": "shared windows with all nine finite in-domain features, same rows for all models",
                   "fit_policy": "earliest normal train windows per host; latest normal train fraction for calibration",
                   "score": "max one-sided scaled deviation over active features",
                   "threshold": "calibration order statistic ceil((m+1)*(1-alpha)); strict >"},
        "input_shared_windows": meta["shared_windows"], "eligible_windows": len(rows),
        "excluded_shared_windows": len(exclusions), "scored_rows": len(scored),
        "cohort_counts": [{"entity_id": h, "split": s, "eligible": n, "excluded": excluded_counts[h, s]}
                          for (h, s), n in sorted(cohorts.items())],
        "models": models, "metrics": metric_rows, "label_pair_alerts": strata,
        "warnings": warning_list, "elapsed_seconds": None,
    }
    stage = output.parent / ("." + output.name + ".staging-" + uuid.uuid4().hex)
    output.parent.mkdir(parents=True, exist_ok=True)
    stage.mkdir()
    try:
        write_json(stage / "models.json", {"version": VERSION, "snapshot_id": meta["snapshot_id"], "config": summary["config"], "models": models})
        write_json(stage / "input_checksums.json", provenance)
        write_json(stage / "metrics.json", metric_rows)
        write_csv(stage / "metrics.csv", metric_rows)
        write_csv(stage / "training_roles.csv", roles)
        write_csv(stage / "excluded_windows.csv", exclusions,
                  ["entity_id", "window_start", "split", "label_observed", "reasons"])
        schema = pa.schema([
            ("entity_id", pa.string()), ("window_start", pa.int64()), ("window_start_utc", pa.string()),
            ("split", pa.string()), ("training_role", pa.string()), ("model", pa.string()),
            ("score", pa.float64()), ("threshold", pa.float64()), ("prediction", pa.int8()),
            ("top_feature", pa.string()), ("top_feature_value", pa.float64()), ("top_feature_scores_json", pa.string()),
            ("label_network", pa.int8()), ("label_os", pa.int8()), ("label_observed", pa.int8()),
            ("snapshot_id", pa.string())])
        pq.write_table(pa.Table.from_pylist(scored, schema=schema), stage / "scores.parquet")
        summary["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        write_json(stage / "summary.json", summary)
        if output.exists():
            raise ValueError("Output appeared during run; refusing overwrite")
        stage.rename(output)
    finally:
        if stage.exists():
            shutil.rmtree(stage)
    print(f"[4/4] DONE: {output / 'summary.json'}", flush=True)
    print(f"Shared: {meta['shared_windows']}; eligible: {len(rows)}; excluded: {len(exclusions)}")
    for host, model in models.items():
        print(f"{host}: normal train={model['normal_train_count']}, fit={model['fit_count']}, calibration={model['calibration_count']}")
    print("Test metrics (observed-event target):")
    for r in metric_rows:
        if r["split"] == "test" and r["target"] == "label_observed":
            print(json.dumps({k: r[k] for k in ("entity_id", "model", "n", "precision", "recall", "f1", "false_positive_rate")}))
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Prepared host_eval_v2 directory")
    parser.add_argument("--output", required=True, help="New sibling experiment directory")
    parser.add_argument("--calibration-fraction", type=float, default=0.2)
    parser.add_argument("--alpha", type=float, default=0.05)
    parser.add_argument("--max-rows", type=int, default=200000)
    args = parser.parse_args()
    run(args.input, args.output, args.calibration_fraction, args.alpha, args.max_rows)


if __name__ == "__main__":
    main()
