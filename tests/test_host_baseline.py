import copy
import json
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from run_host_baseline import (FEATURES, build_models, calibrate_threshold, fit_feature,
                               index_rows, metrics, run, score_group)
import pyarrow as pa
import pyarrow.parquet as pq


def fixture_rows():
    rows = []
    for i in range(180):
        part = "train" if i < 120 else "validation" if i < 140 else "test"
        attack = int(100 <= i < 140 or i >= 160)
        row = {"entity_id": "host", "window_start": 1554076800 + i * 60,
               "snapshot_id": "fixture", "window_seconds": 60, "split": part,
               "label_network": attack, "label_os": attack, "label_observed": attack}
        for j, name in enumerate(FEATURES):
            row[name] = float(10 + j + i % 7)
        if attack:
            row["n_network"] = 100000.0
        rows.append(row)
    # Different source labels, same observed attack label.
    rows[160]["label_os"] = 0
    return rows


def write_fixture(path, rows):
    path.mkdir(parents=True)
    features = [{k: r[k] for k in ["entity_id", "window_start", "snapshot_id", "window_seconds"] + FEATURES} for r in rows]
    splits = [{"entity_id": r["entity_id"], "window_start": r["window_start"], "split": r["split"],
               "shared_window": True, "has_network": True, "has_os": True} for r in rows]
    labels = [{k: r[k] for k in ("entity_id", "window_start", "label_network", "label_os", "label_observed")} for r in rows]
    for name, data in (("features", features), ("splits", splits), ("window_labels", labels)):
        (path / name).mkdir()
        pq.write_table(pa.Table.from_pylist(data), path / name / "part-000.parquet")
    meta = {"status": "prepared", "snapshot_id": "fixture", "window_seconds": 60,
            "host_windows": len(rows), "shared_windows": len(rows), "split_policy": {
                "validation_start_epoch": 1554076800 + 120 * 60,
                "test_start_epoch": 1554076800 + 140 * 60}}
    (path / "summary.json").write_text(json.dumps(meta))


class BaselineTests(unittest.TestCase):
    def test_scale_fallbacks(self):
        self.assertEqual(fit_feature([1] * 100, "n_network")["scale_method"], "constant_feature_excluded")
        self.assertEqual(fit_feature([0] * 90 + [1] * 10, "n_network")["scale_method"], "std_fallback_for_zero_MAD_and_IQR")
        self.assertEqual(fit_feature([0] * 60 + [1] * 40, "host_cpu_mean")["scale_method"], "IQR_fallback")
        self.assertEqual(fit_feature(list(range(100)), "host_cpu_mean")["scale_method"], "scaled_MAD")

    def test_calibration_rank_and_ties(self):
        threshold, rank = calibrate_threshold(list(range(1, 21)), 0.1)
        self.assertEqual((threshold, rank), (19, 19))
        self.assertFalse(19 > threshold)
        self.assertTrue(20 > threshold)
        with self.assertRaises(ValueError):
            calibrate_threshold([0, 1], 0.01)

    def test_undefined_metrics(self):
        result = metrics([(1, 1), (1, 0)])
        self.assertIsNone(result["false_positive_rate"])
        self.assertIsNone(result["balanced_accuracy"])
        self.assertEqual(result["recall"], 0.5)
        self.assertFalse(result["has_both_classes"])
        self.assertIsNone(metrics([])["f1"])

    def test_duplicate_guard(self):
        with self.assertRaises(ValueError):
            index_rows([{"entity_id": "h", "window_start": 0}] * 2, "fixture")

    def test_future_data_cannot_change_model(self):
        before = fixture_rows()
        after = copy.deepcopy(before)
        for row in after:
            if row["split"] != "train":
                for name in FEATURES:
                    row[name] += 1e9
                row["label_network"] = row["label_os"] = row["label_observed"] = 0
        self.assertEqual(build_models(before), build_models(after))
        models, roles = build_models(before)
        self.assertEqual(models["host"]["fit_count"], 80)
        self.assertEqual(models["host"]["calibration_count"], 20)
        self.assertEqual(len(roles), 100)
        params = models["host"]["features"]
        row = before[-1]
        self.assertEqual(score_group(row, params, "combined")[0],
                         max(score_group(row, params, "network")[0], score_group(row, params, "windows")[0]))

    def test_end_to_end_shared_cohort(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = fixture_rows()
            rows[141]["host_cpu_mean"] = None
            inp, out = root / "input", root / "output"
            write_fixture(inp, rows)
            before = (inp / "summary.json").read_bytes()
            report = run(inp, out)
            self.assertEqual(report["excluded_shared_windows"], 1)
            self.assertEqual(report["eligible_windows"], 179)
            self.assertEqual(report["scored_rows"], 179 * 3)
            self.assertEqual((inp / "summary.json").read_bytes(), before)
            primary = [r for r in report["metrics"] if r["target"] == "label_observed"]
            self.assertEqual({r["n"] for r in primary if r["split"] == "test"}, {39})
            self.assertTrue(all(r["false_positive_rate"] is None for r in primary if r["split"] == "validation"))
            scored = pq.read_table(out / "scores.parquet").to_pylist()
            self.assertEqual(len(scored), 537)
            json.loads((out / "models.json").read_text())
            self.assertTrue((out / "excluded_windows.csv").exists())
            with self.assertRaises(ValueError):
                run(inp, out)

    def test_bad_chronological_split_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            rows = fixture_rows()
            rows[-1]["split"] = "train"
            write_fixture(root / "input", rows)
            with self.assertRaisesRegex(ValueError, "Time split mismatch"):
                run(root / "input", root / "output")


if __name__ == "__main__":
    unittest.main(verbosity=2)
