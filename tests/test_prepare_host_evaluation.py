"""Synthetic integration test; never reads or changes the user's lake."""
import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from prepare_host_evaluation import prepare, require_unique, split_boundaries, configure_runtime
from pyspark.sql import SparkSession


class PreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (SparkSession.builder.master("local[2]").appName("IT5427 preparation tests")
                     .config("spark.sql.session.timeZone", "UTC")
                     .config("spark.sql.shuffle.partitions", "2").getOrCreate())
        cls.spark.sparkContext.setLogLevel("ERROR")
        configure_runtime(cls.spark, shuffle_partitions=4)

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_boundaries(self):
        self.assertEqual(split_boundaries([i * 60 for i in range(10)] * 2), (360, 480, 10))
        with self.assertRaises(ValueError):
            split_boundaries([0, 60])
        with self.assertRaises(ValueError):
            split_boundaries(range(10), 0.8, 0.3)

    def test_duplicate_label_guard(self):
        rows = self.spark.createDataFrame([("same", 0), ("same", 1)], "event_id string, label int")
        with self.assertRaisesRegex(ValueError, "duplicate keys"):
            require_unique(rows, ["event_id"], "Labels")

    def test_full_snapshot_to_experiment(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            snapshot = root / "lake" / "snapshots" / "fixture"
            snapshot.mkdir(parents=True)
            metadata = {"snapshot_id": "fixture", "status": "committed", "spec": {
                "window_seconds": 60, "entities": {"hosts": [
                    {"entity_id": "h7", "source_id": "windows7"},
                    {"entity_id": "h10", "source_id": "windows10"}]}}}
            (snapshot / "snapshot.json").write_text(json.dumps(metadata))
            gold, net, os, labels = [], [], [], []
            for i in range(10):
                w = 1554076800 + i * 60
                timestamp = datetime.fromtimestamp(w + 1, timezone.utc)
                if i != 5:
                    labels.append((f"n{i}", "network", None if i == 3 else 1 if i in (1, 8) else 0, timestamp))
                for host, source in (("h7", "windows7"), ("h10", "windows10")):
                    hn, ho = i != 5, i != 4
                    gold.append((host, w, 1 if hn else None, 1 if ho else None, hn, ho, ho, 60, "fixture", 10.0 if ho else None))
                    if hn:
                        net.append((host, w, f"n{i}"))
                    if ho:
                        os.append((host, w, f"o{host}{i}"))
                        # i=2 missing label record; i=3 attack with unknown network label;
                        # i=6 invalid label; i=1 cross-source label disagreement.
                        if i != 2:
                            labels.append((f"o{host}{i}", source, 1 if i in (3, 8) else 2 if i == 6 else 0, timestamp))
            schema = ("entity_id string, window_start long, n_network long, n_os long, "
                      "has_network boolean, has_os boolean, has_cpu boolean, window_seconds int, "
                      "snapshot_id string, host_cpu_mean double")
            self.spark.createDataFrame(gold, schema).write.parquet(str(snapshot / "gold/host_window_features"))
            edge_schema = "entity_id string, window_start long, event_id string"
            self.spark.createDataFrame(net, edge_schema).write.parquet(str(snapshot / "lineage/network_host_edges"))
            self.spark.createDataFrame(os, edge_schema).write.parquet(str(snapshot / "lineage/os_host_edges"))
            self.spark.createDataFrame(labels, "event_id string, source_id string, label int, event_time_utc timestamp").write.parquet(
                str(snapshot / "evaluation/record_labels"))
            output = root / "experiments" / "host_v1"
            before = (snapshot / "snapshot.json").read_bytes()
            result = prepare(self.spark, snapshot, output)
            self.assertEqual(result["host_windows"], 20)
            self.assertEqual(result["shared_windows"], 16)
            self.assertEqual((snapshot / "snapshot.json").read_bytes(), before)
            self.assertEqual(result["unknown_label_links"]["os_missing_label_links"], 2)
            targets = self.spark.read.parquet(str(output / "window_labels"))
            by_key = {(r.entity_id, (r.window_start - 1554076800) // 60): r for r in targets.collect()}
            self.assertEqual(len(by_key), 20)
            self.assertEqual(by_key["h7", 0].label_observed, 0)
            self.assertTrue(by_key["h7", 1].source_label_disagreement)
            self.assertIsNone(by_key["h7", 2].label_observed)
            self.assertEqual(by_key["h7", 3].label_observed, 1)
            self.assertEqual(by_key["h7", 4].label_observed, 0)
            self.assertIsNone(by_key["h7", 4].label_os)
            self.assertIsNone(by_key["h7", 5].label_network)
            self.assertIsNone(by_key["h7", 6].label_observed)
            self.assertEqual(by_key["h7", 1].network_events, 1)
            self.assertEqual(by_key["h10", 1].network_events, 1)
            splits = self.spark.read.parquet(str(output / "splits")).collect()
            for r in splits:
                i = (r.window_start - 1554076800) // 60
                self.assertEqual(r.split, "train" if i < 6 else "validation" if i < 8 else "test")
            features = self.spark.read.parquet(str(output / "features"))
            self.assertFalse(any("label" in c or "split" in c for c in features.columns))
            with self.assertRaisesRegex(ValueError, "Output exists"):
                prepare(self.spark, snapshot, output)
            with self.assertRaisesRegex(ValueError, "outside the source lake"):
                prepare(self.spark, snapshot, root / "lake" / "bad_output")


if __name__ == "__main__":
    unittest.main(verbosity=2)
