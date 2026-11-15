"""Synthetic fixtures test invariants only; never used as research observations."""
import csv
import tempfile
import unittest
from pathlib import Path

import pyarrow.parquet as pq

from toniot.common import canonical_ip, host_columns, read_json, write_json
from toniot.ingest import ingest_file, manifests
from toniot.timecheck import verify_time
from toniot.spark_job import build

ROOT = Path(__file__).resolve().parents[1]
T = 1556137080


def write_csv(path, header, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def parquet(path):
    return pq.ParquetDataset(path).read().to_pylist()


class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.lake = self.root / "lake"

    def tearDown(self):
        self.tmp.cleanup()

    def network(self):
        p = self.root / "Network_dataset_1.csv"
        header = "ts,src_ip,dst_ip,src_port,dst_port,proto,duration,src_bytes,dst_bytes,src_pkts,dst_pkts,conn_state,label,type".split(",")
        row = [T, "192.168.1.193", "192.168.1.30", 5000, 80, "tcp", 1, 100, 20, 2, 1, "SF", 0, "normal"]
        rows = [row, row[:], [T + 30, "192.168.1.30", "192.168.1.193", 6000, 443, "tcp", 2, 300, 40, 3, 2, "SF", 1, "scanning"],
                [T + 60, "192.168.1.193", "192.168.1.193", 80, 80, "tcp", 1, 10, 5, 1, 1, "SF", 0, "normal"],
                ["bad", "192.168.1.193", "192.168.1.30", 1, 2, "tcp", 0, 0, 0, 0, 0, "S0", 0, "normal"],
                [T, "999.1.1.1", "192.168.1.193", 1, 2, "tcp", 0, 0, 0, 0, 0, "S0", 0, "normal"]]
        write_csv(p, header, rows)
        return p

    def windows(self):
        p = self.root / "windows7_dataset.csv"
        write_csv(p, ["ts", "Processor(_Total) pct_ Processor Time", "Memory Available Bytes", "Memory pct_ Committed Bytes In Use", "label", "type"],
                  [[T + 5, 10, 1000, 30, 0, "normal"], [T + 15, 30, 800, 40, 1, "backdoor"],
                   [T + 120, "  ", 700, 45, 0, "normal"]])
        return p

    def test_cpu_scope(self):
        self.assertEqual(host_columns(["Processor_pct_ Processor_Time"])["host_cpu_pct"], "Processor_pct_ Processor_Time")
        with self.assertRaises(ValueError):
            host_columns(["Process(_Total) pct_ Processor Time"])

    def test_ipv6_is_valid_and_canonical(self):
        self.assertEqual(canonical_ip("FE80:0000:0000:0000:0000:0000:0000:0001"), "fe80::1")
        self.assertIsNone(canonical_ip("999.1.1.1"))

    def test_ingest_versions_and_failure(self):
        p = self.network()
        a = ingest_file(p, self.lake, "synthetic_unit_test")
        b = ingest_file(p, self.lake, "synthetic_unit_test")
        self.assertEqual(a["source_file_id"], b["source_file_id"])
        self.assertEqual(b["ingest_action"], "already_committed")
        self.assertEqual(len(manifests(self.lake)), 1)
        data = pq.read_table(next((self.lake / "bronze").glob("*/*/records.parquet"))).to_pylist()
        self.assertEqual([r["_it_row_ref"] for r in data], list(range(1, 7)))
        self.assertEqual(a["input_rows"], 6)
        broken = self.root / "Network_dataset_2.csv"
        broken.write_text(p.read_text(encoding="utf-8-sig") + "1,too,few\n", encoding="utf-8")
        with self.assertRaises(Exception):
            ingest_file(broken, self.lake)
        self.assertEqual(len(manifests(self.lake)), 1)

    def test_time_rule_evidence(self):
        p = self.root / "ground_truth.csv"
        write_csv(p, ["ts", "data", "time", "type"], [[1556076029 + i, "23-Apr-19", f"20:20:{29+i:02d}", "test_fixture"] for i in range(10)])
        output = self.root / "rules.json"
        result = verify_time(p, "iot_weather", ROOT / "config/time_rules.json", output)
        self.assertEqual(result["offset_seconds"], 25200)
        write_csv(p, ["ts", "data", "time"], [[1556076029, "23-Apr-19", "20:20:29"], [1556076030, "23-Apr-19", "20:20:29"]] * 5)
        with self.assertRaises(ValueError):
            verify_time(p, "iot_weather", ROOT / "config/time_rules.json", output)

    def test_spark_end_to_end(self):
        net, win = self.network(), self.windows()
        garage = self.root / "IoT_Garage_Door.csv"
        write_csv(garage, ["date", "time", "door_state", "sphone_signal", "label", "type"],
                  [["25-Apr-19", "00:18:00", "open", " true  ", 0, "normal"],
                   ["25-Apr-19", "00:18:01", "closed", "0", 0, "normal"],
                   ["25-Apr-19", "00:18:02", "", "", 0, "normal"],
                   ["", "", "open", "1", 1, "test_fixture"]])
        rules = read_json(ROOT / "config/time_rules.json")
        rules["iot"]["iot_garage_door"] = {"offset_seconds": 25200, "status": "synthetic_test_fixture"}
        rulefile = self.root / "rules.json"
        write_json(rulefile, rules)
        unresolved_lake = self.root / "unresolved_lake"
        ingest_file(garage, unresolved_lake, "synthetic_unit_test")
        unresolved = build(unresolved_lake, ROOT / "config/entities.json", ROOT / "config/time_rules.json",
                           expected_files=[garage.name])
        self.assertEqual(unresolved["source_accounting"][0]["quarantined_source_rows"], 4)
        self.assertEqual(unresolved["source_accounting"][0]["accepted_source_rows"], 0)
        for p in (net, garage):
            ingest_file(p, self.lake, "synthetic_unit_test")
        expected = [net.name, win.name, garage.name]
        partial = build(self.lake, ROOT / "config/entities.json", rulefile, expected_files=expected)
        self.assertFalse(partial["input_scope_complete"])
        self.assertIn(win.name, partial["missing_expected_files"])
        ingest_file(win, self.lake, "synthetic_unit_test")
        m = build(self.lake, ROOT / "config/entities.json", rulefile,
                  expected_files=expected, strict=True)
        snap = self.lake / "snapshots" / m["snapshot_id"]
        self.assertEqual([(x["input_rows"], x["accepted_source_rows"], x["quarantined_source_rows"])
                          for x in sorted(m["source_accounting"], key=lambda x: x["file_name"])], [(4, 3, 1), (6, 4, 2), (3, 3, 0)])
        rows = parquet(snap / "gold/host_window_features")
        self.assertEqual(len(rows), 3)
        self.assertEqual(len({(r["entity_id"], r["window_start"]) for r in rows}), 3)
        first = next(r for r in rows if r["window_start"] == T)
        self.assertEqual(first["n_network"], 3)
        self.assertEqual(first["n_os"], 2)
        self.assertAlmostEqual(first["host_cpu_mean"], 20)
        self.assertEqual(first["flow_start_sent_bytes"], 240)
        self.assertEqual(first["flow_start_received_bytes"], 340)
        second = next(r for r in rows if r["window_start"] == T + 60)
        self.assertEqual(second["n_network"], 1)  # self-flow counted once
        self.assertFalse(second["has_os"])
        third = next(r for r in rows if r["window_start"] == T + 120)
        self.assertTrue(third["has_os"])
        self.assertFalse(third["has_cpu"])
        self.assertIsNone(third["host_cpu_mean"])
        for row in rows:
            self.assertNotIn("label", row)
            self.assertNotIn("attack_type", row)
        sensors = parquet(snap / "silver/sensor_observation")
        phone = sorted([r for r in sensors if r["metric_name"] == "sphone_signal"], key=lambda r: r["source_row_ref"])
        self.assertEqual([r["boolean_value"] for r in phone], [True, False, None])
        network = parquet(snap / "silver/network_flow")
        self.assertEqual(len({r["event_id"] for r in network}), 4)  # exact repeated measurement kept
        again = build(self.lake, ROOT / "config/entities.json", rulefile, expected_files=expected, strict=True)
        self.assertEqual(m["snapshot_id"], again["snapshot_id"])
        build(self.lake, ROOT / "config/entities.json", rulefile,
              selected_ids=partial["spec"]["input_manifest_ids"], expected_files=expected)
        self.assertEqual(read_json(self.lake / "latest.json")["snapshot_id"], partial["snapshot_id"])
        # Same complete file set, different arrival order: exact same logical Gold.
        fresh = self.root / "full_batch_lake"
        for p in (win, garage, net):
            ingest_file(p, fresh, "synthetic_unit_test")
        full = build(fresh, ROOT / "config/entities.json", rulefile, expected_files=expected, strict=True)
        full_rows = parquet(fresh / "snapshots" / full["snapshot_id"] / "gold/host_window_features")
        self.assertEqual(sorted(rows, key=lambda r: r["window_start"]), sorted(full_rows, key=lambda r: r["window_start"]))
        with self.assertRaises(ValueError):
            build(self.lake, ROOT / "config/entities.json", rulefile, strict=True)


if __name__ == "__main__":
    unittest.main()
