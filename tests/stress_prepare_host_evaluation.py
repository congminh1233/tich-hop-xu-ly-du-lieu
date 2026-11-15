"""Synthetic memory regression: 1M network events, 2M event-host links.

Run with spark-submit --driver-memory 1g; source data lives in a temporary folder.
This is not a substitute for running the user's full TON_IoT snapshot.
"""
import argparse
import json
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from prepare_host_evaluation import prepare, configure_runtime, stop_safely
from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", type=int, default=1000000)
    parser.add_argument("--report", default="stress_result.json")
    args = parser.parse_args()
    if args.events < 2000 or args.events % 200:
        parser.error("--events must be >=2000 and divisible by 200")
    spark = SparkSession.builder.master("local[2]").appName("IT5427 memory regression").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    configure_runtime(spark)
    try:
        with tempfile.TemporaryDirectory(prefix="it5427-memory-test-") as tmp:
            root = Path(tmp)
            snap = root / "lake" / "snapshots" / "stress"
            snap.mkdir(parents=True)
            hosts = [("h7", "windows7"), ("h10", "windows10")]
            metadata = {"snapshot_id": "stress", "status": "committed", "spec": {
                "window_seconds": 60, "entities": {"hosts": [
                    {"entity_id": a, "source_id": b} for a, b in hosts]}}}
            (snap / "snapshot.json").write_text(json.dumps(metadata))
            windows = args.events // 200
            base_epoch = 1554076800
            network = (spark.range(args.events, numPartitions=32)
                       .withColumn("event_id", F.sha2(F.concat(F.lit("network-"), F.col("id")), 256))
                       .withColumn("window_start", (F.lit(base_epoch) + F.floor(F.col("id") / 200) * 60).cast("long"))
                       .withColumn("label", F.when((F.floor(F.col("id") / 200) % 2 == 1) & (F.col("id") % 200 == 0), 1).otherwise(0)))
            host_dim = spark.createDataFrame(hosts, "entity_id string, source_id string")
            net_edges = network.crossJoin(F.broadcast(host_dim)).select("entity_id", "window_start", "event_id")
            net_edges.write.parquet(str(snap / "lineage/network_host_edges"))
            os = (spark.range(windows).crossJoin(F.broadcast(host_dim))
                  .withColumn("event_id", F.sha2(F.concat(F.lit("os-"), F.col("entity_id"), F.col("id")), 256))
                  .withColumn("window_start", (F.lit(base_epoch) + F.col("id") * 60).cast("long")))
            os.select("entity_id", "window_start", "event_id").write.parquet(str(snap / "lineage/os_host_edges"))
            net_labels = network.select("event_id", F.lit("network").alias("source_id"), "label",
                                        (F.col("window_start") + 1).cast("timestamp").alias("event_time_utc"))
            os_labels = os.select("event_id", "source_id", F.lit(0).alias("label"),
                                  (F.col("window_start") + 2).cast("timestamp").alias("event_time_utc"))
            net_labels.unionByName(os_labels).write.parquet(str(snap / "evaluation/record_labels"))
            gold = os.select("entity_id", "window_start", F.lit(200).cast("long").alias("n_network"),
                             F.lit(1).cast("long").alias("n_os"), F.lit(True).alias("has_network"),
                             F.lit(True).alias("has_os"), F.lit(True).alias("has_cpu"),
                             F.lit(60).alias("window_seconds"), F.lit("stress").alias("snapshot_id"))
            gold.write.parquet(str(snap / "gold/host_window_features"))
            result = prepare(spark, snap, root / "experiment")
            assert result["host_windows"] == windows * 2
            assert result["shared_windows"] == windows * 2
            labels = spark.read.parquet(str(root / "experiment/window_labels"))
            attack_windows = labels.filter("label_observed = 1").count()
            normal_windows = labels.filter("label_observed = 0").count()
            assert attack_windows == (windows // 2) * 2
            assert normal_windows == ((windows + 1) // 2) * 2
            assert labels.filter("network_events != 200 OR os_events != 1").count() == 0
            report = {"status": "passed", "network_events": args.events, "network_host_links": args.events * 2,
                      "os_events": windows * 2, "host_windows": windows * 2,
                      "attack_windows": attack_windows, "normal_windows": normal_windows,
                      "jvm_max_heap_bytes": result["jvm_max_heap_bytes"],
                      "runtime_settings": result["runtime_settings"],
                      "elapsed_prepare_seconds": result["elapsed_seconds"], "spark_version": spark.version,
                      "limitation": "Synthetic stress workload; not the complete TON_IoT dataset."}
            Path(args.report).write_text(json.dumps(report, indent=2))
            print(json.dumps(report, indent=2), flush=True)
    finally:
        stop_safely(spark)


if __name__ == "__main__":
    main()
