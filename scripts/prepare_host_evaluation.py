#!/usr/bin/env python3
"""Read one local TON_IoT snapshot; write a separate evaluation preparation run.

Python 3.10+, PySpark 3.5.7, Java 17. No model training, no source mutation.
"""
import argparse
import hashlib
import json
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F

KEYS = ["entity_id", "window_start"]
VERSION = "host-evaluation-prep-1.1-memory-fix"


def configure_runtime(spark, shuffle_partitions=64, cache_batch_size=1000):
    """Bound per-task work; large intermediates are persisted on disk below."""
    if shuffle_partitions < 1 or cache_batch_size < 1:
        raise ValueError("Partition and batch sizes must be positive")
    settings = {
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.shuffle.partitions": str(shuffle_partitions),
        "spark.sql.inMemoryColumnarStorage.batchSize": str(cache_batch_size),
        "spark.sql.files.maxPartitionBytes": str(32 * 1024 * 1024),
        "spark.sql.parquet.columnarReaderBatchSize": "1024",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "false",
        "spark.sql.autoBroadcastJoinThreshold": "-1",
        "spark.sql.adaptive.autoBroadcastJoinThreshold": "-1",
    }
    for key, value in settings.items():
        spark.conf.set(key, value)
    return settings


def stop_safely(spark):
    # A dead JVM must not replace the original task error with a cleanup error.
    try:
        spark.stop()
    except Exception as exc:
        print(f"WARN: Spark cleanup failed ({type(exc).__name__}); inspect the first error above.", file=sys.stderr)


def require_columns(df, names, context):
    missing = set(names) - set(df.columns)
    if missing:
        raise ValueError(f"{context}: missing columns {sorted(missing)}")


def reject_rows(df, reason):
    sample = df.limit(3).collect()
    if sample:
        raise ValueError(f"{reason}; examples: {[r.asDict() for r in sample]}")


def require_unique(df, keys, context):
    reject_rows(df.groupBy(*keys).count().filter(F.col("count") > 1),
                f"{context}: duplicate keys {keys}")


def window_label(total, attacks, unknown):
    return (F.when(F.col(attacks) > 0, F.lit(1))
            .when((F.col(total) > 0) & (F.col(unknown) == 0), F.lit(0))
            .otherwise(F.lit(None).cast("int")))


def aggregate_labels(joined, gold):
    """Aggregate each source independently: no Network x OS event join."""
    expressions = []
    counts = []
    for source in ("network", "os"):
        active = F.col("edge_source") == source
        valid = F.coalesce(F.col("label").isin(0, 1), F.lit(False))
        conditions = {
            "events": active,
            "normal": active & (F.col("label") == 0),
            "attack": active & (F.col("label") == 1),
            "unknown": active & ~valid,
            "missing_label_record": active & F.col("source_id").isNull(),
        }
        for suffix, condition in conditions.items():
            name = f"{source}_{suffix}"
            counts.append(name)
            expressions.append(F.sum(F.when(condition, 1).otherwise(0)).alias(name))
    result = (gold.select(*KEYS).join(joined.groupBy(*KEYS).agg(*expressions), KEYS, "left")
              .fillna(0, subset=counts))
    for source in ("network", "os"):
        result = result.withColumn(f"label_{source}", window_label(
            f"{source}_events", f"{source}_attack", f"{source}_unknown"))
    result = (result.withColumn("observed_events", F.col("network_events") + F.col("os_events"))
              .withColumn("attack_events", F.col("network_attack") + F.col("os_attack"))
              .withColumn("unknown_events", F.col("network_unknown") + F.col("os_unknown"))
              .withColumn("label_observed", window_label("observed_events", "attack_events", "unknown_events"))
              .withColumn("source_label_disagreement", F.when(
                  F.col("label_network").isNotNull() & F.col("label_os").isNotNull(),
                  F.col("label_network") != F.col("label_os")).otherwise(F.lit(None).cast("boolean"))))
    return result


def split_boundaries(windows, train_ratio=0.6, validation_ratio=0.2):
    if not (0 < train_ratio < 1 and 0 < validation_ratio < 1
            and train_ratio + validation_ratio < 1):
        raise ValueError("Ratios must be positive and sum to less than 1")
    values = sorted(set(windows))
    n = len(values)
    a, b = int(n * train_ratio), int(n * (train_ratio + validation_ratio))
    if not (0 < a < b < n):
        raise ValueError("Too few distinct windows for three nonempty chronological splits")
    return values[a], values[b], n


def utc(epoch):
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat() if epoch is not None else None


def collect_rows(df):
    return [r.asDict(recursive=True) for r in df.collect()]


def prepare(spark, snapshot, output, train_ratio=0.6, validation_ratio=0.2):
    snapshot, output = Path(snapshot).resolve(), Path(output).resolve()
    if output.exists():
        raise ValueError(f"Output exists; choose another --output: {output}")
    # Do not write an experiment anywhere inside the source lake.
    lake = snapshot.parent.parent
    if output == lake or lake in output.parents:
        raise ValueError("--output must be outside the source lake")
    meta_path = snapshot / "snapshot.json"
    metadata = json.loads(meta_path.read_text(encoding="utf-8"))
    if metadata.get("status") != "committed" or metadata.get("snapshot_id") != snapshot.name:
        raise ValueError("Snapshot must be committed and its ID must match its directory")
    seconds = int(metadata["spec"]["window_seconds"])
    if seconds <= 0:
        raise ValueError("Invalid window length")
    host_map = [(h["entity_id"], h["source_id"]) for h in metadata["spec"]["entities"]["hosts"]]
    if len({h[0] for h in host_map}) != len(host_map):
        raise ValueError("Duplicate entity_id in snapshot entity mapping")
    expected_sources = spark.createDataFrame(host_map, "entity_id string, expected_os_source string")
    cached = []
    stage = None
    started = time.perf_counter()

    def cache(df, large=False):
        # DISK_ONLY avoids accumulating event-level tables in the Java heap.
        # Smaller cache batches still matter while Spark builds a disk block.
        cached.append(df.persist(StorageLevel.DISK_ONLY if large else StorageLevel.MEMORY_AND_DISK))
        return cached[-1]

    def read(relative):
        path = snapshot / relative
        if not path.is_dir():
            raise ValueError(f"Missing snapshot table: {path}")
        return spark.read.parquet(str(path))

    try:
        print("[1/5] Checking Gold keys, coverage and snapshot identity...", flush=True)
        gold = cache(read("gold/host_window_features"))
        require_columns(gold, KEYS + ["n_network", "n_os", "has_network", "has_os", "has_cpu",
                                     "window_seconds", "snapshot_id"], "Gold")
        forbidden = [c for c in gold.columns if "label" in c.lower() or "attack" in c.lower()
                     or c.lower() in ("type", "split")]
        if forbidden:
            raise ValueError(f"Unexpected target/split fields in Gold: {forbidden}")
        require_unique(gold, KEYS, "Gold")
        reject_rows(gold.filter(F.col("entity_id").isNull() | F.col("window_start").isNull()
                               | (F.col("window_start") % seconds != 0)), "Invalid Gold keys")
        reject_rows(gold.filter(~F.col("snapshot_id").eqNullSafe(snapshot.name)
                               | ~F.col("window_seconds").eqNullSafe(seconds)), "Mixed snapshot/window length")
        reject_rows(gold.join(expected_sources, "entity_id", "left_anti"), "Unknown Gold entity")
        for flag, count in (("has_network", "n_network"), ("has_os", "n_os")):
            reject_rows(gold.filter(~F.col(flag).eqNullSafe(F.coalesce(F.col(count), F.lit(0)) > 0)),
                        f"Inconsistent {flag}")
        print("[2/5] Checking lineage and matching record labels (may take several minutes)...", flush=True)
        edges = []
        for source, table in (("network", "network_host_edges"), ("os", "os_host_edges")):
            frame = read(f"lineage/{table}").select(*KEYS, "event_id").withColumn("edge_source", F.lit(source))
            edges.append(frame)
        edges = cache(edges[0].unionByName(edges[1]), large=True)
        reject_rows(edges.filter(F.col("event_id").isNull() | F.col("entity_id").isNull()
                                | F.col("window_start").isNull()), "Null lineage keys")
        # Same flow may legitimately touch two different hosts. Deduplicate neither host away.
        require_unique(edges, KEYS + ["event_id", "edge_source"], "Lineage")
        reject_rows(edges.select(*KEYS).distinct().join(gold.select(*KEYS), KEYS, "left_anti"),
                    "Lineage window absent from Gold")
        all_labels = read("evaluation/record_labels")
        require_columns(all_labels, ["event_id", "source_id", "label", "event_time_utc"], "Record labels")
        labels = cache(all_labels.select("event_id", "source_id", "label", "event_time_utc")
                       .join(edges.select("event_id").distinct(), "event_id", "left_semi"), large=True)
        print("[2a/5] Checking referenced label uniqueness; large intermediates use disk...", flush=True)
        require_unique(labels, ["event_id"], "Referenced record labels")
        joined = cache(edges.join(labels, "event_id", "left").join(F.broadcast(expected_sources), "entity_id", "left"), large=True)
        reject_rows(joined.filter(F.col("source_id").isNotNull() & (
            ((F.col("edge_source") == "network") & (F.col("source_id") != "network")) |
            ((F.col("edge_source") == "os") & ~F.col("source_id").eqNullSafe(F.col("expected_os_source"))))),
            "Label source does not match lineage/entity mapping")
        computed_window = (F.floor(F.col("event_time_utc").cast("double") / seconds) * seconds).cast("long")
        reject_rows(joined.filter(F.col("source_id").isNotNull() & ~F.col("window_start").eqNullSafe(computed_window)),
                    "Label event time does not match lineage window")
        targets = cache(aggregate_labels(joined, gold))
        # Finish the small window table before releasing the event-level caches.
        target_count = targets.count()
        check = targets.join(gold.select(*KEYS, "n_network", "n_os"), KEYS)
        reject_rows(check.filter((F.col("network_events") != F.coalesce(F.col("n_network"), F.lit(0))) |
                                (F.col("os_events") != F.coalesce(F.col("n_os"), F.lit(0)))),
                    "Lineage event counts disagree with Gold")
        for large_frame in (joined, labels, edges):
            large_frame.unpersist(blocking=True)
        print(f"[2b/5] Aggregated {target_count} host windows; released event-level caches.", flush=True)
        print("[3/5] Creating global chronological splits...", flush=True)
        values = [r[0] for r in gold.select("window_start").distinct().collect()]
        validation_start, test_start, distinct_windows = split_boundaries(values, train_ratio, validation_ratio)
        splits = (gold.select(*KEYS, "has_network", "has_os", "has_cpu")
                  .withColumn("split", F.when(F.col("window_start") < validation_start, "train")
                              .when(F.col("window_start") < test_start, "validation").otherwise("test"))
                  .withColumn("shared_window", F.col("has_network") & F.col("has_os")))
        evaluation = cache(targets.join(splits, KEYS))
        coverage = collect_rows(gold.groupBy("entity_id", "has_network", "has_os", "has_cpu").count()
                                .orderBy("entity_id", "has_network", "has_os", "has_cpu"))
        distributions = collect_rows(evaluation.groupBy("entity_id", "split", "shared_window", "label_observed")
                                     .count().orderBy("entity_id", "split", "shared_window", "label_observed"))
        stats = []
        for scope, subset in (("all_observed", evaluation), ("shared", evaluation.filter("shared_window"))):
            rows = collect_rows(subset.groupBy("entity_id", "split").agg(
                F.count("*").alias("windows"),
                F.sum(F.when(F.col("label_observed") == 0, 1).otherwise(0)).alias("normal"),
                F.sum(F.when(F.col("label_observed") == 1, 1).otherwise(0)).alias("attack"),
                F.sum(F.when(F.col("label_observed").isNull(), 1).otherwise(0)).alias("unknown"),
                F.sum(F.when(F.col("source_label_disagreement"), 1).otherwise(0)).alias("source_disagreements"),
                F.sum(F.when(F.col("has_cpu"), 1).otherwise(0)).alias("with_cpu"),
                F.min("window_start").alias("first_epoch"), F.max("window_start").alias("last_epoch")))
            for row in rows:
                row.update(scope=scope, first_utc=utc(row["first_epoch"]), last_utc=utc(row["last_epoch"]))
            stats.extend(rows)
        readiness = []
        for entity, _ in host_map:
            entity_stats = {r["split"]: r for r in stats if r["scope"] == "shared" and r["entity_id"] == entity}
            issues = []
            for name in ("train", "validation", "test"):
                if name not in entity_stats:
                    issues.append(f"no_shared_windows_in_{name}")
            if entity_stats.get("train", {}).get("normal", 0) == 0:
                issues.append("no_known_normal_train_windows")
            elif entity_stats["train"]["normal"] < 30:
                issues.append("fewer_than_30_normal_train_windows_review_sample_size")
            for name in ("validation", "test"):
                if any(entity_stats.get(name, {}).get(c, 0) == 0 for c in ("normal", "attack")):
                    issues.append(f"{name}_does_not_contain_both_known_classes")
            readiness.append({"entity_id": entity, "issues": issues,
                              "preliminary_support_check_passed": not issues})
        unknown_totals = evaluation.agg(F.sum("unknown_events").alias("unknown_event_host_links"),
                                       F.sum("network_missing_label_record").alias("network_missing_label_links"),
                                       F.sum("os_missing_label_record").alias("os_missing_label_links")).first().asDict()
        summary = {
            "status": "prepared", "script_version": VERSION, "snapshot_id": snapshot.name,
            "snapshot_path": str(snapshot), "window_seconds": seconds,
            "snapshot_metadata_sha256": hashlib.sha256(meta_path.read_bytes()).hexdigest(),
            "script_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
            "pyspark_version": spark.version, "master": spark.sparkContext.master,
            "memory_strategy": "DISK_ONLY event tables; MEMORY_AND_DISK window tables; release event caches after aggregation",
            "runtime_settings": {key: spark.conf.get(key) for key in (
                "spark.sql.shuffle.partitions", "spark.sql.inMemoryColumnarStorage.batchSize",
                "spark.sql.files.maxPartitionBytes", "spark.sql.parquet.columnarReaderBatchSize",
                "spark.sql.adaptive.coalescePartitions.enabled", "spark.sql.autoBroadcastJoinThreshold",
                "spark.sql.adaptive.autoBroadcastJoinThreshold")},
            "jvm_max_heap_bytes": spark._jvm.java.lang.Runtime.getRuntime().maxMemory(),
            "created_at_utc": datetime.now(timezone.utc).isoformat(),
            "host_windows": gold.count(), "shared_windows": evaluation.filter("shared_window").count(),
            "split_policy": {"basis": "globally_sorted_distinct_observed_window_start",
                             "train_ratio": train_ratio, "validation_ratio": validation_ratio,
                             "distinct_windows": distinct_windows,
                             "validation_start_utc": utc(validation_start), "test_start_utc": utc(test_start),
                             "validation_start_epoch": validation_start, "test_start_epoch": test_start},
            "label_policy": "any attack => 1; all observed labels valid normal => 0; otherwise null; absent source stays unknown",
            "coverage": coverage, "label_distribution": distributions,
            "split_statistics": sorted(stats, key=lambda r: (r["scope"], r["entity_id"], r["first_epoch"])),
            "preliminary_support": readiness, "unknown_label_links": unknown_totals,
            "limits": ["Observed-event labels do not prove that a host was compromised.",
                       "Shared-window coverage is required for a fair Network/OS/combined comparison.",
                       "Source disagreements are retained; any-attack labels may favor the network model.",
                       "A flow touching two mapped hosts contributes once to each host.",
                       "Support checks are not a guarantee of model quality or sufficient sample size.",
                       "Feature null handling and future rolling-feature leakage checks remain to be implemented.",
                       "Timestamp consistency does not independently verify the source clock or IP ownership."],
        }
        print("[4/5] Writing separate features, labels, splits and reports...", flush=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        stage = output.parent / ("." + output.name + ".staging-" + uuid.uuid4().hex)
        stage.mkdir()
        gold.write.mode("error").parquet(str(stage / "features"))
        targets.write.mode("error").parquet(str(stage / "window_labels"))
        splits.write.mode("error").parquet(str(stage / "splits"))
        summary["elapsed_seconds"] = round(time.perf_counter() - started, 3)
        (stage / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
        (stage / "source_snapshot.json").write_bytes(meta_path.read_bytes())
        # Single-writer local operation. Do not run two writers for the same output.
        if output.exists():
            raise ValueError("Output appeared during processing; refusing to overwrite")
        stage.rename(output)
        stage = None
        print("[5/5] DONE: " + str(output / "summary.json"), flush=True)
        print(json.dumps({k: summary[k] for k in ("snapshot_id", "host_windows", "shared_windows", "preliminary_support")},
                         ensure_ascii=False, indent=2), flush=True)
        return summary
    finally:
        for df in reversed(cached):
            try:
                df.unpersist()
            except Exception:
                print("WARN: Cannot release Spark cache; JVM may have stopped. Inspect the first error above.", file=sys.stderr)
                break
        if stage is not None and stage.exists():
            shutil.rmtree(stage)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lake", default="lake")
    parser.add_argument("--snapshot", required=True, help="Committed snapshot directory name (explicitly pinned)")
    parser.add_argument("--output", required=True, help="New local experiment directory outside lake")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--shuffle-partitions", type=int, default=64)
    parser.add_argument("--cache-batch-size", type=int, default=1000)
    parser.add_argument("--train-ratio", type=float, default=0.6)
    parser.add_argument("--validation-ratio", type=float, default=0.2)
    args = parser.parse_args()
    if Path(args.snapshot).name != args.snapshot or args.snapshot in (".", ".."):
        parser.error("--snapshot must be a directory name, not a path")
    if args.shuffle_partitions < 1 or args.cache_batch_size < 1:
        parser.error("--shuffle-partitions and --cache-batch-size must be positive")
    spark = (SparkSession.builder.appName("IT5427 Host Evaluation Preparation").master(args.master)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    try:
        settings = configure_runtime(spark, args.shuffle_partitions, args.cache_batch_size)
        max_heap = spark._jvm.java.lang.Runtime.getRuntime().maxMemory() / (1024 ** 3)
        print(f"{VERSION}: JVM max heap {max_heap:.2f} GiB; large tables DISK_ONLY", flush=True)
        print("Runtime: " + json.dumps(settings), flush=True)
        prepare(spark, Path(args.lake) / "snapshots" / args.snapshot, args.output,
                args.train_ratio, args.validation_ratio)
    finally:
        stop_safely(spark)


if __name__ == "__main__":
    main()
