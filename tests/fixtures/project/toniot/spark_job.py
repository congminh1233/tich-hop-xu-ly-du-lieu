"""Explicit mappings, source accounting, independent aggregation, then host join."""
import json
import shutil
import time
import uuid
from functools import reduce
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

from .common import SENSORS, canonical_ip, digest, host_columns, now, sha256, read_json, write_json
from .ingest import select_inputs


def col(name):
    return F.col("`" + name.replace("`", "``") + "`")


def clean(name):
    x = F.trim(col(name))
    return F.when(x.isin("", "-", "null", "NULL", "NaN", "nan"), F.lit(None)).otherwise(x)


def number(name):
    x = clean(name).cast("double")
    return F.when((~F.isnan(x)) & (F.abs(x) < 1e300), x)


def flags(*conditions):
    return F.filter(F.array(*[F.when(c, F.lit(s)) for c, s in conditions]), lambda x: x.isNotNull())


def union(frames):
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames) if frames else None


def base(df, manifest, rules):
    source = manifest["source_id"]
    if source in SENSORS:
        raw = F.concat_ws(" ", F.trim(col("date")), F.trim(col("time")))
        patterns = (
            "d-MMM-yy HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "d-MMM-yyyy HH:mm:ss",
            "d-MMM-yy H:mm:ss",
            "yyyy-MM-dd H:mm:ss",
            "d-MMM-yyyy H:mm:ss",
        )
        parsed = F.coalesce(
            *[F.to_timestamp(raw, pattern) for pattern in patterns]
        )
        rule = rules.get("iot", {}).get(source)
        epoch = (parsed.cast("double") + F.lit(rule["offset_seconds"])) if rule else F.lit(None).cast("double")
        quality = rule.get("status", "provisional") + "_normal_rows_assumed" if rule else "unresolved_time_rule"
    else:
        raw, epoch, quality = col("ts"), number("ts"), "unix_seconds"
    # Unit errors (milliseconds) and corrupt timestamps do not reach temporal joins.
    epoch = F.when(epoch.between(946684800, 4102444799), epoch)
    return (df.withColumn("event_id", F.sha2(F.concat_ws(":", col("_it_file_id"), col("_it_row_ref")), 256))
            .withColumn("source_id", col("_it_source_id"))
            .withColumn("source_file_id", col("_it_file_id"))
            .withColumn("source_row_ref", col("_it_row_ref"))
            .withColumn("event_time_raw", raw)
            .withColumn("event_time_utc", epoch.cast("timestamp"))
            .withColumn("time_quality", F.lit(quality))
            .withColumn("time_rule_version", F.lit(rules["version"]))
            .withColumn("ingested_at", F.lit(manifest["committed_at"]))
            .withColumn("event_date", F.to_date("event_time_utc")))


META = ["event_id", "source_id", "source_file_id", "source_row_ref", "event_time_raw",
        "event_time_utc", "event_date", "time_quality", "time_rule_version", "ingested_at"]


def normalize(df, manifest, rules, entities):
    source = manifest["source_id"]
    b = base(df, manifest, rules)
    invalid = col("event_time_utc").isNull()
    if source == "network":
        # TON_IoT includes IPv6. Preserve both families; canonicalize before identity matching.
        ip_parser = F.udf(canonical_ip, "string")
        b = b.withColumn("_src_ip_norm", ip_parser(col("src_ip"))).withColumn("_dst_ip_norm", ip_parser(col("dst_ip")))
        valid_ip = col("_src_ip_norm").isNotNull() & col("_dst_ip_norm").isNotNull()
        invalid = invalid | ~valid_ip
    reasons = flags((col("event_time_utc").isNull(), "invalid_or_unresolved_time"),
                    *(([(~valid_ip, "invalid_ip")] if source == "network" else [])))
    quarantine = b.filter(invalid).select(*META, reasons.alias("quality_flags"))
    valid = b.filter(~invalid)
    label = F.when(clean("label").isin("0", "1"), clean("label").cast("int"))
    targets = valid.select(*META, label.alias("label"), clean("type").alias("attack_type"),
                           col("label").alias("label_raw"))
    if source == "network":
        nums = {"src_port": "src_port", "dst_port": "dst_port", "duration_s": "duration",
                "src_bytes": "src_bytes", "dst_bytes": "dst_bytes", "src_pkts": "src_pkts", "dst_pkts": "dst_pkts"}
        q = []
        for out, original in nums.items():
            q.append((clean(original).isNotNull() & number(original).isNull(), "parse_fail:" + original))
            q.append((number(original) < 0, "negative:" + original))
        q.extend([(number(p).isNotNull() & ((number(p) > 65535) | (number(p) != F.floor(number(p)))), "invalid_port:" + p)
                  for p in ("src_port", "dst_port")])
        silver = valid.select(*META, col("_src_ip_norm").alias("src_ip"), col("_dst_ip_norm").alias("dst_ip"),
            clean("proto").alias("proto"), clean("conn_state").alias("conn_state"),
            *[number(c).alias(out) for out, c in nums.items()], flags(*q).alias("quality_flags"))
        return "network_flow", silver, targets, quarantine, valid
    if source.startswith("windows"):
        matched = [h for h in entities["hosts"] if h["source_id"] == source]
        if len(matched) != 1:
            raise ValueError("Missing or ambiguous Windows identity mapping")
        frames = []
        for metric, original in host_columns(manifest["columns"]).items():
            val = number(original)
            q = flags((clean(original).isNotNull() & val.isNull(), "numeric_parse_fail"),
                      (val.isNull(), "missing_metric"),
                      ((val < 0) | ((val > 100) if metric.endswith("pct") else F.lit(False)), "outside_expected_range"))
            frames.append(valid.select(*META, F.lit(matched[0]["entity_id"]).alias("entity_id"),
                F.lit(metric).alias("metric_name"), val.alias("numeric_value"), col(original).alias("raw_value"),
                F.lit("percent" if metric.endswith("pct") else "bytes").alias("unit"),
                F.lit("host").alias("scope"), q.alias("quality_flags")))
        return "host_metric", union(frames), targets, quarantine, valid
    frames = []
    for metric, kind in SENSORS[source].items():
        value = clean(metric)
        num = number(metric) if kind == "number" else F.lit(None).cast("double")
        boolean = F.when(F.lower(value).isin("true", "1"), F.lit(True)).when(F.lower(value).isin("false", "0"), F.lit(False)) if kind == "boolean" else F.lit(None).cast("boolean")
        category = F.lower(value) if kind == "category" else F.lit(None).cast("string")
        parse_fail = value.isNotNull() & (num.isNull() if kind == "number" else boolean.isNull() if kind == "boolean" else F.lit(False))
        bad_range = (num < 0) if metric == "pressure" else (~num.between(-90, 90)) if metric == "latitude" else (~num.between(-180, 180)) if metric == "longitude" else F.lit(False)
        q = flags((parse_fail, "value_parse_fail"), (value.isNull(), "missing_metric"),
                  (bad_range, "semantic_range_review_required"))
        frames.append(valid.select(*META, F.lit(source).alias("sensor_stream_id"),
             F.lit(metric).alias("metric_name"), num.alias("numeric_value"), category.alias("categorical_value"),
             boolean.alias("boolean_value"), col(metric).alias("raw_value"), F.lit("unknown").alias("unit"), q.alias("quality_flags")))
    return "sensor_observation", union(frames), targets, quarantine, valid


def add_window(df, seconds):
    return df.withColumn("window_start", (F.floor(col("event_time_utc").cast("double") / seconds) * seconds).cast("long"))


def host_edges(network, entities, seconds):
    dim = network.sparkSession.createDataFrame([(h["entity_id"], h["ip"]) for h in entities["hosts"]], "entity_id string, ip string")
    # array_distinct ensures a self-flow creates one edge per host.
    endpoints = network.withColumn("ip", F.explode(F.array_distinct(F.array("src_ip", "dst_ip"))))
    e = add_window(endpoints.join(F.broadcast(dim), "ip", "inner"), seconds)
    return (e.withColumn("remote_ip", F.when(col("src_ip") == col("ip"), col("dst_ip")).otherwise(col("src_ip")))
        .withColumn("sent_bytes", F.when(col("src_ip") == col("ip"), col("src_bytes")).otherwise(col("dst_bytes")))
        .withColumn("received_bytes", F.when(col("src_ip") == col("ip"), col("dst_bytes")).otherwise(col("src_bytes"))))


def aggregate_network(edges):
    return edges.groupBy("entity_id", "window_start").agg(
        F.count("*").alias("n_network"), F.countDistinct("remote_ip").alias("unique_remote_ips"),
        F.countDistinct(F.when(col("dst_port").between(0, 65535), col("dst_port"))).alias("unique_dst_ports"),
        F.sum(F.when(col("sent_bytes") >= 0, col("sent_bytes"))).alias("flow_start_sent_bytes"),
        F.sum(F.when(col("received_bytes") >= 0, col("received_bytes"))).alias("flow_start_received_bytes"),
        F.sum(F.when(col("quality_flags").getItem(0).isNotNull(), 1).otherwise(0)).alias("network_quality_rows"))


def aggregate_hosts(host, seconds):
    h = add_window(host, seconds)
    def metric(name):
        return F.when(col("metric_name") == name, col("numeric_value"))
    return h.groupBy("entity_id", "window_start").agg(
        F.countDistinct("event_id").alias("n_os"),
        F.avg(metric("host_cpu_pct")).alias("host_cpu_mean"), F.max(metric("host_cpu_pct")).alias("host_cpu_max"),
        F.count(metric("host_cpu_pct")).alias("n_cpu_valid"),
        F.min(metric("memory_available_bytes")).alias("memory_available_min"),
        F.avg(metric("memory_committed_pct")).alias("memory_committed_mean"))


def build(lake, entity_path, time_path, window_seconds=60, master="local[2]", selected_ids=None,
          expected_files=None, strict=False):
    if window_seconds not in (30, 60, 120):
        raise ValueError("Supported non-overlapping window sizes: 30, 60, 120")
    lake = Path(lake).resolve()
    inputs = select_inputs(lake, selected_ids)
    entities, rules = read_json(entity_path), read_json(time_path)
    expected = expected_files or ([f"Network_dataset_{i}.csv" for i in range(1, 24)] +
        ["windows7_dataset.csv", "windows10_dataset.csv", "IoT_Weather.csv", "IoT_Modbus.csv", "IoT_Garage_Door.csv"])
    missing = sorted(set(expected) - {m["file_name"] for _, m in inputs})
    if strict and missing:
        raise ValueError(f"Strict snapshot missing files: {missing}")
    if len({h["ip"] for h in entities["hosts"]}) != len(entities["hosts"]):
        raise ValueError("Ambiguous IP mapping")
    code_hash = digest({p.name: sha256(p) for p in Path(__file__).parent.glob("*.py")})
    spec = {"input_manifest_ids": sorted(m["source_file_id"] for _, m in inputs), "entities": entities,
            "time_rules": rules, "window_seconds": window_seconds, "code_hash": code_hash,
            "expected_files": sorted(expected)}
    snapshot_id = digest(spec)[:24]
    dest = lake / "snapshots" / snapshot_id
    if (dest / "snapshot.json").exists():
        write_json(lake / "latest.json", {"snapshot_id": snapshot_id})
        return read_json(dest / "snapshot.json")
    stage = lake / ".staging" / ("snapshot-" + uuid.uuid4().hex)
    stage.mkdir(parents=True)
    start = time.perf_counter()
    spark = (SparkSession.builder.appName("IT5427 TON IoT Integration").master(master)
             .config("spark.sql.session.timeZone", "UTC").config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.shuffle.partitions", "4")
             .config("spark.sql.adaptive.enabled", "true").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    tables, targets, rejected, accounting = {}, [], [], []
    try:
        for folder, manifest in inputs:
            df = spark.read.parquet(str(folder / "records.parquet"))
            name, silver, labels, quarantine, valid = normalize(df, manifest, rules, entities)
            accepted, quarantined = valid.count(), quarantine.count()
            if accepted + quarantined != manifest["input_rows"]:
                raise ValueError("Source-row accounting failed")
            accounting.append({"source_file_id": manifest["source_file_id"], "file_name": manifest["file_name"],
                               "input_rows": manifest["input_rows"], "accepted_source_rows": accepted,
                               "quarantined_source_rows": quarantined})
            tables.setdefault(name, []).append(silver)
            targets.append(labels)
            rejected.append(quarantine)
        tables = {name: union(frames) for name, frames in tables.items()}
        for name, df in tables.items():
            path = str(stage / "silver" / name)
            df.write.mode("error").partitionBy("event_date").parquet(path)
            # An entirely quarantined source may write no data part when partitioned.
            tables[name] = spark.read.schema(df.schema).parquet(path)
        union(targets).write.parquet(str(stage / "evaluation" / "record_labels"))
        union(rejected).write.parquet(str(stage / "quality" / "quarantine"))
        net = tables.get("network_flow")
        host = tables.get("host_metric")
        net_windows, host_windows, edges = None, None, None
        if net is not None:
            edges = host_edges(net, entities, window_seconds).cache()
            net_windows = aggregate_network(edges)
            edges.select("entity_id", "window_start", "event_id", "source_file_id", "source_row_ref").write.parquet(str(stage / "lineage" / "network_host_edges"))
            add_window(net, window_seconds).groupBy("window_start").agg(F.count("*").alias("flow_count")).write.parquet(str(stage / "gold" / "network_overview"))
        if host is not None:
            host_windows = aggregate_hosts(host, window_seconds)
            add_window(host, window_seconds).select("entity_id", "window_start", "event_id", "source_file_id", "source_row_ref").distinct().write.parquet(str(stage / "lineage" / "os_host_edges"))
        gold = None
        if net_windows is not None and host_windows is not None:
            gold = net_windows.join(host_windows, ["entity_id", "window_start"], "full")
        else:
            gold = net_windows if net_windows is not None else host_windows
        coverage = []
        if gold is not None:
            for c in ("n_network", "n_os", "n_cpu_valid"):
                if c not in gold.columns:
                    gold = gold.withColumn(c, F.lit(None).cast("long"))
            gold = (gold.withColumn("has_network", F.coalesce(col("n_network") > 0, F.lit(False)))
                    .withColumn("has_os", F.coalesce(col("n_os") > 0, F.lit(False)))
                    .withColumn("has_cpu", F.coalesce(col("n_cpu_valid") > 0, F.lit(False)))
                    .withColumn("window_seconds", F.lit(window_seconds))
                    .withColumn("snapshot_id", F.lit(snapshot_id))
                    .withColumn("mapping_version", F.lit(entities["version"])))
            coverage = [r.asDict() for r in gold.groupBy("entity_id", "has_network", "has_os", "has_cpu").count().collect()]
            (stage / "explain_host_windows.txt").write_text(gold._jdf.queryExecution().toString(), encoding="utf-8")
            gold.write.parquet(str(stage / "gold" / "host_window_features"))
        sensor = tables.get("sensor_observation")
        if sensor is not None:
            # State ordering within the same second is unknown. Do not invent transitions.
            add_window(sensor, window_seconds).groupBy("sensor_stream_id", "metric_name", "window_start").agg(
                F.count("*").alias("observed_count"), F.count("numeric_value").alias("numeric_count"),
                F.avg("numeric_value").alias("mean"), F.min("numeric_value").alias("min"),
                F.max("numeric_value").alias("max"), F.stddev_samp("numeric_value").alias("stddev"),
                F.collect_set("categorical_value").alias("states_seen"),
                F.collect_set("boolean_value").alias("boolean_values_seen")
            ).write.parquet(str(stage / "gold" / "sensor_window_features"))
        if edges is not None:
            edges.unpersist()
        metadata = {"snapshot_id": snapshot_id, "status": "committed", "created_at": now(),
                    "spec": spec, "input_file_names": [m["file_name"] for _, m in inputs],
                    "missing_expected_files": missing, "input_scope_complete": not missing,
                    "scope_note": "Completeness of file ingestion is distinct from per-window observation coverage.",
                    "source_accounting": accounting, "host_coverage": coverage,
                    "elapsed_seconds": time.perf_counter() - start, "execution_master": master,
                    "detector_status": "not_implemented_in_milestone_1"}
        write_json(stage / "snapshot.json", metadata)
        dest.parent.mkdir(parents=True, exist_ok=True)
        stage.rename(dest)
        write_json(lake / "latest.json", {"snapshot_id": snapshot_id})
        return metadata
    except Exception:
        shutil.rmtree(stage, ignore_errors=True)
        raise
    finally:
        spark.stop()
