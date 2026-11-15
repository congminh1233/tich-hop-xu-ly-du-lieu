import csv
from collections import Counter
from datetime import datetime, timezone

from .common import read_json, write_json, sha256, digest


def text_time(date, time):
    value = f"{date.strip()} {time.strip()}"
    for fmt in ("%d-%b-%y %H:%M:%S", "%Y-%m-%d %H:%M:%S", "%d-%b-%Y %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc).timestamp()
        except ValueError:
            pass
    raise ValueError(value)


def verify_time(ground_truth, source, rules_path, output):
    if not source.startswith("iot_"):
        raise ValueError("This validator expects one IoT stream at a time")
    offsets, failures = Counter(), 0
    with open(ground_truth, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        fields = set(reader.fieldnames or [])
        if not {"ts", "time"} <= fields or not {"date", "data"} & fields:
            raise ValueError("Ground truth requires ts, date/data, time")
        for row in reader:
            try:
                diff = float(row["ts"]) - text_time(row.get("date", row.get("data", "")), row["time"])
                if abs(diff) > 86400 or not diff.is_integer():
                    raise ValueError("implausible/noninteger offset")
                offsets[int(diff)] += 1
            except (ValueError, TypeError, OverflowError):
                failures += 1
    evidence = {"source_id": source, "ground_truth_sha256": sha256(ground_truth),
                "offset_counts": dict(offsets), "parse_failures": failures}
    if len(offsets) != 1 or sum(offsets.values()) < 10 or failures:
        raise ValueError(f"Cannot automatically approve time rule: {evidence}")
    rules = read_json(rules_path)
    rules["iot"][source] = {"offset_seconds": next(iter(offsets)),
        "evidence": evidence, "status": "verified_on_ground_truth",
        "normal_rows_assumption": "Same encoding within stream; not independently verified on normal rows",
        "timezone": "unknown"}
    rules["version"] = "time-" + digest(rules["iot"])[:16]
    write_json(output, rules)
    return rules["iot"][source]
