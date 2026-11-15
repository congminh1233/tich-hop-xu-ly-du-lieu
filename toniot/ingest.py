"""Bounded-memory CSV ingest. Bronze bytes + stable logical-record index + profile.

Single writer per lake in v0.1. CSV syntax/width errors fail the file transaction;
type/time errors are accounted for by the Silver job. No silent bad-line skipping.
"""
import csv
import shutil
import time
import uuid
from collections import Counter
from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
import pyarrow.parquet as pq

from .common import digest, now, sha256, source_id, validate_header, write_json, read_json


def ingest_file(path, lake, origin="user_local_processed_csv"):
    path, lake = Path(path), Path(lake)
    source = source_id(path.name)
    checksum = sha256(path)
    file_id = digest([source, checksum])
    dest = lake / "bronze" / source / file_id
    if (dest / "manifest.json").exists():
        manifest = read_json(dest / "manifest.json")
        if manifest["status"] == "committed":
            return dict(manifest, ingest_action="already_committed")
    start = time.perf_counter()
    stage = lake / ".staging" / uuid.uuid4().hex
    stage.mkdir(parents=True)
    try:
        raw = stage / path.name
        shutil.copyfile(path, raw)
        if sha256(raw) != checksum:
            raise ValueError("Source changed during copy; no commit")
        with raw.open(encoding="utf-8-sig", newline="") as f:
            columns = next(csv.reader(f))
        validate_header(source, columns)
        profile = {c: {"empty": 0, "whitespace_only": 0, "trim_changes": 0, "dash": 0}
                   for c in columns}
        labels, types = Counter(), Counter()
        n, min_ts, max_ts, backward, previous = 0, None, None, 0, None
        # Close the writer, Arrow reader AND the underlying file before commit
        # or cleanup. Windows can refuse to rename a directory with open files.
        with ExitStack() as resources:
            raw_stream = resources.enter_context(raw.open("rb"))
            reader = pacsv.open_csv(raw_stream,
                read_options=pacsv.ReadOptions(block_size=8 * 1024 * 1024, use_threads=False),
                parse_options=pacsv.ParseOptions(newlines_in_values=True, ignore_empty_lines=False),
                convert_options=pacsv.ConvertOptions(column_types={c: pa.string() for c in columns},
                                                     strings_can_be_null=False, null_values=[]))
            resources.callback(reader.close)
            writer = None
            for batch in reader:
                table = pa.Table.from_batches([batch])
                for c in columns:
                    a = table[c]
                    trim = pc.utf8_trim_whitespace(a)
                    def count(mask):
                        return pc.sum(pc.cast(mask, pa.int64())).as_py() or 0
                    profile[c]["empty"] += count(pc.equal(a, ""))
                    profile[c]["whitespace_only"] += count(pc.and_(pc.equal(trim, ""), pc.not_equal(a, "")))
                    profile[c]["trim_changes"] += count(pc.not_equal(trim, a))
                    profile[c]["dash"] += count(pc.equal(trim, "-"))
                for c, target in (("label", labels), ("type", types)):
                    target.update({v["values"]: v["counts"] for v in pc.value_counts(table[c]).to_pylist()})
                if "ts" in columns:
                    for raw_ts in table["ts"].to_pylist():
                        try:
                            t = float(raw_ts)
                            if not (0 <= t < 4102444800):
                                continue
                        except (TypeError, ValueError):
                            continue
                        min_ts = t if min_ts is None else min(min_ts, t)
                        max_ts = t if max_ts is None else max(max_ts, t)
                        backward += int(previous is not None and t < previous)
                        previous = t
                count_rows = table.num_rows
                table = table.append_column("_it_row_ref", pa.array(range(n + 1, n + count_rows + 1), pa.int64()))
                table = table.append_column("_it_file_id", pa.array([file_id] * count_rows))
                table = table.append_column("_it_source_id", pa.array([source] * count_rows))
                if writer is None:
                    writer = pq.ParquetWriter(stage / "records.parquet", table.schema, compression="snappy")
                    resources.callback(writer.close)
                writer.write_table(table)
                n += count_rows
            if n == 0:
                raise ValueError("Empty source: no data records")
        manifest = {"status": "committed", "manifest_version": "v1", "source_id": source,
                    "source_file_id": file_id, "file_name": path.name, "sha256": checksum,
                    "size_bytes": raw.stat().st_size, "input_rows": n,
                    "columns": columns, "schema_fingerprint": digest(columns), "profile": profile,
                    "labels": dict(labels), "attack_types": dict(types), "min_ts": min_ts,
                    "max_ts": max_ts, "out_of_order_adjacent_pairs": backward,
                    "origin": origin, "committed_at": now(),
                    "ingest_seconds": time.perf_counter() - start,
                    "row_ref_semantics": "1-based CSV logical record excluding header; not physical line"}
        write_json(stage / "manifest.json", manifest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        stage.rename(dest)
        return dict(manifest, ingest_action="committed")
    except Exception as exc:
        write_json(lake / "failed" / f"{file_id}-{uuid.uuid4().hex}.json",
                   {"status": "failed", "file_name": path.name, "sha256": checksum, "error": str(exc)})
        shutil.rmtree(stage, ignore_errors=True)
        raise


def manifests(lake):
    return [(p.parent, read_json(p)) for p in sorted((Path(lake) / "bronze").glob("*/*/manifest.json"))]


def select_inputs(lake, selected_ids=None):
    inputs = manifests(lake)
    if selected_ids is not None:
        ids = set(selected_ids)
        inputs = [(p, m) for p, m in inputs if m["source_file_id"] in ids]
        if {m["source_file_id"] for _, m in inputs} != ids:
            raise ValueError("Selected manifest IDs do not all exist")
    seen = {}
    for _, m in inputs:
        key = (m["source_id"], m["file_name"])
        if key in seen:
            raise ValueError(f"Multiple versions of {key}; use --manifest-selection JSON list of file IDs")
        seen[key] = m["sha256"]
    if not inputs:
        raise ValueError("No committed inputs")
    return inputs
