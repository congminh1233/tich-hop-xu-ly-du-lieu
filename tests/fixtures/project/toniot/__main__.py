import argparse
import json
from pathlib import Path

from .common import read_json, source_id, write_json


def main():
    parser = argparse.ArgumentParser(description="IT5427 TON_IoT integration milestone 1")
    sub = parser.add_subparsers(dest="command", required=True)
    ingest = sub.add_parser("ingest", help="Checksum, immutable Bronze, stable record index, profile")
    ingest.add_argument("input", type=Path)
    ingest.add_argument("--lake", default="lake")
    ingest.add_argument("--origin", default="user_local_processed_csv")
    build = sub.add_parser("build", help="Spark Silver and Gold snapshot")
    build.add_argument("--lake", default="lake")
    build.add_argument("--entities", default="config/entities.json")
    build.add_argument("--time-rules", default="config/time_rules.json")
    build.add_argument("--window", type=int, default=60, choices=[30, 60, 120])
    build.add_argument("--master", default="local[2]")
    build.add_argument("--manifest-selection", help="JSON array of source_file_id values")
    build.add_argument("--strict", action="store_true", help="Require all expected files")
    build.add_argument("--expected-files", help="JSON array defining this experiment's exact scope")
    catalog = sub.add_parser("catalog")
    catalog.add_argument("--lake", default="lake")
    catalog.add_argument("--output", default="reports/catalog.json")
    time = sub.add_parser("validate-time")
    time.add_argument("ground_truth")
    time.add_argument("--source", required=True)
    time.add_argument("--rules", default="config/time_rules.json")
    time.add_argument("--output", required=True)
    report = sub.add_parser("report", help="Create local HTML overview, without detector claims")
    report.add_argument("--lake", default="lake")
    report.add_argument("--snapshot", help="Default: latest committed snapshot")
    report.add_argument("--output", default="reports/overview.html")
    args = parser.parse_args()
    if args.command == "ingest":
        from .ingest import ingest_file
        paths = sorted(args.input.rglob("*.csv")) if args.input.is_dir() else [args.input]
        count = 0
        for path in paths:
            try:
                source_id(path.name)
            except ValueError:
                if not args.input.is_dir():
                    raise
                print(f"Skipped unsupported file: {path.name}")
                continue
            result = ingest_file(path, args.lake, args.origin)
            count += 1
            print(json.dumps({k: result[k] for k in ("file_name", "input_rows", "source_file_id", "ingest_action")}, ensure_ascii=False))
        if not count:
            raise SystemExit("No supported Processed CSV found")
    elif args.command == "build":
        from .spark_job import build as run
        result = run(args.lake, args.entities, args.time_rules, args.window, args.master,
                     read_json(args.manifest_selection) if args.manifest_selection else None,
                     read_json(args.expected_files) if args.expected_files else None, args.strict)
        print(json.dumps({k: result[k] for k in ("snapshot_id", "status", "input_scope_complete", "source_accounting", "host_coverage")}, ensure_ascii=False, indent=2))
    elif args.command == "catalog":
        from .ingest import manifests
        result = [m for _, m in manifests(args.lake)]
        write_json(args.output, result)
        print(f"Wrote {len(result)} source manifests to {args.output}")
    elif args.command == "validate-time":
        from .timecheck import verify_time
        print(json.dumps(verify_time(args.ground_truth, args.source, args.rules, args.output), indent=2))
    elif args.command == "report":
        from .report import create_report
        print(create_report(args.lake, args.output, args.snapshot))


if __name__ == "__main__":
    main()
