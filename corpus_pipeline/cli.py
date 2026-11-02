from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .core import SHARDS, identifier, read_json, read_ready, root_path, write_json, download_shards
from .fixtures import create_fixtures
from .pipeline import build_release, ingest_clean, validate_publish
from .transform import spark_session


def main(argv=None):
    parser = argparse.ArgumentParser(description="Local corpus processing; all text remains in the project data volume")
    parser.add_argument("--root", default=os.environ.get("CORPUS_DATA_ROOT", "data"))
    commands = parser.add_subparsers(dest="command", required=True)
    download = commands.add_parser("download", help="Explicitly download allowlisted pinned public shards")
    download.add_argument("--shards", nargs="+", choices=SHARDS, default=[SHARDS[2]])
    fixture = commands.add_parser("make-fixtures")
    fixture.add_argument("--output")
    ingest = commands.add_parser("ingest-clean")
    ingest.add_argument("--input", required=True)
    ingest.add_argument("--source-kind", choices=["wikimedia", "synthetic"], default="wikimedia")
    ingest.add_argument("--scope", choices=["full", "10k", "100k"], default="full")
    build = commands.add_parser("build-release")
    build.add_argument("--release-id", required=True)
    build.add_argument("--batches", nargs="+", required=True)
    publish = commands.add_parser("validate-publish")
    publish.add_argument("--release-id", required=True)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("--release-id", required=True)
    audit = commands.add_parser("audit-near-duplicates")
    audit.add_argument("--release-id", required=True)
    demo = commands.add_parser("run-demo")
    demo.add_argument("--release-id", default="synthetic-cli-v2")
    benchmark = commands.add_parser("benchmark")
    benchmark.add_argument("--input", required=True)
    benchmark.add_argument("--source-kind", choices=["wikimedia", "synthetic"], default="wikimedia")
    args = parser.parse_args(argv)
    root = root_path(args.root)
    if args.command == "download":
        result = {"files": download_shards(root, args.shards)}
    elif args.command == "make-fixtures":
        result = {"files": create_fixtures(args.output or root / "fixtures")}
    elif args.command == "ingest-clean":
        result = ingest_clean(root, args.input, args.scope, args.source_kind)
    elif args.command == "build-release":
        result = build_release(root, args.release_id, args.batches)
        write_json(root / "contexts" / (identifier(args.release_id) + ".json"), result)
    elif args.command == "validate-publish":
        context = read_json(root / "contexts" / (identifier(args.release_id) + ".json"))
        result = validate_publish(context)
    elif args.command == "inspect":
        result = read_ready(root, args.release_id)
    elif args.command == "audit-near-duplicates":
        from .audit import audit_near_duplicates
        result = audit_near_duplicates(root, args.release_id)
    elif args.command == "benchmark":
        from .benchmark import benchmark
        result = benchmark(root, args.input, args.source_kind)
    else:
        files = create_fixtures(root / "fixtures")
        session = spark_session()
        try:
            batches = [ingest_clean(root, path, source_kind="synthetic", spark=session)["batch_id"] for path in files]
            context = build_release(root, args.release_id, batches, spark=session)
            result = validate_publish(context)
        finally:
            session.stop()
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return result


if __name__ == "__main__":
    main()
