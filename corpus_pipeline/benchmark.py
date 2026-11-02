"""Six fresh Linux worker processes, real end-to-end runs and sampled process-tree RSS."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

from .core import new_stage, read_json, root_path, write_json


def process_tree_rss(pid: int) -> int:
    processes = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            values = dict(line.split(":", 1) for line in (entry / "status").read_text().splitlines() if ":" in line)
            processes[int(entry.name)] = (int(values["PPid"].strip()), int(values.get("VmRSS", "0 kB").split()[0]) * 1024)
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    children = {pid}
    while True:
        expanded = children | {child for child, (parent, _) in processes.items() if parent in children}
        if expanded == children:
            break
        children = expanded
    return sum(processes.get(child, (0, 0))[1] for child in children)


def benchmark(root, input_file, source_kind="wikimedia") -> dict:
    if not sys.platform.startswith("linux"):
        raise ValueError("Run the benchmark in the project's Linux Docker image to measure process-tree RSS")
    directory = new_stage(root_path(root), "benchmark")
    runs = []
    for master in ("local[1]", "local[2]"):
        for iteration in range(1, 4):
            run = directory / f"workers-{master[6]}-run-{iteration}"
            run.mkdir()
            command = [sys.executable, "-m", "corpus_pipeline.benchmark", "--worker", "--root", str(run),
                       "--input", str(Path(input_file).resolve()), "--source-kind", source_kind, "--master", master]
            started = time.perf_counter()
            with (run / "worker.log").open("w", encoding="utf-8") as log:
                process = subprocess.Popen(command, stdout=log, stderr=log)
                peak = 0
                while process.poll() is None:
                    peak = max(peak, process_tree_rss(process.pid))
                    time.sleep(0.2)
            if process.returncode:
                raise RuntimeError(f"Benchmark worker failed; inspect {run / 'worker.log'}")
            report = read_json(run / "result.json")
            runs.append({"master": master, "iteration": iteration, "wall_seconds": time.perf_counter() - started,
                         "sampled_process_tree_peak_rss_bytes": peak, "raw_count": report["report"]["raw_count"],
                         "accepted_count": report["report"]["accepted_count"]})
    if len({(r["raw_count"], r["accepted_count"]) for r in runs}) != 1:
        raise ValueError("Benchmark runs produced inconsistent counts")
    summary = {master: statistics.median(r["wall_seconds"] for r in runs if r["master"] == master) for master in ("local[1]", "local[2]")}
    result = {"runs": runs, "median_wall_seconds": summary, "source_bytes": Path(input_file).stat().st_size,
              "includes_jvm_startup_and_all_io": True, "cache_control": "OS page cache is not flushed; fresh JVM per run",
              "memory_method": "sum of sampled Linux process-tree RSS every 0.2s; may double-count shared pages and miss short peaks",
              "requested_rows": 100_000, "actual_rows": runs[0]["raw_count"], "output_dir": str(directory)}
    write_json(directory / "benchmark.json", result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker", action="store_true", required=True)
    parser.add_argument("--root", required=True)
    parser.add_argument("--input", required=True)
    parser.add_argument("--source-kind", required=True)
    parser.add_argument("--master", required=True)
    args = parser.parse_args()
    from .pipeline import build_release, ingest_clean, validate_publish
    from .transform import spark_session
    session = spark_session("CorpusBenchmark", master=args.master)
    try:
        batch = ingest_clean(args.root, args.input, scope="100k", source_kind=args.source_kind, spark=session)
        result = validate_publish(build_release(args.root, "benchmark-r001", [batch["batch_id"]], spark=session))
        write_json(Path(args.root) / "result.json", result)
    finally:
        session.stop()


if __name__ == "__main__":
    main()
