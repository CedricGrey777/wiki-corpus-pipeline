"""Immutable shard outputs, cumulative exact releases and recoverable READY registration."""

from __future__ import annotations

from pathlib import Path

import duckdb
from pyspark.sql import functions as F

from .core import (RECIPE, REVISION, SCOPES, SHARDS, SNAPSHOT, batch_manifests,
                   canonical, catalog, file_digest, identifier, implementation_id,
                   insert_metrics, new_stage, parquet_files, read_json, read_ready,
                   register_batch, root_path, write_json)
from .transform import clean_frame, deduplicate, spark_session


def ingest_clean(root, input_file, scope="full", source_kind="wikimedia", spark=None, fail_at=None) -> dict:
    root = root_path(root)
    path = Path(input_file).resolve()
    if scope not in SCOPES or source_kind not in ("wikimedia", "synthetic"):
        raise ValueError("Unknown scope or source kind")
    if not path.is_file():
        raise ValueError("Input must be an existing Parquet file")
    if source_kind == "wikimedia" and path.name not in SHARDS:
        raise ValueError("Wikimedia inputs must use one of the six pinned shard names")
    batch_id = identifier(f"{source_kind}-{path.stem}-{scope}-{RECIPE}")
    identity = {"source_kind": source_kind, "revision": REVISION if source_kind == "wikimedia" else "synthetic-v1",
                "snapshot": SNAPSHOT if source_kind == "wikimedia" else "synthetic",
                "source_file": path.name, "source_sha256": file_digest(path), "scope": scope,
                "recipe_version": RECIPE, "implementation_id": implementation_id()}
    destination = root / "cleaned" / batch_id
    if destination.exists():
        manifest = read_json(destination / "manifest.json")
        if manifest["identity"] != identity:
            raise ValueError("Batch ID already refers to different input or implementation")
        parquet_files(destination / "documents")
        parquet_files(destination / "quarantine")
        register_batch(root, manifest, destination)
        return {"batch_id": batch_id, "status": "CLEANED", "reused": True}
    stage = new_stage(root, batch_id)
    owned = spark is None
    session = spark or spark_session()
    try:
        frame = session.read.parquet(str(path))
        # Validate the schema before selecting/orderBy so errors are actionable.
        clean_frame(frame, batch_id)
        if SCOPES[scope] is not None:
            frame = frame.orderBy(*[F.col(c).asc_nulls_first() for c in ("id", "url", "title", "text")]).limit(SCOPES[scope])
        raw_count = frame.count()
        valid, quarantine = clean_frame(frame, batch_id)
        valid.write.mode("error").parquet(str(stage / "documents"))
        quarantine.write.mode("error").parquet(str(stage / "quarantine"))
        valid_count = session.read.parquet(str(stage / "documents")).count()
        quarantine_count = session.read.parquet(str(stage / "quarantine")).count()
        if raw_count != valid_count + quarantine_count:
            raise ValueError("Ingestion row conservation failed")
        report = {"raw_count": raw_count, "valid_count": valid_count, "quarantine_count": quarantine_count}
        manifest = {"batch_id": batch_id, "identity": identity, "report": report}
        write_json(stage / "manifest.json", manifest)
        destination.parent.mkdir(parents=True, exist_ok=True)
        stage.rename(destination)
        if fail_at == "after_batch_rename":
            raise RuntimeError("Injected interruption after batch rename")
        register_batch(root, manifest, destination)
        return {"batch_id": batch_id, "status": "CLEANED", "reused": False}
    finally:
        if owned:
            session.stop()


def release_identity(manifests: list[dict]) -> dict:
    kinds = {(m["identity"]["source_kind"], m["identity"]["revision"], m["identity"]["scope"]) for m in manifests}
    if len(kinds) != 1:
        raise ValueError("One release cannot mix source kinds, source revisions or sample scopes")
    if any(m["identity"]["recipe_version"] != RECIPE or m["identity"]["implementation_id"] != implementation_id() for m in manifests):
        raise ValueError("Re-clean batches after changing the implementation or recipe")
    return {"batches": [{"batch_id": m["batch_id"], **m["identity"]} for m in manifests],
            "recipe_version": RECIPE, "implementation_id": implementation_id(),
            "deduplication": "normalized_text_equality", "split": "sha256_prefix_mod100_lt5_validation"}


def build_release(root, release_id: str, batch_ids: list[str], spark=None) -> dict:
    root = root_path(root)
    identifier(release_id)
    manifests = batch_manifests(root, batch_ids)
    identity = release_identity(manifests)
    destination = root / "releases" / release_id
    # Also recover an orphaned final directory, which is NOT consumer-visible yet.
    if destination.exists():
        previous = read_json(destination / "manifest.json")
        if previous["release_id"] != release_id or previous["identity"] != identity:
            raise ValueError("Release ID already refers to different input or implementation")
        return {"root": str(root), "release_id": release_id, "run_dir": str(destination), "identity": identity}
    stage = new_stage(root, release_id)
    owned = spark is None
    session = spark or spark_session()
    try:
        files = [file for m in manifests for file in parquet_files(Path(m["directory"]) / "documents")]
        frame = session.read.parquet(*files)
        unique_sources, accepted, duplicates = deduplicate(frame)
        accepted.write.mode("error").parquet(str(stage / "documents"))
        duplicates.write.mode("error").parquet(str(stage / "duplicates"))
        accepted_count = session.read.parquet(str(stage / "documents")).count()
        duplicate_count = session.read.parquet(str(stage / "duplicates")).count()
        source_unique_count = unique_sources.count()
        report = {key: sum(m["report"][key] for m in manifests) for key in ("raw_count", "valid_count", "quarantine_count")}
        report.update({"source_unique_count": source_unique_count,
                       "source_duplicate_count": report["valid_count"] - source_unique_count,
                       "exact_duplicate_count": duplicate_count, "accepted_count": accepted_count})
        write_json(stage / "manifest.json", {"release_id": release_id, "identity": identity, "report": report})
        return {"root": str(root), "release_id": release_id, "run_dir": str(stage), "identity": identity}
    finally:
        if owned:
            session.stop()


def validate_directory(directory: Path) -> dict:
    manifest = read_json(directory / "manifest.json")
    report = dict(manifest["report"])
    with duckdb.connect() as connection:
        connection.read_parquet(parquet_files(directory / "documents")).create_view("docs")
        connection.read_parquet(parquet_files(directory / "duplicates")).create_view("duplicates")
        actual = connection.execute("SELECT count(*), count(DISTINCT doc_id) FROM docs").fetchone()
        if actual[0] != report["accepted_count"] or actual[0] != actual[1] or actual[0] == 0:
            raise ValueError("Published documents must be nonempty with unique IDs and matching row counts")
        if connection.execute("SELECT count(*) FROM (SELECT clean_text FROM docs GROUP BY clean_text HAVING count(*)>1)").fetchone()[0]:
            raise ValueError("Exact duplicate text survived deduplication")
        if connection.execute("SELECT count(*) FROM docs WHERE clean_text IS NULL OR length(trim(clean_text))=0 OR content_hash IS NULL OR content_hash<>sha256(clean_text)").fetchone()[0]:
            raise ValueError("Invalid content or content hash")
        invalid_split = connection.execute("""SELECT count(*) FROM docs
            WHERE split IS NULL OR split <> CASE WHEN ('0x' || substr(content_hash,1,8))::UBIGINT % 100 < 5
            THEN 'validation' ELSE 'train' END""").fetchone()[0]
        if invalid_split:
            raise ValueError("Content-hash split assignment mismatch")
        duplicate_count = connection.execute("SELECT count(*) FROM duplicates").fetchone()[0]
        if duplicate_count != report["exact_duplicate_count"]:
            raise ValueError("Duplicate mapping row count mismatch")
        if connection.execute("SELECT count(*) FROM (SELECT doc_id FROM duplicates GROUP BY doc_id HAVING count(*)>1)").fetchone()[0]:
            raise ValueError("Duplicate mapping has repeated document IDs")
        if connection.execute("""SELECT count(*) FROM duplicates d LEFT JOIN docs a ON d.representative_doc_id=a.doc_id
                WHERE a.doc_id IS NULL OR d.content_hash<>a.content_hash OR d.doc_id=d.representative_doc_id""").fetchone()[0]:
            raise ValueError("Invalid representative in duplicate mapping")
        if connection.execute("SELECT count(*) FROM duplicates d JOIN docs a ON d.doc_id=a.doc_id").fetchone()[0]:
            raise ValueError("Removed duplicate still appears in the release")
        report["train_count"] = connection.execute("SELECT count(*) FROM docs WHERE split='train'").fetchone()[0]
        report["validation_count"] = actual[0] - report["train_count"]
        report["short_text_count"] = connection.execute("SELECT count(*) FROM docs WHERE contains(quality_flags,'short_text')").fetchone()[0]
        report["long_text_count"] = connection.execute("SELECT count(*) FROM docs WHERE contains(quality_flags,'long_text')").fetchone()[0]
        report["repeated_lines_count"] = connection.execute("SELECT count(*) FROM docs WHERE repeated_line_ratio>0.5").fetchone()[0]
    equations = [report["raw_count"] == report["valid_count"] + report["quarantine_count"],
                 report["valid_count"] == report["source_unique_count"] + report["source_duplicate_count"],
                 report["source_unique_count"] == report["exact_duplicate_count"] + report["accepted_count"],
                 report["accepted_count"] == report["train_count"] + report["validation_count"]]
    if not all(equations) or any(value < 0 for value in report.values()):
        raise ValueError("Release row conservation failed")
    return report


def validate_publish(context: dict, fail_at=None) -> dict:
    root = root_path(context["root"])
    release_id = identifier(context["release_id"])
    stage = Path(context["run_dir"]).resolve()
    destination = root / "releases" / release_id
    if stage.parent not in (root / "staging", root / "releases") or (stage.parent == root / "releases" and stage != destination):
        raise ValueError("Publish context must point at this project's staging or matching release directory")
    # Rename may have completed before the catalog commit or before acknowledgement.
    if not stage.exists() and destination.exists():
        stage = destination
    manifest = read_json(stage / "manifest.json")
    if manifest["release_id"] != release_id:
        raise ValueError("Release context and manifest disagree")
    if manifest["identity"] != context["identity"]:
        raise ValueError("Release context refers to different input or implementation")
    report = validate_directory(stage)
    if stage != destination:
        manifest = {**manifest, "report": report}
        write_json(stage / "manifest.json", manifest)
    if fail_at == "before_rename":
        raise RuntimeError("Injected interruption before release rename")
    if stage != destination:
        destination.parent.mkdir(parents=True, exist_ok=True)
        if destination.exists():
            previous = read_json(destination / "manifest.json")
            if previous["identity"] != manifest["identity"]:
                raise ValueError("Release ID already refers to different content")
            validate_directory(destination)
        else:
            stage.rename(destination)
    if fail_at == "after_rename":
        raise RuntimeError("Injected interruption after release rename, before READY registration")
    final = read_json(destination / "manifest.json")
    if final["identity"] != manifest["identity"]:
        raise ValueError("Cannot register a different immutable manifest")
    with catalog(root) as connection:
        existing = connection.execute("SELECT identity_json FROM releases WHERE release_id=?", [release_id]).fetchone()
        if existing and existing[0] != canonical(manifest["identity"]):
            raise ValueError("READY ID already refers to a different manifest")
        connection.execute("BEGIN TRANSACTION")
        connection.execute("INSERT OR REPLACE INTO releases VALUES (?,?,?,?, 'READY')", [release_id, canonical(manifest["identity"]), str(destination), canonical(report)])
        insert_metrics(connection, release_id, report)
        if fail_at == "before_commit":
            connection.execute("ROLLBACK")
            raise RuntimeError("Injected interruption before READY commit")
        connection.execute("COMMIT")
    if fail_at == "after_commit":
        raise RuntimeError("Injected acknowledgement loss after successful READY commit")
    return {"release_id": release_id, "status": "READY", "report": report, "directory": str(destination)}
