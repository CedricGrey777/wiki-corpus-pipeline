"""Small manifests and single-writer metadata, separate from all corpus text."""

from __future__ import annotations

import hashlib
import json
import os
import re
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path

import duckdb

REVISION = "5428a5d9ebb8d68732fc1227ebc5e8d5cec28592"
SNAPSHOT = "20231101.zh"
RECIPE = "recipe_v1"
SHARDS = tuple(f"train-{i:05d}-of-00006.parquet" for i in range(6))
SCOPES = {"full": None, "10k": 10_000, "100k": 100_000}
KEY_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,149}$")


def identifier(value: str) -> str:
    if not KEY_PATTERN.fullmatch(value):
        raise ValueError("Identifiers must be 1-150 letters, digits, dots, hyphens or underscores")
    return value


def canonical(value) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    temporary.write_text(canonical(value) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def implementation_id() -> str:
    """Bind releases to shipped Python source even when the Docker image has no .git."""
    digest = hashlib.sha256()
    for path in sorted(Path(__file__).parent.glob("*.py")):
        digest.update(path.name.encode())
        digest.update(path.read_bytes())
    return digest.hexdigest()


def root_path(root) -> Path:
    path = Path(root).resolve()
    path.mkdir(parents=True, exist_ok=True)
    return path


@contextmanager
def catalog(root):
    path = root_path(root)
    connection = duckdb.connect(str(path / "catalog.duckdb"))
    try:
        connection.execute("""
            CREATE TABLE IF NOT EXISTS batches (
                batch_id VARCHAR PRIMARY KEY, identity_json VARCHAR NOT NULL,
                path VARCHAR NOT NULL, report_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL CHECK (status='CLEANED'));
            CREATE TABLE IF NOT EXISTS releases (
                release_id VARCHAR PRIMARY KEY, identity_json VARCHAR NOT NULL,
                path VARCHAR NOT NULL, report_json VARCHAR NOT NULL,
                status VARCHAR NOT NULL CHECK (status='READY'));
            CREATE TABLE IF NOT EXISTS quality_metrics (
                run_id VARCHAR, metric VARCHAR, value BIGINT,
                PRIMARY KEY(run_id, metric));
        """)
        yield connection
    finally:
        connection.close()


def insert_metrics(connection, run_id: str, report: dict):
    for key, value in report.items():
        if type(value) is int:
            connection.execute("INSERT OR REPLACE INTO quality_metrics VALUES (?,?,?)", [run_id, key, value])


def register_batch(root, manifest: dict, directory: Path) -> None:
    with catalog(root) as connection:
        existing = connection.execute("SELECT identity_json FROM batches WHERE batch_id=?", [manifest["batch_id"]]).fetchone()
        if existing and existing[0] != canonical(manifest["identity"]):
            raise ValueError("Batch ID already refers to different input or implementation")
        connection.execute("BEGIN TRANSACTION")
        connection.execute("INSERT OR REPLACE INTO batches VALUES (?,?,?,?, 'CLEANED')", [manifest["batch_id"], canonical(manifest["identity"]), str(directory), canonical(manifest["report"])])
        insert_metrics(connection, manifest["batch_id"], manifest["report"])
        connection.execute("COMMIT")


def batch_manifests(root, batch_ids: list[str]) -> list[dict]:
    if not batch_ids or len(batch_ids) != len(set(batch_ids)):
        raise ValueError("Provide a non-empty list of distinct batch IDs")
    result = []
    with catalog(root) as connection:
        for batch_id in sorted(batch_ids):
            identifier(batch_id)
            row = connection.execute("SELECT path, identity_json FROM batches WHERE batch_id=? AND status='CLEANED'", [batch_id]).fetchone()
            if not row:
                raise ValueError(f"Batch is not CLEANED: {batch_id}")
            manifest = read_json(Path(row[0]) / "manifest.json")
            if canonical(manifest["identity"]) != row[1]:
                raise ValueError("Batch manifest differs from its catalog entry")
            result.append({**manifest, "directory": row[0]})
    return result


def read_ready(root, release_id: str) -> dict:
    """The only consumer discovery interface: an unregistered directory is not a release."""
    identifier(release_id)
    with catalog(root) as connection:
        row = connection.execute("SELECT path, identity_json FROM releases WHERE release_id=? AND status='READY'", [release_id]).fetchone()
    if not row:
        raise ValueError(f"Release is not READY: {release_id}")
    manifest = read_json(Path(row[0]) / "manifest.json")
    if manifest["release_id"] != release_id or canonical(manifest["identity"]) != row[1]:
        raise ValueError("Release manifest differs from its READY catalog entry")
    return {**manifest, "directory": row[0]}


def new_stage(root: Path, prefix: str) -> Path:
    path = root / "staging" / f"{identifier(prefix)}-{uuid.uuid4().hex}"
    path.mkdir(parents=True)
    return path


def parquet_files(directory: Path) -> list[str]:
    paths = sorted(str(path) for path in directory.glob("*.parquet"))
    if not paths:
        raise ValueError(f"No Parquet parts in expected dataset: {directory.name}")
    return paths


def download_shards(root, shards: list[str]) -> list[str]:
    """Explicit opt-in download; fixed revision and allowlisted files, no corpus redistribution."""
    if not shards or any(name not in SHARDS for name in shards):
        raise ValueError("Select only the six pinned Wikipedia shard names")
    target = root_path(root) / "raw" / REVISION / SNAPSHOT
    target.mkdir(parents=True, exist_ok=True)
    results = []
    for name in shards:
        path = target / name
        if not path.exists():
            temporary = target / (name + ".part")
            url = f"https://huggingface.co/datasets/wikimedia/wikipedia/resolve/{REVISION}/{SNAPSHOT}/{name}"
            with urllib.request.urlopen(url, timeout=60) as response, temporary.open("wb") as output:
                for block in iter(lambda: response.read(1024 * 1024), b""):
                    output.write(block)
            # A failed transfer remains .part; it is never passed to the Parquet reader.
            os.replace(temporary, path)
        results.append(str(path))
    return results
