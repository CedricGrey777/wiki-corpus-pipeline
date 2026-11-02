"""Aggregate-only comparison of two immutable READY releases, without Spark."""

from __future__ import annotations

from pathlib import Path

import duckdb

from corpus_pipeline.core import parquet_files, read_ready


def _source_keys(identity: dict, fields: tuple[str, ...]) -> list[tuple]:
    return sorted({tuple(batch[field] for field in fields) for batch in identity["batches"]})


def compare_releases(root, left_release_id: str, right_release_id: str) -> dict:
    """Compare published representatives, not source article revision history.

    Text and split changes are measured only for IDs present in both releases.
    Source equality is a comparison of stored manifest metadata, not a fresh
    checksum of the original input files. No article text or individual IDs are
    returned, and Parquet files are only read.
    """
    left = read_ready(root, left_release_id)
    right = read_ready(root, right_release_id)
    with duckdb.connect(config={"memory_limit": "1GB", "threads": 2}) as connection:
        for name, manifest in (("left_docs", left), ("right_docs", right)):
            connection.read_parquet(
                parquet_files(Path(manifest["directory"]) / "documents")
            ).project("doc_id, clean_text, split").create_view(name)
        row = connection.execute("""
            SELECT
                count(*) FILTER (WHERE l.doc_id IS NOT NULL),
                count(*) FILTER (WHERE r.doc_id IS NOT NULL),
                count(*) FILTER (WHERE l.doc_id IS NOT NULL AND r.doc_id IS NOT NULL),
                count(*) FILTER (WHERE l.doc_id IS NOT NULL AND r.doc_id IS NULL),
                count(*) FILTER (WHERE l.doc_id IS NULL AND r.doc_id IS NOT NULL),
                count(*) FILTER (WHERE l.doc_id IS NOT NULL AND r.doc_id IS NOT NULL
                                  AND l.clean_text IS DISTINCT FROM r.clean_text),
                count(*) FILTER (WHERE l.split='train' AND r.split='train'),
                count(*) FILTER (WHERE l.split='train' AND r.split='validation'),
                count(*) FILTER (WHERE l.split='validation' AND r.split='train'),
                count(*) FILTER (WHERE l.split='validation' AND r.split='validation')
            FROM left_docs l FULL OUTER JOIN right_docs r USING (doc_id)
        """).fetchone()

    keys = ("left_count", "right_count", "common_count", "removed_count",
            "added_count", "common_text_changed_count")
    documents = dict(zip(keys, row[:6]))
    documents["common_text_unchanged_count"] = documents["common_count"] - documents["common_text_changed_count"]
    transitions = dict(zip(("train_to_train", "train_to_validation",
                            "validation_to_train", "validation_to_validation"), row[6:]))
    documents["common_split_changed_count"] = transitions["train_to_validation"] + transitions["validation_to_train"]

    metric_names = sorted({key for manifest in (left, right)
                           for key, value in manifest["report"].items() if type(value) is int})
    metric_deltas = {}
    for metric in metric_names:
        left_value = left["report"].get(metric)
        right_value = right["report"].get(metric)
        metric_deltas[metric] = {"left": left_value, "right": right_value,
                                 "delta": right_value - left_value if left_value is not None and right_value is not None else None}

    source_revision_fields = ("source_kind", "revision", "snapshot")
    source_input_fields = (*source_revision_fields, "source_file", "source_sha256", "scope")
    return {
        "left": {"release_id": left["release_id"], "identity": left["identity"]},
        "right": {"release_id": right["release_id"], "identity": right["identity"]},
        "source_comparison": {
            "same_source_revisions": _source_keys(left["identity"], source_revision_fields) == _source_keys(right["identity"], source_revision_fields),
            "same_source_inputs": _source_keys(left["identity"], source_input_fields) == _source_keys(right["identity"], source_input_fields),
            "basis": "Stored manifest source kind, revision, snapshot, file SHA-256 and scope; raw files are not rehashed.",
        },
        "documents": documents,
        "common_split_transitions": transitions,
        "reported_metric_deltas": metric_deltas,
        "missing_metric_policy": "Missing metrics are null, not zero. A delta is computed only when both releases measured the metric.",
        "interpretation": "Added/removed IDs describe published representative sets, not source article deletion history. Text changes compare clean_text for common doc_id values. Split migrations count matched articles, not differences between split totals.",
    }
