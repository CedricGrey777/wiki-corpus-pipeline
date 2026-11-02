"""Import explicit human decisions without treating pending or uncertain rows as truth."""

from __future__ import annotations

import csv
import math
from datetime import datetime
from pathlib import Path

from corpus_pipeline.core import read_json, write_json

HUMAN_FIELDS = ("human_label", "reviewer", "reviewed_at", "notes")
NEAR_LABELS = ("near_duplicate", "distinct", "uncertain")


def load_labels(path, expected_rows, key, metadata_fields, labels):
    expected = {row[key]: row for row in expected_rows}
    if len(expected) != len(expected_rows):
        raise ValueError("Review queue contains duplicate IDs")
    with Path(path).open(encoding="utf-8-sig", newline="") as stream:
        reader = csv.DictReader(stream)
        fields = list(metadata_fields) + list(HUMAN_FIELDS)
        if reader.fieldnames is None or len(reader.fieldnames) != len(set(reader.fieldnames)) or set(reader.fieldnames) != set(fields):
            raise ValueError("CSV columns must match the exported review template")
        rows = list(reader)
    seen = set()
    for row in rows:
        item_id = row.get(key)
        if item_id not in expected or item_id in seen:
            raise ValueError("Unknown or duplicate review ID")
        seen.add(item_id)
        for field in metadata_fields:
            matches = row[field] == str(expected[item_id][field])
            if field == "jaccard":
                try:
                    matches = math.isclose(float(row[field]), float(expected[item_id][field]), rel_tol=1e-12, abs_tol=1e-12)
                except (ValueError, TypeError):
                    matches = False
            if not matches:
                raise ValueError(f"Review metadata changed: {field}")
        if any(row[field] is None for field in HUMAN_FIELDS) or None in row:
            raise ValueError("Malformed review CSV row")
        row["human_label"] = row["human_label"].strip()
        label = row["human_label"]
        if label and label not in labels:
            raise ValueError(f"Unknown human label: {label}")
        if label:
            if not row["reviewer"].strip() or not row["reviewed_at"].strip():
                raise ValueError("Labeled rows require reviewer and reviewed_at")
            try:
                datetime.fromisoformat(row["reviewed_at"].strip().replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError("reviewed_at must be an ISO date or datetime") from error
    if seen != set(expected):
        raise ValueError("CSV must retain every queued row; leave unfinished labels blank")
    return rows


def decision_counts(rows, positive, negative):
    counts = {label: sum(row["human_label"] == label for row in rows)
              for label in (positive, negative, "uncertain")}
    adjudicated = counts[positive] + counts[negative]
    return {"queue_count": len(rows), "pending_count": sum(not row["human_label"] for row in rows),
            "reviewed_count": sum(counts.values()), "adjudicated_count": adjudicated,
            **{f"{label}_count": count for label, count in counts.items()},
            "positive_fraction_among_adjudicated": counts[positive] / adjudicated if adjudicated else None}


def summarize_near_review(audit_dir, labels_path=None):
    directory = Path(audit_dir)
    context = read_json(directory / "review_context.json")
    summary = read_json(directory / "summary.json")
    if context["audit_identity"] != summary["audit_identity"]:
        raise ValueError("Review context and audit identity differ")
    metadata = ("release_id", "pair_id", "left_id", "right_id", "left_split", "right_split", "jaccard")
    rows = load_labels(labels_path or directory / "manual_review.csv", context["pairs"], "pair_id", metadata, NEAR_LABELS)
    cross = [row for row in rows if row["left_split"] != row["right_split"]]
    report = {"audit_identity": context["audit_identity"], "candidate_count": summary["candidate_count"],
              "decisions": decision_counts(rows, "near_duplicate", "distinct"),
              "cross_split_decisions": decision_counts(cross, "near_duplicate", "distinct"),
              "interpretation": "Only supplied human labels are counted. Candidate-review agreement is not full-corpus recall; uncertain rows are excluded from the fraction."}
    write_json(directory / "review-summary.json", report)
    return report
