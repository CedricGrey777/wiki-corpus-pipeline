import csv
import json

import pytest

from corpus_governance.review import HUMAN_FIELDS, summarize_near_review
from corpus_pipeline.core import read_json, write_json


METADATA = ("release_id", "pair_id", "left_id", "right_id", "left_split", "right_split", "jaccard")


def write_review_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(METADATA) + list(HUMAN_FIELDS))
        writer.writeheader()
        writer.writerows(rows)


@pytest.fixture
def near_queue(tmp_path):
    directory = tmp_path / "audit-synthetic"
    identity = {"audit_id": directory.name, "release_id": "synthetic-ready",
                "source_release_identity": {"recipe_version": "recipe_v2"}}
    splits = [("train", "validation"), ("train", "train"),
              ("validation", "train"), ("validation", "validation")]
    pairs = [{"release_id": "synthetic-ready", "pair_id": f"pair-{index}",
              "left_id": f"private-left-{index}", "right_id": f"private-right-{index}",
              "left_split": left, "right_split": right, "jaccard": 0.9,
              "left_text": f"LOCAL-ONLY-LEFT-BODY-{index}",
              "right_text": f"LOCAL-ONLY-RIGHT-BODY-{index}"}
             for index, (left, right) in enumerate(splits)]
    write_json(directory / "review_context.json", {"audit_identity": identity, "pairs": pairs})
    write_json(directory / "summary.json", {"audit_identity": identity, "candidate_count": 4})
    rows = [{**{field: str(pair[field]) for field in METADATA},
             **dict.fromkeys(HUMAN_FIELDS, "")} for pair in pairs]
    write_review_csv(directory / "manual_review.csv", rows)
    return directory, rows


def test_near_review_without_labels_is_pending_not_a_zero_score(near_queue):
    directory, _ = near_queue
    context_before = (directory / "review_context.json").read_bytes()
    result = summarize_near_review(directory)
    decisions = result["decisions"]
    assert decisions["queue_count"] == decisions["pending_count"] == 4
    assert decisions["reviewed_count"] == decisions["adjudicated_count"] == 0
    assert decisions["positive_fraction_among_adjudicated"] is None
    assert result["cross_split_decisions"]["queue_count"] == 2
    assert result["cross_split_decisions"]["pending_count"] == 2
    assert result["cross_split_decisions"]["positive_fraction_among_adjudicated"] is None
    assert read_json(directory / "review-summary.json") == result
    assert "LOCAL-ONLY" not in json.dumps(result)
    assert (directory / "review_context.json").read_bytes() == context_before


def test_near_review_counts_only_explicit_decisions_and_excludes_uncertain(near_queue):
    directory, rows = near_queue
    for row, label in zip(rows, ("near_duplicate", "distinct", "uncertain")):
        row.update(human_label=label, reviewer="Synthetic reviewer", reviewed_at="2026-11-02T15:30:00+08:00",
                   notes="LOCAL-ONLY-REVIEW-NOTES")
    labels = directory / "supplied-labels.csv"
    write_review_csv(labels, rows)
    result = summarize_near_review(directory, labels)
    decisions = result["decisions"]
    assert decisions["near_duplicate_count"] == decisions["distinct_count"] == decisions["uncertain_count"] == 1
    assert decisions["pending_count"] == 1
    assert decisions["reviewed_count"] == 3
    assert decisions["adjudicated_count"] == 2
    assert decisions["positive_fraction_among_adjudicated"] == 0.5
    cross = result["cross_split_decisions"]
    assert cross["queue_count"] == cross["reviewed_count"] == 2
    assert cross["uncertain_count"] == cross["adjudicated_count"] == 1
    assert cross["positive_fraction_among_adjudicated"] == 1.0
    assert "LOCAL-ONLY" not in json.dumps(result)
    # Importing a separate label file does not fill the original pending template.
    with (directory / "manual_review.csv").open(encoding="utf-8-sig", newline="") as stream:
        assert all(not row["human_label"] for row in csv.DictReader(stream))


@pytest.mark.parametrize(("problem", "message"), [
    ("unknown_id", "Unknown or duplicate review ID"),
    ("missing_row", "retain every queued row"),
    ("duplicate_id", "Unknown or duplicate review ID"),
    ("changed_metadata", "Review metadata changed"),
    ("changed_score", "Review metadata changed: jaccard"),
    ("invalid_label", "Unknown human label"),
    ("missing_reviewer", "require reviewer and reviewed_at"),
    ("missing_time", "require reviewer and reviewed_at"),
    ("invalid_time", "ISO date or datetime"),
])
def test_near_review_rejects_invalid_or_unattributed_labels(near_queue, problem, message):
    directory, rows = near_queue
    rows[0].update(human_label="near_duplicate", reviewer="Synthetic reviewer", reviewed_at="2026-11-02")
    if problem == "unknown_id":
        rows[0]["pair_id"] = "foreign-pair"
    elif problem == "missing_row":
        rows.pop()
    elif problem == "duplicate_id":
        rows.append(dict(rows[0]))
    elif problem == "changed_metadata":
        rows[0]["left_split"] = "validation"
    elif problem == "changed_score":
        rows[0]["jaccard"] = "0.91"
    elif problem == "invalid_label":
        rows[0]["human_label"] = "automatically-approved"
    elif problem == "missing_reviewer":
        rows[0]["reviewer"] = " "
    elif problem == "missing_time":
        rows[0]["reviewed_at"] = ""
    else:
        rows[0]["reviewed_at"] = "not-a-date"
    write_review_csv(directory / "manual_review.csv", rows)
    with pytest.raises(ValueError, match=message):
        summarize_near_review(directory)
    assert not (directory / "review-summary.json").exists()


def test_near_review_accepts_display_rounding_without_changing_source_score(near_queue):
    directory, rows = near_queue
    rows[0]["jaccard"] = "0.9000000000001"
    rows[1]["jaccard"] = "0.9000000000000"
    write_review_csv(directory / "manual_review.csv", rows)
    result = summarize_near_review(directory)
    assert result["decisions"]["pending_count"] == 4
    assert read_json(directory / "review_context.json")["pairs"][0]["jaccard"] == 0.9


def test_near_review_rejects_context_from_another_audit(near_queue):
    directory, _ = near_queue
    summary = read_json(directory / "summary.json")
    summary["audit_identity"]["audit_id"] = "another-audit"
    write_json(directory / "summary.json", summary)
    with pytest.raises(ValueError, match="audit identity differ"):
        summarize_near_review(directory)
