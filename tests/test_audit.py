import csv
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from corpus_governance.audit import (REVIEW_FIELDS, audit_near_duplicates, candidate_counts,
                                     candidate_pairs, review_pair_id, synthetic_pairs)
from corpus_pipeline.core import canonical, catalog, read_json, write_json
from corpus_pipeline.transform import spark_session


pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = spark_session("CorpusAuditTests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def audit_documents(spark):
    pairs = [pair for pair in synthetic_pairs() if pair["expected_near"]][:3]
    rows = []
    for name, pair, splits in zip(
        ("cross", "training", "validation"), pairs,
        (("train", "validation"), ("train", "train"), ("validation", "validation")),
    ):
        rows.extend([(f"{name}-a", pair["left"], splits[0]),
                     (f"{name}-b", pair["right"], splits[1])])
    return spark.createDataFrame(rows, "doc_id string, clean_text string, split string")


@pytest.fixture(scope="module")
def known_candidates(audit_documents):
    candidates = candidate_pairs(audit_documents).cache()
    candidates.count()
    yield candidates
    candidates.unpersist()


def test_real_spark_candidates_preserve_split_and_text(known_candidates):
    found = {(row.left_id, row.right_id): row for row in known_candidates.collect()}
    assert set(found) == {(f"{name}-a", f"{name}-b") for name in ("cross", "training", "validation")}
    cross = found[("cross-a", "cross-b")]
    assert (cross.left_split, cross.right_split, cross.cross_split) == ("train", "validation", True)
    assert cross.left_text != cross.right_text
    assert 0.8 <= cross.jaccard < 1
    assert found[("training-a", "training-b")].cross_split is False
    assert found[("validation-a", "validation-b")].cross_split is False


def test_same_and_cross_split_counts_conserve_candidates(known_candidates):
    counts = candidate_counts(known_candidates)
    assert counts == {"candidate_count": 3, "train_train_count": 1,
                      "validation_validation_count": 1, "cross_split_count": 1}
    assert counts["candidate_count"] == sum(counts[key] for key in (
        "train_train_count", "validation_validation_count", "cross_split_count"))


@pytest.mark.parametrize("split", ["test", None])
def test_unknown_split_is_rejected(spark, split):
    frame = spark.createDataFrame([("one", "合成正文" * 30, split)],
                                  "doc_id string, clean_text string, split string")
    with pytest.raises(ValueError, match="split must be train or validation"):
        candidate_pairs(frame)


def test_split_column_is_required(spark):
    frame = spark.createDataFrame([("one", "合成正文" * 30)], "doc_id string, clean_text string")
    with pytest.raises(ValueError, match="requires string columns"):
        candidate_pairs(frame)


def test_no_pairs_produce_zero_counts(spark):
    frame = spark.createDataFrame([("one", "合成正文" * 30, "train")],
                                  "doc_id string, clean_text string, split string")
    assert candidate_counts(candidate_pairs(frame)) == {
        "candidate_count": 0, "train_train_count": 0,
        "validation_validation_count": 0, "cross_split_count": 0,
    }


def test_pair_identifier_binds_release_and_unambiguous_ids():
    assert review_pair_id("r1", "a", "b") == review_pair_id("r1", "a", "b")
    assert review_pair_id("r1", "a", "b") != review_pair_id("r2", "a", "b")
    assert review_pair_id("r1", "a|b", "c") != review_pair_id("r1", "a", "b|c")


def test_audit_exports_pending_review_queue_without_changing_release(tmp_path, spark, audit_documents):
    release_id = "audit-fixture-v2"
    directory = tmp_path / "releases" / release_id
    (audit_documents.withColumn("title", F.concat(F.lit("合成标题-"), "doc_id"))
     .withColumn("url", F.concat(F.lit("https://example.invalid/"), "doc_id"))
     .write.parquet(str(directory / "documents")))
    identity = {"recipe_version": "recipe_v2", "implementation_id": "audit-test-fixture"}
    report = {"accepted_count": 6}
    write_json(directory / "manifest.json", {"release_id": release_id, "identity": identity, "report": report})
    with catalog(tmp_path) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           [release_id, canonical(identity), str(directory), canonical(report)])
    before = {path.relative_to(directory): path.read_bytes() for path in directory.rglob("*") if path.is_file()}
    result = audit_near_duplicates(tmp_path, release_id, spark=spark)
    assert result["candidate_count"] == result["review_queue_count"] == 3
    assert result["cross_split_count"] == 1
    assert result["changes_release"] is False
    assert "manual_review_count" not in result
    output = Path(result["output_dir"])
    with (output / "manual_review.csv").open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        assert reader.fieldnames == REVIEW_FIELDS
        rows = list(reader)
    assert len(rows) == 3
    for row in rows:
        assert row["release_id"] == release_id
        assert row["pair_id"] == review_pair_id(release_id, row["left_id"], row["right_id"])
        assert row["human_label"] == row["reviewer"] == row["reviewed_at"] == row["notes"] == ""
    assert read_json(output / "summary.json")["review_queue_count"] == 3
    context = read_json(output / "review_context.json")
    assert context["audit_identity"] == result["audit_identity"]
    assert context["audit_identity"]["audit_id"] == output.name
    assert context["audit_identity"]["source_release_identity"] == identity
    assert context["audit_identity"]["sampling"]["scope"] == "length_filtered_deterministic_sample"
    assert {pair["pair_id"] for pair in context["pairs"]} == {row["pair_id"] for row in rows}
    for pair in context["pairs"]:
        assert pair["left_text"] and pair["right_text"]
        assert pair["left_title"] == f"合成标题-{pair['left_id']}"
        assert pair["right_url"] == f"https://example.invalid/{pair['right_id']}"
    assert "left_text" not in REVIEW_FIELDS and "right_text" not in REVIEW_FIELDS
    assert before == {path.relative_to(directory): path.read_bytes() for path in directory.rglob("*") if path.is_file()}


def test_empty_audit_sample_exports_empty_queue_and_context(tmp_path, spark):
    release_id = "short-only-v2"
    directory = tmp_path / "releases" / release_id
    spark.createDataFrame([("short", "合成短文", "train")],
                          "doc_id string, clean_text string, split string").write.parquet(str(directory / "documents"))
    identity = {"recipe_version": "recipe_v2", "implementation_id": "short-audit-fixture"}
    report = {"accepted_count": 1}
    write_json(directory / "manifest.json", {"release_id": release_id, "identity": identity, "report": report})
    with catalog(tmp_path) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           [release_id, canonical(identity), str(directory), canonical(report)])
    result = audit_near_duplicates(tmp_path, release_id, spark=spark)
    assert result["sample_count"] == result["candidate_count"] == result["review_queue_count"] == 0
    assert result["train_train_count"] == result["validation_validation_count"] == result["cross_split_count"] == 0
    output = Path(result["output_dir"])
    assert read_json(output / "review_context.json")["pairs"] == []
    with (output / "manual_review.csv").open(encoding="utf-8-sig", newline="") as source:
        reader = csv.DictReader(source)
        assert reader.fieldnames == REVIEW_FIELDS
        assert list(reader) == []
