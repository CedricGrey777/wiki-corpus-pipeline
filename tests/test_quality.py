import csv
import json
import shutil
from collections import Counter
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from corpus_governance.quality import METADATA, RULES, prepare_quality_review, summarize_quality_review
from corpus_governance.review import HUMAN_FIELDS
from corpus_pipeline.core import canonical, catalog, read_json, read_ready, write_json


@pytest.fixture
def quality_release(tmp_path):
    release_id = "synthetic-quality"
    directory = tmp_path / "releases" / release_id
    documents = directory / "documents"
    documents.mkdir(parents=True)
    # Persisted rule labels form a synthetic READY snapshot; normalization is
    # exercised separately by the pipeline tests, not by this sampling test.
    flags = ["short_text,language_undetermined", "short_text,low_chinese_ratio",
             "short_text,low_chinese_ratio", "long_text,repeated_lines",
             "long_text,repeated_lines,low_chinese_ratio", "long_text,repeated_lines",
             "", "control_chars_removed"]
    rows = [{"doc_id": f"private-quality-doc-{index}", "title": f"LOCAL-ONLY-TITLE-{index}",
             "url": f"https://example.invalid/local-only/{index}",
             "clean_text": f"LOCAL-ONLY-BODY-{index}", "quality_flags": value}
            for index, value in enumerate(flags)]
    schema = pa.schema([(name, pa.string()) for name in ("doc_id", "title", "url", "clean_text", "quality_flags")])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), documents / "part-00000.parquet")
    identity = {"recipe_version": "recipe_v2", "implementation_id": "synthetic-implementation",
                "batches": [{"batch_id": "synthetic-batch", "source_kind": "synthetic"}]}
    report = {"accepted_count": len(rows)}
    write_json(directory / "manifest.json", {"release_id": release_id, "identity": identity, "report": report})
    with catalog(tmp_path) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           [release_id, canonical(identity), str(directory), canonical(report)])
    return tmp_path, release_id, directory, rows


def write_quality_csv(path, rows):
    with path.open("w", encoding="utf-8-sig", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(METADATA) + list(HUMAN_FIELDS))
        writer.writeheader()
        writer.writerows(rows)


def test_quality_sampling_is_stable_bounded_and_preserves_ready_source(quality_release):
    root, release_id, release_dir, source_rows = quality_release
    source_before = {path.relative_to(release_dir): path.read_bytes()
                     for path in release_dir.rglob("*") if path.is_file()}
    ready_before = read_ready(root, release_id)
    first = prepare_quality_review(root, release_id, per_stratum=2)
    # An equivalent synthetic release with reversed physical Parquet row order
    # must choose the same article-rule sample IDs.
    reordered_root = root / "reordered-copy"
    reordered_dir = reordered_root / "releases" / release_id
    shutil.copytree(release_dir, reordered_dir)
    reordered_file = reordered_dir / "documents" / "part-00000.parquet"
    table = pq.read_table(reordered_file)
    pq.write_table(table.take(pa.array(list(reversed(range(table.num_rows))))), reordered_file)
    with catalog(reordered_root) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           [release_id, canonical(ready_before["identity"]), str(reordered_dir), canonical(ready_before["report"])])
    second = prepare_quality_review(reordered_root, release_id, per_stratum=2)
    first_dir, second_dir = Path(first["output_dir"]), Path(second["output_dir"])
    context = read_json(first_dir / "review_context.json")
    assert first_dir != second_dir
    assert context["rows"] == read_json(second_dir / "review_context.json")["rows"]
    assert first["strata"] == second["strata"]
    assert set(RULES) == {"short_text", "long_text", "repeated_lines", "low_chinese_ratio", "language_undetermined"}
    assert len(first["strata"]) == 10
    flagged_population = {"short_text": 3, "long_text": 3, "repeated_lines": 3,
                          "low_chinese_ratio": 3, "language_undetermined": 1}
    for stratum in first["strata"]:
        expected = flagged_population[stratum["rule"]]
        if stratum["flagged"] == "false":
            expected = len(source_rows) - expected
        assert stratum["population_count"] == expected
        assert stratum["sample_count"] == min(2, expected)
    assert first["queue_count"] == 19
    assert len({row["sample_id"] for row in context["rows"]}) == 19
    distinct_ids = {row["doc_id"] for row in context["rows"]}
    assert first["unique_article_count"] == len(distinct_ids) < first["queue_count"]
    assert any(count > 1 for count in Counter(row["doc_id"] for row in context["rows"]).values())
    only_undetermined = [row for row in context["rows"]
                         if row["rule"] == "language_undetermined" and row["flagged"] == "true"]
    assert len(only_undetermined) == 1
    assert only_undetermined[0]["doc_id"] == "private-quality-doc-0"

    pending = summarize_quality_review(first_dir)
    assert pending["decisions"]["queue_count"] == pending["decisions"]["pending_count"] == 19
    assert "positive_fraction_among_adjudicated" not in pending["decisions"]
    assert all(stratum["decisions"]["positive_fraction_among_adjudicated"] is None for stratum in pending["strata"])
    for document in context["documents"]:
        assert document["doc_id"] in distinct_ids
        assert document["clean_text"].startswith("LOCAL-ONLY-BODY")
    exports = json.dumps([first, pending, read_json(first_dir / "summary.json")])
    csv_text = (first_dir / "quality_review.csv").read_text(encoding="utf-8-sig")
    for row in source_rows:
        for field in ("title", "url", "clean_text"):
            assert row[field] not in exports
            assert row[field] not in csv_text
    assert {path.relative_to(release_dir): path.read_bytes()
            for path in release_dir.rglob("*") if path.is_file()} == source_before
    assert read_ready(root, release_id) == ready_before


def test_quality_review_counts_article_rule_labels_within_each_stratum(quality_release):
    root, release_id, _, _ = quality_release
    selection = prepare_quality_review(root, release_id, per_stratum=2)
    directory = Path(selection["output_dir"])
    with (directory / "quality_review.csv").open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.DictReader(stream))
    short_flagged = [row for row in rows if row["rule"] == "short_text" and row["flagged"] == "true"]
    for row, label in zip(short_flagged, ("issue", "acceptable")):
        row.update(human_label=label, reviewer="Synthetic reviewer", reviewed_at="2026-11-02",
                   notes="LOCAL-ONLY-QUALITY-NOTES")
    # The same article can receive a separate judgment about another rule.
    other_rule = next(row for row in rows if row["doc_id"] == short_flagged[0]["doc_id"]
                      and row["rule"] != "short_text")
    other_rule.update(human_label="uncertain", reviewer="Synthetic reviewer", reviewed_at="2026-11-02T15:30:00+08:00")
    supplied = directory / "supplied-labels.csv"
    write_quality_csv(supplied, rows)
    report = summarize_quality_review(directory, supplied)
    overall = report["decisions"]
    assert overall["queue_count"] == 19
    assert overall["reviewed_count"] == 3
    assert overall["adjudicated_count"] == 2
    assert overall["issue_count"] == overall["acceptable_count"] == overall["uncertain_count"] == 1
    assert overall["pending_count"] == 16
    assert "positive_fraction_among_adjudicated" not in overall
    strata = {(item["rule"], item["flagged"]): item["decisions"] for item in report["strata"]}
    assert strata[("short_text", "true")]["positive_fraction_among_adjudicated"] == 0.5
    uncertain = strata[(other_rule["rule"], other_rule["flagged"])]
    assert uncertain["uncertain_count"] == 1
    assert uncertain["adjudicated_count"] == 0
    assert uncertain["positive_fraction_among_adjudicated"] is None
    assert report["unique_article_count"] == selection["unique_article_count"] < overall["queue_count"]
    assert "LOCAL-ONLY" not in json.dumps(report)
    for item in (report, overall, *strata.values()):
        assert not {"accuracy", "precision", "recall", "corpus_accuracy", "corpus_recall"}.intersection(item)


@pytest.mark.parametrize("per_stratum", [0, 101])
def test_quality_sampling_rejects_out_of_contract_limits(quality_release, per_stratum):
    root, release_id, _, _ = quality_release
    with pytest.raises(ValueError, match="between 1 and 100"):
        prepare_quality_review(root, release_id, per_stratum)


def test_quality_sampling_requires_ready_release(tmp_path):
    (tmp_path / "releases" / "orphan").mkdir(parents=True)
    with pytest.raises(ValueError, match="not READY"):
        prepare_quality_review(tmp_path, "orphan", per_stratum=2)
