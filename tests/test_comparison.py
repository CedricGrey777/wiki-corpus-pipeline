import json

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from corpus_governance.comparison import compare_releases
from corpus_pipeline.core import canonical, catalog, write_json


def ready_release(root, release_id, rows, recipe="recipe_v1", revision="source-r1", source_sha="a" * 64, report=None):
    directory = root / "releases" / release_id
    documents = directory / "documents"
    documents.mkdir(parents=True)
    schema = pa.schema([(name, pa.string()) for name in ("doc_id", "clean_text", "split")])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), documents / "part-00000.parquet")
    identity = {
        "recipe_version": recipe,
        "implementation_id": f"implementation-{recipe}",
        "batches": [{"batch_id": f"synthetic-full-{recipe}", "source_kind": "synthetic",
                     "revision": revision, "snapshot": "synthetic", "source_file": "source.parquet",
                     "source_sha256": source_sha, "scope": "full", "recipe_version": recipe,
                     "implementation_id": f"implementation-{recipe}"}],
    }
    report = {"accepted_count": len(rows),
              "train_count": sum(row["split"] == "train" for row in rows),
              "validation_count": sum(row["split"] == "validation" for row in rows), **(report or {})}
    manifest = {"release_id": release_id, "identity": identity, "report": report}
    write_json(directory / "manifest.json", manifest)
    with catalog(root) as connection:
        connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                           [release_id, canonical(identity), str(directory), canonical(report)])
    return directory


def article(doc_id, text, split):
    return {"doc_id": doc_id, "clean_text": text, "split": split}


def test_compare_ready_releases_counts_actual_migrations_and_text_changes(tmp_path):
    left_rows = [article("private-doc-same", "unchanged-private-text", "train"),
                 article("private-doc-changed", "ＡＢＣ-private-before", "train"),
                 article("private-doc-t-to-v", "migration-private-before", "train"),
                 article("private-doc-v-to-t", "reverse-private-before", "validation"),
                 article("private-doc-stable-v", "stable-validation-private", "validation"),
                 article("private-doc-removed", "removed-private", "train")]
    right_rows = [article("private-doc-same", "unchanged-private-text", "train"),
                  article("private-doc-changed", "ABC-private-after", "train"),
                  article("private-doc-t-to-v", "migration-private-after", "validation"),
                  article("private-doc-v-to-t", "reverse-private-after", "train"),
                  article("private-doc-stable-v", "stable-validation-private", "validation"),
                  article("private-doc-added", "added-private", "train")]
    left_dir = ready_release(tmp_path, "before", left_rows, report={"short_text_count": 6})
    right_dir = ready_release(tmp_path, "after", right_rows, recipe="recipe_v2",
                              report={"short_text_count": 5, "low_chinese_ratio_count": 3})
    manifests_before = [(directory / "manifest.json").read_bytes() for directory in (left_dir, right_dir)]

    result = compare_releases(tmp_path, "before", "after")

    assert result["documents"] == {"left_count": 6, "right_count": 6, "common_count": 5,
                                   "removed_count": 1, "added_count": 1,
                                   "common_text_changed_count": 3, "common_text_unchanged_count": 2,
                                   "common_split_changed_count": 2}
    assert result["common_split_transitions"] == {"train_to_train": 2, "train_to_validation": 1,
                                                  "validation_to_train": 1, "validation_to_validation": 1}
    # Both split totals are unchanged, but two matched articles changed membership.
    assert result["reported_metric_deltas"]["train_count"]["delta"] == 0
    assert result["reported_metric_deltas"]["validation_count"]["delta"] == 0
    assert result["reported_metric_deltas"]["short_text_count"]["delta"] == -1
    assert result["reported_metric_deltas"]["low_chinese_ratio_count"] == {
        "left": None, "right": 3, "delta": None}
    assert result["source_comparison"]["same_source_revisions"] is True
    assert result["source_comparison"]["same_source_inputs"] is True
    assert result["left"]["identity"]["recipe_version"] == "recipe_v1"
    assert result["right"]["identity"]["recipe_version"] == "recipe_v2"
    assert result["left"]["identity"]["batches"][0]["batch_id"] == "synthetic-full-recipe_v1"
    serialized = json.dumps(result)
    assert all(row["clean_text"] not in serialized for row in left_rows + right_rows)
    assert all(row["doc_id"] not in serialized for row in left_rows + right_rows)
    assert [(directory / "manifest.json").read_bytes() for directory in (left_dir, right_dir)] == manifests_before


@pytest.mark.parametrize(("revision", "source_sha", "same_revision"), [
    ("source-r2", "a" * 64, False),
    ("source-r1", "b" * 64, True),
])
def test_compare_reports_manifest_source_differences(tmp_path, revision, source_sha, same_revision):
    rows = [article("article-1", "private-text", "train")]
    ready_release(tmp_path, "before", rows)
    ready_release(tmp_path, "after", rows, recipe="recipe_v2", revision=revision, source_sha=source_sha)
    result = compare_releases(tmp_path, "before", "after")
    assert result["source_comparison"]["same_source_revisions"] is same_revision
    assert result["source_comparison"]["same_source_inputs"] is False
    assert result["documents"]["common_text_changed_count"] == 0


def test_compare_requires_both_releases_to_be_ready(tmp_path):
    ready_release(tmp_path, "ready", [article("article-1", "private-text", "train")])
    (tmp_path / "releases" / "orphan").mkdir(parents=True)
    with pytest.raises(ValueError, match="not READY: orphan"):
        compare_releases(tmp_path, "ready", "orphan")
    with pytest.raises(ValueError, match="not READY: orphan"):
        compare_releases(tmp_path, "orphan", "ready")
