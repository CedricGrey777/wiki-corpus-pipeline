import json
from pathlib import Path
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from corpus_governance.cli import main
from corpus_pipeline.core import canonical, catalog, read_json, write_json


@pytest.fixture
def ready_releases(tmp_path):
    source = {"source_kind": "synthetic", "revision": "synthetic-v1", "snapshot": "synthetic",
              "source_file": "fixture.parquet", "source_sha256": "0" * 64, "scope": "full"}
    for release_id, text, split in (("cli-left", "ＡＢＣ", "train"), ("cli-right", "ABC", "validation")):
        directory = tmp_path / "releases" / release_id
        documents = directory / "documents"
        documents.mkdir(parents=True)
        rows = [{"doc_id": "synthetic-one", "title": "合成一", "url": "https://example.invalid/one",
                 "clean_text": text, "split": split, "quality_flags": "short_text,low_chinese_ratio"},
                {"doc_id": "synthetic-two", "title": "合成二", "url": "https://example.invalid/two",
                 "clean_text": "合成中文正文", "split": "validation", "quality_flags": ""}]
        pq.write_table(pa.Table.from_pylist(rows), documents / "part-00000.parquet")
        identity = {"recipe_version": release_id, "implementation_id": "synthetic-cli-fixture", "batches": [source]}
        report = {"accepted_count": len(rows)}
        write_json(directory / "manifest.json", {"release_id": release_id, "identity": identity, "report": report})
        with catalog(tmp_path) as connection:
            connection.execute("INSERT INTO releases VALUES (?,?,?,?, 'READY')",
                               [release_id, canonical(identity), str(directory), canonical(report)])
    return tmp_path


def test_cli_quality_workflow_and_comparison_without_spark(ready_releases, monkeypatch, capsys):
    monkeypatch.setenv("CORPUS_DATA_ROOT", str(ready_releases))
    assert main(["prepare-quality-review", "--release-id", "cli-right", "--per-stratum", "1"]) == 0
    prepared = json.loads(capsys.readouterr().out)
    assert prepared["review_identity"]["per_stratum"] == 1
    assert 0 < prepared["queue_count"] <= 10
    directory = Path(prepared["output_dir"])
    assert main(["summarize-quality-review", "--review-dir", str(directory),
                 "--labels", str(directory / "quality_review.csv")]) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["decisions"]["pending_count"] == prepared["queue_count"]
    assert summary["decisions"]["reviewed_count"] == 0
    assert all(item["decisions"]["positive_fraction_among_adjudicated"] is None for item in summary["strata"])
    assert read_json(directory / "review-summary.json") == summary
    assert main(["--root", str(ready_releases), "compare-releases",
                 "--left-release-id", "cli-left", "--right-release-id", "cli-right"]) == 0
    comparison = json.loads(capsys.readouterr().out)
    assert comparison["source_comparison"]["same_source_inputs"] is True
    assert comparison["documents"]["common_count"] == 2
    assert comparison["documents"]["common_text_changed_count"] == 1
    assert comparison["documents"]["common_split_changed_count"] == 1


def test_cli_routes_near_audit_and_supplied_review_labels(tmp_path, monkeypatch, capsys):
    from corpus_governance import audit, review

    audit_call = Mock(return_value={"output_dir": str(tmp_path / "audit")})
    review_call = Mock(return_value={"decisions": {"pending_count": 1}})
    monkeypatch.setattr(audit, "audit_near_duplicates", audit_call)
    monkeypatch.setattr(review, "summarize_near_review", review_call)
    assert main(["--root", str(tmp_path), "audit-near-duplicates", "--release-id", "synthetic-ready"]) == 0
    audit_call.assert_called_once_with(str(tmp_path), "synthetic-ready")
    assert json.loads(capsys.readouterr().out) == audit_call.return_value
    directory = str(tmp_path / "audit")
    labels = str(tmp_path / "labels.csv")
    assert main(["summarize-near-review", "--audit-dir", directory, "--labels", labels]) == 0
    review_call.assert_called_once_with(directory, labels)
    assert json.loads(capsys.readouterr().out) == review_call.return_value
