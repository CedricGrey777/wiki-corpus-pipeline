import json
import shutil
from pathlib import Path

import duckdb
import pytest
from pyspark.sql import functions as F

from corpus_pipeline.audit import candidate_pairs, run_synthetic_audit
from corpus_pipeline.core import catalog, read_json, read_ready, write_json
from corpus_pipeline.fixtures import create_fixtures, row, write_rows
from corpus_pipeline.pipeline import build_release, ingest_clean, validate_directory, validate_publish
from corpus_pipeline.transform import clean_frame, content_split, deduplicate, spark_session, text_hash

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def spark():
    session = spark_session("CorpusIntegrationTests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def prepared(tmp_path_factory, spark):
    directory = tmp_path_factory.mktemp("corpus")
    files = create_fixtures(directory / "fixtures")
    root = directory / "warehouse"
    batches = [ingest_clean(root, path, source_kind="synthetic", spark=spark)["batch_id"] for path in files]
    context = build_release(root, "base-r001", batches, spark=spark)
    return root, files, batches, context


def copy_context(prepared, tmp_path, release_id="test-r001"):
    _, _, _, original = prepared
    directory = tmp_path / "staging" / "test-stage"
    source = Path(original["run_dir"])
    if not source.exists():
        source = Path(original["root"]) / "releases" / original["release_id"]
    shutil.copytree(source, directory)
    manifest = read_json(directory / "manifest.json")
    manifest["release_id"] = release_id
    write_json(directory / "manifest.json", manifest)
    return {"root": str(tmp_path), "release_id": release_id, "run_dir": str(directory), "identity": manifest["identity"]}


def read_documents(directory):
    with duckdb.connect() as connection:
        files = sorted(str(p) for p in (Path(directory) / "documents").glob("*.parquet"))
        return connection.execute("SELECT doc_id, clean_text, content_hash, split FROM read_parquet(?) ORDER BY doc_id", [files]).fetchall()


def test_real_spark_quality_and_exact_equality(prepared):
    _, _, _, context = prepared
    report = validate_directory(Path(context["run_dir"]))
    assert report["raw_count"] == 9
    assert report["quarantine_count"] == 2
    assert report["valid_count"] == 7
    assert report["source_duplicate_count"] == 1
    assert report["source_unique_count"] == 6
    assert report["exact_duplicate_count"] == 2
    assert report["accepted_count"] == 4
    rows = read_documents(context["run_dir"])
    assert [r[0] for r in rows] == ["wikipedia:zh:02", "wikipedia:zh:20", "wikipedia:zh:30", "wikipedia:zh:50"]
    assert rows[1][1].startswith("  if x:\n    print")
    assert rows[2][1].startswith("Café")


def test_v2_audit_flags_deduplication_and_release_metrics(tmp_path, spark):
    source = tmp_path / "v2-audit.parquet"
    write_rows(source, [
        row("01", "\x00ＡＢＣ\u3000Ｃａｆｅ\u0301"),
        row("02", "ABC Café"),
        row("03", "中文AB"),
        row("04", "123!?👩\u200d💻"),
        row("05", "\x00\x7f"),
        row("06", "中文\r\n\t内容"),
        row("07", "𠀀𰀀AB"),
    ])
    root = tmp_path / "warehouse"
    batch = ingest_clean(root, source, source_kind="synthetic", spark=spark)["batch_id"]
    release = validate_publish(build_release(root, "v2-audit", [batch], spark=spark))
    report = release["report"]
    assert report["raw_count"] == 7
    assert report["quarantine_count"] == 1
    assert report["valid_count"] == 6
    assert report["exact_duplicate_count"] == 1
    assert report["accepted_count"] == 5
    assert report["control_chars_removed_count"] == 1
    assert report["low_chinese_ratio_count"] == 1
    assert report["language_undetermined_count"] == 1
    assert report["short_text_count"] == 5
    rows = read_documents(release["directory"])
    assert [row[0] for row in rows] == [f"wikipedia:zh:{key}" for key in ("01", "03", "04", "06", "07")]
    assert rows[0][1] == "ABC Café"
    for _, text, content_hash, split in rows:
        assert content_hash == text_hash(text)
        assert split == content_split(content_hash)
    with duckdb.connect() as connection:
        files = sorted(str(p) for p in (Path(release["directory"]) / "documents").glob("*.parquet"))
        flags = dict(connection.execute("SELECT doc_id, quality_flags FROM read_parquet(?)", [files]).fetchall())
    assert set(flags["wikipedia:zh:01"].split(",")) == {"short_text", "control_chars_removed", "low_chinese_ratio"}
    assert set(flags["wikipedia:zh:04"].split(",")) == {"short_text", "language_undetermined"}
    for key in ("03", "06", "07"):
        assert flags[f"wikipedia:zh:{key}"] == "short_text"
    with catalog(root) as connection:
        metrics = dict(connection.execute("SELECT metric, value FROM quality_metrics WHERE run_id='v2-audit'").fetchall())
    for metric in ("control_chars_removed_count", "low_chinese_ratio_count", "language_undetermined_count"):
        assert metrics[metric] == report[metric]
    assert read_ready(root, "v2-audit")["report"] == report


@pytest.mark.parametrize("failure", ["before_rename", "after_rename", "before_commit", "after_commit"])
def test_publication_recovery_and_ready_gate(prepared, tmp_path, failure):
    context = copy_context(prepared, tmp_path)
    with pytest.raises(RuntimeError, match="Injected"):
        validate_publish(context, fail_at=failure)
    if failure == "after_commit":
        assert read_ready(tmp_path, "test-r001")["report"]["accepted_count"] == 4
    else:
        with pytest.raises(ValueError, match="not READY"):
            read_ready(tmp_path, "test-r001")
    result = validate_publish(context)
    assert result["status"] == "READY"
    assert validate_publish(context)["report"] == result["report"]
    with catalog(tmp_path) as connection:
        assert connection.execute("SELECT count(*) FROM releases").fetchone()[0] == 1
        assert connection.execute("SELECT value FROM quality_metrics WHERE run_id='test-r001' AND metric='accepted_count'").fetchone()[0] == 4


def test_mismatched_context_does_not_register_orphan(prepared, tmp_path):
    context = copy_context(prepared, tmp_path)
    with pytest.raises(RuntimeError):
        validate_publish(context, fail_at="after_rename")
    context["identity"] = {**context["identity"], "recipe_version": "different"}
    with pytest.raises(ValueError, match="different input"):
        validate_publish(context)
    with pytest.raises(ValueError, match="not READY"):
        read_ready(tmp_path, "test-r001")


def test_conservation_failure_never_publishes(prepared, tmp_path):
    context = copy_context(prepared, tmp_path)
    manifest_path = Path(context["run_dir"]) / "manifest.json"
    manifest = read_json(manifest_path)
    manifest["report"]["raw_count"] += 1
    write_json(manifest_path, manifest)
    with pytest.raises(ValueError, match="conservation"):
        validate_publish(context)
    assert not (tmp_path / "releases" / "test-r001").exists()


def test_incremental_clean_reuse_new_release_and_old_version(prepared, spark):
    root, files, batches, context = prepared
    first = validate_publish(build_release(root, "first-r001", batches[:1], spark=spark))
    before = read_documents(first["directory"])
    assert ingest_clean(root, files[0], source_kind="synthetic", spark=spark)["reused"] is True
    combined = validate_publish(context)
    assert combined["report"]["accepted_count"] == 4
    assert read_documents(first["directory"]) == before
    # Source representative changes with a new shard; content split does not.
    old = {r[2]: r[3] for r in before}
    new = {r[2]: r[3] for r in read_documents(combined["directory"])}
    assert all(new[key] == split for key, split in old.items())
    with pytest.raises(ValueError, match="different input"):
        build_release(root, "base-r001", batches[:1], spark=spark)


def test_batch_rename_recovery_and_changed_input_rejection(tmp_path, spark):
    source = tmp_path / "input.parquet"
    write_rows(source, [row("1", "合成樣本")])
    root = tmp_path / "warehouse"
    with pytest.raises(RuntimeError, match="after batch rename"):
        ingest_clean(root, source, source_kind="synthetic", spark=spark, fail_at="after_batch_rename")
    assert ingest_clean(root, source, source_kind="synthetic", spark=spark)["reused"] is True
    write_rows(source, [row("1", "變更了的樣本")])
    with pytest.raises(ValueError, match="different input"):
        ingest_clean(root, source, source_kind="synthetic", spark=spark)


def test_source_id_conflict_blocks_cross_batch_release(tmp_path, spark):
    paths = [tmp_path / "a.parquet", tmp_path / "b.parquet"]
    write_rows(paths[0], [row("1", "第一個版本")])
    write_rows(paths[1], [row("1", "互相衝突的另一版本")])
    root = tmp_path / "warehouse"
    batches = [ingest_clean(root, path, source_kind="synthetic", spark=spark)["batch_id"] for path in paths]
    with pytest.raises(ValueError, match="Conflicting source ID"):
        build_release(root, "conflict-r001", batches, spark=spark)
    with pytest.raises(ValueError, match="not READY"):
        read_ready(root, "conflict-r001")


def test_schema_and_repeated_line_quality(spark):
    with pytest.raises(ValueError, match="Missing source columns"):
        clean_frame(spark.createDataFrame([(1,)], "wrong int"), "batch")
    with pytest.raises(ValueError, match="string types"):
        clean_frame(spark.createDataFrame([(1, "url", "title", "text")], "id int, url string, title string, text string"), "batch")
    frame = spark.createDataFrame([("1", "url", "title", "重複\n重複\n\n重複\n獨立")], "id string, url string, title string, text string")
    valid, quarantine = clean_frame(frame, "batch")
    record = valid.first()
    assert record.repeated_line_ratio == 0.5  # empty lines are excluded
    assert quarantine.count() == 0
    assert "repeated_lines" not in record.quality_flags  # threshold is strictly >0.5
    repeated = frame.withColumn("text", F.lit("重複\n重複\n重複"))
    record = clean_frame(repeated, "batch")[0].first()
    assert record.repeated_line_ratio == pytest.approx(2 / 3)
    assert "repeated_lines" in record.quality_flags


def test_hash_equality_alone_does_not_merge_different_text(spark):
    frame = spark.createDataFrame([("1", "url1", "title1", "不同正文甲"), ("2", "url2", "title2", "不同正文乙")], "id string, url string, title string, text string")
    valid = clean_frame(frame, "batch")[0].withColumn("content_hash", F.lit("0" * 64))
    _, accepted, duplicates = deduplicate(valid)
    assert accepted.count() == 2
    assert duplicates.count() == 0


def test_real_spark_minhash_synthetic_pairs(spark):
    result = run_synthetic_audit(spark)
    assert result["pair_count"] == 40
    assert result["positive_detected"] == 20
    assert result["negative_flagged"] == 0


def test_deterministic_10k_scope(tmp_path, spark):
    rows = [row(str(index), f"合成文章 {index}") for index in range(10_005)]
    first, second = tmp_path / "ascending.parquet", tmp_path / "descending.parquet"
    write_rows(first, rows)
    write_rows(second, list(reversed(rows)))
    root = tmp_path / "warehouse"
    batches = [ingest_clean(root, path, scope="10k", source_kind="synthetic", spark=spark)["batch_id"] for path in (first, second)]
    with catalog(root) as connection:
        paths = [Path(connection.execute("SELECT path FROM batches WHERE batch_id=?", [batch]).fetchone()[0]) for batch in batches]
    with duckdb.connect() as connection:
        for label, path in zip(("a", "b"), paths):
            connection.read_parquet([str(p) for p in (path / "documents").glob("*.parquet")]).create_view(label)
        assert connection.execute("SELECT count(*) FROM a").fetchone()[0] == 10_000
        assert connection.execute("SELECT count(*) FROM (SELECT doc_id,clean_text FROM a EXCEPT SELECT doc_id,clean_text FROM b)").fetchone()[0] == 0
