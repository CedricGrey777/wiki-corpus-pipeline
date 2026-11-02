"""Deterministic, bounded rule-review samples from an existing READY release."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import duckdb

from corpus_pipeline.core import new_stage, parquet_files, read_json, read_ready, root_path, write_json
from .review import HUMAN_FIELDS, decision_counts, load_labels

RULES = {
    "short_text": "Does the article lack enough standalone context? Short length alone is not an issue.",
    "long_text": "Does excessive padding or irrelevant repetition impair the article? Long length alone is not an issue.",
    "repeated_lines": "Are repeated lines redundant noise rather than intentional structure?",
    "low_chinese_ratio": "Is this article unsuitable for a primarily Chinese-language text collection? Mixed language is not automatically an issue.",
    "language_undetermined": "Does the absence of letter content make the article unsuitable as language-training text?",
}
METADATA = ("release_id", "sample_id", "doc_id", "rule", "flagged")
QUALITY_LABELS = ("issue", "acceptable", "uncertain")


def prepare_quality_review(root, release_id, per_stratum=20):
    if not 1 <= per_stratum <= 100:
        raise ValueError("per_stratum must be between 1 and 100")
    root = root_path(root)
    release = read_ready(root, release_id)
    directory = new_stage(root, "quality-review-" + release_id)
    queued, strata = [], []
    with duckdb.connect(config={"memory_limit": "1GB", "threads": 2}) as connection:
        connection.read_parquet(parquet_files(Path(release["directory"]) / "documents")).create_view("docs")
        connection.execute("CREATE TEMP TABLE metadata AS SELECT doc_id, quality_flags FROM docs")
        for rule in RULES:
            for flagged in (True, False):
                expression = "list_contains(string_split(quality_flags, ','), ?) = ?"
                count = connection.execute("SELECT count(*) FROM metadata WHERE " + expression, [rule, flagged]).fetchone()[0]
                chosen = connection.execute("SELECT doc_id FROM metadata WHERE " + expression + " ORDER BY sha256(doc_id), doc_id LIMIT ?", [rule, flagged, per_stratum]).fetchall()
                strata.append({"rule": rule, "flagged": str(flagged).lower(), "population_count": count, "sample_count": len(chosen)})
                for (doc_id,) in chosen:
                    sample_id = hashlib.sha256(json.dumps([release_id, rule, flagged, doc_id], ensure_ascii=False).encode()).hexdigest()
                    queued.append({"release_id": release_id, "sample_id": sample_id, "doc_id": doc_id,
                                   "rule": rule, "flagged": str(flagged).lower()})
        ids = sorted({row["doc_id"] for row in queued})
        documents = [dict(zip(("doc_id", "title", "url", "clean_text"), row)) for row in connection.execute(
            "SELECT doc_id,title,url,clean_text FROM docs WHERE doc_id IN (SELECT unnest(?)) ORDER BY doc_id", [ids]).fetchall()]
    identity = {"review_id": directory.name, "release_id": release_id, "source_release_identity": release["identity"],
                "sampling": "For each rule, independently take up to N flagged and N unflagged articles ordered by SHA256(doc_id), doc_id.",
                "per_stratum": per_stratum, "rules": RULES}
    write_json(directory / "review_context.json", {"review_identity": identity, "rows": queued, "documents": documents})
    with (directory / "quality_review.csv").open("w", encoding="utf-8-sig", newline="") as target:
        writer = csv.DictWriter(target, fieldnames=list(METADATA) + list(HUMAN_FIELDS))
        writer.writeheader()
        writer.writerows({**row, **dict.fromkeys(HUMAN_FIELDS, "")} for row in queued)
    report = {"review_identity": identity, "queue_count": len(queued), "unique_article_count": len(ids), "strata": strata,
              "interpretation": "Rows are article-rule assessments, not distinct articles. Control-character removal is a processing event and is not scored as a quality defect. No labels have been inferred."}
    write_json(directory / "summary.json", report)
    return {**report, "output_dir": str(directory)}


def summarize_quality_review(review_dir, labels_path=None):
    directory = Path(review_dir)
    context, selection = read_json(directory / "review_context.json"), read_json(directory / "summary.json")
    if context["review_identity"] != selection["review_identity"]:
        raise ValueError("Review context and sample identity differ")
    rows = load_labels(labels_path or directory / "quality_review.csv", context["rows"], "sample_id", METADATA, QUALITY_LABELS)
    strata = []
    for stratum in selection["strata"]:
        selected = [row for row in rows if row["rule"] == stratum["rule"] and row["flagged"] == stratum["flagged"]]
        strata.append({**stratum, "decisions": decision_counts(selected, "issue", "acceptable")})
    overall = decision_counts(rows, "issue", "acceptable")
    overall.pop("positive_fraction_among_adjudicated")
    report = {"review_identity": context["review_identity"], "unique_article_count": selection["unique_article_count"],
              "decisions": overall, "strata": strata,
              "interpretation": "Fractions describe adjudicated rows within each sampling stratum. Balanced strata and incomplete labels do not establish corpus-wide accuracy, recall, or training benefit."}
    write_json(directory / "review-summary.json", report)
    return report
