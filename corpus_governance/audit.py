"""Bounded near-duplicate evidence, never a production deletion stage."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType

from corpus_pipeline.core import new_stage, read_ready, root_path, write_json
from corpus_pipeline.audit import jaccard, shingles, synthetic_pairs
from corpus_pipeline.transform import content_split, spark_session, text_hash


REVIEW_FIELDS = ["release_id", "pair_id", "left_id", "right_id", "left_split", "right_split",
                 "jaccard", "human_label", "reviewer", "reviewed_at", "notes"]


def review_pair_id(release_id: str, left_id: str, right_id: str) -> str:
    """Identify one canonically ordered candidate pair within its source release."""
    identity = json.dumps([release_id, left_id, right_id], ensure_ascii=False, separators=(",", ":"))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def candidate_pairs(frame, threshold=0.8):
    from pyspark.ml.feature import HashingTF, MinHashLSH
    fields = {field.name: field.dataType for field in frame.schema.fields}
    required = ("doc_id", "clean_text", "split")
    if any(name not in fields or not isinstance(fields[name], StringType) for name in required):
        raise ValueError("Candidate input requires string columns: doc_id, clean_text, split")
    if frame.filter(F.col("split").isNull() | ~F.col("split").isin("train", "validation")).limit(1).count():
        raise ValueError("Candidate input split must be train or validation")
    tokenize = F.udf(lambda text: sorted(shingles(text)), ArrayType(StringType()))
    tokens = frame.withColumn("shingles", tokenize("clean_text"))
    features = HashingTF(inputCol="shingles", outputCol="features", numFeatures=262144, binary=True).transform(tokens)
    model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=8, seed=42).fit(features)
    joined = model.approxSimilarityJoin(features, features, 1 - threshold, distCol="hashed_distance")
    exact = F.udf(jaccard, DoubleType())
    return (joined.filter(F.col("datasetA.doc_id") < F.col("datasetB.doc_id"))
            .select(F.col("datasetA.doc_id").alias("left_id"), F.col("datasetB.doc_id").alias("right_id"),
                    F.col("datasetA.clean_text").alias("left_text"), F.col("datasetB.clean_text").alias("right_text"),
                    F.col("datasetA.split").alias("left_split"), F.col("datasetB.split").alias("right_split"), "hashed_distance")
            .withColumn("cross_split", F.col("left_split") != F.col("right_split"))
            .withColumn("jaccard", exact("left_text", "right_text"))
            .filter(F.col("jaccard") >= threshold))


def candidate_counts(candidates) -> dict:
    return candidates.agg(
        F.count(F.lit(1)).alias("candidate_count"),
        F.count(F.when((F.col("left_split") == "train") & (F.col("right_split") == "train"), 1)).alias("train_train_count"),
        F.count(F.when((F.col("left_split") == "validation") & (F.col("right_split") == "validation"), 1)).alias("validation_validation_count"),
        F.count(F.when(F.col("cross_split"), 1)).alias("cross_split_count"),
    ).first().asDict()


def run_synthetic_audit(session) -> dict:
    pairs = synthetic_pairs()
    rows = [(f"{pair['pair_id']}-{side}", pair[side], content_split(text_hash(pair[side])))
            for pair in pairs for side in ("left", "right")]
    frame = session.createDataFrame(rows, "doc_id string, clean_text string, split string")
    expected = {tuple(sorted((f"{pair['pair_id']}-left", f"{pair['pair_id']}-right"))): pair for pair in pairs}
    # This collection is strictly bounded: 80 synthetic documents, at most 3160 pairs.
    detected = {(row.left_id, row.right_id) for row in candidate_pairs(frame).select("left_id", "right_id").collect()}
    near = [key for key, pair in expected.items() if pair["expected_near"]]
    far = [key for key, pair in expected.items() if not pair["expected_near"]]
    return {"pair_count": 40, "positive_pairs": 20, "negative_pairs": 20,
            "positive_detected": sum(key in detected for key in near),
            "negative_flagged": sum(key in detected for key in far),
            "threshold": 0.8,
            "interpretation": "Synthetic perturbation detection only; not real-corpus semantic recall"}


def audit_near_duplicates(root, release_id, spark=None) -> dict:
    root = root_path(root)
    release = read_ready(root, release_id)
    output = new_stage(root, "audit-" + release_id)
    audit_identity = {
        "audit_id": output.name,
        "release_id": release_id,
        "source_release_identity": release["identity"],
        "sampling": {"scope": "length_filtered_deterministic_sample", "min_chars": 100,
                     "max_chars": 2000, "max_documents": 2000, "order": ["sha256(doc_id)", "doc_id"]},
        "matching": {"character_ngram": 5, "hashing_tf_features": 262144,
                     "minhash_tables": 8, "seed": 42, "min_exact_jaccard": 0.8},
        "review_limit": 50,
        "review_order": ["jaccard_desc", "left_id", "right_id"],
    }
    owned = spark is None
    session = spark or spark_session("WikiCorpusAudit")
    try:
        docs = session.read.parquet(str(Path(release["directory"]) / "documents"))
        sample = (docs.filter(F.length("clean_text").between(100, 2000))
                  .orderBy(F.sha2("doc_id", 256), "doc_id").limit(2000).select("doc_id", "clean_text", "split"))
        sample_count = sample.count()
        # Independent bounded local artifact, not a column appended to the released dataset.
        if sample_count:
            identify_pair = F.udf(review_pair_id, StringType())
            (candidate_pairs(sample).withColumn("release_id", F.lit(release_id))
             .withColumn("pair_id", identify_pair("release_id", "left_id", "right_id"))
             .write.mode("error").parquet(str(output / "candidate_pairs")))
            candidates = session.read.parquet(str(output / "candidate_pairs"))
            counts = candidate_counts(candidates)
            review = candidates.orderBy(F.desc("jaccard"), "left_id", "right_id").limit(50).collect()
        else:
            counts = {"candidate_count": 0, "train_train_count": 0,
                      "validation_validation_count": 0, "cross_split_count": 0}
            review = []
        metadata_fields = [field for field in ("title", "url") if field in docs.columns]
        metadata = {}
        if review and metadata_fields:
            review_ids = sorted({doc_id for row in review for doc_id in (row.left_id, row.right_id)})
            metadata = {row.doc_id: row.asDict() for row in docs.filter(F.col("doc_id").isin(review_ids))
                        .select("doc_id", *metadata_fields).collect()}
        context_pairs = []
        with (output / "manual_review.csv").open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=REVIEW_FIELDS)
            writer.writeheader()
            for row in review:
                writer.writerow({"release_id": release_id, "pair_id": row.pair_id,
                                 "left_id": row.left_id, "right_id": row.right_id,
                                 "left_split": row.left_split, "right_split": row.right_split,
                                 "jaccard": row.jaccard, "human_label": "", "reviewer": "", "reviewed_at": "", "notes": ""})
                pair = {"release_id": release_id, "pair_id": row.pair_id,
                        "left_id": row.left_id, "right_id": row.right_id,
                        "left_split": row.left_split, "right_split": row.right_split,
                        "jaccard": row.jaccard, "left_text": row.left_text, "right_text": row.right_text}
                for side in ("left", "right"):
                    document = metadata.get(pair[f"{side}_id"], {})
                    for field in metadata_fields:
                        pair[f"{side}_{field}"] = document.get(field)
                context_pairs.append(pair)
        write_json(output / "review_context.json", {"audit_identity": audit_identity, "pairs": context_pairs})
        summary = {"release_id": release_id, "audit_identity": audit_identity, "sample_count": sample_count, **counts,
                   "review_queue_count": len(review), "synthetic": run_synthetic_audit(session),
                   "changes_release": False, "semantic_recall_measured": False}
        write_json(output / "summary.json", summary)
        return {**summary, "output_dir": str(output)}
    finally:
        if owned:
            session.stop()
