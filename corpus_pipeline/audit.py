"""Bounded near-duplicate evidence, never a production deletion stage."""

from __future__ import annotations

import csv
import random
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, StringType

from .core import new_stage, read_ready, root_path, write_json
from .transform import spark_session


def shingles(text: str) -> set[str]:
    return {text[i:i + 5] for i in range(max(0, len(text) - 4))}


def jaccard(left: str, right: str) -> float:
    a, b = shingles(left), shingles(right)
    return len(a & b) / len(a | b) if a or b else 1.0


def synthetic_pairs() -> list[dict]:
    """Twenty lightly changed and twenty unrelated pairs; only test fixtures, not corpus."""
    rng = random.Random(42)
    alphabet = "天地玄黃宇宙洪荒日月盈昃辰宿列張寒來暑往秋收冬藏甲乙丙丁戊己庚辛壬癸"
    result = []
    for index in range(20):
        base = "".join(rng.choices(alphabet, k=400))
        edited = base[:200] + "測試" + base[202:]
        unrelated = "".join(rng.choices(alphabet, k=400))
        result.extend([{"pair_id": f"near-{index:02}", "left": base, "right": edited, "expected_near": True},
                       {"pair_id": f"far-{index:02}", "left": base, "right": unrelated, "expected_near": False}])
    return result


def candidate_pairs(frame, threshold=0.8):
    from pyspark.ml.feature import HashingTF, MinHashLSH
    tokenize = F.udf(lambda text: sorted(shingles(text)), ArrayType(StringType()))
    tokens = frame.withColumn("shingles", tokenize("clean_text"))
    features = HashingTF(inputCol="shingles", outputCol="features", numFeatures=262144, binary=True).transform(tokens)
    model = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=8, seed=42).fit(features)
    joined = model.approxSimilarityJoin(features, features, 1 - threshold, distCol="hashed_distance")
    exact = F.udf(jaccard, DoubleType())
    return (joined.filter(F.col("datasetA.doc_id") < F.col("datasetB.doc_id"))
            .select(F.col("datasetA.doc_id").alias("left_id"), F.col("datasetB.doc_id").alias("right_id"),
                    F.col("datasetA.clean_text").alias("left_text"), F.col("datasetB.clean_text").alias("right_text"), "hashed_distance")
            .withColumn("jaccard", exact("left_text", "right_text"))
            .filter(F.col("jaccard") >= threshold))


def run_synthetic_audit(session) -> dict:
    pairs = synthetic_pairs()
    rows = [(f"{pair['pair_id']}-{side}", pair[side]) for pair in pairs for side in ("left", "right")]
    frame = session.createDataFrame(rows, "doc_id string, clean_text string")
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
    owned = spark is None
    session = spark or spark_session("WikiCorpusAudit")
    try:
        docs = session.read.parquet(str(Path(release["directory"]) / "documents"))
        sample = (docs.filter(F.length("clean_text").between(100, 2000))
                  .orderBy(F.sha2("doc_id", 256), "doc_id").limit(2000).select("doc_id", "clean_text"))
        sample_count = sample.count()
        # Independent bounded local artifact, not a column appended to the released dataset.
        if sample_count:
            candidate_pairs(sample).write.mode("error").parquet(str(output / "candidate_pairs"))
            candidates = session.read.parquet(str(output / "candidate_pairs"))
            candidate_count = candidates.count()
            review = candidates.orderBy(F.desc("jaccard"), "left_id", "right_id").limit(50).collect()
        else:
            candidate_count, review = 0, []
        with (output / "manual_review.csv").open("w", encoding="utf-8-sig", newline="") as target:
            writer = csv.DictWriter(target, fieldnames=["left_id", "right_id", "jaccard", "human_label", "notes"])
            writer.writeheader()
            for row in review:
                writer.writerow({"left_id": row.left_id, "right_id": row.right_id, "jaccard": row.jaccard, "human_label": "", "notes": ""})
        summary = {"release_id": release_id, "sample_count": sample_count, "candidate_count": candidate_count,
                   "manual_review_count": len(review), "synthetic": run_synthetic_audit(session),
                   "changes_release": False, "semantic_recall_measured": False}
        write_json(output / "summary.json", summary)
        return {**summary, "output_dir": str(output)}
    finally:
        if owned:
            session.stop()
