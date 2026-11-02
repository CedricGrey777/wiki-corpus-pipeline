"""Real Spark transformations. No Data-Juicer/Ray runtime and no driver corpus collection."""

from __future__ import annotations

import hashlib
import unicodedata

from pyspark.sql import SparkSession, Window, functions as F
from pyspark.sql.types import StringType


def normalize_text(text: str | None) -> str | None:
    if text is None:
        return None
    if not text.strip():
        return ""
    text = unicodedata.normalize("NFC", text.replace("\r\n", "\n").replace("\r", "\n"))
    lines = [line.rstrip(" \t") for line in text.split("\n")]
    while lines and not lines[0]:
        lines.pop(0)
    while lines and not lines[-1]:
        lines.pop()
    output = []
    for line in lines:
        if line or not output or output[-1]:
            output.append(line)
    return "\n".join(output)


def content_split(content_hash: str) -> str:
    return "validation" if int(content_hash[:8], 16) % 100 < 5 else "train"


def text_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def spark_session(name="WikiCorpus", master="local[2]", driver_memory="3g"):
    if master not in ("local[1]", "local[2]"):
        raise ValueError("This local project supports local[1] or local[2]")
    return (SparkSession.builder.appName(name).master(master)
            .config("spark.driver.memory", driver_memory)
            .config("spark.sql.shuffle.partitions", "8")
            .config("spark.sql.files.maxPartitionBytes", str(64 * 1024 * 1024))
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.ui.enabled", "false")
            .getOrCreate())


def clean_frame(frame, batch_id: str):
    required = ("id", "url", "title", "text")
    fields = {field.name: field.dataType for field in frame.schema.fields}
    if any(name not in fields for name in required):
        raise ValueError("Missing source columns: id, url, title, text are required")
    if any(not isinstance(fields[name], StringType) for name in required):
        raise ValueError("Source columns must all have string types")
    frame = frame.select(*required)
    clean = F.udf(normalize_text, StringType())
    frame = frame.withColumn("clean_text", clean("text"))
    checks = [("missing_id", F.col("id").isNull() | (F.length(F.trim("id")) == 0)),
              ("missing_url", F.col("url").isNull() | (F.length(F.trim("url")) == 0)),
              ("missing_title", F.col("title").isNull() | (F.length(F.trim("title")) == 0)),
              ("empty_text", F.col("clean_text").isNull() | (F.length(F.trim("clean_text")) == 0))]
    reasons = F.concat_ws(",", *[F.when(condition, F.lit(name)) for name, condition in checks])
    frame = frame.withColumn("reasons", reasons)
    quarantine = frame.filter(F.col("reasons") != "").select("id", "url", "title", "text", "reasons")
    valid = frame.filter(F.col("reasons") == "")
    lines = F.filter(F.split(F.col("clean_text"), "\n"), lambda line: F.length(F.trim(line)) > 0)
    repeated_ratio = F.when(F.size(lines) > 0, (F.size(lines) - F.size(F.array_distinct(lines))) / F.size(lines)).otherwise(F.lit(0.0))
    valid = (valid.withColumn("doc_id", F.concat(F.lit("wikipedia:zh:"), F.col("id")))
             .withColumn("batch_id", F.lit(batch_id))
             .withColumn("source_record_hash", F.sha2(F.to_json(F.struct(*[F.col(c) for c in required])), 256))
             .withColumn("content_hash", F.sha2("clean_text", 256))
             .withColumn("repeated_line_ratio", repeated_ratio)
             .withColumn("quality_flags", F.concat_ws(",", F.when(F.length("clean_text") < 100, F.lit("short_text")), F.when(F.length("clean_text") > 100_000, F.lit("long_text")), F.when(F.col("repeated_line_ratio") > 0.5, F.lit("repeated_lines"))))
             .select("batch_id", "doc_id", "url", "title", "clean_text", "content_hash", "source_record_hash", "quality_flags", "repeated_line_ratio"))
    return valid, quarantine


def deduplicate(frame):
    # Conflicts are checked across every included batch, not only inside a shard.
    conflicts = frame.groupBy("doc_id").agg(F.countDistinct("source_record_hash").alias("variants")).filter("variants > 1")
    if conflicts.limit(1).count():
        raise ValueError("Conflicting source ID: different source records share one article ID")
    by_source = Window.partitionBy("doc_id").orderBy("batch_id", "source_record_hash")
    unique_sources = frame.withColumn("source_rank", F.row_number().over(by_source)).filter("source_rank=1").drop("source_rank")
    # Equality of clean_text is part of the grouping, rather than assuming hashes prove equality.
    by_content = Window.partitionBy("content_hash", "clean_text").orderBy("doc_id")
    ranked = unique_sources.withColumn("representative_doc_id", F.first("doc_id").over(by_content))
    duplicates = ranked.filter(F.col("doc_id") != F.col("representative_doc_id")).select("doc_id", "representative_doc_id", "content_hash")
    accepted = (ranked.filter(F.col("doc_id") == F.col("representative_doc_id"))
                .withColumn("split", F.when(F.conv(F.substring("content_hash", 1, 8), 16, 10).cast("long") % 100 < 5, F.lit("validation")).otherwise(F.lit("train")))
                .select("doc_id", "url", "title", "clean_text", "content_hash", "quality_flags", "repeated_line_ratio", "split"))
    return unique_sources, accepted, duplicates
