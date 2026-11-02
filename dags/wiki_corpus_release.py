"""Manual source/scope/release parameters. Synthetic mode never downloads public text."""

import os
from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG, Param, get_current_context, task

from corpus_pipeline.core import SHARDS, download_shards
from corpus_pipeline.fixtures import create_fixtures
from corpus_pipeline.pipeline import build_release, ingest_clean, validate_publish
from corpus_pipeline.transform import spark_session

with DAG(
    dag_id="wiki_corpus_release",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    params={
        "source_kind": Param("synthetic", type="string", enum=["synthetic", "wikimedia"]),
        "shards": Param([SHARDS[2]], type="array", minItems=1, uniqueItems=True, items={"type": "string", "enum": list(SHARDS)}),
        "scope": Param("10k", type="string", enum=["10k", "100k", "full"]),
        "release_id": Param("synthetic-r001", type="string", pattern=r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,149}$"),
        "run_audit": Param(False, type="boolean"),
    },
    default_args={"retries": 1, "retry_delay": timedelta(seconds=30)},
    tags=["corpus", "batch", "local-development"],
) as dag:
    @task
    def prepare():
        params = get_current_context()["params"]
        root = os.environ.get("CORPUS_DATA_ROOT", "/opt/airflow/data")
        files = create_fixtures(f"{root}/fixtures") if params["source_kind"] == "synthetic" else download_shards(root, params["shards"])
        return {"root": root, "files": files, **{key: params[key] for key in ("source_kind", "scope", "release_id", "run_audit")}}

    @task
    def ingest(context):
        session = spark_session("CorpusAirflowIngest")
        try:
            batches = [ingest_clean(context["root"], path, context["scope"], context["source_kind"], spark=session)["batch_id"] for path in context["files"]]
            return {**context, "batch_ids": batches}
        finally:
            session.stop()

    @task
    def build(context):
        result = build_release(context["root"], context["release_id"], context["batch_ids"])
        return {**result, "run_audit": context["run_audit"]}

    @task
    def publish(context):
        result = validate_publish(context)
        return {"root": context["root"], "release_id": result["release_id"], "status": result["status"], "run_audit": context["run_audit"]}

    @task
    def audit(context):
        if not context["run_audit"]:
            return {"audit": "not_requested", "release_status": context["status"]}
        from corpus_pipeline.audit import audit_near_duplicates
        return audit_near_duplicates(context["root"], context["release_id"])

    audit(publish(build(ingest(prepare()))))
