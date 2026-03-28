"""
Module 5 - Airflow DAG
Orchestrates the end-to-end retail ELT pipeline on Cloudera CDE.

Job execution order:
  ingest_raw → validate_data → transform → load_curated

Uses CDEJobRunOperator to trigger CDE Spark jobs by name.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator

# ── Configuration ────────────────────────────────────────────────────────────
# S3 is optional. If S3_BUCKET is not set, jobs fall back to inline sample
# data and local CDE storage (/tmp/workshop/...) automatically.
#
# To enable S3, set these Airflow Variables in the CDE Airflow UI or CLI:
#   airflow variables set S3_BUCKET     "s3://your-bucket"
#   airflow variables set HIVE_DATABASE "workshop_db"
#   airflow variables set GE_ROOT_DIR   "/app/mount/great_expectations"

S3_BUCKET   = Variable.get("S3_BUCKET",    default_var="")
HIVE_DB     = Variable.get("HIVE_DATABASE", default_var="")
GE_ROOT_DIR = Variable.get("GE_ROOT_DIR",  default_var="")

# Only build S3 paths if a bucket is actually configured
USE_S3 = bool(S3_BUCKET)

LANDING_PATH   = f"{S3_BUCKET}/landing"   if USE_S3 else ""
RAW_PATH       = f"{S3_BUCKET}/raw"       if USE_S3 else ""
VALIDATED_PATH = f"{S3_BUCKET}/validated" if USE_S3 else ""
CURATED_PATH   = f"{S3_BUCKET}/curated"   if USE_S3 else ""

# ── Default args ─────────────────────────────────────────────────────────────
default_args = {
    "owner": "workshop",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG ──────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="retail_etl_pipeline",
    description="Retail ELT pipeline: ingest → validate → transform → load",
    default_args=default_args,
    schedule_interval="*/5 * * * *",    # Every 5 minutes
    start_date=datetime(2025, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["workshop", "pyspark", "great-expectations"],
) as dag:

    ingest = CDEJobRunOperator(
        task_id="ingest_raw",
        job_name="workshop-ingest-raw",
        overrides={"spark": {"args": [LANDING_PATH, RAW_PATH]}} if USE_S3 else {},
        wait=True,
    )

    validate = CDEJobRunOperator(
        task_id="validate_data",
        job_name="workshop-validate-data",
        overrides={"spark": {"args": [RAW_PATH, GE_ROOT_DIR or "/app/mount/great_expectations"]}} if USE_S3 else {},
        wait=True,
    )

    transform = CDEJobRunOperator(
        task_id="transform",
        job_name="workshop-transform",
        overrides={"spark": {"args": [RAW_PATH, VALIDATED_PATH]}} if USE_S3 else {},
        wait=True,
    )

    load = CDEJobRunOperator(
        task_id="load_curated",
        job_name="workshop-load-curated",
        overrides={"spark": {"args": [VALIDATED_PATH, CURATED_PATH, HIVE_DB]}} if USE_S3 else {},
        wait=True,
    )

    # Pipeline dependency chain
    ingest >> validate >> transform >> load
