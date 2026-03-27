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
# Set these Airflow Variables in the CDE Airflow UI or via CLI:
#   airflow variables set S3_BUCKET          "s3://your-bucket"
#   airflow variables set HIVE_DATABASE      "workshop_db"
#   airflow variables set GE_ROOT_DIR        "/app/mount/great_expectations"

S3_BUCKET    = Variable.get("S3_BUCKET",    default_var="s3://workshop-bucket")
HIVE_DB      = Variable.get("HIVE_DATABASE", default_var="workshop_db")
GE_ROOT_DIR  = Variable.get("GE_ROOT_DIR",  default_var="/app/mount/great_expectations")

LANDING_PATH   = f"{S3_BUCKET}/landing"
RAW_PATH       = f"{S3_BUCKET}/raw"
VALIDATED_PATH = f"{S3_BUCKET}/validated"
CURATED_PATH   = f"{S3_BUCKET}/curated"

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
        variables={
            "landing_path": LANDING_PATH,
            "raw_path": RAW_PATH,
        },
        wait=True,
    )

    validate = CDEJobRunOperator(
        task_id="validate_data",
        job_name="workshop-validate-data",
        variables={
            "raw_path": RAW_PATH,
            "ge_root_dir": GE_ROOT_DIR,
        },
        wait=True,
    )

    transform = CDEJobRunOperator(
        task_id="transform",
        job_name="workshop-transform",
        variables={
            "raw_path": RAW_PATH,
            "validated_path": VALIDATED_PATH,
        },
        wait=True,
    )

    load = CDEJobRunOperator(
        task_id="load_curated",
        job_name="workshop-load-curated",
        variables={
            "validated_path": VALIDATED_PATH,
            "curated_path": CURATED_PATH,
            "hive_database": HIVE_DB,
        },
        wait=True,
    )

    # Pipeline dependency chain
    ingest >> validate >> transform >> load
