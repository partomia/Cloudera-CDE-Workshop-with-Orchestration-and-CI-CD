"""
Module 06 Exercise — Airflow DAG: Orchestrate the Retail ELT Pipeline

Complete each TODO to wire the four CDE Spark jobs into a DAG.

Pre-requisites:
  - All four CDE jobs deployed (run scripts/deploy_jobs.sh)
  - Airflow Variables set in CDE Airflow UI:
      S3_BUCKET, HIVE_DATABASE, GE_ROOT_DIR

Deploy:
    ./scripts/deploy_dag.sh

Reference solution: dags/etl_pipeline_dag.py
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator


# ── Configuration ─────────────────────────────────────────────────────────────

# TODO 1: Read three Airflow Variables using Variable.get()
#
# Use default_var so the DAG parses cleanly even if a variable is not yet set.
#
# Variables to read:
#   S3_BUCKET      → default "s3://workshop-bucket"
#   HIVE_DATABASE  → default "workshop_db"
#   GE_ROOT_DIR    → default "/app/mount/great_expectations"
#
# Hint: Variable.get("S3_BUCKET", default_var="s3://workshop-bucket")

S3_BUCKET   = raise NotImplementedError
HIVE_DB     = raise NotImplementedError
GE_ROOT_DIR = raise NotImplementedError

# Derived paths — no TODO needed
LANDING_PATH   = f"{S3_BUCKET}/landing"
RAW_PATH       = f"{S3_BUCKET}/raw"
VALIDATED_PATH = f"{S3_BUCKET}/validated"
CURATED_PATH   = f"{S3_BUCKET}/curated"


# ── Default args ──────────────────────────────────────────────────────────────

# TODO 2: Define the default_args dictionary
#
# Required keys:
#   owner           = "workshop"
#   depends_on_past = False          (each run is independent)
#   email_on_failure= True
#   email_on_retry  = False
#   retries         = 1              (retry once before marking as failed)
#   retry_delay     = timedelta(minutes=5)
#
# Hint: default_args = {"owner": "workshop", "depends_on_past": False, ...}

default_args = raise NotImplementedError


# ── DAG ───────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="retail_etl_pipeline",
    description="Retail ELT pipeline: ingest → validate → transform → load",
    default_args=default_args,
    schedule_interval="0 6 * * *",   # Daily at 06:00 UTC
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["workshop", "pyspark", "great-expectations"],
) as dag:

    # TODO 3: Define the 'ingest' task
    #
    # Use CDEJobRunOperator with:
    #   task_id  = "ingest_raw"
    #   job_name = "workshop-ingest-raw"   ← must match the deployed CDE job name
    #   variables = {
    #       "landing_path": LANDING_PATH,
    #       "raw_path": RAW_PATH,
    #   }
    #   wait = True   ← wait for the CDE job to finish before marking task complete
    #
    # Hint: CDEJobRunOperator(task_id="ingest_raw", job_name="...", variables={...}, wait=True)

    ingest = raise NotImplementedError

    # TODO 4: Define the 'validate' task
    #
    #   task_id  = "validate_data"
    #   job_name = "workshop-validate-data"
    #   variables = {
    #       "raw_path": RAW_PATH,
    #       "ge_root_dir": GE_ROOT_DIR,
    #   }
    #   wait = True

    validate = raise NotImplementedError

    # TODO 5: Define the 'transform' task
    #
    #   task_id  = "transform"
    #   job_name = "workshop-transform"
    #   variables = {
    #       "raw_path": RAW_PATH,
    #       "validated_path": VALIDATED_PATH,
    #   }
    #   wait = True

    transform = raise NotImplementedError

    # TODO 6: Define the 'load' task
    #
    #   task_id  = "load_curated"
    #   job_name = "workshop-load-curated"
    #   variables = {
    #       "validated_path": VALIDATED_PATH,
    #       "curated_path": CURATED_PATH,
    #       "hive_database": HIVE_DB,
    #   }
    #   wait = True

    load = raise NotImplementedError

    # TODO 7: Set the dependency chain
    #
    # Tasks must run in this order: ingest → validate → transform → load
    # If any task fails, all downstream tasks are automatically skipped.
    #
    # Hint: use the >> operator   e.g.  task_a >> task_b >> task_c >> task_d

    raise NotImplementedError
