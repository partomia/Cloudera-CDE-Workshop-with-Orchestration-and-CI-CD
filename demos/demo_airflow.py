"""
POC Demo 3 — Airflow DAG Status via CDE

Press Play to run. No arguments needed.

What this demonstrates:
  - Lists Spark jobs deployed from this GitHub repo (Git integration)
  - Shows the Airflow DAG status on CDE
  - Shows recent DAG runs with pass/fail per task
  - Optionally triggers a new DAG run
"""

import json
import subprocess
import sys

# ── Configuration ─────────────────────────────────────────────────────────────
DAG_JOB_NAME = "retail-etl-pipeline-dag"   # CDE Airflow job name
TRIGGER_RUN  = False                        # Set True to trigger a new DAG run

SPARK_JOBS = [
    "workshop-ingest-raw",
    "workshop-validate-data",
    "workshop-transform",
    "workshop-load-curated",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def cde(*args) -> dict | list | None:
    """Run a CDE CLI command with JSON output. Returns parsed result or None on error."""
    cmd = ["cde"] + list(args) + ["--output", "json"]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        return None
    try:
        return json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return None


def cde_text(*args) -> str:
    """Run a CDE CLI command and return raw text output."""
    result = subprocess.run(["cde"] + list(args), capture_output=True, text=True)
    return result.stdout.strip() or result.stderr.strip()


def status_icon(status: str) -> str:
    icons = {
        "succeeded": "✓",
        "success":   "✓",
        "failed":    "✗",
        "running":   "⟳",
        "starting":  "⟳",
        "queued":    "○",
        "skipped":   "–",
    }
    return icons.get((status or "").lower(), "?")


# ── Main ──────────────────────────────────────────────────────────────────────

sep("STEP 1 — DEPLOYED CDE SPARK JOBS  (sourced from GitHub repo)")

all_jobs_found = True
for job_name in SPARK_JOBS:
    info = cde("job", "describe", "--name", job_name)
    if info:
        modified = info.get("modified", info.get("created", "N/A"))
        print(f"  {status_icon('succeeded')}  {job_name:<35}  last updated: {modified}")
    else:
        print(f"  ✗  {job_name:<35}  NOT FOUND — run scripts/deploy_jobs.sh")
        all_jobs_found = False

if not all_jobs_found:
    print("\n  Tip: deploy jobs first:")
    print("       ./scripts/deploy_jobs.sh")


sep("STEP 2 — AIRFLOW DAG")

dag_info = cde("job", "describe", "--name", DAG_JOB_NAME)
if not dag_info:
    print(f"  ✗  DAG '{DAG_JOB_NAME}' not found on CDE.")
    print("     Deploy it first: ./scripts/deploy_dag.sh")
    sys.exit(1)

dag_status  = dag_info.get("status", "N/A")
dag_created = dag_info.get("created", "N/A")
dag_file    = dag_info.get("airflowJobDetails", {}).get("dagFile", "N/A")

print(f"  DAG job   : {DAG_JOB_NAME}")
print(f"  DAG file  : {dag_file}")
print(f"  Status    : {status_icon(dag_status)}  {dag_status}")
print(f"  Created   : {dag_created}")


sep("STEP 3 — RECENT DAG RUNS")

runs = cde("run", "list", "--filter", f"job-name[eq]{DAG_JOB_NAME}")
if not runs:
    print(f"  No runs found for '{DAG_JOB_NAME}'.")
    print("  Trigger the DAG from the CDE Airflow UI, or set TRIGGER_RUN=True above.")
else:
    # Show latest 5 runs
    recent = runs[:5] if isinstance(runs, list) else []
    print(f"  Showing {len(recent)} most recent run(s):\n")

    for run in recent:
        run_id     = run.get("id", "?")
        run_status = run.get("status", "?")
        started    = run.get("started", "?")
        ended      = run.get("ended", "—")
        icon       = status_icon(run_status)

        print(f"  {icon}  Run {run_id}   {run_status:<12}  started: {started}  ended: {ended}")

        # Show task-level detail if available
        tasks = run.get("tasks") or []
        for task in tasks:
            task_name   = task.get("name", "?")
            task_status = task.get("status", "?")
            task_icon   = status_icon(task_status)
            print(f"       {task_icon}  {task_name:<30}  {task_status}")

        print()


sep("STEP 4 — PIPELINE FLOW")

print("""
  ingest_raw
      │  Reads inline data → Parquet (raw zone)
      ▼
  validate_data
      │  Great Expectations checks on raw data
      │  Any failure here stops the pipeline
      ▼
  transform
      │  Clean → Enrich → Aggregate
      ▼
  load_curated
         Writes curated tables + registers in Hive
""")

print("  DAG runs daily at 06:00 UTC  (schedule_interval: '0 6 * * *')")
print("  Trigger manually: CDE Airflow UI → DAGs → retail_etl_pipeline → ▶ Trigger\n")


# ── Optional: trigger a new run ───────────────────────────────────────────────

if TRIGGER_RUN:
    sep("TRIGGERING NEW DAG RUN")
    output = cde_text("job", "run", "--name", DAG_JOB_NAME)
    print(f"  {output}")
    print("  Monitor progress in the CDE Airflow UI.")
