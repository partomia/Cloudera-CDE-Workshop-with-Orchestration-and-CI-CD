# Module 06 — Orchestrate with Airflow on CDE

**Estimated time:** 40 minutes
**Prerequisite:** All four CDE jobs deployed (Modules 02–05)
**Exercise file:** `exercise_dag.py`
**Reference solution:** `dags/etl_pipeline_dag.py`

---

## Learning Objectives

By the end of this module you will be able to:

- Write an Airflow DAG that orchestrates four CDE Spark jobs in sequence
- Use `CDEJobRunOperator` to trigger a CDE job and wait for it to complete
- Configure Airflow Variables for environment-specific settings
- Trigger the DAG from the CDE Airflow UI and read task logs

---

## Key Concepts

**Why Airflow?**
Running four `cde job run` commands manually works — but it has no retry logic, no scheduling, and no visibility into what succeeded or failed. Airflow solves all three: it retries failed tasks automatically, runs on a schedule, and gives you a visual graph of every run.

**DAG (Directed Acyclic Graph)**
A DAG is a definition of tasks and their dependencies. "Directed" means the order is fixed. "Acyclic" means there are no loops. Your pipeline looks like:

```
ingest_raw ──► validate_data ──► transform ──► load_curated
```

If any task fails, Airflow stops and marks all downstream tasks as skipped — preventing bad data from flowing forward.

**`CDEJobRunOperator`**
This is a Cloudera-provided Airflow operator. It submits a `cde job run` and polls until the CDE job finishes. `wait=True` means the Airflow task does not complete until the CDE job succeeds or fails.

**`Variable.get()`**
Airflow Variables are key-value pairs stored in the Airflow database. Using `Variable.get("S3_BUCKET", default_var="s3://workshop-bucket")` allows the same DAG code to work in different environments (dev, prod) by changing variables — without changing code.

**`catchup=False`**
If the DAG is paused and then re-enabled, `catchup=False` prevents Airflow from backfilling all the missed scheduled runs. It just runs the most recent one.

**The `>>` operator**
`task_a >> task_b` sets `task_b` as a downstream dependency of `task_a`. Airflow will not start `task_b` until `task_a` succeeds. You can chain multiple tasks: `a >> b >> c >> d`.

---

## Before You Start

Set these Airflow Variables in the CDE Airflow UI: **Admin → Variables → `+`**

| Key | Example Value |
|-----|---------------|
| `S3_BUCKET` | `s3://your-bucket` |
| `HIVE_DATABASE` | `workshop_db` |
| `GE_ROOT_DIR` | `/app/mount/great_expectations` |

Also confirm all four CDE jobs exist:
```bash
cde job list | grep workshop
```
You should see: `workshop-ingest-raw`, `workshop-validate-data`, `workshop-transform`, `workshop-load-curated`.

---

## Steps

1. Open `modules/06-orchestrate/exercise_dag.py`
2. Implement the 7 TODOs in order
3. Deploy the DAG to CDE:
   ```bash
   ./scripts/deploy_dag.sh
   ```
4. In the CDE Airflow UI, navigate to **DAGs → `retail_etl_pipeline`**
5. Click **Trigger DAG** (play button)
6. Watch the graph view — tasks turn green as they complete
7. Click any task box → **View Log** to see the Spark job output

---

## What to Expect in the Graph View

```
[ ingest_raw ]  ──►  [ validate_data ]  ──►  [ transform ]  ──►  [ load_curated ]
   running              queued               queued              queued
```

Each box turns green on success, red on failure. If `validate_data` fails (bad data), `transform` and `load_curated` are automatically skipped.

---

## Next Module

Move to **[Module 07 — CI/CD with GitHub Actions](../07-cicd/README.md)**.
