# Data Engineering Workshop: PySpark + Great Expectations + Airflow on Cloudera CDE

Build a production-style data pipeline from scratch — running entirely on **Cloudera Data Engineering (CDE)** — and learn five real-world skills along the way:

| Skill | What you practise |
|-------|------------------|
| **IDE ↔ CDE Spark Connect** | Write and run PySpark from IntelliJ directly on a live CDE session |
| **ELT Pipeline** | Ingest → validate → transform → load a retail orders dataset |
| **Data Quality** | Great Expectations validates every batch and fails the pipeline on bad data |
| **Airflow Orchestration** | A DAG sequences all jobs on CDE, with automatic retry and alerting |
| **CI/CD** | GitHub Actions runs quality gates on every PR and deploys on merge to `main` |

---

## The Pipeline You Will Build

```
 CSV file (S3 landing)
        │
        ▼
  Step 1 ── Explore    — understand the raw dataset before touching it
        │
        ▼
  Step 2 ── Ingest     — CSV → Parquet, partitioned by date  (S3 raw zone)
        │
        ▼
  Step 3 ── Validate   — Great Expectations checks: nulls, types, ranges, categories
        │                 pipeline stops here if data is bad
        ▼
  Step 4 ── Transform  — clean, enrich (revenue, month, year), aggregate by customer & category
        │                 (S3 validated zone)
        ▼
  Step 5 ── Load       — write curated Parquet + register Hive tables  (S3 curated zone)
        │
        ▼
  Step 6 ── Orchestrate — Airflow DAG ties all five jobs together on CDE
        │
        ▼
  Step 7 ── CI/CD      — GitHub Actions: lint + test on PR, auto-deploy on merge
```

> **Note:** Validation runs between Ingest and Transform — this is intentional. Catching bad data early
> prevents corrupt records from propagating into your curated tables.

---

## Prerequisites

Make sure you have the following before starting:

- [ ] **Python 3.11** on your laptop (`python3.11 --version`)
- [ ] **IntelliJ IDEA** or **PyCharm** installed
- [ ] **Git** installed and configured
- [ ] Access to a **Cloudera CDE virtual cluster** — your instructor will provide the endpoint URL
- [ ] **AWS credentials** with read/write access to the workshop S3 bucket

---

## Environment Setup

Work through these steps once before the first module. They take about 15 minutes.

### Step 1 — Clone the repo

```bash
git clone <repo-url>
cd Spark-Workshop-Airflow-Git
```

### Step 2 — Create a Python virtual environment

```bash
python3.11 -m venv ~/venvs/cde-spark
source ~/venvs/cde-spark/bin/activate     # macOS / Linux
# .\venvs\cde-spark\Scripts\activate      # Windows
```

### Step 3 — Install the CDE Spark Connect packages

Download the two tarballs from **CDE console → Sessions → your session → Spark Connect → Configuration**, then:

```bash
pip install /path/to/cdeconnect-*.tar.gz
pip install /path/to/pyspark-3.5.*.tar.gz    # must match the cdeconnect version
pip install -r requirements.txt
```

> **Why do I need both tarballs?**
> `cdeconnect` is Cloudera's thin client that routes your Spark calls to the remote CDE virtual
> cluster. `pyspark` must be the exact version CDE uses so the wire protocol matches.

### Step 4 — Configure the CDE CLI

Ensure `~/.cde/config.yaml` points to your virtual cluster:

```yaml
vcluster-endpoint: https://<your-cde-endpoint>/dex/api/v1
```

Your instructor will give you this URL.

### Step 5 — Register the Python interpreter in IntelliJ

The project is pre-configured (`.idea/modules.xml` + `.idea/Spark-Workshop-Airflow-Git.iml`)
to use an SDK named **`Python 3.11 (cde-spark)`**. IntelliJ just needs to know where that
interpreter lives on your machine:

1. **File → Project Structure → SDKs → `+` → Add Python SDK → Existing environment**
2. Path: `~/venvs/cde-spark/bin/python`
3. Name it exactly **`Python 3.11 (cde-spark)`** (must match)
4. Click **OK / Apply** — IntelliJ will index the environment and the red
   _"No Python interpreter"_ banner will disappear automatically

> **Why does this work?** The `.iml` file tells IntelliJ to look for an SDK named
> `Python 3.11 (cde-spark)`. Once you register it once, all run configurations inherit it.
> You never need to set it per-file again.

> **Troubleshooting:** If you still see _"Cannot find Python interpreter"_ after adding the SDK,
> go to **File → Project Structure → Project** and confirm the **Project SDK** dropdown shows
> `Python 3.11 (cde-spark)`. If it shows `<No SDK>`, select it from the dropdown.

### Step 6 — Verify your connection

Open `tests/test_cde_connect.py`, set `CDE_SESSION_NAME` at the top to your session name, then run it:

```bash
python tests/test_cde_connect.py
```

Expected output:
```
Connecting to CDE session: <session-name>
Spark version  : 3.5.x
spark.range(10).count() = 10
SUCCESS: IDE -> CDE Spark Connect session is working.
```

If you see this, you are ready to start.

---

## Important: Spark Connect Coding Patterns

The PySpark version bundled with CDE uses a JVM-bridge implementation of
`pyspark.sql.functions` (`F.col()`, `F.sum()`, `F.upper()`, etc.) that requires a local
`SparkContext`. Spark Connect has no local JVM, so those calls fail with:

```
AssertionError: SparkContext._active_spark_context is not None
```

**All code in this project uses Spark Connect-safe alternatives:**

| Instead of | Use |
|---|---|
| `F.col("qty").cast(IntegerType())` | `selectExpr("CAST(qty AS INT) AS qty")` |
| `F.upper(F.trim(F.col("cat")))` | `selectExpr("UPPER(TRIM(cat)) AS cat")` |
| `F.sum("revenue")` in `.agg()` | `spark.sql("SELECT SUM(revenue) ...")` via temp view |
| `F.col("x").desc()` | `.orderBy("x", ascending=False)` |
| `withColumn("ts", current_timestamp())` | `selectExpr("current_timestamp() AS ts")` |

These patterns work identically on Spark Connect (IDE → CDE) and classic Spark (unit tests, CDE jobs).

---

## Run the Demos First

Before diving into exercises, run the demo scripts to see the full pipeline working end-to-end.
Each script connects to your CDE session and prints results — no arguments needed, no S3 required.

Set your session name at the top of each file:
```python
CDE_SESSION_NAME = "your-session-name"   # one line at the top of each demo
```

Then run in order:

| # | Script | What it shows |
|---|--------|--------------|
| 1 | `demos/demo_explore.py` | Schema inspection, row counts, null checks, distributions |
| 2 | `demos/demo_etl.py` | Full pipeline on inline data: raw → clean → enrich → aggregate |

> The demos use **inline sample data** (no S3 dependency) so you can run them immediately after
> completing environment setup.

---

## Workshop Modules

Now work through the modules in order. Each module has:
- A **README** with concepts and step-by-step instructions
- An **exercise file** with `TODO` items for you to complete
- A **reference solution** in `jobs/` (modules 02–05) or `dags/` (module 06)

| Module | Topic | Exercise file | Time |
|--------|-------|--------------|------|
| [01 — Explore](modules/01-explore/README.md) | Explore the dataset with PySpark | `demos/demo_explore.py` | 20 min |
| [02 — Ingest](modules/02-ingest/README.md) | CSV → partitioned Parquet | `modules/02-ingest/exercise_ingest.py` | 30 min |
| [03 — Transform](modules/03-transform/README.md) | Clean, enrich, aggregate | `modules/03-transform/exercise_transform.py` | 45 min |
| [04 — Load](modules/04-load/README.md) | Curated Parquet + Hive tables | `modules/04-load/exercise_load.py` | 30 min |
| [05 — Data Quality](modules/05-data-quality/README.md) | Great Expectations validation | `modules/05-data-quality/exercise_validate.py` | 45 min |
| [06 — Orchestrate](modules/06-orchestrate/README.md) | Airflow DAG on CDE | `modules/06-orchestrate/exercise_dag.py` | 40 min |
| [07 — CI/CD](modules/07-cicd/README.md) | GitHub Actions: lint, test, deploy | *(config files)* | 30 min |

**Total time:** ~4 hours

> Stuck on a TODO? The reference solution is always one folder away:
> `jobs/` for modules 02–05, `dags/` for module 06.

---

## What Each Step Produces

| Step | Input | Output | Location |
|------|-------|--------|----------|
| Ingest | `retail_orders.csv` | Parquet partitioned by `order_date` | `s3://bucket/raw/` |
| Validate | raw Parquet | Pass/fail report + HTML Data Docs | exits 0 or 1 |
| Transform | raw Parquet | `orders/`, `customer_summary/`, `category_summary/` | `s3://bucket/validated/` |
| Load | validated Parquet | Hive tables in `workshop_db` | `s3://bucket/curated/` |
| Airflow | all four jobs | Scheduled daily pipeline at 06:00 UTC | CDE Airflow UI |

---

## Running Jobs on CDE

Once you have completed the exercises, deploy and run the reference solutions as CDE jobs:

```bash
# Ingest
cde job run --name workshop-ingest-raw \
  --arg s3://your-bucket/landing \
  --arg s3://your-bucket/raw

# Validate
cde job run --name workshop-validate-data \
  --arg s3://your-bucket/raw \
  --arg /app/mount/great_expectations

# Transform
cde job run --name workshop-transform \
  --arg s3://your-bucket/raw \
  --arg s3://your-bucket/validated

# Load
cde job run --name workshop-load-curated \
  --arg s3://your-bucket/validated \
  --arg s3://your-bucket/curated \
  --arg workshop_db
```

After load completes, verify the tables in Cloudera:
```sql
SELECT * FROM workshop_db.orders           LIMIT 10;
SELECT * FROM workshop_db.customer_summary ORDER BY total_revenue DESC;
SELECT * FROM workshop_db.category_summary ORDER BY total_revenue DESC;
```

---

## Airflow DAG

**File:** `dags/etl_pipeline_dag.py`

The DAG runs all four jobs in sequence, daily at 06:00 UTC:

```
ingest_raw  →  validate_data  →  transform  →  load_curated
```

If any job fails (e.g. a data quality check), the pipeline stops immediately — preventing bad data from reaching the curated zone.

**Set Airflow Variables before triggering:**
```bash
airflow variables set S3_BUCKET     "s3://your-bucket"
airflow variables set HIVE_DATABASE "workshop_db"
airflow variables set GE_ROOT_DIR   "/app/mount/great_expectations"
```

**Deploy and trigger:**
```bash
./scripts/deploy_dag.sh
# Then: CDE Airflow UI → DAGs → retail_etl_pipeline → Trigger DAG
```

---

## CI/CD with GitHub Actions

**Files:** `.github/workflows/ci.yml`, `.github/workflows/cd.yml`

**On every Pull Request (CI):**

| Check | Tool | Catches |
|-------|------|---------|
| Code style | `black` | Formatting |
| Code quality | `pylint` ≥ 8.0 | Bugs, bad patterns |
| Unit tests | `pytest` | Logic errors |
| GE dry-run | Great Expectations | Broken expectation suite |

**On merge to `main` (CD):** deploys all Spark jobs and the Airflow DAG to CDE automatically.

**Required GitHub Secrets** (Settings → Secrets and variables → Actions):

| Secret | Value |
|--------|-------|
| `CDP_ENDPOINT` | Cloudera CDP control plane URL |
| `CDP_ACCESS_KEY` | CDP access key ID |
| `CDP_PRIVATE_KEY` | CDP private key |
| `CDE_VC_ENDPOINT` | CDE virtual cluster endpoint URL |

---

## Repository Structure

```
├── .github/workflows/
│   ├── ci.yml                      # PR checks: lint, tests, GE dry-run
│   └── cd.yml                      # Deploy to CDE on merge to main
├── dags/
│   └── etl_pipeline_dag.py         # Airflow DAG (module 06)
├── data/sample/
│   └── retail_orders.csv           # 20-row sample dataset
├── demos/
│   ├── demo_explore.py             # One-click: dataset exploration on CDE
│   ├── demo_etl.py                 # One-click: full ELT pipeline on CDE
│   └── sample_data.py              # Inline data used by demos (no S3 needed)
├── great_expectations/
│   ├── expectations/
│   │   └── retail_raw_suite.json   # Data quality rules (module 05)
│   └── checkpoints/
│       └── raw_checkpoint.yml
├── jobs/                           # Reference solutions (deployed as CDE jobs)
│   ├── ingest/ingest_raw.py        # Module 02
│   ├── transform/transform.py      # Module 03
│   ├── load/load_curated.py        # Module 04
│   └── validate/validate_data.py   # Module 05
├── modules/                        # Hands-on exercises — work through in order
│   ├── 01-explore/
│   ├── 02-ingest/
│   ├── 03-transform/
│   ├── 04-load/
│   ├── 05-data-quality/
│   ├── 06-orchestrate/
│   └── 07-cicd/
├── scripts/
│   ├── deploy_jobs.sh              # Deploys all CDE Spark jobs
│   └── deploy_dag.sh               # Deploys the Airflow DAG
├── tests/
│   ├── test_cde_connect.py         # IDE → CDE connectivity smoke test
│   └── unit/                       # Unit tests for job logic
├── cde.py                          # CDE Spark Connect session helper
└── requirements.txt
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Runtime |
| PySpark | 3.5.x | Distributed data processing |
| Great Expectations | 0.18.x | Data quality validation |
| Apache Airflow | 2.6.x | Pipeline orchestration |
| Cloudera CDE | 7.x | Managed Spark + Airflow on AWS |
| GitHub Actions | — | CI/CD automation |
