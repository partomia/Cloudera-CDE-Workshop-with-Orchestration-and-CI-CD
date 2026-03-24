# Spark Workshop: PySpark + Great Expectations + Airflow on Cloudera CDE

This is a hands-on data engineering workshop. You will take a raw CSV file of retail orders,
process it through a full ELT pipeline on Cloudera Data Engineering (CDE), validate data quality,
and automate everything with Airflow and GitHub Actions.

---

## What You Will Build

```
Raw CSV file (retail orders)
       │
       ▼
  Step 1 ── Explore the dataset
       │
       ▼
  Step 2 ── Ingest: CSV → Parquet (raw zone on S3)
       │
       ▼
  Step 3 ── Transform: clean, enrich, aggregate (validated zone on S3)
       │
       ▼
  Step 4 ── Load: write curated tables + register in Hive (curated zone on S3)
       │
       ▼
  Step 5 ── Data Quality: Great Expectations checks on raw data
       │
       ▼
  Step 6 ── Orchestrate: Airflow DAG ties it all together on CDE
       │
       ▼
  Step 7 ── CI/CD: GitHub Actions automates testing and deployment
```

---

## Workshop Modules

Each step has a dedicated folder in `modules/` with a README, hands-on exercise file, and hints
pointing to the reference solution in `jobs/`. Work through them in order.

| Module | Topic | Exercise | Est. Time |
|--------|-------|----------|-----------|
| [01 - Explore](modules/01-explore/README.md) | Explore the dataset with PySpark | `exercise_explore.ipynb` | 20 min |
| [02 - Ingest](modules/02-ingest/README.md) | CSV → Parquet ingest job | `exercise_ingest.py` | 30 min |
| [03 - Transform](modules/03-transform/README.md) | Clean, enrich, aggregate | `exercise_transform.py` | 45 min |
| [04 - Load](modules/04-load/README.md) | Curated Parquet + Hive tables | `exercise_load.py` | 30 min |
| [05 - Data Quality](modules/05-data-quality/README.md) | Great Expectations validation | `exercise_validate.py` | 45 min |
| [06 - Orchestrate](modules/06-orchestrate/README.md) | Airflow DAG on CDE | `exercise_dag.py` | 40 min |
| [07 - CI/CD](modules/07-cicd/README.md) | GitHub Actions automation | *(config-based)* | 30 min |

> Stuck on a TODO? The reference solution is always in `jobs/` (modules 02–05) or `dags/` (module 06).

---

## Prerequisites

Before you begin, make sure you have:

- [ ] Access to a **Cloudera CDE virtual cluster** (your instructor will provide the endpoint)
- [ ] **Python 3.11** installed on your laptop
- [ ] **IntelliJ IDEA** (or PyCharm) installed
- [ ] **Git** installed and this repo cloned locally
- [ ] **AWS credentials** with read/write access to the workshop S3 bucket

---

## Environment Setup

### 1. Clone the repo

```bash
git clone <repo-url>
cd Spark-Workshop-Airflow-Git
```

### 2. Create the Python virtual environment

```bash
python3.11 -m venv ~/venvs/cde-spark
source ~/venvs/cde-spark/bin/activate
```

### 3. Install CDE Spark Connect packages

Download the tarballs from **CDE console → Sessions → Spark Connect → Configuration**, then:

```bash
pip install /path/to/cdeconnect-*.tar.gz
pip install /path/to/pyspark-3.5.*.tar.gz   # version must match cdeconnect
pip install -r requirements.txt
```

### 4. Configure the CDE CLI

Ensure `~/.cde/config.yaml` has your virtual cluster endpoint:

```yaml
vcluster-endpoint: https://<your-cde-endpoint>/dex/api/v1
```

### 5. Set up IntelliJ to use the right Python interpreter

1. **File → Project Structure → SDKs → `+` → Add Python SDK → Existing environment**
2. Set path to: `~/venvs/cde-spark/bin/python` — name it `cde-spark`
3. **File → Project Structure → Project → SDK**: select `cde-spark`
4. **Run → Edit Configurations** → for any run config, set **Python interpreter** to `Project Default (Python 3.11 (cde-spark))`

> **Troubleshooting:** If you see _"Cannot find Python interpreter for this run configuration"_,
> the run config has a stale interpreter override. Follow step 4 above to fix it.

### 6. Verify your connection to CDE

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

---

## Step 1 — Explore the Dataset

**File:** `data/sample/retail_orders.csv`

This is the raw data you will process. Open it and take a look:

| Column | Description |
|--------|-------------|
| `order_id` | Unique order identifier (e.g. ORD0001) |
| `customer_id` | Customer identifier (e.g. CUST001) |
| `product_id` | Product identifier (e.g. PROD001) |
| `order_date` | Date of order in `YYYY-MM-DD` format |
| `quantity` | Number of units ordered |
| `unit_price` | Price per unit in USD |
| `category` | Product category (electronics, books, clothing, food, furniture, sports) |

**Sample rows:**

```
order_id,customer_id,product_id,order_date,quantity,unit_price,category
ORD0001,CUST001,PROD001,2026-01-05,2,29.99,electronics
ORD0002,CUST002,PROD002,2026-01-06,1,9.99,books
ORD0003,CUST003,PROD003,2026-01-06,3,49.99,clothing
```

The file has **20 orders** across 5 customers and 6 product categories.

---

## Step 2 — Ingest: Raw CSV → Parquet

**File:** `jobs/ingest/ingest_raw.py`

**What it does:**
- Reads the CSV from the S3 landing zone
- Adds two audit columns: `_ingested_at` (timestamp) and `_source_file` (file path)
- Writes the result as **Parquet** to the S3 raw zone, partitioned by `order_date`

**Run on CDE:**

```bash
cde job run --name workshop-ingest-raw \
  --arg s3://your-bucket/landing \
  --arg s3://your-bucket/raw
```

**Output:** Parquet files at `s3://your-bucket/raw/` partitioned by `order_date=YYYY-MM-DD/`

---

## Step 3 — Transform: Clean, Enrich & Aggregate

**File:** `jobs/transform/transform.py`

**What it does:**

| Step | Action |
|------|--------|
| Clean | Drops rows missing `order_id`, `customer_id`, or `product_id`; filters out zero/negative quantities and prices; standardises `category` to uppercase |
| Enrich | Adds `revenue = quantity × unit_price`, `order_month`, `order_year` |
| Aggregate | Creates two summary tables: revenue by customer per month, revenue by category per month |

**Run on CDE:**

```bash
cde job run --name workshop-transform \
  --arg s3://your-bucket/raw \
  --arg s3://your-bucket/validated
```

**Output:** Three Parquet datasets at `s3://your-bucket/validated/`:
- `orders/` — enriched order-level data
- `customer_summary/` — monthly revenue per customer
- `category_summary/` — monthly revenue per product category

---

## Step 4 — Load: Write Curated Tables

**File:** `jobs/load/load_curated.py`

**What it does:**
- Reads the three validated datasets
- Writes them to the S3 **curated zone**
- Registers each as an **external Hive table** so you can query them with SQL in Cloudera

**Run on CDE:**

```bash
cde job run --name workshop-load-curated \
  --arg s3://your-bucket/validated \
  --arg s3://your-bucket/curated \
  --arg workshop_db
```

**Output:** Three queryable Hive tables in database `workshop_db`:

```sql
SELECT * FROM workshop_db.orders LIMIT 10;
SELECT * FROM workshop_db.customer_summary ORDER BY total_revenue DESC;
SELECT * FROM workshop_db.category_summary ORDER BY total_revenue DESC;
```

---

## Step 5 — Data Quality with Great Expectations

**Files:** `jobs/validate/validate_data.py`, `great_expectations/`

**What it does:** Runs a suite of automated checks on the raw data. If any check fails, the job
exits with an error and Airflow marks the task as failed — stopping bad data from flowing downstream.

**Checks configured** (`great_expectations/expectations/retail_raw_suite.json`):

| Check | Rule |
|-------|------|
| Row count | Table must not be empty |
| Required columns | `order_id`, `customer_id`, `product_id`, `order_date`, `quantity`, `unit_price`, `category` must all exist |
| No nulls | `order_id` and `customer_id` must never be null |
| Uniqueness | `order_id` must be a primary key (no duplicates) |
| Valid range | `quantity` between 1–10,000; `unit_price` between $0.01–$100,000 |
| Date format | `order_date` must match `YYYY-MM-DD` |
| Known categories | `category` must be one of: ELECTRONICS, CLOTHING, FOOD, FURNITURE, SPORTS, BOOKS, TOYS |

**Run on CDE:**

```bash
cde job run --name workshop-validate-data \
  --arg s3://your-bucket/raw \
  --arg /app/mount/great_expectations
```

> In the full pipeline, validation runs **after ingest and before transform** to catch bad data early.

---

## Step 6 — Orchestrate with Airflow

**File:** `dags/etl_pipeline_dag.py`

The Airflow DAG ties all four jobs together and runs them in sequence every day at 06:00 UTC:

```
ingest_raw → validate_data → transform → load_curated
```

If any step fails (e.g. data quality checks), the pipeline stops and does not proceed to the next step.

**Set these Airflow Variables** in the CDE Airflow UI before triggering the DAG:

```bash
airflow variables set S3_BUCKET      "s3://your-bucket"
airflow variables set HIVE_DATABASE  "workshop_db"
airflow variables set GE_ROOT_DIR    "/app/mount/great_expectations"
```

**Deploy the DAG to CDE:**

```bash
./scripts/deploy_dag.sh
```

**Trigger manually from CDE Airflow UI:** go to DAGs → `retail_etl_pipeline` → click **Trigger DAG**.

---

## Step 7 — CI/CD with GitHub Actions

**Files:** `.github/workflows/ci.yml`, `.github/workflows/cd.yml`

### CI — runs on every Pull Request

Automatically checks your code before it can be merged:

| Check | Tool | What it catches |
|-------|------|-----------------|
| Code style | `black` | Formatting issues |
| Code quality | `pylint` (min score 8.0) | Bugs, bad patterns |
| Unit tests | `pytest` | Logic errors in jobs |
| Data quality dry-run | Great Expectations | Broken expectation suite or sample data failures |

### CD — runs when code is merged to `main`

Automatically deploys to CDE:

1. Installs and configures the CDE CLI using GitHub Secrets
2. Deploys all Spark jobs (`scripts/deploy_jobs.sh`)
3. Deploys the Airflow DAG (`scripts/deploy_dag.sh`)

**GitHub Secrets required** (set in repo Settings → Secrets):

| Secret | Description |
|--------|-------------|
| `CDP_ENDPOINT` | Cloudera CDP control plane URL |
| `CDP_ACCESS_KEY` | CDP access key ID |
| `CDP_PRIVATE_KEY` | CDP private key |
| `CDE_VC_ENDPOINT` | CDE virtual cluster endpoint URL |

---

## Repository Structure

```
├── .github/workflows/
│   ├── ci.yml                  # PR checks: lint, tests, GE dry-run
│   └── cd.yml                  # Deploy to CDE on merge to main
├── dags/
│   └── etl_pipeline_dag.py     # Airflow DAG (Step 6)
├── data/sample/
│   └── retail_orders.csv       # Sample dataset (Step 1)
├── great_expectations/
│   ├── expectations/
│   │   └── retail_raw_suite.json   # Data quality rules (Step 5)
│   └── checkpoints/
│       └── raw_checkpoint.yml
├── jobs/
│   ├── ingest/ingest_raw.py        # Step 2 — reference solution
│   ├── transform/transform.py      # Step 3 — reference solution
│   ├── load/load_curated.py        # Step 4 — reference solution
│   └── validate/validate_data.py   # Step 5 — reference solution
├── modules/
│   ├── 01-explore/             # Dataset exploration
│   ├── 02-ingest/              # CSV → Parquet exercise
│   ├── 03-transform/           # Clean, enrich, aggregate exercise
│   ├── 04-load/                # Curated tables exercise
│   ├── 05-data-quality/        # Great Expectations exercise
│   ├── 06-orchestrate/         # Airflow DAG exercise
│   └── 07-cicd/                # CI/CD walkthrough
├── scripts/
│   ├── deploy_jobs.sh          # CDE job deployment
│   └── deploy_dag.sh           # CDE DAG deployment
├── tests/
│   ├── test_cde_connect.py     # IDE → CDE connectivity smoke test
│   └── unit/                   # Unit tests for jobs
└── requirements.txt
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| PySpark | 3.5.x | Data processing |
| Great Expectations | 0.18.x | Data quality |
| Apache Airflow | 2.6.x | Orchestration |
| Python | 3.11 | Runtime |
| Cloudera CDE | 7.x | Managed Spark + Airflow on AWS |
| GitHub Actions | — | CI/CD automation |
