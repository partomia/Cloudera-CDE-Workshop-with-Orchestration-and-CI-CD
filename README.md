# Spark Workshop: PySpark + Great Expectations + Airflow on Cloudera CDE

End-to-end data engineering workshop using:
- **PySpark** for ELT/ETL
- **Great Expectations** for data quality validation
- **Apache Airflow** (via Cloudera CDE) for orchestration
- **Cloudera Data Engineering (CDE)** on AWS
- **GitHub Actions** for CI/CD

---

## Architecture

```
GitHub Repo
    │
    ├── CI (GitHub Actions) ── PR checks: lint, unit tests, GE validation
    └── CD (GitHub Actions) ── Merge to main: deploy jobs + DAGs to CDE
                                        │
                            Cloudera Data Engineering (CDE) on AWS
                                        │
                    ┌───────────────────┼───────────────────┐
                    │                   │                   │
              Airflow Service     Spark Job Service        S3 Data Lake
              (orchestration)     (job execution)       ┌── raw/
                    │                   │               ├── validated/
                    └── DAG triggers ───┘               └── curated/
                        Spark jobs
```

## Pipeline Stages

| Stage | Job | Description |
|-------|-----|-------------|
| 1. Ingest | `jobs/ingest/ingest_raw.py` | Read CSV/JSON from S3 landing zone → raw zone |
| 2. Validate | `jobs/validate/validate_data.py` | Great Expectations quality checks on raw data |
| 3. Transform | `jobs/transform/transform.py` | PySpark ELT transformations |
| 4. Load | `jobs/load/load_curated.py` | Write curated Parquet to S3 / Hive |

---

## Repository Structure

```
├── .github/workflows/      # CI/CD GitHub Actions
├── dags/                   # Airflow DAGs
├── jobs/                   # PySpark jobs (ingest, validate, transform, load)
├── great_expectations/     # GE project config, expectations, checkpoints
├── resources/              # CDE resource definitions, requirements
├── scripts/                # CDE CLI deployment scripts
├── tests/                  # Unit and integration tests
└── data/sample/            # Sample datasets for local dev
```

---

## Prerequisites

- Python 3.8+
- Java 11 (for local Spark)
- CDE CLI configured with your CDE virtual cluster endpoint
- AWS credentials (S3 access)
- Docker + Docker Compose (for local dev)

---

## IDE Setup (IntelliJ / PyCharm)

### 1. Create the `cde-spark` virtual environment

```bash
python3.11 -m venv ~/venvs/cde-spark
source ~/venvs/cde-spark/bin/activate
```

### 2. Install CDE Spark Connect packages

Download the tarballs from **CDE console → Sessions → Spark Connect → Configuration**, then:

```bash
pip install /path/to/cdeconnect-*.tar.gz
pip install /path/to/pyspark-3.5.*.tar.gz   # version must match cdeconnect
```

### 3. Configure CDE CLI

Ensure `~/.cde/config.yaml` has your virtual cluster endpoint:

```yaml
vcluster-endpoint: https://<your-cde-endpoint>/dex/api/v1
```

### 4. Register the interpreter in IntelliJ

1. **File → Project Structure → SDKs → `+` → Add Python SDK → Existing environment**
2. Set path to: `~/venvs/cde-spark/bin/python`
3. Name it `cde-spark`
4. **File → Project Structure → Project → SDK**: select `cde-spark`

### 5. Fix run configurations

If you see _"Cannot find Python interpreter for this run configuration"_:

1. **Run → Edit Configurations** → select the affected script
2. Set **Python interpreter** to **"Project Default (Python 3.11 (cde-spark))"**
3. Click **Apply → OK**

### 6. Verify the connection

```bash
source ~/venvs/cde-spark/bin/activate
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

## Quick Start (Local Dev)

```bash
# Install dev dependencies
pip install -r requirements.txt

# Start local Spark + Airflow
docker-compose up -d

# Run unit tests
make test

# Run GE validation locally
make validate
```

## Deploy to CDE

```bash
# Configure CDE CLI
cde configure

# Deploy all jobs and DAG
make deploy
```

---

## Workshop Modules

1. **Module 1** — Environment setup (CDE, S3, IAM)
2. **Module 2** — PySpark ingest job
3. **Module 3** — Data quality with Great Expectations
4. **Module 4** — ELT transformations
5. **Module 5** — Airflow orchestration on CDE
6. **Module 6** — CI/CD with GitHub Actions

---

## Tech Stack

| Tool | Version |
|------|---------|
| PySpark | 3.3.x |
| Great Expectations | 0.18.x |
| Apache Airflow | 2.6.x |
| Python | 3.8+ |
| CDE Runtime | Cloudera 7.x |
