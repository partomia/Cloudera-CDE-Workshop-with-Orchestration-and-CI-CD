# Module 05 — Data Quality with Great Expectations

**Estimated time:** 45 minutes
**Prerequisite:** Module 02 (raw Parquet must exist on S3)
**Exercise file:** `exercise_validate.py`
**Reference solution:** `jobs/validate/validate_data.py`
**Suite definition:** `great_expectations/expectations/retail_raw_suite.json`

---

## Learning Objectives

By the end of this module you will be able to:

- Explain the core Great Expectations concepts: DataContext, ExpectationSuite, Checkpoint, BatchRequest
- Read and understand the existing expectation suite for the retail dataset
- Implement a validate function using a `RuntimeBatchRequest` (passing a live Spark DataFrame)
- Interpret validation results and understand how a failed check stops the pipeline

---

## Key Concepts

**What is Great Expectations?**
Great Expectations (GE) is a library for defining, running, and documenting data quality checks. Instead of writing `assert df.count() > 0` scattered across your code, you declare all checks in a JSON file (the *expectation suite*) and run them in one go via a *checkpoint*.

**Core concepts:**

| Concept | What it is |
|---------|-----------|
| DataContext | The root configuration object. Loaded from a directory (`great_expectations/`) |
| ExpectationSuite | A named collection of checks (our file: `retail_raw_suite.json`) |
| Checkpoint | A named run configuration that connects a suite to a data source |
| BatchRequest | Tells GE which data to validate. We use `RuntimeBatchRequest` to pass a live DataFrame |

**`RuntimeBatchRequest`**
Instead of pointing GE at a file path, we pass a Spark DataFrame directly. This avoids reading the data twice and works seamlessly on CDE where data lives on S3.

**Exit codes and Airflow**
If any expectation fails, `validate()` returns `False` and the job exits with `sys.exit(1)`. Airflow's `CDEJobRunOperator` treats a non-zero exit code as task failure — stopping the pipeline before bad data reaches the transform and load steps.

---

## Expectation Suite Explained

Open `great_expectations/expectations/retail_raw_suite.json` and read it alongside this table:

| Check | Rule | Why |
|-------|------|-----|
| Row count | Table must not be empty | Catch empty files early |
| Column existence | All 7 columns must be present | Catch schema drift (upstream renamed a column) |
| `order_id` not null | Every row must have an order ID | Required for uniqueness checks downstream |
| `customer_id` not null | Every row must have a customer ID | Required for aggregations |
| `order_id` unique | No duplicate order IDs | `order_id` is a primary key |
| `quantity` range | Between 1 and 10,000 | Catch typos like 99999 |
| `unit_price` range | Between $0.01 and $100,000 | Catch zero-price or unrealistic values |
| `order_date` regex | Must match `YYYY-MM-DD` | Catch format changes in the source file |
| `category` set | Must be one of the 7 known categories | Catch new/misspelled categories |

---

## Steps

1. Open `modules/05-data-quality/exercise_validate.py`
2. Read `great_expectations/expectations/retail_raw_suite.json` to understand the checks
3. Implement the 8 TODOs in order
4. Test locally against the sample CSV (uses pandas, no CDE required):
   ```bash
   python modules/05-data-quality/exercise_validate.py \
     data/sample \
     great_expectations
   ```
5. Deploy and run on CDE:
   ```bash
   cde job run --name workshop-validate-data \
     --arg s3://your-bucket/raw \
     --arg /app/mount/great_expectations
   ```

**Extension task:** Copy `data/sample/retail_orders.csv` to a new file. Add a row with a null `order_id`. Re-run the local test and observe which expectation fails and what the output looks like.

---

## Where Does Validation Fit in the Pipeline?

In the full Airflow pipeline, validation runs **after ingest and before transform**:

```
ingest_raw → validate_data → transform → load_curated
```

If `validate_data` fails (exit code 1), Airflow marks it as FAILED and does not run `transform` or `load_curated`. This prevents bad data from polluting your curated tables.

---

## Next Module

Move to **[Module 06 — Orchestrate with Airflow](../06-orchestrate/README.md)**.
