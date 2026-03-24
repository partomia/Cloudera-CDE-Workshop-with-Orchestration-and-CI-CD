# Module 02 — Ingest: CSV → Parquet

**Estimated time:** 30 minutes
**Prerequisite:** Module 01
**Exercise file:** `exercise_ingest.py`
**Reference solution:** `jobs/ingest/ingest_raw.py`

---

## Learning Objectives

By the end of this module you will be able to:

- Read a CSV file into PySpark with explicit options
- Add audit columns to track data provenance
- Write a DataFrame as partitioned Parquet to an S3 path
- Run a Spark job on CDE via the CLI and verify the output

---

## Key Concepts

**Data lake zones**
The S3 bucket is divided into zones, each representing a stage of processing:

```
s3://bucket/landing/   ← raw files dropped here (CSV, JSON, etc.)
s3://bucket/raw/       ← ingested Parquet, partitioned by date  ← this module
s3://bucket/validated/ ← transformed and quality-checked data
s3://bucket/curated/   ← final tables, registered in Hive
```

**Why Parquet?**
Parquet is a columnar format. Unlike CSV it stores data type information, supports compression, and allows Spark to skip entire column groups it does not need (predicate pushdown). Downstream jobs read faster because Spark only loads relevant columns and partitions.

**Partitioning**
`partitionBy("order_date")` splits the output into sub-folders like `order_date=2026-01-05/`. When a downstream job filters by date, Spark reads only the matching folder — skipping everything else. This is called partition pruning.

**Audit columns**
`_ingested_at` and `_source_file` are added to every row so you can always trace where a record came from and when it was loaded. This is standard practice in data engineering.

**`mode("overwrite")`**
Makes the job re-runnable. If the job fails halfway and is re-run, it replaces the previous partial output cleanly. Without this, Spark would throw an error if the output path already exists.

---

## Steps

1. Open `modules/02-ingest/exercise_ingest.py`
2. Implement `get_spark()` — connect to the remote CDE session
3. Implement `ingest()` — three TODOs: read CSV, add audit columns, write Parquet
4. Implement the `__main__` block
5. Run the unit tests locally:
   ```bash
   pytest tests/unit/test_ingest.py -v
   ```
6. Deploy and run on CDE:
   ```bash
   cde job run --name workshop-ingest-raw \
     --arg s3://your-bucket/landing \
     --arg s3://your-bucket/raw
   ```
7. In the CDE UI, open the job run logs and confirm the `"Ingested N records"` log line

**Expected output structure on S3:**
```
s3://your-bucket/raw/
  order_date=2026-01-05/
    part-00000-*.parquet
  order_date=2026-01-06/
    part-00000-*.parquet
  ...
```

---

## Next Module

Once the raw Parquet is written successfully, move to **[Module 03 — Transform](../03-transform/README.md)**.
