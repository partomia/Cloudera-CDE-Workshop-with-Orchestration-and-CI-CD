# Module 04 — Load: Curated Parquet + Hive Tables

**Estimated time:** 30 minutes
**Prerequisite:** Module 03 (validated Parquet must exist on S3)
**Exercise file:** `exercise_load.py`
**Reference solution:** `jobs/load/load_curated.py`

---

## Learning Objectives

By the end of this module you will be able to:

- Read multiple Parquet datasets and write them to a curated S3 zone
- Register external Hive tables pointing to S3 locations
- Query the curated tables using Spark SQL in a CDE session
- Explain the difference between external and managed Hive tables

---

## Key Concepts

**External vs managed Hive tables**

| | Managed table | External table (this module) |
|---|---|---|
| Data location | Managed by Hive (moved into Hive's warehouse) | Stays in your S3 path |
| DROP TABLE | Deletes the data | Deletes only the metadata |
| Use case | Hive-owned data | S3 data you want to query via SQL |

We always use `EXTERNAL TABLE` so that dropping or recreating the table never destroys the underlying Parquet files.

**`enableHiveSupport()`**
This must be added to the SparkSession builder to allow DDL statements like `CREATE TABLE` and `DROP TABLE`. Without it, Spark will throw an error when you try to execute any SQL that touches the Hive Metastore.

**Idempotency**
The load job uses `DROP TABLE IF EXISTS` before `CREATE EXTERNAL TABLE`. This means you can re-run the job safely — it always replaces the table definition rather than failing with "table already exists".

**Hive Metastore in Cloudera**
Once tables are registered, they are queryable from:
- Spark SQL (in your CDE session or this notebook)
- Cloudera Data Warehouse (Hue, Impala)
- Any tool that connects to the shared Hive Metastore

---

## Steps

1. Open `modules/04-load/exercise_load.py`
2. Implement `get_spark()` — remember to add `enableHiveSupport()`
3. Implement `load()` — iterate over the three tables, write Parquet, register Hive tables
4. Deploy and run on CDE:
   ```bash
   cde job run --name workshop-load-curated \
     --arg s3://your-bucket/validated \
     --arg s3://your-bucket/curated \
     --arg workshop_db
   ```
5. Verify by querying in the CDE Spark session or Hue:

```sql
-- All orders with revenue
SELECT order_id, customer_id, category, revenue
FROM workshop_db.orders
ORDER BY revenue DESC
LIMIT 10;

-- Top customers by monthly revenue
SELECT customer_id, order_month, total_revenue
FROM workshop_db.customer_summary
ORDER BY total_revenue DESC;

-- Revenue by product category
SELECT category, order_month, total_revenue, order_count
FROM workshop_db.category_summary
ORDER BY total_revenue DESC;
```

---

## What Gets Created

After this module, you will have three curated Hive tables:

| Table | Description | Key Columns |
|-------|-------------|-------------|
| `workshop_db.orders` | Enriched order-level data | `order_id`, `revenue`, `order_month` |
| `workshop_db.customer_summary` | Monthly revenue per customer | `customer_id`, `order_month`, `total_revenue` |
| `workshop_db.category_summary` | Monthly revenue per category | `category`, `order_month`, `total_revenue` |

---

## Next Module

With curated tables registered, move to **[Module 05 — Data Quality](../05-data-quality/README.md)**.
