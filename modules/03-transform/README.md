# Module 03 — Transform: Clean, Enrich & Aggregate

**Estimated time:** 45 minutes
**Prerequisite:** Module 02 (raw Parquet must exist on S3)
**Exercise file:** `exercise_transform.py`
**Reference solution:** `jobs/transform/transform.py`

---

## Learning Objectives

By the end of this module you will be able to:

- Apply a three-stage transformation pattern: clean → enrich → aggregate
- Use `dropna`, `selectExpr` for type casting and string standardisation, and string filter predicates
- Derive computed columns (`revenue`, `order_month`, `order_year`) via `selectExpr`
- Produce two aggregation datasets grouped by different business dimensions using `spark.sql()`
- Write multiple output datasets to S3 with appropriate partition keys

---

## Key Concepts

**The three-stage pattern**

| Stage | What it does | Why |
|-------|-------------|-----|
| Clean | Remove bad rows, fix types, standardise strings | Garbage in = garbage out |
| Enrich | Derive new columns from existing ones | Add business meaning |
| Aggregate | Summarise at a coarser grain | Answer analytical questions efficiently |

**Null-safe operations**
`dropna(subset=["order_id"])` removes rows where `order_id` is null. This is safer than filtering because it handles all null representations at once.

**Type casting and string standardisation with `selectExpr`**
`selectExpr` lets you write SQL expressions as strings, which are evaluated server-side — fully compatible with Spark Connect:
```python
df = df.selectExpr(
    "order_id", "customer_id", "product_id", "order_date",
    "CAST(quantity   AS INT)    AS quantity",
    "CAST(unit_price AS DOUBLE) AS unit_price",
    "UPPER(TRIM(category))      AS category",
)
```
If a value cannot be cast (e.g. the string "abc"), Spark returns `null` rather than throwing an error.

> **Why not `F.col()` / `F.upper()` etc.?**
> `pyspark.sql.functions` routes calls through the JVM bridge, which requires a local
> `SparkContext`. Spark Connect (IDE → CDE) has no local JVM. Use `selectExpr` or
> `spark.sql()` instead — both send SQL strings to the server for evaluation.

**Immutability**
Every PySpark transformation returns a **new** DataFrame. The original is never modified. This is why you write `df = df.selectExpr(...)` — you are re-assigning the variable to a new DataFrame.

**Aggregations with `spark.sql()`**
Register a temp view, then use `spark.sql()` for aggregations. This works on both local Spark and Spark Connect:
```python
df.createOrReplaceTempView("_agg_cat")
result = df.sparkSession.sql("""
    SELECT  category, order_month,
            SUM(revenue)             AS total_revenue,
            COUNT(DISTINCT order_id) AS order_count,
            AVG(unit_price)          AS avg_unit_price
    FROM    _agg_cat
    GROUP BY category, order_month
""")
```

**Partition strategy**
- Order-level data is partitioned by `order_year` + `order_month` — coarse enough to avoid too many small files, fine enough to allow efficient date filtering
- Summary tables are partitioned by `order_month` only — they are already small after aggregation

---

## Steps

Work through the four functions in order and run the unit tests after each one:

**1. Implement `clean()`**
```bash
pytest tests/unit/test_transform.py -k clean -v
```

**2. Implement `enrich()`**
```bash
pytest tests/unit/test_transform.py -k enrich -v
```

**3. Implement `aggregate_by_customer()` and `aggregate_by_category()`**
```bash
pytest tests/unit/test_transform.py -k aggregate -v
```

**4. Implement `transform()` to wire all four functions together, then run the full suite:**
```bash
pytest tests/unit/test_transform.py -v
```

**5. Deploy and run on CDE:**
```bash
cde job run --name workshop-transform \
  --arg s3://your-bucket/raw \
  --arg s3://your-bucket/validated
```

**Expected output on S3:**
```
s3://your-bucket/validated/
  orders/
    order_year=2026/order_month=2026-01/part-*.parquet   ← enriched orders
  customer_summary/
    order_month=2026-01/part-*.parquet                   ← monthly revenue per customer
  category_summary/
    order_month=2026-01/part-*.parquet                   ← monthly revenue per category
```

---

## What Does the Enriched Data Look Like?

After `clean()` and `enrich()`, each order row gains three new columns:

| New Column | Example Value | How it is calculated |
|------------|---------------|----------------------|
| `revenue` | 59.98 | `quantity (2) × unit_price (29.99)` |
| `order_month` | 2026-01 | `date_format(order_date, "yyyy-MM")` |
| `order_year` | 2026 | `year(order_date)` |

---

## Next Module

Once validated Parquet is written, move to **[Module 04 — Load](../04-load/README.md)**.
