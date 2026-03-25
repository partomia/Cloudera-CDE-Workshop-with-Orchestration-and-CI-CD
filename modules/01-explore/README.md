# Module 01 — Explore the Dataset

**Estimated time:** 20 minutes
**Prerequisite:** Environment setup complete (root README, sections 1–6)
**Demo file:** `demos/demo_explore.py`

---

## Learning Objectives

By the end of this module you will be able to:

- Read a CSV file into a PySpark DataFrame from your IDE via CDE Spark Connect
- Inspect schema, sample rows, row counts, and null values
- Compute basic statistics and distribution summaries using PySpark
- Recognise data issues in the raw dataset that will be addressed in later modules

---

## The Dataset

The file `data/sample/retail_orders.csv` contains 20 retail orders across 5 customers and 6 product categories.

| Column | Type | Description |
|--------|------|-------------|
| `order_id` | String | Unique order identifier (e.g. ORD0001) |
| `customer_id` | String | Customer identifier (e.g. CUST001) |
| `product_id` | String | Product identifier (e.g. PROD001) |
| `order_date` | String | Date in `YYYY-MM-DD` format |
| `quantity` | Integer | Number of units ordered |
| `unit_price` | Double | Price per unit in USD |
| `category` | String | Product category (electronics, books, clothing, food, furniture, sports) |

---

## Key Concepts

**PySpark DataFrame vs pandas DataFrame**
A PySpark DataFrame is distributed across many machines and uses lazy evaluation — no data moves until you call an action like `.count()` or `.show()`. A pandas DataFrame lives entirely in memory on one machine. On CDE, your IDE is a thin client; the actual computation runs on the virtual cluster.

**CDE Spark Connect**
When you call `SparkSession.builder.getOrCreate()` using the `cde-spark` environment, the session is created on the CDE virtual cluster — not on your laptop. Your code runs locally, but Spark executes remotely.

**Schema inference**
`inferSchema=True` tells Spark to scan the file and guess data types. It is convenient for exploration but not reliable for production — Spark may infer `order_date` as `StringType` instead of `DateType`, and numeric columns may come back as the wrong precision.

---

## Steps

1. Open `demos/demo_explore.py` in IntelliJ
2. Set `CDE_SESSION_NAME` at the top of the file to your session name
3. Press **Play** — the script connects to CDE and prints results for each exploration step

---

## Reflection Questions

Answer these in the last cell of the notebook:

1. What data type did Spark infer for `order_date`? Is it a `DateType` or `StringType`? Why does this matter for the transform job in Module 03?
2. What case is the `category` column stored in? Why does this matter for the data quality check in Module 05?
3. Are there any null values in the 20-row sample? What would happen at scale if `order_id` contained nulls?

---

## Next Module

Once you have completed the exploration, move to **[Module 02 — Ingest](../02-ingest/README.md)**.
