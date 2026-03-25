"""
POC Demo 1 — ELT Pipeline on CDE Spark Connect

Press Play to run. No arguments needed.

What this demonstrates:
  - Live IDE → CDE Spark session connection
  - Inline data loaded as a Spark DataFrame (no S3 / file dependency)
  - Clean → Enrich → Aggregate transformation pipeline
  - Results displayed in the IDE console
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cde import CDESparkConnectSession
from pyspark.sql import DataFrame

from demos.sample_data import ORDERS, SCHEMA

# ── Configuration ─────────────────────────────────────────────────────────────
CDE_SESSION_NAME = "r1rsingh"  # Change to your CDE session name


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


# ── ETL functions ─────────────────────────────────────────────────────────────

def clean(df: DataFrame) -> DataFrame:
    return (
        df.dropna(subset=["order_id", "customer_id", "product_id"])
        .selectExpr(
            "order_id", "customer_id", "product_id", "order_date",
            "CAST(quantity   AS INT)    AS quantity",
            "CAST(unit_price AS DOUBLE) AS unit_price",
            "UPPER(TRIM(category))      AS category",
        )
        .filter("quantity > 0")
        .filter("unit_price > 0")
    )


def enrich(df: DataFrame) -> DataFrame:
    return df.selectExpr(
        "*",
        "quantity * unit_price                AS revenue",
        "date_format(order_date, 'yyyy-MM')   AS order_month",
        "year(order_date)                     AS order_year",
    )


def aggregate_by_category(df: DataFrame) -> DataFrame:
    spark = df.sparkSession
    df.createOrReplaceTempView("_agg_cat")
    return spark.sql("""
        SELECT  category,
                order_month,
                SUM(revenue)             AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                AVG(unit_price)          AS avg_unit_price
        FROM    _agg_cat
        GROUP BY category, order_month
        ORDER BY total_revenue DESC
    """)


def aggregate_by_customer(df: DataFrame) -> DataFrame:
    spark = df.sparkSession
    df.createOrReplaceTempView("_agg_cust")
    return spark.sql("""
        SELECT  customer_id,
                order_month,
                SUM(revenue)             AS total_revenue,
                COUNT(DISTINCT order_id) AS order_count,
                SUM(quantity)            AS total_quantity
        FROM    _agg_cust
        GROUP BY customer_id, order_month
        ORDER BY total_revenue DESC
    """)


# ── Main ──────────────────────────────────────────────────────────────────────

print(f"\nConnecting to CDE session: {CDE_SESSION_NAME} ...")
spark = CDESparkConnectSession.builder.sessionName(CDE_SESSION_NAME).get()
print(f"Connected. Spark version: {spark.version}")

try:
    sep("STEP 1 — RAW DATA  (20 inline orders, no S3 needed)")
    df_raw = spark.createDataFrame(ORDERS, SCHEMA)
    print(f"Row count: {df_raw.count()}")
    df_raw.show(5, truncate=False)

    sep("STEP 2 — AFTER CLEAN  (nulls dropped, types cast, category uppercased)")
    df_clean = clean(df_raw)
    print(f"Row count: {df_clean.count()}")
    df_clean.select("order_id", "category", "quantity", "unit_price").show(5, truncate=False)

    sep("STEP 3 — ENRICHED  (revenue, order_month, order_year added)")
    df_enriched = enrich(df_clean)
    df_enriched.select(
        "order_id", "customer_id", "category", "quantity", "unit_price", "revenue", "order_month"
    ).show(10, truncate=False)

    sep("STEP 4a — CURATED: Revenue by Category")
    aggregate_by_category(df_enriched).show(truncate=False)

    sep("STEP 4b — CURATED: Revenue by Customer")
    aggregate_by_customer(df_enriched).show(truncate=False)

    print("\n✓  ELT pipeline complete. All steps ran on the CDE Spark session.\n")

finally:
    spark.stop()
