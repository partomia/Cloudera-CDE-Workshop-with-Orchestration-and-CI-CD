"""
POC Demo 0 — Explore the Dataset on CDE Spark Connect

Press Play to run. No arguments needed.

What this demonstrates:
  - Live IDE → CDE Spark session connection
  - Inline data loaded as a Spark DataFrame (no S3 / file dependency)
  - Schema inspection, row counts, null checks, distributions
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cde import CDESparkConnectSession

from demos.sample_data import ORDERS, SCHEMA

# ── Configuration ─────────────────────────────────────────────────────────────
CDE_SESSION_NAME = "cdecli"  # Change to your CDE session name


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


# ── Main ──────────────────────────────────────────────────────────────────────

print(f"\nConnecting to CDE session: {CDE_SESSION_NAME} ...")
spark = CDESparkConnectSession.builder.sessionName(CDE_SESSION_NAME).get()
print(f"Connected. Spark version: {spark.version}")

try:
    df = spark.createDataFrame(ORDERS, SCHEMA)

    sep("1 — SCHEMA")
    df.printSchema()

    sep("2 — SAMPLE ROWS  (first 5)")
    df.show(5, truncate=False)

    sep("3 — ROW COUNT & DISTINCT CATEGORIES")
    print(f"  Total rows         : {df.count()}")
    print(f"  Distinct categories: {df.select('category').distinct().count()}")

    sep("4 — SUMMARY STATISTICS  (quantity & unit_price)")
    df.select("quantity", "unit_price").describe().show()

    sep("5 — NULL COUNTS PER COLUMN")
    df.selectExpr([f"sum(int({c} is null)) as {c}" for c in df.columns]).show()

    sep("6 — ORDER COUNT BY CATEGORY")
    df.groupBy("category").count().orderBy("count", ascending=False).show(truncate=False)

    sep("7 — DATE RANGE")
    df.selectExpr("min(order_date) as earliest", "max(order_date) as latest").show()

    print("\n✓  Exploration complete.\n")

finally:
    spark.stop()
