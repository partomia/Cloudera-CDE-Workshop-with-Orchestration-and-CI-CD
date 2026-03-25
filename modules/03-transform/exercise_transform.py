"""
Module 03 Exercise — Transform: Clean, Enrich & Aggregate

Complete each TODO. Run unit tests after each function to verify your work.

Run tests:
    pytest tests/unit/test_transform.py -k clean -v
    pytest tests/unit/test_transform.py -k enrich -v
    pytest tests/unit/test_transform.py -k aggregate -v
    pytest tests/unit/test_transform.py -v   (full suite)

Reference solution: jobs/transform/transform.py
"""

import sys
import logging
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("workshop-transform").getOrCreate()


def clean(df: DataFrame) -> DataFrame:
    """Drop rows with null keys, cast types, standardise strings."""

    # TODO 1: Drop rows where any of these columns is null:
    #         order_id, customer_id, product_id
    # Hint: df.dropna(subset=["order_id", "customer_id", "product_id"])
    raise NotImplementedError

    # TODO 2–4: Use selectExpr to cast types and standardise category in one step,
    # then filter out invalid rows.
    #
    # selectExpr lets you write SQL expressions as strings — safe on Spark Connect.
    #
    # Hint:
    #   df = df.selectExpr(
    #       "order_id", "customer_id", "product_id", "order_date",
    #       "CAST(quantity   AS INT)    AS quantity",
    #       "CAST(unit_price AS DOUBLE) AS unit_price",
    #       "UPPER(TRIM(category))      AS category",
    #   ).filter("quantity > 0").filter("unit_price > 0")
    raise NotImplementedError

    return df


def enrich(df: DataFrame) -> DataFrame:
    """Derive revenue and time-based columns."""

    # TODO 5–7: Add revenue, order_month, and order_year using selectExpr.
    #
    # selectExpr("*", ...) keeps all existing columns and appends new ones.
    #
    # Hint:
    #   return df.selectExpr(
    #       "*",
    #       "quantity * unit_price                AS revenue",
    #       "date_format(order_date, 'yyyy-MM')   AS order_month",
    #       "year(order_date)                     AS order_year",
    #   )
    raise NotImplementedError


def aggregate_by_customer(df: DataFrame) -> DataFrame:
    """Monthly revenue per customer."""

    # TODO 8: Group by customer_id and order_month using spark.sql().
    #
    # Register df as a temp view, then query it with SQL.
    # spark.sql() and createOrReplaceTempView() both work on Spark Connect.
    #
    # Compute:
    #   total_revenue  — SUM(revenue)
    #   order_count    — COUNT(DISTINCT order_id)
    #   total_quantity — SUM(quantity)
    #
    # Hint:
    #   spark = df.sparkSession
    #   df.createOrReplaceTempView("_agg_cust")
    #   return spark.sql("""
    #       SELECT  customer_id, order_month,
    #               SUM(revenue)             AS total_revenue,
    #               COUNT(DISTINCT order_id) AS order_count,
    #               SUM(quantity)            AS total_quantity
    #       FROM    _agg_cust
    #       GROUP BY customer_id, order_month
    #   """)
    raise NotImplementedError


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Monthly revenue per product category."""

    # TODO 9: Group by category and order_month using spark.sql().
    #
    # Same pattern as aggregate_by_customer above.
    #
    # Compute:
    #   total_revenue   — SUM(revenue)
    #   order_count     — COUNT(DISTINCT order_id)
    #   avg_unit_price  — AVG(unit_price)
    raise NotImplementedError


def transform(spark: SparkSession, raw_path: str, validated_path: str):
    logger.info("Reading raw data from: %s", raw_path)
    df_raw = spark.read.parquet(raw_path)

    # TODO 10: Apply the transformation pipeline
    #
    # Step a: call clean(df_raw)        → df_clean
    # Step b: call enrich(df_clean)     → df_enriched
    # Step c: call aggregate_by_customer(df_enriched)  → df_by_customer
    # Step d: call aggregate_by_category(df_enriched)  → df_by_category
    #
    # Then write three datasets to validated_path (all mode="overwrite"):
    #
    #   df_enriched    → f"{validated_path}/orders"
    #                    partitioned by: order_year, order_month
    #
    #   df_by_customer → f"{validated_path}/customer_summary"
    #                    partitioned by: order_month
    #
    #   df_by_category → f"{validated_path}/category_summary"
    #                    partitioned by: order_month
    #
    # Hint: df.write.mode("overwrite").partitionBy(...).parquet(path)
    raise NotImplementedError

    logger.info("Transform complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: exercise_transform.py <raw_s3_path> <validated_s3_path>")
        sys.exit(1)

    spark = get_spark()
    try:
        transform(spark, sys.argv[1], sys.argv[2])
    finally:
        spark.stop()
