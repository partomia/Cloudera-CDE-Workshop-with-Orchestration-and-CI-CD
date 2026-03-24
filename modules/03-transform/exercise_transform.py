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
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("workshop-transform").getOrCreate()


def clean(df: DataFrame) -> DataFrame:
    """Drop rows with null keys, cast types, standardise strings."""

    # TODO 1: Drop rows where any of these columns is null:
    #         order_id, customer_id, product_id
    # Hint: df.dropna(subset=["order_id", "customer_id", "product_id"])
    df = raise NotImplementedError

    # TODO 2: Cast 'quantity' to IntegerType and 'unit_price' to DoubleType
    # Hint: df.withColumn("quantity", F.col("quantity").cast(IntegerType()))
    #         .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
    df = raise NotImplementedError

    # TODO 3: Standardise 'category' — convert to UPPERCASE and strip whitespace
    # Hint: F.upper(F.trim(F.col("category")))
    df = raise NotImplementedError

    # TODO 4: Filter out rows where quantity <= 0 or unit_price <= 0
    # These are data errors — negative or zero values are not valid orders
    # Hint: df.filter(F.col("quantity") > 0).filter(F.col("unit_price") > 0)
    df = raise NotImplementedError

    return df


def enrich(df: DataFrame) -> DataFrame:
    """Derive revenue and time-based columns."""

    # TODO 5: Add 'revenue' = quantity × unit_price
    # Hint: F.col("quantity") * F.col("unit_price")
    df = raise NotImplementedError

    # TODO 6: Add 'order_month' formatted as "yyyy-MM" (e.g. "2026-01")
    # Hint: F.date_format(F.col("order_date"), "yyyy-MM")
    df = raise NotImplementedError

    # TODO 7: Add 'order_year' as an integer (e.g. 2026)
    # Hint: F.year(F.col("order_date"))
    df = raise NotImplementedError

    return df


def aggregate_by_customer(df: DataFrame) -> DataFrame:
    """Monthly revenue per customer."""

    # TODO 8: Group by customer_id and order_month
    # Compute:
    #   total_revenue  — sum of revenue
    #   order_count    — count of distinct order_ids
    #   total_quantity — sum of quantity
    #
    # Hint:
    #   df.groupBy("customer_id", "order_month").agg(
    #       F.sum("revenue").alias("total_revenue"),
    #       F.countDistinct("order_id").alias("order_count"),
    #       F.sum("quantity").alias("total_quantity"),
    #   )
    raise NotImplementedError


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Monthly revenue per product category."""

    # TODO 9: Group by category and order_month
    # Compute:
    #   total_revenue   — sum of revenue
    #   order_count     — count of distinct order_ids
    #   avg_unit_price  — average of unit_price
    #
    # Hint: same pattern as aggregate_by_customer above
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
