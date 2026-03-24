"""
Module 03 - Transform Job
Applies ELT business logic to raw Parquet:
  - Clean nulls and type-cast columns
  - Derive new columns (revenue, order_month)
  - Aggregate by customer and product category
Writes transformed Parquet to S3 validated zone.
Reference solution for: modules/03-transform/exercise_transform.py
"""

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("workshop-transform")
        .getOrCreate()
    )


def clean(df: DataFrame) -> DataFrame:
    """Drop rows with null keys, cast types, standardise strings."""
    return (
        df
        .dropna(subset=["order_id", "customer_id", "product_id"])
        .withColumn("quantity", F.col("quantity").cast(IntegerType()))
        .withColumn("unit_price", F.col("unit_price").cast(DoubleType()))
        .withColumn("category", F.upper(F.trim(F.col("category"))))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
    )


def enrich(df: DataFrame) -> DataFrame:
    """Derive revenue and time-based columns."""
    return (
        df
        .withColumn("revenue", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_month", F.date_format(F.col("order_date"), "yyyy-MM"))
        .withColumn("order_year", F.year(F.col("order_date")))
    )


def aggregate_by_customer(df: DataFrame) -> DataFrame:
    """Monthly revenue per customer."""
    return (
        df.groupBy("customer_id", "order_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("total_quantity"),
        )
    )


def aggregate_by_category(df: DataFrame) -> DataFrame:
    """Monthly revenue per product category."""
    return (
        df.groupBy("category", "order_month")
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.countDistinct("order_id").alias("order_count"),
            F.avg("unit_price").alias("avg_unit_price"),
        )
    )


def transform(spark: SparkSession, raw_path: str, validated_path: str):
    logger.info("Reading raw data from: %s", raw_path)
    df_raw = spark.read.parquet(raw_path)

    df_clean = clean(df_raw)
    df_enriched = enrich(df_clean)

    df_by_customer = aggregate_by_customer(df_enriched)
    df_by_category = aggregate_by_category(df_enriched)

    logger.info("Writing enriched orders to: %s/orders", validated_path)
    (
        df_enriched.write
        .mode("overwrite")
        .partitionBy("order_year", "order_month")
        .parquet(f"{validated_path}/orders")
    )

    logger.info("Writing customer aggregates to: %s/customer_summary", validated_path)
    (
        df_by_customer.write
        .mode("overwrite")
        .partitionBy("order_month")
        .parquet(f"{validated_path}/customer_summary")
    )

    logger.info("Writing category aggregates to: %s/category_summary", validated_path)
    (
        df_by_category.write
        .mode("overwrite")
        .partitionBy("order_month")
        .parquet(f"{validated_path}/category_summary")
    )

    logger.info("Transform complete.")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: transform.py <raw_s3_path> <validated_s3_path>")
        sys.exit(1)

    raw_s3 = sys.argv[1]
    validated_s3 = sys.argv[2]

    spark = get_spark()
    try:
        transform(spark, raw_s3, validated_s3)
    finally:
        spark.stop()
