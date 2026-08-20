"""
Module 03 - Transform Job
Applies ELT business logic to raw Parquet:
  - Clean nulls and type-cast columns
  - Derive new columns (revenue, order_month)
  - Aggregate by customer and product category
Writes transformed Parquet to validated zone (S3 or local CDE storage).
If no paths provided, reads from and writes to default local CDE paths.

Reference solution for: modules/03-transform/exercise_transform.py
"""

import sys
import logging
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DEFAULT_RAW_PATH       = "s3a://go01-demo/workshop/raw"        # old go01-dem environment bucket
# DEFAULT_VALIDATED_PATH = "s3a://go01-demo/workshop/validated"  # old go01-dem environment bucket
DEFAULT_RAW_PATH       = "s3a://federal-buk-574bcea0/workshop/raw"
DEFAULT_VALIDATED_PATH = "s3a://federal-buk-574bcea0/workshop/validated"


def _is_unset(val: str) -> bool:
    return not val or (val.startswith("{{") and val.endswith("}}"))


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("workshop-transform").getOrCreate()


def clean(df: DataFrame) -> DataFrame:
    """Drop rows with null keys, cast types, standardise strings."""
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
    """Derive revenue and time-based columns."""
    return df.selectExpr(
        "*",
        "quantity * unit_price                AS revenue",
        "date_format(order_date, 'yyyy-MM')   AS order_month",
        "year(order_date)                     AS order_year",
    )


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
    """)


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
    """)


def transform(spark: SparkSession, raw_path: str, validated_path: str):
    logger.info("Reading raw data from: %s", raw_path)
    df_raw = spark.read.parquet(raw_path)

    df_clean    = clean(df_raw)
    df_enriched = enrich(df_clean)

    df_by_customer = aggregate_by_customer(df_enriched)
    df_by_category = aggregate_by_category(df_enriched)

    logger.info("Writing enriched orders to: %s/orders", validated_path)
    df_enriched.write.mode("overwrite").partitionBy("order_year", "order_month").parquet(f"{validated_path}/orders")

    logger.info("Writing customer aggregates to: %s/customer_summary", validated_path)
    df_by_customer.write.mode("overwrite").partitionBy("order_month").parquet(f"{validated_path}/customer_summary")

    logger.info("Writing category aggregates to: %s/category_summary", validated_path)
    df_by_category.write.mode("overwrite").partitionBy("order_month").parquet(f"{validated_path}/category_summary")

    logger.info("Transform complete.")


if __name__ == "__main__":
    raw       = sys.argv[1] if len(sys.argv) > 1 else None
    validated = sys.argv[2] if len(sys.argv) > 2 else None

    raw_path       = raw       if raw       and not _is_unset(raw)       else DEFAULT_RAW_PATH
    validated_path = validated if validated and not _is_unset(validated) else DEFAULT_VALIDATED_PATH

    logger.info("raw_path=%s  validated_path=%s", raw_path, validated_path)

    spark = get_spark()
    try:
        transform(spark, raw_path, validated_path)
    finally:
        spark.stop()
