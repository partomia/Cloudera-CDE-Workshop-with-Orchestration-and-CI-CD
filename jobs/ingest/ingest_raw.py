"""
Module 02 - Ingest Job
Reads raw retail orders CSV from S3 landing zone and writes Parquet to S3 raw zone.
If no S3 paths are provided, creates an inline sample dataset and writes to local CDE storage.

Reference solution for: modules/02-ingest/exercise_ingest.py
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# DEFAULT_RAW_PATH = "s3a://go01-demo/workshop/raw"  # old go01-dem environment bucket
DEFAULT_RAW_PATH = "s3a://federal-buk-574bcea0/workshop/raw"

SAMPLE_SCHEMA = StructType([
    StructField("order_id",    StringType(),  True),
    StructField("customer_id", StringType(),  True),
    StructField("product_id",  StringType(),  True),
    StructField("order_date",  StringType(),  True),
    StructField("quantity",    IntegerType(), True),
    StructField("unit_price",  DoubleType(),  True),
    StructField("category",    StringType(),  True),
])

SAMPLE_ORDERS = [
    ("ORD0001", "CUST001", "PROD001", "2026-01-05", 2,  29.99, "electronics"),
    ("ORD0002", "CUST002", "PROD002", "2026-01-06", 1,   9.99, "books"),
    ("ORD0003", "CUST003", "PROD003", "2026-01-06", 3,  49.99, "clothing"),
    ("ORD0004", "CUST001", "PROD004", "2026-01-07", 1, 199.99, "electronics"),
    ("ORD0005", "CUST004", "PROD005", "2026-01-08", 5,   4.99, "food"),
    ("ORD0006", "CUST005", "PROD006", "2026-01-08", 2,  89.99, "furniture"),
    ("ORD0007", "CUST002", "PROD001", "2026-01-09", 1,  29.99, "electronics"),
    ("ORD0008", "CUST006", "PROD007", "2026-01-10", 4,  14.99, "sports"),
    ("ORD0009", "CUST003", "PROD008", "2026-01-11", 2,  24.99, "clothing"),
    ("ORD0010", "CUST007", "PROD009", "2026-01-12", 1,   7.99, "books"),
    ("ORD0011", "CUST001", "PROD010", "2026-01-13", 3,  12.99, "food"),
    ("ORD0012", "CUST008", "PROD011", "2026-01-14", 1, 299.99, "electronics"),
    ("ORD0013", "CUST004", "PROD012", "2026-01-15", 2,  39.99, "sports"),
    ("ORD0014", "CUST009", "PROD003", "2026-01-16", 1,  49.99, "clothing"),
    ("ORD0015", "CUST010", "PROD013", "2026-01-17", 6,   3.49, "food"),
    ("ORD0016", "CUST005", "PROD014", "2026-01-18", 1, 149.99, "furniture"),
    ("ORD0017", "CUST002", "PROD015", "2026-01-19", 2,  19.99, "books"),
    ("ORD0018", "CUST006", "PROD016", "2026-01-20", 3,  59.99, "sports"),
    ("ORD0019", "CUST011", "PROD017", "2026-01-21", 1,   9.99, "food"),
    ("ORD0020", "CUST012", "PROD001", "2026-01-22", 2,  29.99, "electronics"),
]


def _is_unset(val: str) -> bool:
    """Returns True if arg is a CDE unsubstituted template like {{landing_path}}."""
    return not val or (val.startswith("{{") and val.endswith("}}"))


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("workshop-ingest-raw").getOrCreate()


def ingest_from_s3(spark: SparkSession, landing_path: str, raw_path: str) -> int:
    logger.info("Reading CSV from landing zone: %s", landing_path)
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .csv(landing_path)
    )
    df = df.selectExpr(
        "*",
        "current_timestamp() AS _ingested_at",
        "input_file_name()   AS _source_file",
    )
    count = df.count()
    logger.info("Ingested %d records — writing to: %s", count, raw_path)
    df.write.mode("overwrite").partitionBy("order_date").parquet(raw_path)
    return count


def ingest_inline(spark: SparkSession, raw_path: str) -> int:
    logger.info("No S3 path provided — using inline sample dataset")
    df = spark.createDataFrame(SAMPLE_ORDERS, schema=SAMPLE_SCHEMA)
    df = df.selectExpr(
        "*",
        "current_timestamp() AS _ingested_at",
        "'inline_sample'     AS _source_file",
    )
    count = df.count()
    logger.info("Inline dataset: %d records — writing to: %s", count, raw_path)
    df.write.mode("overwrite").partitionBy("order_date").parquet(raw_path)
    return count


if __name__ == "__main__":
    landing = sys.argv[1] if len(sys.argv) > 1 else None
    raw     = sys.argv[2] if len(sys.argv) > 2 else None

    spark = get_spark()
    try:
        if landing and not _is_unset(landing) and raw and not _is_unset(raw):
            ingest_from_s3(spark, landing, raw)
        else:
            ingest_inline(spark, raw if raw and not _is_unset(raw) else DEFAULT_RAW_PATH)
    finally:
        spark.stop()
