"""
Module 02 - Ingest Job
Reads raw retail orders CSV from S3 landing zone and writes Parquet to S3 raw zone.
Reference solution for: modules/02-ingest/exercise_ingest.py
"""

import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("workshop-ingest-raw")
        .getOrCreate()
    )


def ingest(spark: SparkSession, landing_path: str, raw_path: str) -> int:
    """
    Read CSV from landing zone, add audit columns, write Parquet to raw zone.
    Returns count of ingested records.
    """
    logger.info("Reading from landing zone: %s", landing_path)

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("multiLine", "true")
        .csv(landing_path)
    )

    df = (
        df
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    count = df.count()
    logger.info("Ingested %d records", count)

    logger.info("Writing to raw zone: %s", raw_path)
    (
        df.write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(raw_path)
    )

    logger.info("Ingest complete.")
    return count


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ingest_raw.py <landing_s3_path> <raw_s3_path>")
        sys.exit(1)

    landing_s3 = sys.argv[1]
    raw_s3 = sys.argv[2]

    spark = get_spark()
    try:
        ingest(spark, landing_s3, raw_s3)
    finally:
        spark.stop()
