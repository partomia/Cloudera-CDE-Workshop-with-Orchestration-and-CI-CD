"""
Module 02 Exercise — Ingest: CSV → Parquet

Complete each TODO below. When all unit tests pass, deploy and run on CDE.

Run tests:
    pytest tests/unit/test_ingest.py -v

Reference solution: jobs/ingest/ingest_raw.py
"""

import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    # TODO 1: Create and return a SparkSession with appName "workshop-ingest-raw"
    #
    # On CDE, this connects to the remote virtual cluster via Spark Connect.
    # You do not need any special configuration — the cde-spark environment
    # handles the connection automatically.
    #
    # Hint: SparkSession.builder.appName("workshop-ingest-raw").getOrCreate()
    raise NotImplementedError


def ingest(spark: SparkSession, landing_path: str, raw_path: str) -> int:
    """
    Read CSV from landing_path, add audit columns, write Parquet to raw_path.
    Returns the count of ingested records.
    """
    logger.info("Reading from landing zone: %s", landing_path)

    # TODO 2: Read the CSV file into a DataFrame called df
    #
    # Requirements:
    #   - header=True        (the first row contains column names)
    #   - inferSchema=True   (let Spark detect data types automatically)
    #   - multiLine=True     (safe default for values containing newlines)
    #
    # Hint: spark.read.option("header", "true") \
    #              .option("inferSchema", "true") \
    #              .option("multiLine", "true") \
    #              .csv(landing_path)
    raise NotImplementedError

    # TODO 3: Add two audit columns to df using selectExpr
    #
    #   "_ingested_at"  — the timestamp when this job ran
    #                     SQL function: current_timestamp()
    #
    #   "_source_file"  — the full S3 path of the source file
    #                     SQL function: input_file_name()
    #
    # Hint: df = df.selectExpr(
    #               "*",
    #               "current_timestamp() AS _ingested_at",
    #               "input_file_name()   AS _source_file",
    #           )
    raise NotImplementedError

    count = df.count()
    logger.info("Ingested %d records", count)

    # TODO 4: Write df as Parquet to raw_path
    #
    # Requirements:
    #   - mode="overwrite"            (so the job is re-runnable)
    #   - partitioned by "order_date" (enables partition pruning downstream)
    #
    # Hint: df.write.mode("overwrite").partitionBy("order_date").parquet(raw_path)
    raise NotImplementedError

    logger.info("Ingest complete.")
    return count


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: exercise_ingest.py <landing_s3_path> <raw_s3_path>")
        sys.exit(1)

    landing_s3 = sys.argv[1]
    raw_s3 = sys.argv[2]

    # TODO 5: Wire it together
    #
    # (a) Call get_spark() to create the session
    # (b) Call ingest() inside a try/finally block
    # (c) Always call spark.stop() in the finally block
    #
    # Hint:
    #   spark = get_spark()
    #   try:
    #       ingest(spark, landing_s3, raw_s3)
    #   finally:
    #       spark.stop()
    raise NotImplementedError
