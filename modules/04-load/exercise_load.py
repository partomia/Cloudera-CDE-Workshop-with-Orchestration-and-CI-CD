"""
Module 04 Exercise — Load: Write Curated Tables + Register in Hive

Complete each TODO. There are no unit tests for this module — verify by running
the SQL queries in the README after deploying to CDE.

Reference solution: jobs/load/load_curated.py
"""

import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLES = ["orders", "customer_summary", "category_summary"]


def get_spark() -> SparkSession:
    # TODO 1: Create a SparkSession with appName "workshop-load-curated"
    #
    # IMPORTANT: You must call .enableHiveSupport() on the builder.
    # Without it, CREATE TABLE and DROP TABLE statements will fail.
    #
    # Hint: SparkSession.builder.appName("workshop-load-curated") \
    #           .enableHiveSupport() \
    #           .getOrCreate()
    raise NotImplementedError


def load(spark: SparkSession, validated_path: str, curated_path: str, database: str):
    # TODO 2: Create the Hive database if it does not already exist
    # Hint: spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")
    raise NotImplementedError

    for table in TABLES:
        src = f"{validated_path}/{table}"
        dst = f"{curated_path}/{table}"
        logger.info("Loading table '%s': %s -> %s", table, src, dst)

        # TODO 3: Read the validated Parquet for this table
        # Hint: spark.read.parquet(src)
        raise NotImplementedError

        # TODO 4: Write df to the curated S3 zone
        # Requirements: mode="overwrite", format=parquet
        # Hint: df.write.mode("overwrite").parquet(dst)
        raise NotImplementedError

        # TODO 5: Register as an external Hive table (two SQL statements)
        #
        # Step a — drop the table if it already exists (idempotency)
        # Hint: spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
        #
        # Step b — create an external table pointing to the curated S3 path
        # Hint:
        #   spark.sql(f"""
        #       CREATE EXTERNAL TABLE {database}.{table}
        #       STORED AS PARQUET
        #       LOCATION '{dst}'
        #   """)
        raise NotImplementedError

        logger.info("Registered: %s.%s (%d rows)", database, table, df.count())

    logger.info("Load complete. All tables registered in: %s", database)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: exercise_load.py <validated_s3> <curated_s3> <hive_database>")
        sys.exit(1)

    spark = get_spark()
    try:
        load(spark, sys.argv[1], sys.argv[2], sys.argv[3])
    finally:
        spark.stop()
