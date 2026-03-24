"""
Module 04 - Load Job
Reads validated Parquet and writes final curated tables to S3 curated zone.
Registers tables in Hive Metastore for SQL access in Cloudera.
Reference solution for: modules/04-load/exercise_load.py
"""

import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLES = ["orders", "customer_summary", "category_summary"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("workshop-load-curated")
        .enableHiveSupport()
        .getOrCreate()
    )


def load(spark: SparkSession, validated_path: str, curated_path: str, database: str):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {database}")

    for table in TABLES:
        src = f"{validated_path}/{table}"
        dst = f"{curated_path}/{table}"

        logger.info("Loading table '%s' from %s -> %s", table, src, dst)
        df = spark.read.parquet(src)

        # Write to curated S3 zone
        df.write.mode("overwrite").parquet(dst)

        # Register as external Hive table
        spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
        spark.sql(f"""
            CREATE EXTERNAL TABLE {database}.{table}
            STORED AS PARQUET
            LOCATION '{dst}'
        """)

        logger.info("Registered Hive table: %s.%s (%d rows)", database, table, df.count())

    logger.info("Load complete. All tables registered in database: %s", database)


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: load_curated.py <validated_s3_path> <curated_s3_path> <hive_database>")
        sys.exit(1)

    validated_s3 = sys.argv[1]
    curated_s3 = sys.argv[2]
    hive_db = sys.argv[3]

    spark = get_spark()
    try:
        load(spark, validated_s3, curated_s3, hive_db)
    finally:
        spark.stop()
