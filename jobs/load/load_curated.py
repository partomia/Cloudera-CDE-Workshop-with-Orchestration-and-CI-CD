"""
Module 04 - Load Job
Reads validated Parquet and writes final curated tables.
Registers tables in Hive Metastore for SQL access in Cloudera.
If no paths provided, reads from and writes to default local CDE paths.

Reference solution for: modules/04-load/exercise_load.py
"""

import sys
import logging
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TABLES = ["orders", "customer_summary", "category_summary"]

DEFAULT_VALIDATED_PATH = "s3a://go01-demo/workshop/validated"
DEFAULT_CURATED_PATH   = "s3a://go01-demo/workshop/curated"
DEFAULT_DATABASE       = "workshop_db"


def _is_unset(val: str) -> bool:
    return not val or (val.startswith("{{") and val.endswith("}}"))


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

        logger.info("Loading table '%s': %s -> %s", table, src, dst)
        df = spark.read.parquet(src)

        df.write.mode("overwrite").parquet(dst)

        spark.sql(f"DROP TABLE IF EXISTS {database}.{table}")
        spark.sql(f"""
            CREATE TABLE {database}.{table}
            USING PARQUET
            LOCATION '{dst}'
        """)

        logger.info("Registered Hive table: %s.%s (%d rows)", database, table, df.count())

    logger.info("Load complete. All tables registered in: %s", database)


if __name__ == "__main__":
    validated = sys.argv[1] if len(sys.argv) > 1 else None
    curated   = sys.argv[2] if len(sys.argv) > 2 else None
    database  = sys.argv[3] if len(sys.argv) > 3 else None

    validated_path = validated if validated and not _is_unset(validated) else DEFAULT_VALIDATED_PATH
    curated_path   = curated   if curated   and not _is_unset(curated)   else DEFAULT_CURATED_PATH
    hive_db        = database  if database  and not _is_unset(database)  else DEFAULT_DATABASE

    logger.info("validated=%s  curated=%s  database=%s", validated_path, curated_path, hive_db)

    spark = get_spark()
    try:
        load(spark, validated_path, curated_path, hive_db)
    finally:
        spark.stop()
