"""
Module 3 - Validate Job
Runs Great Expectations checkpoint on raw Parquet data.
Exits with code 1 if any expectation fails — Airflow marks the task FAILED.
"""

import sys
import logging
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("workshop-validate-data")
        .getOrCreate()
    )


def validate(spark: SparkSession, raw_path: str, ge_root_dir: str) -> bool:
    """
    Load raw Parquet into a Spark DataFrame, run GE checkpoint.
    Returns True if all expectations pass, False otherwise.
    """
    logger.info("Loading raw data from: %s", raw_path)
    df = spark.read.parquet(raw_path)
    df.createOrReplaceTempView("raw_orders")

    logger.info("Initialising Great Expectations context from: %s", ge_root_dir)
    context = gx.get_context(context_root_dir=ge_root_dir)

    batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="raw_orders",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "workshop_run"},
    )

    logger.info("Running checkpoint: raw_checkpoint")
    result = context.run_checkpoint(
        checkpoint_name="raw_checkpoint",
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": "retail_raw_suite",
            }
        ],
    )

    if result.success:
        logger.info("All expectations passed.")
    else:
        logger.error("Expectation failures detected! Review Data Docs.")
        for vr in result.run_results.values():
            stats = vr["validation_result"]["statistics"]
            logger.error(
                "Evaluated: %d  Successful: %d  Failed: %d",
                stats["evaluated_expectations"],
                stats["successful_expectations"],
                stats["unsuccessful_expectations"],
            )

    return result.success


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: validate_data.py <raw_s3_path> <ge_root_dir>")
        sys.exit(1)

    raw_s3 = sys.argv[1]
    ge_dir = sys.argv[2]

    spark = get_spark()
    try:
        success = validate(spark, raw_s3, ge_dir)
    finally:
        spark.stop()

    sys.exit(0 if success else 1)
