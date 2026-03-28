"""
Module 05 - Validate Job
Runs Great Expectations checkpoint on raw Parquet data.
Exits with code 1 if any expectation fails — Airflow marks the task FAILED.
If no paths provided, reads from default local CDE path and uses bundled GE config.

Reference solution for: modules/05-data-quality/exercise_validate.py
"""

import sys
import logging
from pyspark.sql import SparkSession, DataFrame

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DEFAULT_RAW_PATH = "/tmp/workshop/raw"
DEFAULT_GE_ROOT  = "/app/mount/great_expectations"

try:
    import great_expectations as gx
    from great_expectations.core.batch import RuntimeBatchRequest
    GE_AVAILABLE = True
    logger.info("Great Expectations available — using GE validation.")
except ImportError:
    GE_AVAILABLE = False
    logger.warning("Great Expectations not installed — using native Spark validation fallback.")


def _is_unset(val: str) -> bool:
    return not val or (val.startswith("{{") and val.endswith("}}"))


def get_spark() -> SparkSession:
    return SparkSession.builder.appName("workshop-validate-data").getOrCreate()


def validate_with_ge(spark: SparkSession, df: DataFrame, ge_root_dir: str) -> bool:
    context = gx.get_context(context_root_dir=ge_root_dir)
    batch_request = RuntimeBatchRequest(
        datasource_name="spark_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="raw_orders",
        runtime_parameters={"batch_data": df},
        batch_identifiers={"run_id": "workshop_run"},
    )
    result = context.run_checkpoint(
        checkpoint_name="raw_checkpoint",
        validations=[{"batch_request": batch_request, "expectation_suite_name": "retail_raw_suite"}],
    )
    if result.success:
        logger.info("GE: all expectations passed.")
    else:
        for vr in result.run_results.values():
            stats = vr["validation_result"]["statistics"]
            logger.error("GE failures — evaluated: %d  passed: %d  failed: %d",
                stats["evaluated_expectations"],
                stats["successful_expectations"],
                stats["unsuccessful_expectations"])
    return result.success


def validate_with_spark(spark: SparkSession, df: DataFrame) -> bool:
    """Native Spark fallback: basic data quality assertions."""
    failures = []

    total = df.count()
    if total == 0:
        failures.append("Dataset is empty")

    nulls = df.filter(
        "order_id IS NULL OR customer_id IS NULL OR product_id IS NULL OR order_date IS NULL"
    ).count()
    if nulls > 0:
        failures.append(f"{nulls} rows with NULL in required key columns")

    bad_qty = df.filter("quantity <= 0").count()
    if bad_qty > 0:
        failures.append(f"{bad_qty} rows with quantity <= 0")

    bad_price = df.filter("unit_price <= 0").count()
    if bad_price > 0:
        failures.append(f"{bad_price} rows with unit_price <= 0")

    logger.info("Native Spark validation — total rows: %d", total)
    if failures:
        for f in failures:
            logger.error("VALIDATION FAILED: %s", f)
        return False

    logger.info("Native Spark validation passed — all checks OK.")
    return True


def validate(spark: SparkSession, raw_path: str, ge_root_dir: str) -> bool:
    logger.info("Loading raw data from: %s", raw_path)
    df = spark.read.parquet(raw_path)

    if GE_AVAILABLE:
        return validate_with_ge(spark, df, ge_root_dir)
    else:
        return validate_with_spark(spark, df)


if __name__ == "__main__":
    raw    = sys.argv[1] if len(sys.argv) > 1 else None
    ge_dir = sys.argv[2] if len(sys.argv) > 2 else None

    raw_path   = raw    if raw    and not _is_unset(raw)    else DEFAULT_RAW_PATH
    ge_root    = ge_dir if ge_dir and not _is_unset(ge_dir) else DEFAULT_GE_ROOT

    logger.info("raw_path=%s  ge_root=%s", raw_path, ge_root)

    spark = get_spark()
    try:
        success = validate(spark, raw_path, ge_root)
    finally:
        spark.stop()

    sys.exit(0 if success else 1)
