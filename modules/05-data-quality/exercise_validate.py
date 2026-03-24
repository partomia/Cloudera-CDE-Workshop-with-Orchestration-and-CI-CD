"""
Module 05 Exercise — Data Quality with Great Expectations

Complete each TODO. Test locally first, then deploy to CDE.

Local test (uses pandas, no CDE required):
    python modules/05-data-quality/exercise_validate.py data/sample great_expectations

CDE run:
    cde job run --name workshop-validate-data \\
      --arg s3://your-bucket/raw \\
      --arg /app/mount/great_expectations

Reference solution: jobs/validate/validate_data.py
Suite definition:   great_expectations/expectations/retail_raw_suite.json
"""

import sys
import logging
from pyspark.sql import SparkSession
import great_expectations as gx
from great_expectations.core.batch import RuntimeBatchRequest

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_spark() -> SparkSession:
    # TODO 1: Create a SparkSession with appName "workshop-validate-data"
    # Hint: SparkSession.builder.appName("workshop-validate-data").getOrCreate()
    raise NotImplementedError


def validate(spark: SparkSession, raw_path: str, ge_root_dir: str) -> bool:
    """
    Load raw Parquet into a Spark DataFrame, run the GE checkpoint.
    Returns True if all expectations pass, False otherwise.
    """
    logger.info("Loading raw data from: %s", raw_path)

    # TODO 2: Read the raw Parquet from raw_path into a DataFrame called df
    # Hint: spark.read.parquet(raw_path)
    df = raise NotImplementedError

    # TODO 3: Register df as a Spark temporary view named "raw_orders"
    # GE's spark datasource references this view name internally.
    # Hint: df.createOrReplaceTempView("raw_orders")
    raise NotImplementedError

    logger.info("Initialising GE context from: %s", ge_root_dir)

    # TODO 4: Initialise the GE DataContext from ge_root_dir
    # Hint: gx.get_context(context_root_dir=ge_root_dir)
    context = raise NotImplementedError

    # TODO 5: Build a RuntimeBatchRequest that passes the DataFrame directly to GE
    #
    # Required fields:
    #   datasource_name      = "spark_datasource"
    #   data_connector_name  = "runtime_data_connector"
    #   data_asset_name      = "raw_orders"
    #   runtime_parameters   = {"batch_data": df}
    #   batch_identifiers    = {"run_id": "workshop_run"}
    #
    # Hint: RuntimeBatchRequest(
    #           datasource_name="spark_datasource",
    #           data_connector_name="runtime_data_connector",
    #           data_asset_name="raw_orders",
    #           runtime_parameters={"batch_data": df},
    #           batch_identifiers={"run_id": "workshop_run"},
    #       )
    batch_request = raise NotImplementedError

    logger.info("Running checkpoint: raw_checkpoint")

    # TODO 6: Run the checkpoint named "raw_checkpoint" against "retail_raw_suite"
    #
    # Hint: context.run_checkpoint(
    #           checkpoint_name="raw_checkpoint",
    #           validations=[{
    #               "batch_request": batch_request,
    #               "expectation_suite_name": "retail_raw_suite",
    #           }]
    #       )
    result = raise NotImplementedError

    # TODO 7: Log the results and return result.success
    #
    # If result.success is True:   log "All expectations passed."
    # If result.success is False:  log "Expectation failures detected!"
    #                              then iterate over result.run_results.values()
    #                              and log the stats from:
    #                              vr["validation_result"]["statistics"]
    #                              (keys: evaluated_expectations,
    #                                     successful_expectations,
    #                                     unsuccessful_expectations)
    raise NotImplementedError

    return result.success


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: exercise_validate.py <raw_path> <ge_root_dir>")
        sys.exit(1)

    spark = get_spark()
    try:
        success = validate(spark, sys.argv[1], sys.argv[2])
    finally:
        spark.stop()

    # TODO 8: Exit with the correct code
    #
    # A non-zero exit code tells Airflow's CDEJobRunOperator that this task failed,
    # which stops the pipeline before bad data reaches the transform step.
    #
    # Hint: sys.exit(0 if success else 1)
    raise NotImplementedError
