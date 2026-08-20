"""
POC Demo 2 — Data Quality with Great Expectations

Press Play to run. No arguments needed.

What this demonstrates:
  - Live IDE → CDE Spark session connection
  - GE validation runs on inline data (no S3 / file dependency)
  - Round 1: clean data  → all 15 expectations PASS
  - Round 2: dirty data  → expectations FAIL, showing exactly which rules were broken
  - HTML Data Docs report generated locally and opened in your browser
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from cde import CDESparkConnectSession
from great_expectations.core.batch import RuntimeBatchRequest
import great_expectations as gx

from demos.sample_data import ORDERS, BAD_ORDERS, SCHEMA

# ── Configuration ─────────────────────────────────────────────────────────────
CDE_SESSION_NAME = "cdecli"   # Change to your CDE session name
GE_ROOT_DIR      = "great_expectations"


def sep(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print('=' * 60)


def run_validation(context, df_pandas, label: str):
    """Run the GE retail_raw_suite against a pandas DataFrame and print results."""
    batch_request = RuntimeBatchRequest(
        datasource_name="pandas_datasource",
        data_connector_name="runtime_data_connector",
        data_asset_name="raw_orders",
        runtime_parameters={"batch_data": df_pandas},
        batch_identifiers={"run_id": label},
    )

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name="retail_raw_suite",
    )
    result = validator.validate()

    stats = result.statistics
    total     = stats["evaluated_expectations"]
    passed    = stats["successful_expectations"]
    failed    = stats["unsuccessful_expectations"]

    if result.success:
        print(f"  ✓  All {total} expectations PASSED.\n")
    else:
        print(f"  ✗  {failed} of {total} expectations FAILED:\n")
        for er in result.results:
            if not er.success:
                exp_type = er.expectation_config.expectation_type
                col      = er.expectation_config.kwargs.get("column", "table-level")
                print(f"      FAILED  [{col}]  {exp_type}")
        print()

    return result.success


# ── Main ──────────────────────────────────────────────────────────────────────

print(f"\nConnecting to CDE session: {CDE_SESSION_NAME} ...")
spark = CDESparkConnectSession.builder.sessionName(CDE_SESSION_NAME).get()
print(f"Connected. Spark version: {spark.version}")

context = gx.get_context(context_root_dir=GE_ROOT_DIR)

try:
    # ── Round 1: clean data ───────────────────────────────────────────────────
    sep("ROUND 1 — CLEAN DATA  (20 valid orders)")
    df_clean   = spark.createDataFrame(ORDERS, SCHEMA)
    df_clean_pd = df_clean.toPandas()
    print(f"  Rows: {len(df_clean_pd)}")
    run_validation(context, df_clean_pd, label="clean_run")

    # ── Round 2: dirty data ───────────────────────────────────────────────────
    sep("ROUND 2 — DIRTY DATA  (6 bad rows injected)")
    df_dirty    = spark.createDataFrame(BAD_ORDERS, SCHEMA)
    df_dirty_pd = df_dirty.toPandas()
    print(f"  Rows: {len(df_dirty_pd)}  (+6 intentionally bad rows)\n")
    print("  Bad rows injected:")
    print("    - 1 row with null order_id")
    print("    - 1 duplicate order_id (ORD0001)")
    print("    - 1 row with zero quantity")
    print("    - 1 row with negative unit_price")
    print("    - 1 row with invalid category ('unknown')")
    print("    - 1 row with bad date format ('20260127')")
    print()
    run_validation(context, df_dirty_pd, label="dirty_run")

    # ── Open Data Docs ────────────────────────────────────────────────────────
    sep("DATA DOCS REPORT")
    docs_path = os.path.abspath(
        os.path.join(GE_ROOT_DIR, "data_docs", "local_site", "index.html")
    )
    print(f"  Report saved to: {docs_path}")
    print("  Opening in browser ...")
    context.open_data_docs()

finally:
    spark.stop()
