"""
Smoke test: connect to the CDE spark-connect session using the cdeconnect package.

Prerequisites:
    1. Download cdeconnect tarball from CDE console → Sessions → Spark Connect → Configuration
    2. pip install /path/to/cdeconnect-*.tar.gz
    3. pip install /path/to/pyspark-3.5.*.tar.gz  (version must match cdeconnect)
    4. ~/.cde/config.yaml must have vcluster-endpoint set (already done)

Usage:
    python tests/test_cde_connect.py
"""

from cde import CDESparkConnectSession

SESSION_NAME = "r1rsingh"

print(f"Connecting to CDE session: {SESSION_NAME}")
spark = CDESparkConnectSession.builder.sessionName(SESSION_NAME).get()

print(f"Spark version  : {spark.version}")

# Simple test: create a DataFrame and count rows
df = spark.range(10)
count = df.count()
print(f"spark.range(10).count() = {count}")
assert count == 10, f"Expected 10, got {count}"

print("\nSUCCESS: IDE -> CDE Spark Connect session is working.")
spark.stop()
