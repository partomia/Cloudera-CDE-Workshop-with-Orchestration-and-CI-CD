#!/usr/bin/env bash
# Upload Airflow DAG to CDE Airflow service.
# Requires CDE CLI and an Airflow-enabled CDE service.
#
# Usage:
#   export CDE_VC_ENDPOINT=https://<your-vc-endpoint>.cde.cloudera.com
#   ./scripts/deploy_dag.sh

set -euo pipefail

DAG_RESOURCE="workshop-dags"
DAG_FILE="dags/etl_pipeline_dag.py"

echo "==> Creating/updating DAG resource: ${DAG_RESOURCE}"
cde resource create --name "${DAG_RESOURCE}" --type files 2>/dev/null || true

echo "==> Uploading DAG: ${DAG_FILE}"
cde resource upload --name "${DAG_RESOURCE}" --local-path "${DAG_FILE}"

echo "==> Creating/updating Airflow job for DAG"
if cde job describe --name "retail-etl-pipeline-dag" &>/dev/null; then
  cde job update --name "retail-etl-pipeline-dag" \
    --dag-file "etl_pipeline_dag.py" \
    --mount-1-resource "${DAG_RESOURCE}"
else
  cde job create --name "retail-etl-pipeline-dag" \
    --type airflow \
    --dag-file "etl_pipeline_dag.py" \
    --mount-1-resource "${DAG_RESOURCE}"
fi

echo "DAG deployed successfully."
