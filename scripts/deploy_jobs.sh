#!/usr/bin/env bash
# Deploy / update all CDE Spark jobs using CDE CLI.
# Run from repo root. Requires CDE CLI configured with --vcluster-endpoint.
#
# Usage:
#   export CDE_VC_ENDPOINT=https://<your-vc-endpoint>.cde.cloudera.com
#   ./scripts/deploy_jobs.sh

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESOURCE_NAME="workshop-files"
PYTHON_ENV="workshop-python-env"

echo "==> Creating/updating CDE file resource: ${RESOURCE_NAME}"
cde resource create --name "${RESOURCE_NAME}" --type files 2>/dev/null || true

echo "==> Uploading job scripts"
cde resource upload --name "${RESOURCE_NAME}" \
  --local-path "${REPO_ROOT}/jobs/ingest/ingest_raw.py" \
  --local-path "${REPO_ROOT}/jobs/validate/validate_data.py" \
  --local-path "${REPO_ROOT}/jobs/transform/transform.py" \
  --local-path "${REPO_ROOT}/jobs/load/load_curated.py"

echo "==> Uploading GE config"
cde resource upload --name "${RESOURCE_NAME}" \
  --local-path "${REPO_ROOT}/great_expectations/great_expectations.yml" \
  --local-path "${REPO_ROOT}/great_expectations/expectations/retail_raw_suite.json" \
  --local-path "${REPO_ROOT}/great_expectations/checkpoints/raw_checkpoint.yml"

echo "==> Creating/updating Python environment resource: ${PYTHON_ENV}"
cde resource create --name "${PYTHON_ENV}" --type python-env 2>/dev/null || true
cde resource upload --name "${PYTHON_ENV}" \
  --local-path "${REPO_ROOT}/resources/requirements.txt"

# ── CDE Job definitions ───────────────────────────────────────────────────────

create_or_update_job() {
  local JOB_NAME=$1
  local SCRIPT=$2

  if cde job describe --name "${JOB_NAME}" &>/dev/null; then
    echo "==> Updating job: ${JOB_NAME}"
    cde job update --name "${JOB_NAME}" \
      --application-file "${SCRIPT}" \
      --mount-1-resource "${RESOURCE_NAME}" \
      --python-env-resource-name "${PYTHON_ENV}"
  else
    echo "==> Creating job: ${JOB_NAME}"
    cde job create --name "${JOB_NAME}" \
      --type spark \
      --application-file "${SCRIPT}" \
      --mount-1-resource "${RESOURCE_NAME}" \
      --python-env-resource-name "${PYTHON_ENV}"
  fi
}

# No --arg flags: jobs use built-in defaults (/tmp/workshop/...).
# S3 paths are passed at runtime via CDEJobRunOperator overrides in the DAG.
create_or_update_job "workshop-ingest-raw"    "ingest_raw.py"
create_or_update_job "workshop-validate-data" "validate_data.py"
create_or_update_job "workshop-transform"     "transform.py"
create_or_update_job "workshop-load-curated"  "load_curated.py"

echo ""
echo "All CDE jobs deployed successfully."
