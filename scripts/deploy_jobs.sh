#!/usr/bin/env bash
# Deploy / update all CDE Spark jobs using CDE CLI.
# Run from repo root. Requires CDE CLI configured with --vcluster-endpoint.
#
# Usage:
#   export CDE_VC_ENDPOINT=https://<your-vc-endpoint>.cde.cloudera.com
#   ./scripts/deploy_jobs.sh

set -euo pipefail

RESOURCE_NAME="workshop-files"
PYTHON_ENV="workshop-python-env"

echo "==> Creating/updating CDE file resource: ${RESOURCE_NAME}"
cde resource create --name "${RESOURCE_NAME}" --type files 2>/dev/null || true

echo "==> Uploading job scripts"
cde resource upload --name "${RESOURCE_NAME}" \
  --local-path jobs/ingest/ingest_raw.py \
  --local-path jobs/validate/validate_data.py \
  --local-path jobs/transform/transform.py \
  --local-path jobs/load/load_curated.py

echo "==> Uploading GE config"
cde resource upload --name "${RESOURCE_NAME}" \
  --local-path great_expectations/great_expectations.yml \
  --local-path great_expectations/expectations/retail_raw_suite.json \
  --local-path great_expectations/checkpoints/raw_checkpoint.yml

echo "==> Creating/updating Python environment resource: ${PYTHON_ENV}"
cde resource create --name "${PYTHON_ENV}" --type python-env 2>/dev/null || true
cde resource upload --name "${PYTHON_ENV}" \
  --local-path resources/requirements.txt

# ── CDE Job definitions ───────────────────────────────────────────────────────

create_or_update_job() {
  local JOB_NAME=$1
  local SCRIPT=$2
  local ARGS=$3

  if cde job describe --name "${JOB_NAME}" &>/dev/null; then
    echo "==> Updating job: ${JOB_NAME}"
    cde job update --name "${JOB_NAME}" \
      --application-file "${SCRIPT}" \
      --arg "${ARGS}" \
      --mount-1-resource "${RESOURCE_NAME}" \
      --python-env-resource-name "${PYTHON_ENV}"
  else
    echo "==> Creating job: ${JOB_NAME}"
    cde job create --name "${JOB_NAME}" \
      --type spark \
      --application-file "${SCRIPT}" \
      --arg "${ARGS}" \
      --mount-1-resource "${RESOURCE_NAME}" \
      --python-env-resource-name "${PYTHON_ENV}"
  fi
}

create_or_update_job "workshop-ingest-raw" \
  "ingest_raw.py" \
  "{{landing_path}} {{raw_path}}"

create_or_update_job "workshop-validate-data" \
  "validate_data.py" \
  "{{raw_path}} {{ge_root_dir}}"

create_or_update_job "workshop-transform" \
  "transform.py" \
  "{{raw_path}} {{validated_path}}"

create_or_update_job "workshop-load-curated" \
  "load_curated.py" \
  "{{validated_path}} {{curated_path}} {{hive_database}}"

echo ""
echo "All CDE jobs deployed successfully."
