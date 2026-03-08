#!/usr/bin/env bash
set -euo pipefail

# What this does:
#   Run the configured JOB training benchmark and export the results as training JSONL.
# How to run:
#   ./shell/run_postgres_job_training.sh
#
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/_common.sh"
require_venv

# Supported values: postgres, hyper, umbra, cedardb
DBMS_NAME="duckdb"
BENCHMARK_YAML="${REPO_ROOT}/test/gendba/job.training.yaml"
RESULT_DIR="${REPO_ROOT}/test/gendba/${DBMS_NAME}_job_training"
INPUT_CSV="${RESULT_DIR}/job.csv"
OUTPUT_JSONL="${RESULT_DIR}/job.training.jsonl"
INCLUDE_NON_SUCCESS=0

cd "${REPO_ROOT}"

"${REPO_ROOT}/benchmark.sh" --clear --verbose "${BENCHMARK_YAML}"

# Export benchmark CSV to GenDBA JSONL training data.
# Set INCLUDE_NON_SUCCESS above to 1 to include non-success rows as well.
EXPORT_ARGS=(--input-csv "$INPUT_CSV" --output-jsonl "$OUTPUT_JSONL")
if [[ "$INCLUDE_NON_SUCCESS" == "1" ]]; then
  EXPORT_ARGS+=(--include-non-success)
fi

python "${REPO_ROOT}/util/export_gendba_training.py" "${EXPORT_ARGS[@]}"
echo "Training data written to: $OUTPUT_JSONL"
