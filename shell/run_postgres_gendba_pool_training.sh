#!/usr/bin/env bash
set -euo pipefail

# What this does:
#   Run the standard PostgreSQL GenDBA benchmark and export the results as training JSONL.
# How to run:
#   ./shell/run_postgres_gendba_pool_training.sh
#
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/_common.sh"
require_venv

DBMS_NAME="postgres"
BENCHMARK_YAML="${REPO_ROOT}/test/gendba/gendba_pool.training.yaml"
RESULT_DIR="${REPO_ROOT}/test/gendba/${DBMS_NAME}_gendba_pool_training"
INPUT_CSV="${RESULT_DIR}/gendba_pool.csv"
OUTPUT_JSONL="${RESULT_DIR}/gendba_pool.training.jsonl"
INCLUDE_NON_SUCCESS=0

cd "${REPO_ROOT}"

"${REPO_ROOT}/benchmark.sh" --clear --verbose "${BENCHMARK_YAML}"

EXPORT_ARGS=(--input-csv "$INPUT_CSV" --output-jsonl "$OUTPUT_JSONL")
if [[ "$INCLUDE_NON_SUCCESS" == "1" ]]; then
  EXPORT_ARGS+=(--include-non-success)
fi

python "${REPO_ROOT}/util/export_gendba_training.py" "${EXPORT_ARGS[@]}"
echo "Training data written to: $OUTPUT_JSONL"
