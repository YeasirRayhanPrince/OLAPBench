#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Supported values: postgres, hyper, umbra, cedardb
DBMS_NAME="duckdb"
BENCHMARK_YAML="$SCRIPT_DIR/test/gendba/job.training.yaml"
RESULT_DIR="$SCRIPT_DIR/test/gendba/${DBMS_NAME}_job_training"
INPUT_CSV="$RESULT_DIR/job.csv"
OUTPUT_JSONL="$RESULT_DIR/job.training.jsonl"
INCLUDE_NON_SUCCESS=0

cd "$SCRIPT_DIR"
source .venv/bin/activate

./benchmark.sh --clear --verbose "$BENCHMARK_YAML"

# Export benchmark CSV to GenDBA JSONL training data.
# Set INCLUDE_NON_SUCCESS above to 1 to include non-success rows as well.
EXPORT_ARGS=(--input-csv "$INPUT_CSV" --output-jsonl "$OUTPUT_JSONL")
if [[ "$INCLUDE_NON_SUCCESS" == "1" ]]; then
  EXPORT_ARGS+=(--include-non-success)
fi

python util/export_gendba_training.py "${EXPORT_ARGS[@]}"
echo "Training data written to: $OUTPUT_JSONL"
