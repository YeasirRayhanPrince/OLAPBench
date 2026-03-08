#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DBMS_NAME="postgres"
BENCHMARK_YAML="$SCRIPT_DIR/test/gendba/gendba_pool.exhaustive.training.yaml"
RESULT_DIR="$SCRIPT_DIR/test/gendba/${DBMS_NAME}_gendba_pool_exhaustive_training"
INPUT_CSV="$RESULT_DIR/gendba_pool_exhaustive.csv"
OUTPUT_JSONL="$RESULT_DIR/gendba_pool_exhaustive.training.jsonl"
INCLUDE_NON_SUCCESS=0

cd "$SCRIPT_DIR"
source .venv/bin/activate

./benchmark.sh --clear --verbose "$BENCHMARK_YAML"

EXPORT_ARGS=(--input-csv "$INPUT_CSV" --output-jsonl "$OUTPUT_JSONL")
if [[ "$INCLUDE_NON_SUCCESS" == "1" ]]; then
  EXPORT_ARGS+=(--include-non-success)
fi

python util/export_gendba_training.py "${EXPORT_ARGS[@]}"
echo "Training data written to: $OUTPUT_JSONL"
