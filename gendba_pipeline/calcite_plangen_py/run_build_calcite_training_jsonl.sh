#!/usr/bin/env bash
set -euo pipefail

# Run build_calcite_training_jsonl.py with project defaults.
# Defaults:
#   schema:  benchmarks/job/schema/job.dbschema.json
#   queries: benchmarks/job/queries
#   output:  output/calcite_training/job_calcite_training.jsonl
#
# Usage:
#   1) Edit the variables in "CONFIG" below.
#   2) Run: ./run_build_calcite_training_jsonl.sh

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ---------------------------
# CONFIG: edit these values
# ---------------------------
WORKLOAD="sqlstorm"
SCHEMA_PATH="$SCRIPT_DIR/benchmarks/$WORKLOAD/schema/$WORKLOAD.dbschema.json"
QUERIES_DIR="$SCRIPT_DIR/benchmarks/$WORKLOAD/queries"
OUTPUT_FILE="$SCRIPT_DIR/output/calcite_training/${WORKLOAD}_calcite_training.jsonl"

# Required if `plangen` is not installed in PATH.
# Example:
#   PLANGEN_BIN="$HOME/.local/bin/plangen"
PLANGEN_BIN="/scratch1/yrayhan/calcite/benchmarks/plangen/build/install/plangen/bin/plangen"
# Optional working directory for plan artifacts (.plan.json/.plan.txt, etc.).
# - Leave empty to let the Python script create: tmp/calcite_plans_<timestamp>
# - Set a path to persist artifacts in that directory.
WORK_DIR=""
# Only applies when WORK_DIR is empty (auto temp directory mode):
# - false: delete auto temp dir after completion
# - true: keep auto temp dir for inspection/debugging
KEEP_WORK_DIR=false

mkdir -p "$(dirname "$OUTPUT_FILE")"

CMD=(
  python3 "$SCRIPT_DIR/build_calcite_training_jsonl.py"
  --schema-path "$SCHEMA_PATH"
  --queries-dir "$QUERIES_DIR"
  --output-file "$OUTPUT_FILE"
)

if [[ -z "$PLANGEN_BIN" ]]; then
  if command -v plangen >/dev/null 2>&1; then
    PLANGEN_BIN="$(command -v plangen)"
  else
    echo "Error: plangen was not found." >&2
    echo "Set PLANGEN_BIN in this script (e.g., /path/to/plangen), or install plangen in PATH." >&2
    exit 1
  fi
fi
CMD+=(--plangen-bin "$PLANGEN_BIN")

if [[ -n "$WORK_DIR" ]]; then
  CMD+=(--work-dir "$WORK_DIR")
fi

if [[ "$KEEP_WORK_DIR" == "true" ]]; then
  CMD+=(--keep-work-dir)
fi

printf 'Running command:\n  '
printf '%q ' "${CMD[@]}"
printf '\n\n'

"${CMD[@]}"
