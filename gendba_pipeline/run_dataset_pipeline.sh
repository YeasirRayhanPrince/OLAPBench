#!/usr/bin/env bash

set -euo pipefail

# End-to-end launcher for the GenDBA dataset pipeline.
#
# What this script does:
# - Activates the repo virtual environment.
# - Uses the default dataset config that lives under gendba_pipeline/configs/.
# - Runs the full pipeline:
#   1. resolve workload queries from the YAML
#   2. build Calcite logical tokens
#   3. run OLAPBench for physical plans
#   4. write a training JSONL and a manifest JSONL
# - Forwards any extra CLI flags to gendba_pipeline/run.py.
#
# Common examples:
#   ./gendba_pipeline/run_dataset_pipeline.sh
#   ./gendba_pipeline/run_dataset_pipeline.sh --verbose
#   ./gendba_pipeline/run_dataset_pipeline.sh --run-id local_test_001
#   ./gendba_pipeline/run_dataset_pipeline.sh \
#     --config gendba_pipeline/configs/postgres_local_preloaded.dataset.yaml
#
# Notes:
# - The output root and output filenames are controlled by the dataset YAML config.
# - The default config currently targets local preloaded Postgres.
# - Unless you pass --run-id, this script assigns one at start time in UTC
#   using the format YYYYMMDDTHHMMSSZ.
# - The pipeline writes:
#     * training JSONL: compact model-facing rows
#     * manifest JSONL: source/provenance rows keyed by the same id
# - This script intentionally keeps all default pipeline assets under gendba_pipeline/.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Reuse the repo-wide virtualenv/bootstrap helpers.
# This keeps behavior aligned with the existing OLAPBench shell entrypoints.
source "${REPO_ROOT}/_common.sh"
require_venv

# Default config for local preloaded Postgres collection.
# Override with --config if you want a different pipeline definition.
DATASET_CONFIG="${REPO_ROOT}/gendba_pipeline/configs/postgres_local_preloaded.dataset.yaml"
RUN_ID="$(date -u +"%Y%m%dT%H%M%SZ")"

# Accumulate all flags that should be forwarded to run.py unchanged.
PIPELINE_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      if [[ $# -lt 2 ]]; then
        echo "--config requires a path" >&2
        exit 1
      fi

      # Accept either absolute or repo-relative paths.
      if [[ "$2" = /* ]]; then
        DATASET_CONFIG="$2"
      else
        DATASET_CONFIG="$(cd "$PWD" && pwd)/$2"
      fi
      shift 2
      ;;
    --run-id)
      if [[ $# -lt 2 ]]; then
        echo "--run-id requires a value" >&2
        exit 1
      fi
      RUN_ID="$2"
      shift 2
      ;;
    *)
      PIPELINE_ARGS+=("$1")
      shift
      ;;
  esac
done

cd "${REPO_ROOT}"

# Run the full Python pipeline entrypoint with the selected config.
"${PYTHON_BIN}" "${REPO_ROOT}/gendba_pipeline/run.py" \
  --config "${DATASET_CONFIG}" \
  --run-id "${RUN_ID}" \
  "${PIPELINE_ARGS[@]}"
