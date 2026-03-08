#!/usr/bin/env bash

set -euo pipefail

# What this does:
#   Run the self-contained GenDBA dataset pipeline for a local PostgreSQL target.
# How to run:
#   ./shell/run_local_postgres_benchmark.sh
#   ./shell/run_local_postgres_benchmark.sh --config gendba_pipeline/configs/postgres_local_preloaded.dataset.yaml
#
source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/_common.sh"
require_venv

DATASET_CONFIG="${REPO_ROOT}/gendba_pipeline/configs/postgres_local_preloaded.dataset.yaml"
PIPELINE_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --config)
      if [[ $# -lt 2 ]]; then
        echo "--config requires a path" >&2
        exit 1
      fi
      if [[ "$2" = /* ]]; then
        DATASET_CONFIG="$2"
      else
        DATASET_CONFIG="$(cd "$PWD" && pwd)/$2"
      fi
      shift 2
      ;;
    *)
      PIPELINE_ARGS+=("$1")
      shift
      ;;
  esac
done

cd "${REPO_ROOT}"

"${PYTHON_BIN}" "${REPO_ROOT}/gendba_pipeline/run.py" \
  --config "${DATASET_CONFIG}" \
  "${PIPELINE_ARGS[@]}"
