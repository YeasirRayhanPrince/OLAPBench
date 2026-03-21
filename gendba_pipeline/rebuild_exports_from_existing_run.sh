#!/usr/bin/env bash

set -euo pipefail

# Rebuild timestamped GenDBA exports from an existing dataset output folder.
#
# What this script does:
# - Activates the repo virtual environment.
# - Runs gendba_pipeline/rebuild_exports_from_existing_run.py.
# - Reads an already-existing dataset folder under gendba_pipeline/output/.
# - Reconstructs:
#     * gendba_pool.training.<run_id>.jsonl
#     * gendba_pool.manifest.<run_id>.jsonl
# - Skips queries that do not have any successful physical plan in the chosen run.
#
# Typical use cases:
# - A long benchmark run already produced Calcite artifacts and an OLAPBench CSV.
# - You want to regenerate training/manifest JSONLs without rerunning the benchmark.
# - You want to rebuild exports for a specific run id under dataset_root/work/.
#
# Examples:
#   ./gendba_pipeline/rebuild_exports_from_existing_run.sh
#
#   ./gendba_pipeline/rebuild_exports_from_existing_run.sh \
#     --dataset-root gendba_pipeline/output/gendba_local_postgres_preloaded
#
  # ./gendba_pipeline/rebuild_exports_from_existing_run.sh \
  #   --dataset-root gendba_pipeline/output/gendba_local_postgres_preloaded \
  #   --run-id 20260320T062124Z
#
# Notes:
# - If you do not pass --run-id, the Python script uses the latest run in <dataset-root>/work/.
# - The output filenames use that selected run id, not the current wall-clock time.
# - This script does not modify or depend on gendba_pipeline/run.py.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

source "${REPO_ROOT}/_common.sh"
require_venv

# Default dataset folder. Override with --dataset-root if needed.
DATASET_ROOT="${REPO_ROOT}/gendba_pipeline/output/gendba_local_postgres_preloaded"

# Forward all remaining flags to the Python re-export script.
EXPORT_ARGS=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dataset-root)
      if [[ $# -lt 2 ]]; then
        echo "--dataset-root requires a path" >&2
        exit 1
      fi

      if [[ "$2" = /* ]]; then
        DATASET_ROOT="$2"
      else
        DATASET_ROOT="$(cd "$PWD" && pwd)/$2"
      fi
      shift 2
      ;;
    *)
      EXPORT_ARGS+=("$1")
      shift
      ;;
  esac
done

cd "${REPO_ROOT}"

"${PYTHON_BIN}" "${REPO_ROOT}/gendba_pipeline/rebuild_exports_from_existing_run.py" \
  --dataset-root "${DATASET_ROOT}" \
  "${EXPORT_ARGS[@]}"
