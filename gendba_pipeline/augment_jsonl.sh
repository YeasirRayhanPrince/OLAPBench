#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# GenDBA Pipeline — JSONL Augmenter
# ============================================================================
# Merges physical-plan tokens from a patch JSONL (e.g. DuckDB) into a base
# training JSONL (e.g. PostgreSQL). Records are matched by sql_file_name.
# The base file is never modified; output always goes to OUT.
#
# Edit the variables below, then run:
#   bash gendba_pipeline/augment_jsonl.sh
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# ── Config ───────────────────────────────────────────────────────────────────
BASE="${SCRIPT_DIR}/output/gendba_local_postgres_preloaded/gendba_local_postgres_preloaded_full_vn_dpo.training.jsonl"
PATCH="${SCRIPT_DIR}/output/gendba_local_duckdb/gendba_local_duckdb_preloaded_full_vn.training.jsonl"
OUT="${SCRIPT_DIR}/output/dpo/dpo_full_vn.training.jsonl"

# BASE="${SCRIPT_DIR}/output/gendba_local_postgres_preloaded/gendba_local_postgres_preloaded_job.validation.jsonl"
# PATCH="${SCRIPT_DIR}/output/gendba_local_duckdb/gendba_local_duckdb_preloaded_job.validation.jsonl"
# OUT="${SCRIPT_DIR}/output/merged/merged_job.validation.jsonl"

# ============================================================================
# Run — no need to edit below this line
# ============================================================================

echo "Running from ${REPO_ROOT}"
if [[ ! -f "${VENV_ACTIVATE}" ]]; then
    echo "Virtual environment activation script not found: ${VENV_ACTIVATE}" >&2
    exit 1
fi

cd "${REPO_ROOT}"
source "${VENV_ACTIVATE}"
echo "Activated virtual environment: ${VENV_ACTIVATE}"

CMD=(
    python gendba_pipeline/augment_jsonl.py
    --base  "${BASE}"
    --patch "${PATCH}"
    --out   "${OUT}"
)

printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

"${CMD[@]}"
