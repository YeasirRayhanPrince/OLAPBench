#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# GenDBA Pipeline — DuckDB Explain-Only Runner
# ============================================================================
# Runs gendba_pipeline/run.py with --explain-only against a local DuckDB
# database file. Produces a standalone DuckDB training JSONL that can then
# be merged into an existing base JSONL with augment_jsonl.py.
#
# Requirements:
#   - duckdb Python package must be installed in the venv (pip install duckdb)
#   - The dataset YAML must have a `local:` block with `enabled: true` and
#     a `path:` field pointing to the .duckdb file.
#
# Edit the variables below, then run:
#   bash gendba_pipeline/run_explain_duckdb.sh
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# ── Config ───────────────────────────────────────────────────────────────────
# Path to the dataset pipeline YAML (absolute or relative to REPO_ROOT).
CONFIG="${SCRIPT_DIR}/configs/duckdb_local.dataset.yaml"

# ── Optional run settings ────────────────────────────────────────────────────
# Leave RUN_ID empty to use an auto-generated UTC timestamp.
RUN_ID=""

# Set to true to enable verbose benchmark output.
VERBOSE=false

# Set to true to stop at the first engine/group failure.
FAIL_FAST=false

# Number of parallel worker processes for EXPLAIN ANALYZE + plan parsing.
# DuckDB supports concurrent read-only connections, so parallelism is safe.
# Default (0) lets run.py choose min(64, nproc). Set to 1 to disable parallelism.
EXPLAIN_WORKERS=0

# Number of parallel JVM processes for Calcite plan generation + post-processing.
# Default (0) lets run.py choose min(16, nproc). Set to 1 to disable parallelism.
CALCITE_WORKERS=0

# ============================================================================
# Build command — no need to edit below this line
# ============================================================================

CMD=(
    python gendba_pipeline/run.py
    --config "${CONFIG}"
    --explain-only
)

if [[ -n "${RUN_ID}" ]]; then
    CMD+=(--run-id "${RUN_ID}")
fi

if [[ "${VERBOSE}" == true ]]; then
    CMD+=(--verbose)
fi

if [[ "${FAIL_FAST}" == true ]]; then
    CMD+=(--fail-fast)
fi

if [[ "${EXPLAIN_WORKERS}" -gt 0 ]]; then
    CMD+=(--explain-workers "${EXPLAIN_WORKERS}")
fi

if [[ "${CALCITE_WORKERS}" -gt 0 ]]; then
    CMD+=(--calcite-workers "${CALCITE_WORKERS}")
fi

# ── Run ──────────────────────────────────────────────────────────────────────

echo "Running from ${REPO_ROOT}"
if [[ ! -f "${VENV_ACTIVATE}" ]]; then
    echo "Virtual environment activation script not found: ${VENV_ACTIVATE}" >&2
    exit 1
fi

cd "${REPO_ROOT}"
source "${VENV_ACTIVATE}"
echo "Activated virtual environment: ${VENV_ACTIVATE}"

printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

"${CMD[@]}"
