#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# GenDBA Pipeline — Explain-Only DPO Runner
# ============================================================================
# Runs gendba_pipeline/run.py with --explain-only and --dpo-variants N, which
# issues EXPLAIN (format json) for every query (default plan) and additionally
# generates N PostBOUND hint-forced plan variants per query via pg_hint_plan
# Leading(...) hints. The extra variants are written to a new non-breaking
# `plan_variants` field in each JSONL record; all existing fields are unchanged.
#
# Requirements:
#   - PostgreSQL must be reachable with the connection configured in CONFIG.
#   - pg_hint_plan must be installed and enabled (pg_hint_plan.enable_hint = on).
#     If unavailable, variant generation is silently skipped per query.
#   - The dataset YAML must have a `local:` block with `enabled: true`.
#   - psycopg2 must be installed in the venv.
#   - PostBOUND must be present at PostBOUND/ in the repo root.
#
# Edit the variables below, then run:
#   bash gendba_pipeline/run_explain_pgsql_dpo.sh
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# ── Config ───────────────────────────────────────────────────────────────────
# Path to the dataset pipeline YAML (absolute or relative to REPO_ROOT).
CONFIG="${SCRIPT_DIR}/configs/postgres_local_preloaded.dataset.yaml"

# ── Optional run settings ────────────────────────────────────────────────────
# Leave RUN_ID empty to use an auto-generated UTC timestamp.
RUN_ID=""

# Set to true to enable verbose benchmark output.
VERBOSE=false

# Set to true to stop at the first engine/group failure.
FAIL_FAST=false

# Number of parallel worker processes for EXPLAIN + plan parsing.
# Default (0) lets run.py choose min(64, nproc). Set to 1 to disable parallelism.
EXPLAIN_WORKERS=0

# Number of parallel JVM processes for Calcite plan generation + post-processing.
# Default (0) lets run.py choose min(16, nproc). Set to 1 to disable parallelism.
CALCITE_WORKERS=0

# Number of random join-order variants per query (Leading() hints). Requires pg_hint_plan.
DPO_VARIANTS=10

# Number of random physical-operator variants per query (Leading + NestLoop/HashJoin/etc.).
DPO_VARIANTS_OP=10

# Number of cardinality-injection variants per query (Rows() hints with distorted estimates).
DPO_VARIANTS_CARD=10

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

if [[ "${DPO_VARIANTS}" -gt 0 ]]; then
    CMD+=(--dpo-variants "${DPO_VARIANTS}")
fi

if [[ "${DPO_VARIANTS_OP}" -gt 0 ]]; then
    CMD+=(--dpo-variants-op "${DPO_VARIANTS_OP}")
fi

if [[ "${DPO_VARIANTS_CARD}" -gt 0 ]]; then
    CMD+=(--dpo-variants-card "${DPO_VARIANTS_CARD}")
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
