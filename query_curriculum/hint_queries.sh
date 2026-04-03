#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# Hint Queries — Runner Script
# ============================================================================
# Generates pg_hint_plan Leading() hint variants for existing SQL query files
# using PostBOUND's exhaustive join order enumerator.
#
# For each .sql file in queries_<SUFFIX>/ a set of hinted variants is written
# to queries_<SUFFIX>_hinted/:
#   <query>_h0000.sql  →  original SQL, no hint (default plan)
#   <query>_h0001.sql  →  /*+ Leading((...)) */ + original SQL
#   …
#
# Run:
#   bash query_curriculum/hint_queries.sh
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# PostBOUND requires Python 3.12. Use the postbound conda env interpreter.
PYTHON312="/scratch1/yrayhan/condaenvs/postbound/bin/python3.12"
POSTBOUND_DIR="${REPO_ROOT}/PostBOUND"

# ── Config ───────────────────────────────────────────────────────────────────

BENCHMARK="gendba_pool"
SUFFIX="queries_full_vn"          # input dir: benchmarks/<BENCHMARK>/<SUFFIX>/
OUTPUT_SUFFIX="${SUFFIX}_hinted"  # output dir: benchmarks/<BENCHMARK>/<OUTPUT_SUFFIX>/

MAX_PLANS=50              # hint variants per query (not counting the no-hint default)
TREE_STRUCTURE="bushy"    # bushy | left-deep | right-deep
WORKERS=32                 # parallel worker processes

# ── Derived paths ─────────────────────────────────────────────────────────────

QUERIES_DIR="${REPO_ROOT}/benchmarks/${BENCHMARK}/${SUFFIX}"
OUTPUT_DIR="${REPO_ROOT}/benchmarks/${BENCHMARK}/${OUTPUT_SUFFIX}"

# ── Pre-flight checks ────────────────────────────────────────────────────────

if [[ ! -x "${PYTHON312}" ]]; then
    echo "ERROR: Python 3.12 not found at ${PYTHON312}" >&2
    exit 1
fi

if [[ ! -d "${POSTBOUND_DIR}" ]]; then
    echo "ERROR: PostBOUND not found at ${POSTBOUND_DIR}" >&2
    exit 1
fi

if [[ ! -d "${QUERIES_DIR}" ]]; then
    echo "ERROR: Queries dir not found: ${QUERIES_DIR}" >&2
    exit 1
fi

echo "PostBOUND:      ${POSTBOUND_DIR}"
echo "Python 3.12:    ${PYTHON312}"
echo "Queries dir:    ${QUERIES_DIR}"
echo "Output dir:     ${OUTPUT_DIR}"
echo "Max plans:      ${MAX_PLANS}"
echo "Tree structure: ${TREE_STRUCTURE}"
echo "Workers:        ${WORKERS}"
echo ""

# ── Run ──────────────────────────────────────────────────────────────────────

PYTHONPATH="${POSTBOUND_DIR}:${REPO_ROOT}" \
  "${PYTHON312}" \
  "${SCRIPT_DIR}/hint_queries.py" \
  --queries-dir  "${QUERIES_DIR}" \
  --output-dir   "${OUTPUT_DIR}" \
  --max-plans    "${MAX_PLANS}" \
  --tree-structure "${TREE_STRUCTURE}" \
  --workers      "${WORKERS}"
