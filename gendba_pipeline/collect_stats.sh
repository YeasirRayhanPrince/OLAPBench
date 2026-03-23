#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# GenDBA Pipeline — PostgreSQL Stats Collector
# ============================================================================
# Connects to the PostgreSQL instance configured in CONFIG and collects
# optimizer statistics (pg_class, pg_stats, pg_stats_ext) for every table
# in the given SCHEMA file, writing an enriched *.dbschema.stats.json file
# alongside the input schema.
#
# Requirements:
#   - PostgreSQL must be reachable with the connection configured in CONFIG.
#   - The dataset YAML must have a `local:` block with `enabled: true`.
#   - psycopg2 must be installed in the venv.
#
# Edit the variables below, then run:
#   bash gendba_pipeline/collect_stats.sh
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# ── Config ───────────────────────────────────────────────────────────────────
# Path to the dataset pipeline YAML (absolute or relative to REPO_ROOT).
CONFIG="${SCRIPT_DIR}/configs/postgres_local_preloaded.dataset.yaml"

# Path to the input dbschema.json file.
# Output will be written alongside it as *.dbschema.stats.json.
SCHEMA="${REPO_ROOT}/benchmarks/job/job.dbschema.json"

# ============================================================================
# Build command — no need to edit below this line
# ============================================================================

CMD=(
    python gendba_pipeline/collect_stats.py
    --config "${CONFIG}"
    --schema "${SCHEMA}"
)

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
