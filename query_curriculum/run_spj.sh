#!/usr/bin/env bash

set -euo pipefail

# Convenience wrapper for the SPJ curriculum generator.
# Edit the variables below, then run:
#   ./query_curriculum/run_spj.sh
#
# This script does not expect benchmark/suffix/PG settings from the terminal.
# It is meant to be edited directly for your current experiment.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# Core run settings. Change these directly in the file.
BENCHMARK="job"
SUFFIX="spj_v2"
SEED="0"
MAX_JOIN_TABLES="12"
MAX_PREDICATES_PER_TABLE="5"
JOIN_TYPES=("inner" "left")
TEMPLATE_PACKS=("job_like_implicit_joins")
STAGE_1_QUERIES="1000"
STAGE_2_QUERIES="1000"
STAGE_3_QUERIES="1000"
STAGE_4_QUERIES="5000"

# Local PostgreSQL connection settings. Change these directly in the file.
PGHOST_VALUE="127.0.0.1"
PGPORT_VALUE="5432"
PGUSER_VALUE="postgres"
PGPASSWORD_VALUE=""
PGDATABASE_VALUE="postgres"

CMD=(python -m query_curriculum.cli generate --benchmark "${BENCHMARK}" --suffix "${SUFFIX}" --replace-output --seed "${SEED}" --max-join-tables "${MAX_JOIN_TABLES}" --max-predicates-per-table "${MAX_PREDICATES_PER_TABLE}" --join-types "${JOIN_TYPES[@]}" --stage-budget "1_table=${STAGE_1_QUERIES}" --stage-budget "2_table=${STAGE_2_QUERIES}" --stage-budget "3_table=${STAGE_3_QUERIES}" --stage-budget "4_table=${STAGE_4_QUERIES}" --pg-enabled --pg-host "${PGHOST_VALUE}" --pg-port "${PGPORT_VALUE}" --pg-user "${PGUSER_VALUE}" --pg-password "${PGPASSWORD_VALUE}" --pg-database "${PGDATABASE_VALUE}")

for template_pack in "${TEMPLATE_PACKS[@]}"; do
    CMD+=(--template-pack "${template_pack}")
done

echo "Running from ${REPO_ROOT}"
if [[ ! -f "${VENV_ACTIVATE}" ]]; then
    echo "Virtual environment activation script not found: ${VENV_ACTIVATE}" >&2
    exit 1
fi

cd "${REPO_ROOT}"
source "${VENV_ACTIVATE}"
echo "Activated virtual environment: ${VENV_ACTIVATE}"

OUTPUT_DIR="${REPO_ROOT}/benchmarks/${BENCHMARK}/queries_${SUFFIX}"
if [[ -d "${OUTPUT_DIR}" ]]; then
    echo "Warning: output directory already exists and will be replaced: ${OUTPUT_DIR}" >&2
fi

printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

"${CMD[@]}"
