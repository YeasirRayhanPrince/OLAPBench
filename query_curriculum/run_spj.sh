#!/usr/bin/env bash

set -euo pipefail

# ============================================================================
# Query Curriculum Generator — Runner Script
# ============================================================================
# Edit the variables below, then run:
#   bash query_curriculum/run_spj.sh
#
# No terminal arguments needed — all config lives in the variables below.
#
# Branches and what they generate:
#   spj       — Select-Project-Join queries (Scan, Filter, Join, Project)
#   agg       — Aggregation queries (GroupBy, Aggregate functions)
#   subquery  — Subquery queries (EXISTS, IN, scalar subqueries)
#   setop     — Set operation queries (UNION, INTERSECT, EXCEPT)
#   window    — Window function queries (ROW_NUMBER, RANK, aggregates OVER)
#   cte       — Common Table Expression queries (WITH ... AS)
#   full      — All of the above, merged into a single output
# ============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
VENV_ACTIVATE="${REPO_ROOT}/.venv/bin/activate"

# ── Branch ──────────────────────────────────────────────────────────────────
# Valid values: spj, agg, subquery, setop, window, cte, full
BRANCH="full"

# ── Core settings ───────────────────────────────────────────────────────────
BENCHMARK="job"
SUFFIX="${BRANCH}_v0"          # Output lands in queries_<SUFFIX>/
SEED="0"
MAX_JOIN_TABLES="12"
MAX_PREDICATES_PER_TABLE="5"
PROBE_WORKERS="32"
JOIN_TYPES=("inner" "left")
# Valid values: auto, schema_only, schema_plus_llm, stats_only, stats_plus_selective_probes, stats_plus_estimated_probes
#   auto                          — uses stats_plus_selective_probes if PG connected, else schema_only
#   schema_only                   — schema metadata only (no PG needed, less realistic predicates)
#   schema_plus_llm               — schema + LLM-generated predicate seeds (no PG needed, requires OPENAI_API_KEY)
#   stats_only                    — real column stats from pg_stats (requires PG, no live probes)
#   stats_plus_selective_probes   — stats + live COUNT(*) probes to verify selectivity (requires PG, slow on large datasets)
#   stats_plus_estimated_probes   — stats + EXPLAIN-based estimated row counts (requires PG, fast)
STATS_MODE="schema_only"
# OpenAI model for LLM seed generation (only used when STATS_MODE=schema_plus_llm)
# Options (cheapest → best):
#   gpt-4o-mini   — fast & cheapest, good enough for seed generation (default)
#   gpt-4.1-nano  — newer, very cheap, similar to 4o-mini
#   gpt-4.1-mini  — better JSON adherence, slightly pricier
#   gpt-4o        — more creative/diverse values, ~10x cost of 4o-mini
#   gpt-4.1       — best quality, overkill for seeds, ~10x cost of 4.1-mini
LLM_SEED_MODEL="gpt-5"
# OpenAI API key (required when STATS_MODE=schema_plus_llm)
OPENAI_API_KEY=""

# ── SPJ-specific: template packs & stage budgets ───────────────────────────
# Template packs are only used for --branch spj (ignored by other branches).
TEMPLATE_PACKS=("job_like_implicit_joins")

# Stage budgets control how many queries are generated per join-width stage.
# These are SPJ-specific; other branches use their own internal defaults.
STAGE_1_QUERIES="50"
STAGE_2_QUERIES="100"
STAGE_3_QUERIES="100"
STAGE_4_QUERIES="100"

# ── Plan collection (requires PG) ──────────────────────────────────────────
# Set to true to append --collect-plans (collects EXPLAIN plans via PG).
COLLECT_PLANS=false

# ── Local PostgreSQL connection ─────────────────────────────────────────────
PGHOST_VALUE="127.0.0.1"
PGPORT_VALUE="5432"
PGUSER_VALUE="yrayhan"
PGPASSWORD_VALUE=""
PGDATABASE_VALUE="postgres"

# ============================================================================
# Build command — no need to edit below this line
# ============================================================================

CMD=(
    python -m query_curriculum.cli generate
    --benchmark "${BENCHMARK}"
    --suffix "${SUFFIX}"
    --branch "${BRANCH}"
    --replace-output
    --seed "${SEED}"
    --max-join-tables "${MAX_JOIN_TABLES}"
    --max-predicates-per-table "${MAX_PREDICATES_PER_TABLE}"
    --probe-workers "${PROBE_WORKERS}"
    --join-types "${JOIN_TYPES[@]}"
    --stats-mode "${STATS_MODE}"
    --llm-seed-model "${LLM_SEED_MODEL}"
    --pg-enabled
    --pg-host "${PGHOST_VALUE}"
    --pg-port "${PGPORT_VALUE}"
    --pg-user "${PGUSER_VALUE}"
    --pg-password "${PGPASSWORD_VALUE}"
    --pg-database "${PGDATABASE_VALUE}"
)

# SPJ-specific: stage budgets and template packs
if [[ "${BRANCH}" == "spj" || "${BRANCH}" == "full" ]]; then
    CMD+=(
        --stage-budget "1_table=${STAGE_1_QUERIES}"
        --stage-budget "2_table=${STAGE_2_QUERIES}"
        --stage-budget "3_table=${STAGE_3_QUERIES}"
        --stage-budget "4_table=${STAGE_4_QUERIES}"
    )
    for template_pack in "${TEMPLATE_PACKS[@]}"; do
        CMD+=(--template-pack "${template_pack}")
    done
fi

# Optional plan collection
if [[ "${COLLECT_PLANS}" == true ]]; then
    CMD+=(--collect-plans)
fi

# ── Run ─────────────────────────────────────────────────────────────────────

echo "Running from ${REPO_ROOT}"
if [[ ! -f "${VENV_ACTIVATE}" ]]; then
    echo "Virtual environment activation script not found: ${VENV_ACTIVATE}" >&2
    exit 1
fi

cd "${REPO_ROOT}"
source "${VENV_ACTIVATE}"
echo "Activated virtual environment: ${VENV_ACTIVATE}"

if [[ -n "${OPENAI_API_KEY}" ]]; then
    export OPENAI_API_KEY
fi

OUTPUT_DIR="${REPO_ROOT}/benchmarks/${BENCHMARK}/queries_${SUFFIX}"
if [[ -d "${OUTPUT_DIR}" ]]; then
    echo "Warning: output directory already exists and will be replaced: ${OUTPUT_DIR}" >&2
fi

printf 'Command:'
printf ' %q' "${CMD[@]}"
printf '\n'

"${CMD[@]}"
