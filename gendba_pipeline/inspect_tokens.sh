#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configure here ────────────────────────────────────────────────────────────

JSONL="$SCRIPT_DIR/output/gendba_local_postgres_preloaded/gendba_pool.training.jsonl"

QUERIES=(
    # tokenizer_001_cte_rollup
    # tokenizer_002_correlated_exists
    # tokenizer_003_lateral_top_cast
    # tokenizer_004_recursive_episode_chain
    # tokenizer_005_distinct_on_alt_titles
    # tokenizer_006_set_operations
    # tokenizer_007_window_cast_density
    # tokenizer_008_scalar_subquery_having
    # tokenizer_009_case_filtered_agg
    # tokenizer_010_json_aggregation
    # tokenizer_011_derived_tables
    # tokenizer_012_grouping_sets
    # tokenizer_013_right_join
    # tokenizer_014_full_join
    # tokenizer_015_self_join
    # tokenizer_016_join_title_keyword
    # tokenizer_017_join_title_info_aka
    # tokenizer_018_join_cast_role_name
    tokenizer_019_join_company_type
    # tokenizer_020_join_movie_link
    # tokenizer_021_implicit_join_title_keyword
    # tokenizer_022_implicit_join_cast_name
)

# Optional flags: --no-physical  --no-logical
EXTRA_FLAGS=""

# ── Run ───────────────────────────────────────────────────────────────────────

python3 "$SCRIPT_DIR/inspect_tokens.py" \
    --jsonl "$JSONL" \
    --queries "${QUERIES[@]}" \
    $EXTRA_FLAGS
