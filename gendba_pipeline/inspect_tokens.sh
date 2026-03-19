#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# ── Configure here ────────────────────────────────────────────────────────────

JSONL="$SCRIPT_DIR/output/gendba_local_postgres_preloaded/gendba_pool.training.jsonl"

QUERIES=(
    exists
    1a
)

# Optional flags: --no-physical  --no-logical
EXTRA_FLAGS=""

# ── Run ───────────────────────────────────────────────────────────────────────

python3 "$SCRIPT_DIR/inspect_tokens.py" \
    --jsonl "$JSONL" \
    --queries "${QUERIES[@]}" \
    $EXTRA_FLAGS
