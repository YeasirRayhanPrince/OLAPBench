#!/usr/bin/env bash
set -euo pipefail

# Managed entrypoint for the exhaustive gendba_pool workload.
# It keeps outputs separate from the standard profile/query set.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$SCRIPT_DIR/benchmarks/gendba_pool"
VENV_PATH="$SCRIPT_DIR/.venv"
PYTHON_BIN="python3"

if [[ -d "$VENV_PATH" ]]; then
  # shellcheck disable=SC1091
  source "$VENV_PATH/bin/activate"
  PYTHON_BIN="$VENV_PATH/bin/python"
fi

COMMAND="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    profile|queries|all)
      COMMAND="$1"
      shift
      ;;
  esac
fi

cd "$SCRIPT_DIR"
exec "$PYTHON_BIN" benchmarks/gendba_pool/generator.py \
  "$COMMAND" \
  --preset exhaustive \
  --final-query-cap 1000000 \
  --profile "$BENCH_DIR/job.profile.exhaustive.json" \
  --output-dir "$BENCH_DIR/queries_exhaustive" \
  --replace-output \
  --probe-mode managed \
  --probe-port 5060 \
  --probe-base-port 55100 \
  --postgres-version 18 \
  "$@"
