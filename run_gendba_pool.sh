#!/usr/bin/env bash
set -euo pipefail

# Managed entrypoint for the standard gendba_pool workload.
# Heuristic policy lives in benchmarks/gendba_pool/generator.py.

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
  --preset standard \
  --profile "$BENCH_DIR/job.profile.json" \
  --output-dir "$BENCH_DIR/queries" \
  --replace-output \
  --probe-mode managed \
  --probe-port 5060 \
  --probe-base-port 55100 \
  --postgres-version 18 \
  "$@"
