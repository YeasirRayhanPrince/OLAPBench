#!/usr/bin/env bash
set -euo pipefail

# What this does:
#   Generate the standard `gendba_pool` workload/profile under `benchmarks/gendba_pool`.
# How to run:
#   ./shell/run_gendba_pool.sh
#   ./shell/run_gendba_pool.sh profile
#   ./shell/run_gendba_pool.sh queries
#   ./shell/run_gendba_pool.sh all
#
# Managed entrypoint for the standard gendba_pool workload.
# Heuristic policy lives in benchmarks/gendba_pool/generator.py.

source "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/_common.sh"
require_venv

BENCH_DIR="${REPO_ROOT}/benchmarks/gendba_pool"

COMMAND="all"
if [[ $# -gt 0 ]]; then
  case "$1" in
    profile|queries|all)
      COMMAND="$1"
      shift
      ;;
  esac
fi

cd "${REPO_ROOT}"
exec "${PYTHON_BIN}" "${REPO_ROOT}/benchmarks/gendba_pool/generator.py" \
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
