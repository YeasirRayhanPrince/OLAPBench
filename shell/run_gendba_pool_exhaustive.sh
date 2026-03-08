#!/usr/bin/env bash
set -euo pipefail

# What this does:
#   Generate the exhaustive `gendba_pool` workload/profile and keep its outputs separate.
# How to run:
#   ./shell/run_gendba_pool_exhaustive.sh
#   ./shell/run_gendba_pool_exhaustive.sh profile
#   ./shell/run_gendba_pool_exhaustive.sh queries
#   ./shell/run_gendba_pool_exhaustive.sh all
#
# Managed entrypoint for the exhaustive gendba_pool workload.
# It keeps outputs separate from the standard profile/query set.

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
