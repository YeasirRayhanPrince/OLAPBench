#!/usr/bin/env bash

set -euo pipefail

# Canonical user-facing benchmark entrypoint.
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
require_venv

NORETRY=false
VERBOSE=""
CLEAR=""
LAUNCH=""
ORIG_PWD="$(pwd)"

while [[ $# -gt 0 ]]; do
    case $1 in
    --clear)
        CLEAR="--clear"
        shift
        ;;
    --launch)
        LAUNCH="--launch"
        shift
        ;;
    --noretry)
        NORETRY=true
        shift
        ;;
    --verbose | -v)
        VERBOSE="-v"
        shift
        ;;
    --veryverbose | -vv)
        VERBOSE="-vv"
        shift
        ;;
    *)
        break
        ;;
    esac
done

JSON=$1
shift
if [[ "${JSON}" != /* ]]; then
    JSON="${ORIG_PWD}/${JSON}"
fi
BENCHMARK=${*:-default}

cd "${REPO_ROOT}"

while true; do
    "${REPO_ROOT}/benchmark.py" ${VERBOSE} ${CLEAR} ${LAUNCH} -j "${JSON}" ${BENCHMARK}
    EXIT_CODE=$?
    CLEAR=""

    if [[ $EXIT_CODE -eq 0 ]]; then
        echo "Benchmark succeeded."
        break
    fi

    echo "Benchmark failed with exit code $EXIT_CODE. Retrying..."

    if $NORETRY; then
        echo "Exiting without retrying."
        break
    fi
done
