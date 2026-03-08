#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
require_venv

cd "${REPO_ROOT}"
"${REPO_ROOT}/test.py" "$@"
EXIT_CODE=$?

if [[ $EXIT_CODE -eq 0 ]]; then
    echo "Test succeeded."
else
    echo "Test failed with exit code $EXIT_CODE."
fi

exit $EXIT_CODE
