#!/usr/bin/env bash

set -euo pipefail

source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/_common.sh"
require_venv

cd "${REPO_ROOT}"
exec "${PYTHON_BIN}" "${REPO_ROOT}/server.py" "$@"
