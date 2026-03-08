#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${SCRIPT_DIR}"
VENV_PATH="${REPO_ROOT}/.venv"
PYTHON_BIN="python3"

require_venv() {
  if [[ ! -f "${VENV_PATH}/bin/activate" ]]; then
    echo "Missing virtual environment at ${VENV_PATH}. Run ./setup.sh first." >&2
    exit 1
  fi

  # shellcheck disable=SC1091
  source "${VENV_PATH}/bin/activate"
  PYTHON_BIN="${VENV_PATH}/bin/python"
}
