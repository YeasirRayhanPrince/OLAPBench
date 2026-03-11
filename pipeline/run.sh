#!/usr/bin/env bash
# pipeline/run.sh — Unified benchmark training data pipeline
#
# USAGE
#   cd /scratch1/yrayhan/OLAPBench          # must run from repo root
#   source .venv/bin/activate               # activate the virtualenv first
#   bash pipeline/run.sh                    # uses pipeline/pipeline.yaml by default
#   bash pipeline/run.sh /path/to/other.yaml  # override config file
#
# WHAT IT DOES
#   1. Reads pipeline/pipeline.yaml (benchmark, system, run settings)
#   2. Runs Calcite plangen to generate logical IR for all queries
#   3. Generates a benchmark config and runs OLAPBench to collect physical plans
#   4. Exports training + manifest JSONL files via pipeline/export.py
#
# OUTPUT
#   pipeline/output/{dbms}_{benchmark}_{timestamp}/
#     ├── {run_id}.training.jsonl   — GenDBA training rows
#     ├── {run_id}.manifest.jsonl   — per-query source metadata
#     ├── calcite/                  — Calcite logical plan artifacts
#     ├── generated_benchmark.yaml  — auto-generated OLAPBench config
#     └── olapbench_results/        — raw benchmark CSVs
#
# PREREQUISITES
#   - Virtualenv activated (source .venv/bin/activate)
#   - Calcite plangen binary built at the path set in pipeline.yaml
#   - Target database running and accessible (host/port/user in pipeline.yaml)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFIG="${1:-${SCRIPT_DIR}/pipeline.yaml}"

# --------------------------------------------------------------------------
# 1. Parse YAML into shell variables
# --------------------------------------------------------------------------
eval "$(PIPELINE_CONFIG="${CONFIG}" python3 <<'PYEOF'
import os, yaml
with open(os.environ["PIPELINE_CONFIG"]) as f:
    cfg = yaml.safe_load(f)
b   = cfg["benchmark"]
s   = cfg["system"]
r   = cfg["run"]
c   = cfg["calcite"]
qp  = r.get("query_plan", {})
print(f"BENCHMARK={b['name']!r}")
print(f"QUERY_DIR={b.get('query_dir') or ''!r}")
print(f"DBMS={s['dbms']!r}")
print(f"VERSION={s['version']!r}")
print(f"HOST={s.get('host', 'localhost')!r}")
print(f"PORT={s.get('port', 5432)!r}")
print(f"PG_USER={s.get('user', '')!r}")
print(f"PG_DATABASE={s.get('database', 'postgres')!r}")
print(f"LOAD_MODE={s.get('load_mode', 'preloaded')!r}")
print(f"INDEX={s.get('index', 'foreign')!r}")
print(f"REPETITIONS={r.get('repetitions', 1)!r}")
print(f"WARMUP={r.get('warmup', 0)!r}")
print(f"TIMEOUT={r.get('timeout', 300)!r}")
print(f"FETCH_RESULT={str(r.get('fetch_result', False)).lower()!r}")
print(f"QP_RETRIEVE={str(qp.get('retrieve', True)).lower()!r}")
print(f"QP_SYSREPR={str(qp.get('system_representation', True)).lower()!r}")
print(f"PLANGEN_BIN={c['plangen_bin']!r}")
PYEOF
)"

# --------------------------------------------------------------------------
# 2. Set output paths
# --------------------------------------------------------------------------
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${DBMS}_${BENCHMARK}_${TIMESTAMP}"
OUT_DIR="${REPO_ROOT}/pipeline/output/${RUN_ID}"
CALCITE_OUT="${OUT_DIR}/calcite"
RESULTS_OUT="${OUT_DIR}/olapbench_results"
GENERATED_YAML="${OUT_DIR}/generated_benchmark.yaml"

mkdir -p "${CALCITE_OUT}" "${RESULTS_OUT}"

echo "==> Pipeline run: ${RUN_ID}"
echo "    Benchmark : ${BENCHMARK}  (query_dir=${QUERY_DIR})"
echo "    System    : ${DBMS}@${VERSION} on ${HOST}:${PORT}"
echo "    Output    : ${OUT_DIR}"

SCHEMA_PATH="${REPO_ROOT}/benchmarks/${BENCHMARK}/${BENCHMARK}.dbschema.json"
# When query_dir is empty, use the default 'queries/' folder.
# When set (e.g. 'exhaustive'), OLAPBench appends it as 'queries_{query_dir}'.
if [[ -z "${QUERY_DIR}" ]]; then
    QUERIES_PATH="${REPO_ROOT}/benchmarks/${BENCHMARK}/queries"
else
    QUERIES_PATH="${REPO_ROOT}/benchmarks/${BENCHMARK}/queries_${QUERY_DIR}"
fi

# --------------------------------------------------------------------------
# 3. Run Calcite plangen
# --------------------------------------------------------------------------
echo ""
echo "==> [1/3] Running Calcite plangen ..."
"${PLANGEN_BIN}" "${SCHEMA_PATH}" "${QUERIES_PATH}/" "${CALCITE_OUT}/"

# --------------------------------------------------------------------------
# 4. Generate benchmark YAML
# --------------------------------------------------------------------------
echo ""
echo "==> [2/3] Generating benchmark config ..."
python3 - <<PYEOF
import yaml, pathlib

out = pathlib.Path("${GENERATED_YAML}")
out.parent.mkdir(parents=True, exist_ok=True)

bench_def = {
    "title": "${BENCHMARK} ${DBMS}@${VERSION}",
    "repetitions": int("${REPETITIONS}"),
    "warmup": int("${WARMUP}"),
    "timeout": int("${TIMEOUT}"),
    "global_timeout": 0,
    "fetch_result": "${FETCH_RESULT}" == "true",
    "load_mode": "${LOAD_MODE}",
    "query_plan": {
        "retrieve": "${QP_RETRIEVE}" == "true",
        "system_representation": "${QP_SYSREPR}" == "true",
    },
    "output": "${RESULTS_OUT}",
    "systems": [
        {
            "title": "${DBMS}@${VERSION}",
            "dbms": "${DBMS}",
            "parameter": {
                "index": "${INDEX}",
                "version": "${VERSION}",
                "use_local": True,
                "local_host": "${HOST}",
                "local_port": int("${PORT}"),
                "local_user": "${PG_USER}",
                "local_password": None,
                "local_database": "${PG_DATABASE}",
            },
            "settings": {},
            "local": {
                "enabled": True,
                "host": "${HOST}",
                "port": int("${PORT}"),
                "user": "${PG_USER}",
                "database": "${PG_DATABASE}",
            },
        }
    ],
    "benchmarks": [
        {
            "name": "${BENCHMARK}",
            **({"query_dir": "${QUERY_DIR}"} if "${QUERY_DIR}" else {}),
        }
    ],
}

out.write_text(yaml.safe_dump(bench_def, sort_keys=False))
print(f"Generated benchmark config: {out}")
PYEOF

# --------------------------------------------------------------------------
# 5. Run benchmark.py
# --------------------------------------------------------------------------
echo ""
echo "==> [3/3] Running OLAPBench ..."
cd "${REPO_ROOT}"
python3 benchmark.py \
  --clear \
  -j "${GENERATED_YAML}" \
  default

# --------------------------------------------------------------------------
# 6. Export training JSONL
# --------------------------------------------------------------------------
echo ""
echo "==> [4/4] Exporting training JSONL ..."
python3 "${SCRIPT_DIR}/export.py" \
  --calcite-dir  "${CALCITE_OUT}" \
  --results-dir  "${RESULTS_OUT}" \
  --schema-path  "${SCHEMA_PATH}" \
  --dbms         "${DBMS}@${VERSION}" \
  --out-training "${OUT_DIR}/${RUN_ID}.training.jsonl" \
  --out-manifest "${OUT_DIR}/${RUN_ID}.manifest.jsonl" \
  --workload     "${BENCHMARK}" \
  --query-set    "${QUERY_DIR:-queries}" \
  --sql-dir      "${QUERIES_PATH}"

echo ""
echo "==> Done! Outputs in: ${OUT_DIR}"
echo "    Training : ${OUT_DIR}/${RUN_ID}.training.jsonl"
echo "    Manifest : ${OUT_DIR}/${RUN_ID}.manifest.jsonl"
