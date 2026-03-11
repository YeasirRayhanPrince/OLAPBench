#!/usr/bin/env python3
"""
pipeline/export.py — Export Calcite logical IR + OLAPBench physical results
to GenDBA training and manifest JSONL files.

Reuses:
  gendba_pipeline/build_calcite_training_jsonl.py  — logical token builder
  gendba_pipeline/export.py                        — _flatten_plan / _loads helpers
"""
from __future__ import annotations

import argparse
import csv
import sys
from pathlib import Path
from typing import Any

try:
    import simplejson as json
except ImportError:
    import json

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

# Reuse existing helpers — do NOT rewrite from scratch
from gendba_pipeline.export import _flatten_plan, _loads  # noqa: E402
from gendba_pipeline.build_calcite_training_jsonl import (  # noqa: E402
    _load_schema_table_ids,
    _build_logical_token,
    _plan_artifacts_exist,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, allow_nan=True)
    except TypeError:
        return json.dumps(value)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as fh:
        for row in rows:
            fh.write(_dumps(row))
            fh.write("\n")


# ---------------------------------------------------------------------------
# Step 1: Read Calcite output directory → logical records keyed by query stem
# ---------------------------------------------------------------------------

def _load_calcite_records(calcite_dir: Path, schema_path: Path) -> dict[str, dict[str, Any]]:
    """
    Scan calcite_dir for *.plan.json artifacts; build logical token for each.
    Returns dict keyed by query stem (e.g. '1a' from '1a.sql').
    """
    schema_name, table_ids = _load_schema_table_ids(schema_path)
    records: dict[str, dict[str, Any]] = {}

    for plan_json_path in sorted(calcite_dir.glob("*.plan.json")):
        query_stem = plan_json_path.name.replace(".plan.json", "")
        if not _plan_artifacts_exist(calcite_dir, query_stem):
            print(f"  [warn] Missing artifacts for {query_stem}, skipping", file=sys.stderr)
            continue

        plan_json: dict[str, Any] = _loads(plan_json_path.read_text())
        plan_txt = (calcite_dir / f"{query_stem}.plan.txt").read_text()

        logical_tokenized = _build_logical_token(plan_json, table_ids)

        records[query_stem] = {
            "schema": schema_name,
            "query_stem": query_stem,
            "calcite_plan": plan_txt,
            "logical_tokenized_plan": logical_tokenized,
            "plan_json_path": str(plan_json_path),
            "plan_text_path": str(calcite_dir / f"{query_stem}.plan.txt"),
            "global_mapping_path": str(calcite_dir / f"{query_stem}.global-mapping.json"),
        }

    print(f"Loaded {len(records)} Calcite logical records from {calcite_dir}")
    return records


# ---------------------------------------------------------------------------
# Step 2: Read OLAPBench results CSV → physical tokens keyed by query id
# ---------------------------------------------------------------------------

def _load_physical_records(results_dir: Path) -> dict[str, dict[str, Any]]:
    """
    Walk results_dir recursively for *.csv files (OLAPBench output).
    Returns dict keyed by query id (the 'query' column value in the CSV).
    """
    physical: dict[str, dict[str, Any]] = {}
    csv_files = list(results_dir.rglob("*.csv"))

    for csv_path in csv_files:
        with csv_path.open("r", newline="") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                query_id = row.get("query", "")
                if not query_id:
                    continue

                state = row.get("state", "")
                plan_str = row.get("plan", "")

                source_info: dict[str, Any] = {
                    "state": state,
                    "result_csv_path": str(csv_path),
                    "query_id": query_id,
                    "dbms": row.get("dbms"),
                    "version": row.get("version"),
                    "title": row.get("title"),
                    "message": row.get("message"),
                }

                if state != "success" or not plan_str:
                    physical.setdefault(query_id, {})["source"] = source_info
                    continue

                try:
                    plan_doc = _loads(plan_str)
                except Exception:
                    physical.setdefault(query_id, {})["source"] = source_info
                    continue

                plan_root = plan_doc.get("queryPlan") if isinstance(plan_doc, dict) else None
                operators: list[dict[str, Any]] = []
                if isinstance(plan_root, dict):
                    _flatten_plan(plan_root, operators)

                physical[query_id] = {
                    "plan": plan_doc,
                    "operators": operators,
                    "query_text": plan_doc.get("queryText") if isinstance(plan_doc, dict) else None,
                    "source": source_info,
                }

    print(f"Loaded physical records for {len(physical)} queries from {results_dir}")
    return physical


# ---------------------------------------------------------------------------
# Step 3: Pair logical + physical and write JSONL
# ---------------------------------------------------------------------------

def export(
    calcite_dir: Path,
    results_dir: Path,
    schema_path: Path,
    dbms_key: str,
    out_training: Path,
    out_manifest: Path,
    workload: str = "",
    query_set: str = "",
    sql_dir: Path | None = None,
) -> None:
    logical = _load_calcite_records(calcite_dir, schema_path)
    physical = _load_physical_records(results_dir)

    training_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []

    next_id = 1
    for query_stem, log_rec in sorted(logical.items()):
        # Physical key: OLAPBench uses the .sql filename as the query id
        # e.g. "1a.sql" or just "1a" — try both
        phys = physical.get(f"{query_stem}.sql") or physical.get(query_stem)

        plan = phys.get("plan") if phys else None
        operators = phys.get("operators", []) if phys else []
        query_text = phys.get("query_text") if phys else None
        source = phys.get("source", {}) if phys else {}

        sql_file_name = f"{query_stem}.sql"
        sql_source_path = str(sql_dir / sql_file_name) if sql_dir else None

        training_row: dict[str, Any] = {
            "id": next_id,
            "schema": log_rec["schema"],
            "sql_file_name": sql_file_name,
            "sql": query_text,
            "ir_logical_token": log_rec["logical_tokenized_plan"],
            "ir_physical_token": {
                dbms_key: {
                    "plan": plan,
                    "operators": operators,
                }
            } if plan else {},
        }

        manifest_row: dict[str, Any] = {
            "id": next_id,
            "logical_source": {
                "plan_json_path": log_rec.get("plan_json_path"),
                "plan_text_path": log_rec.get("plan_text_path"),
                "global_mapping_path": log_rec.get("global_mapping_path"),
            },
            "physical_sources": {
                dbms_key: {
                    "dbms": source.get("dbms"),
                    "message": source.get("message"),
                    "query_id": sql_file_name,
                    "result_csv_path": source.get("result_csv_path"),
                    "state": source.get("state"),
                    "title": source.get("title"),
                    "version": source.get("version"),
                },
            },
            "query_key": sql_file_name,
            "query_set": query_set,
            "schema": log_rec["schema"],
            "sql_source_path": sql_source_path,
            "workload": workload,
        }

        training_rows.append(training_row)
        manifest_rows.append(manifest_row)
        next_id += 1

    _write_jsonl(out_training, training_rows)
    _write_jsonl(out_manifest, manifest_rows)

    print(f"Wrote {len(training_rows)} training rows  → {out_training}")
    print(f"Wrote {len(manifest_rows)} manifest rows  → {out_manifest}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export Calcite IR + OLAPBench results to GenDBA training JSONL."
    )
    parser.add_argument("--calcite-dir",  required=True, help="Directory with Calcite plan artifacts")
    parser.add_argument("--results-dir",  required=True, help="Directory with OLAPBench result CSVs")
    parser.add_argument("--schema-path",  required=True, help="Path to <benchmark>.dbschema.json")
    parser.add_argument("--dbms",         required=True, help="DBMS key, e.g. postgres@12.5")
    parser.add_argument("--out-training", required=True, help="Output training JSONL path")
    parser.add_argument("--out-manifest", required=True, help="Output manifest JSONL path")
    parser.add_argument("--workload",     default="",    help="Workload name, e.g. job")
    parser.add_argument("--query-set",    default="",    help="Query set name, e.g. queries")
    parser.add_argument("--sql-dir",      default=None,  help="Directory containing .sql source files")
    args = parser.parse_args()

    export(
        calcite_dir=Path(args.calcite_dir),
        results_dir=Path(args.results_dir),
        schema_path=Path(args.schema_path),
        dbms_key=args.dbms,
        out_training=Path(args.out_training),
        out_manifest=Path(args.out_manifest),
        workload=args.workload,
        query_set=args.query_set,
        sql_dir=Path(args.sql_dir) if args.sql_dir else None,
    )


if __name__ == "__main__":
    main()
