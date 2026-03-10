#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

try:
    import simplejson as json
except ImportError:
    import json

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.benchmark import benchmarks as benchmark_descriptions


@dataclass
class QueryEntry:
    group_key: str
    query_id: str
    sql: str
    sql_source_path: str
    schema: str
    workload: str
    query_set: str


def _loads(value: str | None) -> Any:
    if value in (None, ""):
        return None
    try:
        return json.loads(value, allow_nan=True)
    except TypeError:
        return json.loads(value)


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, allow_nan=True, sort_keys=True)
    except TypeError:
        return json.dumps(value, sort_keys=True)


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    slug = re.sub(r"_+", "_", slug).strip("._-")
    return slug or "default"


def _query_set_id(query_dir: str | None) -> str:
    return "default" if not query_dir else _slugify(query_dir)


def _engine_key(dbms: str, version: str) -> str:
    return f"{dbms}@{version}"


def _as_number(value: Any) -> int | float | None:
    if isinstance(value, (int, float)):
        return value
    if isinstance(value, str):
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return None
    return None


def _extract_actual_loops(attrs: dict[str, Any]) -> int | float:
    raw_repr = attrs.get("system_representation")
    if not raw_repr:
        return 1

    if isinstance(raw_repr, list):
        repr_list = raw_repr
    elif isinstance(raw_repr, str):
        try:
            repr_list = _loads(raw_repr)
        except Exception:
            return 1
        if isinstance(repr_list, str):
            try:
                repr_list = _loads(repr_list)
            except Exception:
                return 1
    else:
        return 1

    if not isinstance(repr_list, list) or len(repr_list) == 0:
        return 1

    first = repr_list[0]
    if not isinstance(first, dict):
        return 1

    loops = _as_number(first.get("Actual Loops"))
    return loops if loops is not None else 1


def _flatten_plan(node: dict[str, Any], operators: list[dict[str, Any]]):
    attrs = node.get("_attrs", {})
    actual_per_loop = _as_number(attrs.get("exact_cardinality"))
    actual_loops = _extract_actual_loops(attrs)
    actual_total = None
    if actual_per_loop is not None and actual_loops is not None:
        actual_total = actual_per_loop * actual_loops

    operators.append(
        {
            "operator_id": attrs.get("operator_id"),
            "operator_type": node.get("_label"),
            "actual_rows_per_loop": actual_per_loop,
            "actual_loops": actual_loops,
            "actual_rows_total": actual_total,
        }
    )

    for child in node.get("_children", []):
        if isinstance(child, dict):
            _flatten_plan(child, operators)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        for row in rows:
            file.write(_dumps(row))
            file.write("\n")


def _resolve_dataset_root(dataset_arg: str) -> Path:
    path = Path(dataset_arg)
    if not path.is_absolute():
        path = REPO_ROOT / path
    return path.resolve()


def _select_run_root(dataset_root: Path, run_id: str | None) -> Path:
    work_root = dataset_root / "work"
    if not work_root.exists():
        raise FileNotFoundError(f"Missing work directory under dataset root: {work_root}")

    if run_id:
        run_root = work_root / run_id
        if not run_root.exists():
            raise FileNotFoundError(f"Run does not exist: {run_root}")
        return run_root

    runs = sorted(path for path in work_root.iterdir() if path.is_dir())
    if not runs:
        raise FileNotFoundError(f"No runs found under {work_root}")
    return runs[-1]


def _load_group_specs(run_root: Path) -> dict[str, dict[str, Any]]:
    generated_dir = run_root / "generated_benchmarks"
    group_specs: dict[str, dict[str, Any]] = {}

    for yaml_path in sorted(generated_dir.glob("*.benchmark.yaml")):
        definition = yaml.safe_load(yaml_path.read_text())
        benchmarks = definition.get("benchmarks", [])
        if not benchmarks:
            continue
        systems = definition.get("systems", [])
        if not systems:
            continue

        bench_spec = benchmarks[0]
        description = benchmark_descriptions()[bench_spec["name"]]
        benchmark = description.instantiate(str(REPO_ROOT / "data"), dict(bench_spec))
        group_key = _slugify(
            f"{benchmark.data_dir}_{benchmark.unique_name}_{_query_set_id(benchmark.query_dir)}"
        )
        group_specs[group_key] = {
            "benchmark_spec": dict(bench_spec),
            "system": dict(systems[0]),
            "schema": benchmark.data_dir,
            "workload": benchmark.unique_name,
            "query_set": _query_set_id(benchmark.query_dir),
            "queries_path": str(Path(benchmark.queries_path)),
        }

    if not group_specs:
        raise FileNotFoundError(f"No generated benchmark YAMLs found under {generated_dir}")
    return group_specs


def _load_query_entries(group_specs: dict[str, dict[str, Any]]) -> list[QueryEntry]:
    entries: list[QueryEntry] = []
    for group_key in sorted(group_specs):
        bench_spec = dict(group_specs[group_key]["benchmark_spec"])
        description = benchmark_descriptions()[bench_spec["name"]]
        benchmark = description.instantiate(str(REPO_ROOT / "data"), bench_spec)
        queries, _ = benchmark.queries("__logical__")
        for query_id, sql in queries:
            entries.append(
                QueryEntry(
                    group_key=group_key,
                    query_id=query_id,
                    sql=sql,
                    sql_source_path=str(Path(benchmark.queries_path) / query_id),
                    schema=group_specs[group_key]["schema"],
                    workload=group_specs[group_key]["workload"],
                    query_set=group_specs[group_key]["query_set"],
                )
            )
    return entries


def _load_logical_records(run_root: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    calcite_root = run_root / "calcite"
    for logical_path in sorted(calcite_root.glob("*/logical_training.jsonl")):
        group_key = logical_path.parent.name
        with logical_path.open() as file:
            for line in file:
                record = _loads(line)
                records[f"{group_key}:{record['query_key']}"] = record
    return records


def _load_physical_records(run_root: Path) -> tuple[dict[str, dict[str, dict[str, Any]]], dict[str, dict[str, dict[str, Any]]]]:
    tokens_by_engine: dict[str, dict[str, dict[str, Any]]] = {}
    sources_by_engine: dict[str, dict[str, dict[str, Any]]] = {}

    results_root = run_root / "olapbench_results"
    for csv_path in sorted(results_root.glob("*/*/*.csv")):
        group_key = csv_path.parents[1].name
        engine_dir = csv_path.parent.name

        tokens_by_engine.setdefault(engine_dir, {})
        sources_by_engine.setdefault(engine_dir, {})

        with csv_path.open("r", newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                query_id = row["query"]
                state = row.get("state", "")
                source = {
                    "state": state,
                    "result_csv_path": str(csv_path),
                    "query_id": query_id,
                    "dbms": row.get("dbms"),
                    "version": row.get("version"),
                    "title": row.get("title"),
                    "message": row.get("message"),
                }
                sources_by_engine[engine_dir][f"{group_key}:{query_id}"] = source

                if state != "success":
                    continue

                plan_doc = _loads(row.get("plan"))
                if not isinstance(plan_doc, dict):
                    continue

                plan_root = plan_doc.get("queryPlan")
                if not isinstance(plan_root, dict):
                    continue

                operators: list[dict[str, Any]] = []
                _flatten_plan(plan_root, operators)
                tokens_by_engine[engine_dir][f"{group_key}:{query_id}"] = {
                    "plan": plan_doc,
                    "operators": operators,
                }

    return tokens_by_engine, sources_by_engine


def rebuild_existing_exports(dataset_root: Path, run_id: str | None = None) -> tuple[Path, Path]:
    run_root = _select_run_root(dataset_root, run_id)
    export_timestamp = run_root.name

    group_specs = _load_group_specs(run_root)
    query_entries = _load_query_entries(group_specs)
    logical_records = _load_logical_records(run_root)
    physical_tokens_by_engine, physical_sources_by_engine = _load_physical_records(run_root)

    workload_names = sorted({entry.workload for entry in query_entries})
    export_prefix = workload_names[0] if len(workload_names) == 1 else dataset_root.name
    training_path = dataset_root / f"{export_prefix}.training.{export_timestamp}.jsonl"
    manifest_path = dataset_root / f"{export_prefix}.manifest.{export_timestamp}.jsonl"

    training_rows: list[dict[str, Any]] = []
    manifest_rows: list[dict[str, Any]] = []
    next_id = 1

    for entry in query_entries:
        logical_key = f"{entry.group_key}:{entry.query_id}"
        logical_record = logical_records.get(logical_key)
        if logical_record is None:
            continue

        physical_token_map: dict[str, dict[str, Any]] = {}
        physical_source_map: dict[str, dict[str, Any]] = {}

        for engine_key, engine_tokens in physical_tokens_by_engine.items():
            token = engine_tokens.get(logical_key)
            if token is not None:
                physical_token_map[engine_key] = token

        if not physical_token_map:
            continue

        for engine_key, engine_sources in physical_sources_by_engine.items():
            source = engine_sources.get(logical_key)
            if source is not None:
                physical_source_map[engine_key] = source

        training_rows.append(
            {
                "id": next_id,
                "schema": entry.schema,
                "sql_file_name": entry.query_id,
                "sql": entry.sql,
                "ir_logical_token": logical_record["logical_tokenized_plan"],
                "ir_physical_token": physical_token_map,
            }
        )

        manifest_rows.append(
            {
                "id": next_id,
                "query_key": entry.query_id,
                "schema": entry.schema,
                "workload": entry.workload,
                "query_set": entry.query_set,
                "sql_source_path": entry.sql_source_path,
                "logical_source": {
                    "plan_json_path": logical_record.get("plan_json_path"),
                    "plan_text_path": logical_record.get("plan_text_path"),
                    "global_mapping_path": logical_record.get("global_mapping_path"),
                    "annotated_plan_path": logical_record.get("annotated_plan_path"),
                },
                "physical_sources": physical_source_map,
            }
        )

        next_id += 1

    _write_jsonl(training_path, training_rows)
    _write_jsonl(manifest_path, manifest_rows)
    return training_path, manifest_path


def main():
    parser = argparse.ArgumentParser(
        description="Rebuild GenDBA training and manifest JSONLs from an existing dataset output folder."
    )
    parser.add_argument(
        "--dataset-root",
        required=True,
        help="Dataset output folder, for example gendba_pipeline/output/gendba_local_postgres_preloaded",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run id under <dataset-root>/work. Defaults to the latest run.",
    )
    args = parser.parse_args()

    dataset_root = _resolve_dataset_root(args.dataset_root)
    training_path, manifest_path = rebuild_existing_exports(dataset_root, run_id=args.run_id)
    print(f"Training JSONL: {training_path}")
    print(f"Manifest JSONL: {manifest_path}")


if __name__ == "__main__":
    main()
