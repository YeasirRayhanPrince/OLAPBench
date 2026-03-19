#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import re
import subprocess
import sys
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import jsonschema
import yaml

try:
    import simplejson as json
except ImportError:
    import json

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from benchmarks.benchmark import benchmarks as benchmark_descriptions
from gendba_pipeline.build_calcite_training_jsonl import (
    build_calcite_training_records,
)
from gendba_pipeline.phys_plan.tokenize_physical_plan import (
    load_table_name_to_id,
    tokenize_physical_plan,
)
from util.template import Template, unfold


@dataclass
class QueryEntry:
    dataset_id: int
    group_key: str
    query_id: str
    sql: str
    sql_source_path: str
    schema: str
    workload: str
    query_set: str


@dataclass
class QueryGroup:
    group_key: str
    benchmark_spec: dict[str, Any]
    schema_path: Path
    queries_dir: Path
    schema: str
    workload: str
    query_set: str
    queries: list[QueryEntry]


def _loads(value: str | None) -> Any:
    if value in (None, ""):
        return None
    try:
        return json.loads(value, allow_nan=True)
    except TypeError:
        return json.loads(value)


def _dumps(value: Any, *, indent: int | None = None) -> str:
    try:
        return json.dumps(value, allow_nan=True, indent=indent, sort_keys=True)
    except TypeError:
        return json.dumps(value, indent=indent, sort_keys=True)


def _parse_yaml(path: Path) -> dict[str, Any]:
    with path.open("r") as file:
        return yaml.safe_load(file)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        for row in rows:
            file.write(_dumps(row))
            file.write("\n")


def _append_jsonl(path: Path, rows: list[dict[str, Any]]):
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as file:
        for row in rows:
            file.write(_dumps(row))
            file.write("\n")


def _load_pipeline_spec(spec_path: Path) -> dict[str, Any]:
    schema_path = Path(__file__).resolve().parent / "dataset_pipeline.schema.json"
    schema = _loads(schema_path.read_text())
    instance = _parse_yaml(spec_path)
    jsonschema.validate(instance=instance, schema=schema)
    return instance


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    slug = re.sub(r"_+", "_", slug).strip("._-")
    return slug or "default"


def _timestamp() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    merged.update(override)
    return merged


def _query_set_id(query_dir: str | None) -> str:
    return "default" if not query_dir else _slugify(query_dir)


def _engine_key(dbms: str, version: str) -> str:
    return f"{dbms}@{version}"


def _stringify_attr_value(value: Any) -> str:
    if isinstance(value, (int, float, bool)) or value is None:
        return str(value)
    return str(value).replace("\n", " ").strip()


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


def _resolve_systems(spec: dict[str, Any]) -> list[dict[str, Any]]:
    root_parameters = deepcopy(spec.get("benchmark_defaults", {}).get("parameter", {}))
    root_settings = deepcopy(spec.get("benchmark_defaults", {}).get("settings", {}))

    systems: list[dict[str, Any]] = []
    for system in spec["systems"]:
        if system.get("disabled", False):
            continue

        parameters = _merge_dicts(root_parameters, deepcopy(system.get("parameter", {})))
        settings = _merge_dicts(root_settings, deepcopy(system.get("settings", {})))

        for parameter_values in unfold(parameters):
            for setting_values in unfold(settings):
                params = deepcopy(parameter_values)
                current_settings = deepcopy(setting_values)
                local = deepcopy(system.get("local"))

                if local and local.get("enabled", False):
                    if system["dbms"] != "postgres":
                        raise ValueError(
                            f"Local mode is currently only supported for postgres, not {system['dbms']}"
                        )
                    params["use_local"] = True
                    params["local_host"] = local.get("host", "localhost")
                    params["local_port"] = local.get("port")
                    params["local_user"] = local.get("user")
                    params["local_password"] = local.get("password")
                    params["local_database"] = local.get("database")

                title = Template(system["title"]).substitute(**current_settings, **params)
                version = str(params.get("version", "latest"))
                systems.append(
                    {
                        "title": title,
                        "dbms": system["dbms"],
                        "version": version,
                        "parameter": params,
                        "settings": current_settings,
                        "local": local,
                    }
                )

    return systems


def _instantiate_benchmark(benchmark_spec: dict[str, Any]):
    description = benchmark_descriptions()[benchmark_spec["name"]]
    data_dir = str(REPO_ROOT / "data")
    return description.instantiate(data_dir, deepcopy(benchmark_spec))


def _resolve_query_groups(spec: dict[str, Any]) -> tuple[list[QueryGroup], list[QueryEntry]]:
    groups: list[QueryGroup] = []
    entries: list[QueryEntry] = []
    next_id = 1

    for benchmark_spec in spec["benchmarks"]:
        if benchmark_spec.get("disabled", False):
            continue

        benchmark = _instantiate_benchmark(benchmark_spec)
        group_key = _slugify(f"{benchmark.data_dir}_{benchmark.unique_name}_{_query_set_id(benchmark.query_dir)}")
        queries, _ = benchmark.queries("__logical__")

        group_entries: list[QueryEntry] = []
        for query_id, sql in queries:
            query_entry = QueryEntry(
                dataset_id=next_id,
                group_key=group_key,
                query_id=query_id,
                sql=sql,
                sql_source_path=str(Path(benchmark.queries_path) / query_id),
                schema=benchmark.data_dir,
                workload=benchmark.unique_name,
                query_set=_query_set_id(benchmark.query_dir),
            )
            group_entries.append(query_entry)
            entries.append(query_entry)
            next_id += 1

        groups.append(
            QueryGroup(
                group_key=group_key,
                benchmark_spec=deepcopy(benchmark_spec),
                schema_path=Path(benchmark.path) / f"{benchmark.name}.dbschema.json",
                queries_dir=Path(benchmark.queries_path),
                schema=benchmark.data_dir,
                workload=benchmark.unique_name,
                query_set=_query_set_id(benchmark.query_dir),
                queries=group_entries,
            )
        )

    return groups, entries


def _build_benchmark_definition(group: QueryGroup, system: dict[str, Any], result_dir: Path, defaults: dict[str, Any]) -> dict[str, Any]:
    benchmark_definition = deepcopy(defaults)
    benchmark_definition["title"] = f"{group.workload} {system['title']}"
    benchmark_definition["output"] = str(result_dir)
    benchmark_definition["systems"] = [
        {
            "title": system["title"],
            "dbms": system["dbms"],
            "parameter": deepcopy(system["parameter"]),
            "settings": deepcopy(system["settings"]),
            **({"local": deepcopy(system["local"])} if system["local"] is not None else {}),
        }
    ]
    benchmark_definition["benchmarks"] = [deepcopy(group.benchmark_spec)]

    query_plan = deepcopy(benchmark_definition.get("query_plan", {}))
    query_plan.setdefault("retrieve", True)
    query_plan.setdefault("system_representation", True)
    benchmark_definition["query_plan"] = query_plan
    return benchmark_definition


def _benchmark_command(benchmark_yaml: Path, run_cfg: dict[str, Any]) -> list[str]:
    command = [sys.executable, str(REPO_ROOT / "benchmark.py")]
    if run_cfg.get("verbose", False):
        command.append("-v")
    if run_cfg.get("very_verbose", False):
        command.append("-vv")
    if run_cfg.get("clear", True):
        command.append("--clear")
    if run_cfg.get("db_dir"):
        command.extend(["--db", run_cfg["db_dir"]])
    if run_cfg.get("data_dir"):
        command.extend(["--data", run_cfg["data_dir"]])
    command.extend(["-j", str(benchmark_yaml), "default"])
    return command


def _collect_physical_tokens(result_csv_path: Path) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    tokens: dict[str, dict[str, Any]] = {}
    sources: dict[str, dict[str, Any]] = {}

    with result_csv_path.open("r", newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            query_id = row["query"]
            state = row.get("state", "")
            source_info = {
                "state": state,
                "result_csv_path": str(result_csv_path),
                "query_id": query_id,
                "dbms": row.get("dbms"),
                "version": row.get("version"),
                "title": row.get("title"),
                "message": row.get("message"),
            }
            sources[query_id] = source_info

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
            tokens[query_id] = {
                "plan": plan_doc,
                "operators": operators,
            }

    return tokens, sources


def _run_physical_group(
    group: QueryGroup,
    system: dict[str, Any],
    run_cfg: dict[str, Any],
    work_root: Path,
) -> tuple[dict[str, str], dict[str, dict[str, Any]]]:
    generated_config_dir = work_root / "generated_benchmarks"
    result_dir = work_root / "olapbench_results" / group.group_key / _engine_key(system["dbms"], system["version"])
    benchmark_yaml = generated_config_dir / f"{group.group_key}_{_engine_key(system['dbms'], system['version'])}.benchmark.yaml"

    benchmark_definition = _build_benchmark_definition(
        group=group,
        system=system,
        result_dir=result_dir,
        defaults=run_cfg["benchmark_defaults"],
    )

    benchmark_yaml.parent.mkdir(parents=True, exist_ok=True)
    benchmark_yaml.write_text(yaml.safe_dump(benchmark_definition, sort_keys=False))

    command = _benchmark_command(benchmark_yaml, run_cfg["run"])
    subprocess.run(command, cwd=REPO_ROOT, check=True)

    benchmark = _instantiate_benchmark(group.benchmark_spec)
    result_csv_path = result_dir / f"{benchmark.result_name}.csv"
    if not result_csv_path.exists():
        raise FileNotFoundError(f"Missing result CSV: {result_csv_path}")

    return _collect_physical_tokens(result_csv_path)


def _iter_training_and_manifest_rows(
    query_entries: list[QueryEntry],
    logical_records: dict[str, dict[str, Any]],
    physical_tokens_by_engine: dict[str, dict[str, dict[str, Any]]],
    physical_sources_by_engine: dict[str, dict[str, dict[str, Any]]],
    table_name_to_id_by_group: dict[str, dict[str, int]] | None = None,
):
    next_id = 1

    for entry in query_entries:
        logical_record = logical_records.get(f"{entry.group_key}:{entry.query_id}")
        if logical_record is None:
            continue

        physical_token_map: dict[str, dict[str, Any]] = {}
        physical_source_map: dict[str, dict[str, Any]] = {}

        for engine_key, engine_tokens in physical_tokens_by_engine.items():
            token = engine_tokens.get(f"{entry.group_key}:{entry.query_id}")
            if token is not None:
                physical_token_map[engine_key] = token

        for engine_key, engine_sources in physical_sources_by_engine.items():
            source = engine_sources.get(f"{entry.group_key}:{entry.query_id}")
            if source is not None:
                physical_source_map[engine_key] = source

        logical_ir = logical_record["logical_tokenized_plan"]

        # Tokenize physical plans
        phys_token_strs: dict[str, str] = {}
        phys_cardinalities: dict[str, list[int]] = {}
        if table_name_to_id_by_group:
            tbl_map = table_name_to_id_by_group.get(entry.group_key, {})
            pred_reg = logical_record.get("pred_registry")
            for engine_key, raw_phys in physical_token_map.items():
                result = tokenize_physical_plan(logical_ir, raw_phys, tbl_map,
                                                pred_registry=pred_reg)
                if result:
                    tok, cards = result
                    phys_token_strs[engine_key] = tok
                    phys_cardinalities[engine_key] = cards

        training_row = {
            "id": next_id,
            "schema": entry.schema,
            "sql_file_name": entry.query_id,
            "sql": entry.sql,
            "ir_logical_token": logical_ir,
            "ir_physical_token": physical_token_map,
            "ir_physical_plan_token": phys_token_strs,
            "ir_physical_plan_cardinalities": phys_cardinalities,
        }

        manifest_row = {
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

        yield training_row, manifest_row
        next_id += 1


def run_pipeline(args: argparse.Namespace) -> int:
    spec_path = Path(args.config).resolve()
    spec = _load_pipeline_spec(spec_path)
    dataset_root = (REPO_ROOT / spec["output_root"]).resolve()
    dataset_root.mkdir(parents=True, exist_ok=True)

    run_id = args.run_id or _timestamp()
    work_root = dataset_root / "work" / run_id
    work_root.mkdir(parents=True, exist_ok=True)

    run_cfg = {
        "run": deepcopy(spec.get("run", {})),
        "benchmark_defaults": deepcopy(spec["benchmark_defaults"]),
        "calcite": deepcopy(spec.get("calcite", {})),
    }
    if args.verbose:
        run_cfg["run"]["verbose"] = True
    if args.very_verbose:
        run_cfg["run"]["very_verbose"] = True
    if args.fail_fast:
        run_cfg["run"]["fail_fast"] = True
    if args.no_clear:
        run_cfg["run"]["clear"] = False

    groups, query_entries = _resolve_query_groups(spec)
    systems = _resolve_systems(spec)

    table_name_to_id_by_group: dict[str, dict[str, int]] = {}
    logical_records: dict[str, dict[str, Any]] = {}
    for group in groups:
        calcite_output_dir = work_root / "calcite" / group.group_key
        calcite_output_file = calcite_output_dir / "logical_training.jsonl"
        records = build_calcite_training_records(
            schema_path=str(group.schema_path),
            queries_dir=str(group.queries_dir),
            output_file=str(calcite_output_file),
            plangen_bin=run_cfg["calcite"].get("plangen_bin"),
            work_dir=str(calcite_output_dir / "plans"),
            keep_work_dir=True,
        )
        table_name_to_id_by_group[group.group_key] = load_table_name_to_id(group.schema_path)
        for record in records:
            logical_records[f"{group.group_key}:{record['query_key']}"] = record

    physical_tokens_by_engine: dict[str, dict[str, dict[str, Any]]] = {}
    physical_sources_by_engine: dict[str, dict[str, dict[str, Any]]] = {}

    failures = 0
    for system in systems:
        engine_key = _engine_key(system["dbms"], system["version"])
        physical_tokens_by_engine[engine_key] = {}
        physical_sources_by_engine[engine_key] = {}

        for group in groups:
            try:
                tokens, sources = _run_physical_group(
                    group=group,
                    system=system,
                    run_cfg=run_cfg,
                    work_root=work_root,
                )
            except Exception as exc:
                failures += 1
                if run_cfg["run"].get("fail_fast", False):
                    raise

                for entry in group.queries:
                    physical_sources_by_engine[engine_key][f"{entry.group_key}:{entry.query_id}"] = {
                        "state": "failed",
                        "result_csv_path": None,
                        "query_id": entry.query_id,
                        "dbms": system["dbms"],
                        "version": system["version"],
                        "message": str(exc),
                    }
                continue

            for query_id, token in tokens.items():
                physical_tokens_by_engine[engine_key][f"{group.group_key}:{query_id}"] = token
            for query_id, source in sources.items():
                physical_sources_by_engine[engine_key][f"{group.group_key}:{query_id}"] = source

    export_cfg = spec.get("export", {})
    training_name = export_cfg.get("training_jsonl_name") or export_cfg.get("jsonl_name") or "training.jsonl"
    manifest_name = export_cfg.get("manifest_jsonl_name") or "training_manifest.jsonl"
    training_path = dataset_root / training_name
    manifest_path = dataset_root / manifest_name

    training_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    training_path.write_text("")
    manifest_path.write_text("")

    batch_size = 100
    training_batch: list[dict[str, Any]] = []
    manifest_batch: list[dict[str, Any]] = []
    written_rows = 0

    for training_row, manifest_row in _iter_training_and_manifest_rows(
        query_entries=query_entries,
        logical_records=logical_records,
        physical_tokens_by_engine=physical_tokens_by_engine,
        physical_sources_by_engine=physical_sources_by_engine,
        table_name_to_id_by_group=table_name_to_id_by_group,
    ):
        training_batch.append(training_row)
        manifest_batch.append(manifest_row)

        if len(training_batch) >= batch_size:
            _append_jsonl(training_path, training_batch)
            _append_jsonl(manifest_path, manifest_batch)
            written_rows += len(training_batch)
            training_batch.clear()
            manifest_batch.clear()
            print(f"Flushed {written_rows} rows to JSONL outputs")

    if training_batch:
        _append_jsonl(training_path, training_batch)
        _append_jsonl(manifest_path, manifest_batch)
        written_rows += len(training_batch)
        print(f"Flushed {written_rows} rows to JSONL outputs")

    print(f"Training JSONL: {training_path}")
    print(f"Manifest JSONL: {manifest_path}")
    print(f"Work directory: {work_root}")
    return 1 if failures else 0


def main():
    parser = argparse.ArgumentParser(
        description="Build GenDBA training and manifest JSONLs from one dataset YAML."
    )
    parser.add_argument("--config", required=True, help="Path to the dataset pipeline YAML definition.")
    parser.add_argument("--run-id", default=None, help="Optional run id. Defaults to a UTC timestamp.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose benchmark execution.")
    parser.add_argument("--very-verbose", action="store_true", help="Enable very verbose benchmark execution.")
    parser.add_argument("--no-clear", action="store_true", help="Do not pass --clear to OLAPBench runs.")
    parser.add_argument("--fail-fast", action="store_true", help="Stop at the first engine/group failure.")
    args = parser.parse_args()
    raise SystemExit(run_pipeline(args))


if __name__ == "__main__":
    main()
