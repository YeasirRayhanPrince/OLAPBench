#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import datetime as dt
import multiprocessing
import os
import re
import subprocess
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
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


def _transpile_sql(sql: str, read_dialect: str, write_dialect: str) -> str:
    """Transpile *sql* from *read_dialect* to *write_dialect* using sqlglot.

    Falls back to the original SQL string if sqlglot raises (e.g. unsupported
    syntax), logging a one-line warning so failures are visible but non-fatal.
    """
    try:
        import sqlglot
        stmts = sqlglot.transpile(sql, read=read_dialect, write=write_dialect)
        if stmts:
            return stmts[0]
    except Exception as exc:
        print(f"[transpile] WARNING: could not transpile SQL ({exc}); using original", flush=True)
    return sql


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


def _inject_explain_only_stats(node: dict) -> dict:
    """Recursively inject estimated row counts as Actual Rows for EXPLAIN (no ANALYZE) output.

    PostgresParser unconditionally reads json_plan["Actual Rows"], which is absent when
    EXPLAIN is run without ANALYZE. This helper fills in Plan Rows / Actual Loops = 1 so
    the parser can proceed without modification.
    """
    result = dict(node)
    if "Actual Rows" not in result:
        result["Actual Rows"] = result.get("Plan Rows", 0)
    if "Actual Loops" not in result:
        result["Actual Loops"] = 1
    if "Plans" in result:
        result["Plans"] = [_inject_explain_only_stats(c) for c in result["Plans"]]
    return result


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
                    params["use_local"] = True
                    if system["dbms"] == "duckdb":
                        params["local_path"] = local.get("path")
                        params["local_data_dir"] = local.get("data_dir")
                    elif system["dbms"] == "postgres":
                        params["local_host"] = local.get("host", "localhost")
                        params["local_port"] = local.get("port")
                        params["local_user"] = local.get("user")
                        params["local_password"] = local.get("password")
                        params["local_database"] = local.get("database")
                    else:
                        raise ValueError(
                            f"Local mode is not supported for {system['dbms']}"
                        )

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


def _explain_chunk(
    chunk: list,
    conn_params: dict,
    system_meta: dict,
    chunk_index: int,
    total_chunks: int,
    total_queries: int,
) -> tuple[dict, dict]:
    """Worker: run EXPLAIN (format json) + parse for one chunk of queries in a subprocess.

    Must be a module-level function so ProcessPoolExecutor can pickle it under both
    'fork' and 'spawn' start methods. All heavy imports are done lazily here so they
    are not required at import time of the parent process.
    """
    try:
        import psycopg2
    except ImportError:
        raise RuntimeError("psycopg2 is required for --explain-only mode")

    from queryplan.parsers.postgresparser import PostgresParser
    from queryplan.queryplan import encode_query_plan

    host = conn_params["host"]
    port = conn_params["port"]
    user = conn_params["user"]
    password = conn_params.get("password") or ""
    database = conn_params["database"]

    tokens: dict = {}
    sources: dict = {}

    conn = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=database)
    conn.autocommit = True
    print(
        f"[explain-chunk {chunk_index+1}/{total_chunks}] Connected to PostgreSQL at "
        f"{host}:{port} db={database} user={user} "
        f"({len(chunk)} queries of {total_queries} total)"
    )
    try:
        for i, entry in enumerate(chunk):
            query_id = entry.query_id
            sql = entry.sql.strip()
            source_info: dict = {
                "state": "unknown",
                "result_csv_path": None,
                "query_id": query_id,
                "dbms": system_meta["dbms"],
                "version": system_meta.get("version"),
                "title": system_meta.get("title"),
                "message": None,
            }
            try:
                with conn.cursor() as cur:
                    cur.execute(f"EXPLAIN (format json) {sql}")
                    row = cur.fetchone()

                if not row or not row[0]:
                    source_info["state"] = "error"
                    source_info["message"] = "No plan returned by EXPLAIN"
                    sources[query_id] = source_info
                    continue

                pg_json = row[0]
                if isinstance(pg_json, list):
                    pg_json = pg_json[0]

                augmented = _inject_explain_only_stats(pg_json.get("Plan", pg_json))
                plan_dict = {"Plan": augmented}

                # PostgresParser is stateful (op_counter, ctes) — create one per query
                parser = PostgresParser(include_system_representation=True)
                query_plan = parser.parse_json_plan(sql, plan_dict)
                plan_str = encode_query_plan(query_plan)
                plan_doc = _loads(plan_str)

                if not isinstance(plan_doc, dict):
                    source_info["state"] = "error"
                    source_info["message"] = "Plan encoding produced unexpected type"
                    sources[query_id] = source_info
                    continue

                plan_root = plan_doc.get("queryPlan")
                if not isinstance(plan_root, dict):
                    source_info["state"] = "error"
                    source_info["message"] = "No queryPlan key in encoded plan"
                    sources[query_id] = source_info
                    continue

                operators: list = []
                _flatten_plan(plan_root, operators)
                tokens[query_id] = {"plan": plan_doc, "operators": operators}
                source_info["state"] = "success"
                sources[query_id] = source_info

            except Exception as exc:
                source_info["state"] = "error"
                source_info["message"] = str(exc)
                sources[query_id] = source_info

            if (i + 1) % 500 == 0 or (i + 1) == len(chunk):
                print(
                    f"[explain-chunk {chunk_index+1}/{total_chunks}] "
                    f"{i+1}/{len(chunk)} done"
                )
    finally:
        conn.close()

    return tokens, sources


def _setup_duckdb_database(
    db_path: str,
    data_dir: str,
    schema_path: str,
    index: str = "primary",
) -> None:
    """Create a persistent DuckDB file and load data from CSVs.

    Mirrors Docker load_database() exactly:
      1. index param controls primary_key / foreign_keys (default: primary → PKs only)
      2. _transform_schema(): sql.transform_schema(escape='"', lowercase=False)
      3. DuckDB._create_table_statements(alter_table=False): inline constraints
      4. DuckDB._copy_statements(): copy_statements_postgres(..., supports_text=False)

    If the file already exists, loading is skipped (create-if-not-exists).
    The file is never deleted by the pipeline — it persists for future preloaded runs.
    On failure, any partial file is removed so the next run starts clean.
    """
    try:
        import duckdb as _duckdb
    except ImportError:
        raise RuntimeError("duckdb Python package is required for managed DuckDB loading")

    from util import sql as _sql, schemajson as _schemajson

    _db_path = Path(db_path)
    if _db_path.exists():
        print(f"[setup-duckdb] {_db_path} already exists — skipping load (delete to reload)")
        return

    _db_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"[setup-duckdb] Creating {_db_path} from {data_dir} (index={index}) …")

    # Derive primary_key / foreign_keys from index param — mirrors DBMS.load_database()
    primary_key = index in ("primary", "foreign")
    foreign_keys = index == "foreign"

    # Load schema and inject file paths — mirrors benchmark.get_schema()
    schema = _schemajson.load(str(schema_path), "dbschema.schema.json")
    for table in schema["tables"]:
        table["file"] = table["name"] + "." + schema["file_ending"]
        if not primary_key:
            table.pop("primary key", None)
        if not foreign_keys:
            table.get("foreign keys", []).clear()

    # _transform_schema(): wrap all names in double-quotes
    schema = _sql.transform_schema(schema, escape='"', lowercase=False)

    # DuckDB._create_table_statements(alter_table=False)
    create_stmts = _sql.create_table_statements(schema, alter_table=False)

    # DuckDB._copy_statements() for v1.5.1 (not in singlethreaded list)
    copy_stmts = _sql.copy_statements_postgres(schema, data_dir, supports_text=False)

    conn = _duckdb.connect(str(_db_path))
    try:
        for stmt in create_stmts:
            parts = stmt.split('"')
            label = parts[1] if len(parts) > 1 else "..."
            print(f"[setup-duckdb]   CREATE TABLE {label}")
            conn.execute(stmt)
        for stmt in copy_stmts:
            parts = stmt.split('"')
            table_label = parts[1] if len(parts) > 1 else "..."
            print(f"[setup-duckdb]   LOAD {table_label} …", end="", flush=True)
            conn.execute(stmt)
            count = conn.execute(f'SELECT COUNT(*) FROM "{table_label}"').fetchone()[0]
            print(f" {count:,} rows")
    except Exception:
        conn.close()
        _db_path.unlink(missing_ok=True)
        raise
    conn.close()
    print(f"[setup-duckdb] Done — {_db_path}")


def _explain_chunk_duckdb(
    chunk: list,
    conn_params: dict,
    system_meta: dict,
    chunk_index: int,
    total_chunks: int,
    total_queries: int,
) -> tuple[dict, dict]:
    """Worker: run EXPLAIN ANALYZE (FORMAT JSON) + parse for one chunk of queries via DuckDB.

    Uses the duckdb Python library to connect directly to a local .duckdb file.
    EXPLAIN ANALYZE executes the query and returns real cardinalities, so no stat
    injection is needed. Must be a module-level function so ProcessPoolExecutor can
    pickle it under both 'fork' and 'spawn' start methods.
    """
    try:
        import duckdb
    except ImportError:
        raise RuntimeError("duckdb Python package is required for DuckDB explain-only mode")

    try:
        import simplejson as _json
    except ImportError:
        import json as _json

    from queryplan.parsers.duckdbparser import DuckDBParser
    from queryplan.queryplan import encode_query_plan

    db_path = conn_params["path"]
    tokens: dict = {}
    sources: dict = {}

    conn = duckdb.connect(db_path, read_only=True)
    print(
        f"[explain-chunk-duckdb {chunk_index+1}/{total_chunks}] Connected to DuckDB at "
        f"{db_path} ({len(chunk)} queries of {total_queries} total)"
    )
    try:
        for i, entry in enumerate(chunk):
            query_id = entry.query_id
            sql = entry.sql.strip()
            exec_sql = _transpile_sql(sql, read_dialect="postgres", write_dialect="duckdb")
            source_info: dict = {
                "state": "unknown",
                "result_csv_path": None,
                "query_id": query_id,
                "dbms": system_meta["dbms"],
                "version": system_meta.get("version"),
                "title": system_meta.get("title"),
                "message": None,
            }
            try:
                result = conn.execute(f"EXPLAIN (FORMAT JSON, ANALYZE) {exec_sql}").fetchone()

                if not result or result[1] is None:
                    source_info["state"] = "error"
                    source_info["message"] = "No plan returned by EXPLAIN ANALYZE"
                    sources[query_id] = source_info
                    continue

                json_plan = _json.loads(result[1])

                parser = DuckDBParser(include_system_representation=True)
                query_plan = parser.parse_json_plan(sql, json_plan)
                plan_str = encode_query_plan(query_plan)
                plan_doc = _loads(plan_str)

                if not isinstance(plan_doc, dict):
                    source_info["state"] = "error"
                    source_info["message"] = "Plan encoding produced unexpected type"
                    sources[query_id] = source_info
                    continue

                plan_root = plan_doc.get("queryPlan")
                if not isinstance(plan_root, dict):
                    source_info["state"] = "error"
                    source_info["message"] = "No queryPlan key in encoded plan"
                    sources[query_id] = source_info
                    continue

                operators: list = []
                _flatten_plan(plan_root, operators)
                tokens[query_id] = {"plan": plan_doc, "operators": operators}
                source_info["state"] = "success"
                sources[query_id] = source_info

            except Exception as exc:
                source_info["state"] = "error"
                source_info["message"] = str(exc)
                sources[query_id] = source_info

            if (i + 1) % 500 == 0 or (i + 1) == len(chunk):
                print(
                    f"[explain-chunk-duckdb {chunk_index+1}/{total_chunks}] "
                    f"{i+1}/{len(chunk)} done"
                )
    finally:
        conn.close()

    return tokens, sources


def _run_explain_group(
    group: QueryGroup,
    system: dict[str, Any],
    workers: int = 1,
) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Run EXPLAIN for each query and return plan tokens without executing.

    Routes to a DBMS-specific chunk worker (_explain_chunk for postgres,
    _explain_chunk_duckdb for duckdb). Uses the optimizer's estimated cardinalities
    (or EXPLAIN ANALYZE real cardinalities for DuckDB) so all downstream tokenization
    and JSONL fields are populated in the same structure as the full benchmark path.

    When workers > 1, splits group.queries into equal chunks and dispatches each chunk
    to a separate subprocess via ProcessPoolExecutor, bypassing the GIL for CPU-bound
    plan parsing. Each worker opens its own DB connection.
    """
    params = system["parameter"]
    dbms = system["dbms"]

    if dbms == "postgres":
        try:
            import psycopg2  # noqa: F401 — validate availability before dispatch
        except ImportError:
            raise RuntimeError("psycopg2 is required for --explain-only mode with postgres")
        conn_params = {
            "host": params.get("local_host", "localhost"),
            "port": params.get("local_port", 5432),
            "user": params.get("local_user"),
            "password": params.get("local_password") or "",
            "database": params.get("local_database", "postgres"),
        }
        chunk_fn = _explain_chunk
    elif dbms == "duckdb":
        try:
            import duckdb  # noqa: F401 — validate availability before dispatch
        except ImportError:
            raise RuntimeError("duckdb Python package is required for --explain-only mode with duckdb")
        conn_params = {
            "path": params.get("local_path"),
        }
        chunk_fn = _explain_chunk_duckdb
    else:
        raise ValueError(f"--explain-only mode is not supported for dbms '{dbms}'")

    system_meta = {
        "dbms": system["dbms"],
        "version": system.get("version"),
        "title": system.get("title"),
    }

    all_queries = group.queries
    n = len(all_queries)
    # Plain int tracked in the main process only — no cross-process sharing needed.
    completed = [0]
    completed_lock = threading.Lock()
    progress_interval = 300  # seconds between progress prints

    def _progress_printer(stop_event: threading.Event) -> None:
        start = time.monotonic()
        # Print immediately at t=0 so the user sees the phase has started.
        print(f"[explain-only] Progress: 0/{n} queries done (0%)  —  starting …")
        while not stop_event.wait(timeout=progress_interval):
            with completed_lock:
                done = completed[0]
            elapsed = time.monotonic() - start
            rate = done / elapsed if elapsed > 0 else 0
            eta = (n - done) / rate if rate > 0 else float("inf")
            eta_str = f"{eta/60:.1f} min" if eta != float("inf") else "unknown"
            print(
                f"[explain-only] Progress: {done}/{n} queries done "
                f"({100*done//n}%)  —  {rate:.1f} q/s  —  ETA {eta_str}"
            )

    stop_event = threading.Event()
    printer = threading.Thread(target=_progress_printer, args=(stop_event,), daemon=True)
    printer.start()

    try:
        if workers <= 1:
            print(f"[explain-only] Sequential mode: {n} queries.")
            result = chunk_fn(
                chunk=all_queries,
                conn_params=conn_params,
                system_meta=system_meta,
                chunk_index=0,
                total_chunks=1,
                total_queries=n,
            )
            with completed_lock:
                completed[0] = n
            return result

        chunk_size = max(1, (n + workers - 1) // workers)
        chunks = [all_queries[i : i + chunk_size] for i in range(0, n, chunk_size)]
        actual_workers = len(chunks)

        print(
            f"[explain-only] Parallel mode: {actual_workers} workers, "
            f"{n} queries, ~{chunk_size} queries/chunk."
        )

        merged_tokens: dict[str, dict[str, Any]] = {}
        merged_sources: dict[str, dict[str, Any]] = {}

        with ProcessPoolExecutor(max_workers=actual_workers) as pool:
            futures = {
                pool.submit(
                    chunk_fn,
                    chunk=chunk,
                    conn_params=conn_params,
                    system_meta=system_meta,
                    chunk_index=idx,
                    total_chunks=actual_workers,
                    total_queries=n,
                ): (idx, chunk)
                for idx, chunk in enumerate(chunks)
            }
            for future in as_completed(futures):
                idx, chunk = futures[future]
                exc = future.exception()
                if exc is not None:
                    print(f"[explain-only] Chunk {idx+1}/{actual_workers} failed: {exc}")
                    for entry in chunk:
                        merged_sources[entry.query_id] = {
                            "state": "error",
                            "result_csv_path": None,
                            "query_id": entry.query_id,
                            "dbms": system_meta["dbms"],
                            "version": system_meta.get("version"),
                            "title": system_meta.get("title"),
                            "message": f"[chunk {idx} worker died] {exc}",
                        }
                else:
                    chunk_tokens, chunk_sources = future.result()
                    merged_tokens.update(chunk_tokens)
                    merged_sources.update(chunk_sources)
                    print(
                        f"[explain-only] Chunk {idx+1}/{actual_workers} complete: "
                        f"{len(chunk_tokens)} succeeded, "
                        f"{len(chunk_sources) - len(chunk_tokens)} failed/skipped."
                    )
                with completed_lock:
                    completed[0] += len(chunk)

        return merged_tokens, merged_sources

    finally:
        stop_event.set()
        printer.join()
        print(f"[explain-only] Progress: {n}/{n} queries done (100%)")


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

    explain_workers: int = (
        args.explain_workers if args.explain_workers is not None
        else min(64, os.cpu_count() or 1)
    )
    calcite_workers: int = (
        args.calcite_workers if args.calcite_workers is not None
        else min(16, os.cpu_count() or 1)
    )

    print(f"[pipeline] Loading {len(spec.get('benchmarks', []))} benchmark(s) …")
    groups, query_entries = _resolve_query_groups(spec)
    systems = _resolve_systems(spec)
    print(
        f"[pipeline] Loaded {len(query_entries)} queries across "
        f"{len(groups)} group(s), {len(systems)} system(s)."
    )

    table_name_to_id_by_group: dict[str, dict[str, int]] = {}
    logical_records: dict[str, dict[str, Any]] = {}
    all_failed_queries: list[dict[str, Any]] = []
    for group in groups:
        calcite_output_dir = work_root / "calcite" / group.group_key
        calcite_output_file = calcite_output_dir / "logical_training.jsonl"
        print(
            f"[pipeline] Calcite phase: group={group.group_key}  "
            f"queries={len(group.queries)}  workers={calcite_workers}"
        )
        records, calcite_skipped = build_calcite_training_records(
            schema_path=str(group.schema_path),
            queries_dir=str(group.queries_dir),
            output_file=str(calcite_output_file),
            plangen_bin=run_cfg["calcite"].get("plangen_bin"),
            work_dir=str(calcite_output_dir / "plans"),
            keep_work_dir=True,
            workers=calcite_workers,
        )
        for query_name in calcite_skipped:
            all_failed_queries.append({
                "query_id": query_name,
                "schema": group.schema,
                "workload": group.workload,
                "query_set": group.query_set,
                "failure_stage": "calcite",
                "state": "skipped",
                "message": "Calcite plan artifacts not found",
            })
        table_name_to_id_by_group[group.group_key] = load_table_name_to_id(group.schema_path)
        for record in records:
            logical_records[f"{group.group_key}:{record['query_key']}"] = record

    physical_tokens_by_engine: dict[str, dict[str, dict[str, Any]]] = {}
    physical_sources_by_engine: dict[str, dict[str, dict[str, Any]]] = {}

    load_mode = run_cfg["benchmark_defaults"].get("load_mode", "managed")
    duckdb_loaded: set[tuple[str, str]] = set()

    failures = 0
    for system in systems:
        engine_key = _engine_key(system["dbms"], system["version"])
        physical_tokens_by_engine[engine_key] = {}
        physical_sources_by_engine[engine_key] = {}

        for group in groups:
            # DuckDB managed load: create the .duckdb file from CSVs if it doesn't exist.
            # Runs once per (engine, group) pair. The file is never deleted — it persists
            # for future runs (preloaded or managed both connect to the same file).
            if (args.explain_only
                    and system["dbms"] == "duckdb"
                    and load_mode == "managed"):
                load_key = (engine_key, group.group_key)
                if load_key not in duckdb_loaded:
                    _setup_duckdb_database(
                        db_path=system["parameter"]["local_path"],
                        data_dir=system["parameter"]["local_data_dir"],
                        schema_path=str(group.schema_path),
                        index=system["parameter"].get("index", "primary"),
                    )
                    duckdb_loaded.add(load_key)

            try:
                if args.explain_only:
                    tokens, sources = _run_explain_group(
                        group=group,
                        system=system,
                        workers=explain_workers,
                    )
                else:
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

    composite_key_to_entry: dict[str, QueryEntry] = {
        f"{e.group_key}:{e.query_id}": e for e in query_entries
    }
    for engine_key, engine_sources in physical_sources_by_engine.items():
        for composite_key, source in engine_sources.items():
            if source.get("state") == "success":
                continue
            entry = composite_key_to_entry.get(composite_key)
            all_failed_queries.append({
                "query_id": source.get("query_id", composite_key),
                "schema": entry.schema if entry else None,
                "workload": entry.workload if entry else None,
                "query_set": entry.query_set if entry else None,
                "failure_stage": "postgres",
                "engine": engine_key,
                "state": source.get("state", "unknown"),
                "message": source.get("message", ""),
            })

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

    failed_name = export_cfg.get("failed_queries_jsonl_name") or "failed_queries.jsonl"
    failed_path = dataset_root / failed_name
    _write_jsonl(failed_path, all_failed_queries)

    print(f"Training JSONL: {training_path}")
    print(f"Manifest JSONL: {manifest_path}")
    print(f"Failed queries JSONL: {failed_path}  ({len(all_failed_queries)} entries)")
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
    parser.add_argument(
        "--explain-only",
        action="store_true",
        help=(
            "Use EXPLAIN (format json) without ANALYZE to collect optimizer-estimated plans "
            "without executing queries. Bypasses the OLAPBench benchmark subprocess. "
            "Requires a local PG connection configured in the dataset YAML."
        ),
    )
    parser.add_argument(
        "--explain-workers",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of worker processes for --explain-only parallelism. "
            "Default: min(64, os.cpu_count()). Set to 1 to disable parallelism."
        ),
    )
    parser.add_argument(
        "--calcite-workers",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Number of parallel JVM processes for Calcite plan generation "
            "and post-processing workers. Default: min(16, os.cpu_count()). "
            "Set to 1 to disable parallelism."
        ),
    )
    args = parser.parse_args()
    raise SystemExit(run_pipeline(args))


if __name__ == "__main__":
    main()
