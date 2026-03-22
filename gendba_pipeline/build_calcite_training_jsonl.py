#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

try:
    import simplejson as json
except ImportError:
    import json

from natsort import natsorted

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from gendba_pipeline.calcite_plangen_py import PlanGenerator


def _dumps(value: Any) -> str:
    try:
        return json.dumps(value, allow_nan=True)
    except TypeError:
        return json.dumps(value)


def _write_jsonl(path: Path, rows: list[dict[str, Any]]):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as file:
        for row in rows:
            file.write(_dumps(row))
            file.write("\n")


def _plan_artifacts_exist(work_dir_obj: Path, query_stem: str) -> bool:
    required = [
        work_dir_obj / f"{query_stem}.plan.txt",
        work_dir_obj / f"{query_stem}.plan.json",
        work_dir_obj / f"{query_stem}.global-mapping.json",
    ]
    return all(path.exists() for path in required)


def _normalize_text_block(text: str) -> list[str]:
    return [line.rstrip() for line in text.splitlines() if line.strip()]


def _load_schema_table_ids(schema_path: Path) -> tuple[str | None, dict[str, int]]:
    schema_doc = json.loads(schema_path.read_text())
    schema_name = schema_doc.get("name")
    table_ids: dict[str, int] = {}
    for index, table in enumerate(schema_doc.get("tables", [])):
        table_name = table.get("name")
        if table_name:
            table_ids[table_name] = index
    return schema_name, table_ids


def _is_numeric_type(type_doc: dict[str, Any] | None) -> bool:
    if not isinstance(type_doc, dict):
        return False
    return str(type_doc.get("type", "")).upper() in {
        "BIGINT",
        "DECIMAL",
        "DOUBLE",
        "FLOAT",
        "INTEGER",
        "REAL",
        "SMALLINT",
        "TINYINT",
    }


def _normalize_literal_value(value: Any) -> str:
    normalized = str(value).upper().replace(" ", "_")
    normalized = re.sub(r"_+", "_", normalized)
    return normalized


def _render_scalar_token(expr: dict[str, Any] | int) -> str:
    if isinstance(expr, int):
        return f"[INPUT_{expr}]"
    if "global_ids" in expr and expr["global_ids"]:
        return f"[G{expr['global_ids'][0]['global_id']}]"

    if "literal" in expr:
        literal = expr["literal"]
        if literal is None:
            return "[NULL]"
        if isinstance(literal, bool):
            return f"[NUM] {literal}"
        if isinstance(literal, (int, float)) and _is_numeric_type(expr.get("type")):
            return f"[NUM] {literal}"
        if _is_numeric_type(expr.get("type")):
            return f"[NUM] {literal}"
        return f"[VAL_{_normalize_literal_value(literal)}]"

    if "input" in expr:
        return f"[INPUT_{expr['input']}]"

    return "[UNKNOWN]"


def _extract_pred_info(
    expr: dict[str, Any], table_ids: dict[str, int],
) -> tuple[list[int], set[int]]:
    """Recursively extract global column IDs and table IDs from an expression."""
    global_ids: list[int] = []
    tables: set[int] = set()
    if "global_ids" in expr and expr["global_ids"]:
        for gid_entry in expr["global_ids"]:
            global_ids.append(gid_entry["global_id"])
            tbl = gid_entry.get("table")
            if tbl and tbl in table_ids:
                tables.add(table_ids[tbl])
    for operand in expr.get("operands", []):
        child_gids, child_tbls = _extract_pred_info(operand, table_ids)
        global_ids.extend(child_gids)
        tables.update(child_tbls)
    return global_ids, tables


_SUBQUERY_KINDS = {"EXISTS", "IN", "SCALAR_QUERY", "ARRAY_QUERY",
                   "MULTISET_QUERY", "MAP_QUERY", "SOME", "ALL"}


def _render_expression_lines(
    expr: dict[str, Any], indent: str, table_ids: dict[str, int],
    _counter: list[int] | None = None,
    pred_counter: list[int] | None = None,
    pred_registry: list[dict] | None = None,
    _label_pred: bool = False,
) -> list[str]:
    if "op" not in expr:
        return [f"{indent}{_render_scalar_token(expr)}"]

    kind = str(expr["op"].get("kind", expr["op"].get("name", "UNKNOWN"))).upper()
    operands = expr.get("operands", [])

    if kind in {"AND", "OR"}:
        lines = [f"{indent}[{kind}]"]
        for operand in operands:
            lines.extend(_render_expression_lines(
                operand, indent + "  ", table_ids, _counter,
                pred_counter, pred_registry, _label_pred,
            ))
        return lines

    # Helper: assign [P_n] when labelling is active and expression has column refs.
    def _maybe_pred_tag(ex: dict[str, Any]) -> str:
        if not (_label_pred and pred_counter is not None and pred_registry is not None):
            return ""
        global_ids, tables = _extract_pred_info(ex, table_ids)
        if not global_ids:
            return ""
        p_id = pred_counter[0]
        pred_counter[0] += 1
        pred_registry.append({"pred_id": p_id, "global_ids": global_ids, "tables": sorted(tables)})
        return f"[P_{p_id}] "

    if kind == "NOT":
        p_tag = _maybe_pred_tag(expr)
        # Child does NOT get its own P_n — the NOT wrapper claims it.
        child_lines = _render_expression_lines(
            operands[0], "", table_ids, _counter,
            pred_counter, pred_registry, False,
        )
        if len(child_lines) == 1:
            return [f"{indent}{p_tag}[NOT] {child_lines[0].lstrip().rstrip()} "]
        lines = [f"{indent}{p_tag}[NOT]"]
        for child_line in child_lines:
            lines.append(f"{indent}  {child_line.lstrip()}")
        return lines

    if kind in {"IS_NULL", "IS_NOT_NULL"} and operands:
        p_tag = _maybe_pred_tag(expr)
        return [f"{indent}{p_tag}[{kind}] {_render_scalar_token(operands[0])} "]

    if kind in _SUBQUERY_KINDS:
        scalar_parts = [_render_scalar_token(op) for op in operands]
        suffix = (" " + " ".join(scalar_parts)) if scalar_parts else ""
        lines = [f"{indent}[{kind}]{suffix}"]
        nested_rel = expr.get("rel")
        if isinstance(nested_rel, dict):
            nested_rels = nested_rel.get("rels", [])
            if nested_rels:
                lines.append(f"{indent}  [SUBQUERY]")
                lines.extend(_build_rels_token_lines(
                    nested_rels, table_ids, indent=indent + "    ", _counter=_counter,
                    pred_counter=pred_counter, pred_registry=pred_registry,
                ))
                lines.append(f"{indent}  [/SUBQUERY]")
        return lines

    if len(operands) == 2:
        p_tag = _maybe_pred_tag(expr)
        left = _render_scalar_token(operands[0])
        right = _render_scalar_token(operands[1])
        return [f"{indent}{p_tag}[{kind}] {left} {right} "]

    lines = [f"{indent}[{kind}]"]
    for operand in operands:
        lines.extend(_render_expression_lines(
            operand, indent + "  ", table_ids, _counter,
            pred_counter, pred_registry, _label_pred,
        ))
    return lines


def _render_project_cols(rel: dict[str, Any]) -> str:
    expr_tokens = [_render_scalar_token(expr) for expr in rel.get("exprs", [])]
    return ", ".join(expr_tokens)


def _render_aggregate_lines(rel: dict[str, Any], indent: str) -> list[str]:
    lines: list[str] = []
    for agg in rel.get("aggs", []):
        agg_name = agg.get("agg", {}).get("kind") or agg.get("agg", {}).get("name", "UNKNOWN")
        operands = agg.get("operands", [])
        operand_token = _render_scalar_token(operands[0]) if operands else "[UNKNOWN]"
        lines.append(f"{indent}[AGG] [{str(agg_name).upper()}] {operand_token}")
    return lines


def _rel_inputs(rel: dict[str, Any], ptr_ids: dict[str, int], default_ptr: int | None = None) -> str:
    inputs = rel.get("inputs")
    if inputs is None and "input" in rel:
        inputs = [rel["input"]]
    if not inputs and default_ptr is not None:
        inputs = [default_ptr]
    if not inputs:
        return ""
    return " ".join(f"[PTR_{ptr_ids[str(input_id)]}]" for input_id in inputs)


def _build_rels_token_lines(
    rels: list[dict[str, Any]], table_ids: dict[str, int], indent: str = "  ",
    _counter: list[int] | None = None,
    pred_counter: list[int] | None = None,
    pred_registry: list[dict] | None = None,
) -> list[str]:
    if _counter is None:
        _counter = [0]
    ptr_ids: dict[str, int] = {}
    for rel in rels:
        ptr_ids[str(rel.get("id"))] = _counter[0]
        _counter[0] += 1
    token_lines: list[str] = []
    cond_indent = indent + "  "

    for ptr_index, rel in enumerate(rels):
        operator = rel.get("relOp", "UNKNOWN")
        ptr_val = ptr_ids[str(rel.get("id"))]
        token_lines.append(f"{indent}[PTR_{ptr_val}] [{operator}]" + (
            f" [T{table_ids.get(rel.get('table', ['', ''])[-1], -1)}]"
            if operator == "LogicalTableScan"
            else (f" [{str(rel.get('joinType', '')).upper()}]" if operator == "LogicalJoin" else "")
        ))

        if operator == "LogicalJoin":
            condition_lines = _render_expression_lines(
                rel.get("condition", {}), cond_indent, table_ids, _counter,
                pred_counter, pred_registry, _label_pred=True,
            )
            if condition_lines:
                condition_lines[-1] = condition_lines[-1].rstrip()
                head = condition_lines[0].lstrip()
                token_lines.append(f"{cond_indent}[CONDITION] {head}")
                token_lines.extend(condition_lines[1:])
            input_tokens = _rel_inputs(rel, ptr_ids)
            token_lines.append(f"{cond_indent}[INPUT] {input_tokens}")
            continue

        if operator == "LogicalFilter":
            condition_lines = _render_expression_lines(
                rel.get("condition", {}), cond_indent, table_ids, _counter,
                pred_counter, pred_registry, _label_pred=True,
            )
            if condition_lines:
                condition_lines[-1] = condition_lines[-1].rstrip()
                head = condition_lines[0].lstrip()
                token_lines.append(f"{cond_indent}[CONDITION] {head}")
                token_lines.extend(condition_lines[1:])
            default_ptr = rels[ptr_index - 1].get("id") if ptr_index > 0 else None
            input_tokens = _rel_inputs(rel, ptr_ids, default_ptr)
            token_lines.append(f"{cond_indent}[INPUT] {input_tokens}")
            continue

        if operator == "LogicalProject":
            token_lines.append(f"{cond_indent}[COL] {_render_project_cols(rel)}")
            default_ptr = rels[ptr_index - 1].get("id") if ptr_index > 0 else None
            input_tokens = _rel_inputs(rel, ptr_ids, default_ptr)
            token_lines.append(f"{cond_indent}[INPUT] {input_tokens}")
            continue

        if operator == "LogicalAggregate":
            token_lines.extend(_render_aggregate_lines(rel, cond_indent))
            default_ptr = rels[ptr_index - 1].get("id") if ptr_index > 0 else None
            input_tokens = _rel_inputs(rel, ptr_ids, default_ptr)
            token_lines.append(f"{cond_indent}[INPUT] {input_tokens}")

    return token_lines


def _build_logical_token(plan_json: dict[str, Any], table_ids: dict[str, int]) -> tuple[str, list[dict]]:
    """Return ``(logical_token_string, pred_registry)``."""
    rels = plan_json.get("rels", [])
    counter = [0]
    pred_counter = [0]
    pred_registry: list[dict] = []
    lines = ["[LOGICAL_PLAN]"]
    lines.extend(_build_rels_token_lines(
        rels, table_ids, _counter=counter,
        pred_counter=pred_counter, pred_registry=pred_registry,
    ))
    lines.append("[/LOGICAL_PLAN]")
    return "\n".join(lines), pred_registry


def _build_record_for_query(
    query_file: str,
    queries_dir: str,
    artifact_dir: str,
    schema_name: str | None,
    table_ids: dict[str, int],
    catalog_path: str | None,
) -> dict[str, Any] | None:
    """Worker: build a training record for one query. Returns None if artifacts missing."""
    queries_dir_obj = Path(queries_dir)
    artifact_dir_obj = Path(artifact_dir)
    query_path = queries_dir_obj / query_file
    query_stem = query_path.stem

    if not _plan_artifacts_exist(artifact_dir_obj, query_stem):
        return None

    from gendba_pipeline.calcite_plangen_py import PlanGenerator
    generator = PlanGenerator.__new__(PlanGenerator)  # no jar needed for load_plan
    generator.verbose = False
    generator.jar_path = ""  # not used for load_plan
    plan = generator.load_plan(str(artifact_dir_obj / query_stem))

    logical_ir, pred_registry = _build_logical_token(plan.json_plan, table_ids)

    return {
        "schema": schema_name,
        "query_key": query_file,
        "query_sql": query_path.read_text(),
        "calcite_plan": plan.text_plan,
        "logical_tokenized_plan": logical_ir,
        "pred_registry": pred_registry,
        "plan_json_path": str(artifact_dir_obj / f"{query_stem}.plan.json"),
        "plan_text_path": str(artifact_dir_obj / f"{query_stem}.plan.txt"),
        "global_mapping_path": str(artifact_dir_obj / f"{query_stem}.global-mapping.json"),
        "annotated_plan_path": str(artifact_dir_obj / f"{query_stem}.annotated.txt"),
        "catalog_path": catalog_path,
    }


def _make_progress_printer(label: str, counter_fn, total: int, interval: int = 300):
    """Return (stop_event, thread) for a background 5-minute progress printer.

    counter_fn() is called each tick and must return the current done count.
    """
    stop_event = threading.Event()

    def _run(stop_event: threading.Event) -> None:
        start = time.monotonic()
        # Print immediately at t=0 so the user sees the phase has started.
        print(f"[{label}] Progress: 0/{total} done (0%)  —  starting …")
        while not stop_event.wait(timeout=interval):
            done = counter_fn()
            elapsed = time.monotonic() - start
            rate = done / elapsed if elapsed > 0 else 0
            eta = (total - done) / rate if rate > 0 else float("inf")
            eta_str = f"{eta / 60:.1f} min" if eta != float("inf") else "unknown"
            print(
                f"[{label}] Progress: {done}/{total} done "
                f"({100 * done // total}%)  —  {rate:.1f} q/s  —  ETA {eta_str}"
            )

    thread = threading.Thread(target=_run, args=(stop_event,), daemon=True)
    return stop_event, thread


def build_calcite_training_records(
    schema_path: str,
    queries_dir: str,
    output_file: str | None = None,
    plangen_bin: str | None = None,
    work_dir: str | None = None,
    keep_work_dir: bool = False,
    workers: int = 1,
) -> tuple[list[dict[str, Any]], list[str]]:
    schema_path_obj = Path(schema_path)
    queries_dir_obj = Path(queries_dir)
    schema_name, table_ids = _load_schema_table_ids(schema_path_obj)

    temp_dir: tempfile.TemporaryDirectory[str] | None = None
    if work_dir:
        work_dir_obj = Path(work_dir)
        work_dir_obj.mkdir(parents=True, exist_ok=True)
    else:
        temp_dir = tempfile.TemporaryDirectory(prefix="calcite_plans_")
        work_dir_obj = Path(temp_dir.name)

    sql_files = natsorted(p.name for p in queries_dir_obj.iterdir() if p.suffix == ".sql")
    total = len(sql_files)

    # ── Phase 1: Run plangen (Java) ───────────────────────────────────────────
    generator = PlanGenerator(jar_path=plangen_bin)

    if workers <= 1:
        # Single JVM call — watch work_dir for .plan.txt files as progress proxy.
        def _plan_count() -> int:
            return sum(1 for _ in work_dir_obj.glob("*.plan.txt"))

        stop_ev, printer = _make_progress_printer("calcite-jvm", _plan_count, total)
        printer.start()
        try:
            generator.generate_plans(
                schema_path=str(schema_path_obj),
                queries_dir=str(queries_dir_obj),
                output_dir=str(work_dir_obj),
            )
        finally:
            stop_ev.set()
            printer.join()
            print(f"[calcite-jvm] Progress: {_plan_count()}/{total} done (JVM complete)")

        # All artifacts in work_dir_obj
        stem_to_dir: dict[str, Path] = {Path(f).stem: work_dir_obj for f in sql_files}
        catalog_path = work_dir_obj / "global-column-catalog.json"

    else:
        # Parallel JVM: split queries into N batches, each gets its own temp query dir
        # and output dir, then N plangen processes run in parallel via threads
        # (subprocess.run releases the GIL).
        chunk_size = max(1, (total + workers - 1) // workers)
        chunks = [sql_files[i : i + chunk_size] for i in range(0, total, chunk_size)]
        actual_workers = len(chunks)

        batch_dirs: list[tuple[Path, Path]] = []
        for i, chunk in enumerate(chunks):
            bq_dir = work_dir_obj / f"batch_{i}_queries"
            bo_dir = work_dir_obj / f"batch_{i}_output"
            bq_dir.mkdir(parents=True, exist_ok=True)
            bo_dir.mkdir(parents=True, exist_ok=True)
            for fname in chunk:
                link = bq_dir / fname
                if not link.exists():
                    os.symlink(queries_dir_obj / fname, link)
            batch_dirs.append((bq_dir, bo_dir))

        def _plan_count() -> int:
            return sum(1 for _, bo in batch_dirs for _ in bo.glob("*.plan.txt"))

        print(
            f"[calcite-jvm] Parallel mode: {actual_workers} workers, "
            f"{total} queries, ~{chunk_size} queries/batch."
        )

        stop_ev, printer = _make_progress_printer("calcite-jvm", _plan_count, total)
        printer.start()
        try:
            def _run_batch(args: tuple[Path, Path]) -> None:
                bq_dir, bo_dir = args
                generator.generate_plans(
                    schema_path=str(schema_path_obj),
                    queries_dir=str(bq_dir),
                    output_dir=str(bo_dir),
                )

            with ThreadPoolExecutor(max_workers=actual_workers) as pool:
                futures = [pool.submit(_run_batch, bd) for bd in batch_dirs]
                for f in as_completed(futures):
                    f.result()  # re-raises any JVM exception
        finally:
            stop_ev.set()
            printer.join()
            print(f"[calcite-jvm] Progress: {_plan_count()}/{total} done (all batches complete)")

        # Build stem -> output dir mapping
        stem_to_dir = {}
        for fname in sql_files:
            stem = Path(fname).stem
            for _, bo_dir in batch_dirs:
                if (bo_dir / f"{stem}.plan.txt").exists():
                    stem_to_dir[stem] = bo_dir
                    break

        # Use catalog from first batch that produced one
        catalog_path = work_dir_obj / "global-column-catalog.json"
        if not catalog_path.exists():
            for _, bo_dir in batch_dirs:
                candidate = bo_dir / "global-column-catalog.json"
                if candidate.exists():
                    shutil.copy(candidate, catalog_path)
                    break

    # ── Phase 2: Build training records (parallel Python post-processing) ─────
    catalog_str = str(catalog_path) if catalog_path.exists() else None
    post_workers = max(1, workers)

    done_count = [0]
    done_lock = threading.Lock()

    def _post_count() -> int:
        with done_lock:
            return done_count[0]

    stop_ev2, printer2 = _make_progress_printer("calcite-postproc", _post_count, total)
    printer2.start()

    rows_unordered: list[tuple[int, dict[str, Any]]] = []
    skipped_queries: list[str] = []

    try:
        if post_workers <= 1:
            for i, query_file in enumerate(sql_files):
                query_stem = Path(query_file).stem
                artifact_dir = stem_to_dir.get(query_stem, work_dir_obj)
                row = _build_record_for_query(
                    query_file=query_file,
                    queries_dir=str(queries_dir_obj),
                    artifact_dir=str(artifact_dir),
                    schema_name=schema_name,
                    table_ids=table_ids,
                    catalog_path=catalog_str,
                )
                with done_lock:
                    done_count[0] += 1
                if row is None:
                    skipped_queries.append(query_file)
                else:
                    rows_unordered.append((i, row))
        else:
            indexed_files = list(enumerate(sql_files))
            with ProcessPoolExecutor(max_workers=post_workers) as pool:
                futures_map = {
                    pool.submit(
                        _build_record_for_query,
                        query_file=qf,
                        queries_dir=str(queries_dir_obj),
                        artifact_dir=str(stem_to_dir.get(Path(qf).stem, work_dir_obj)),
                        schema_name=schema_name,
                        table_ids=table_ids,
                        catalog_path=catalog_str,
                    ): (idx, qf)
                    for idx, qf in indexed_files
                }
                for future in as_completed(futures_map):
                    idx, qf = futures_map[future]
                    row = future.result()
                    with done_lock:
                        done_count[0] += 1
                    if row is None:
                        skipped_queries.append(qf)
                    else:
                        rows_unordered.append((idx, row))
    finally:
        stop_ev2.set()
        printer2.join()
        print(f"[calcite-postproc] Progress: {_post_count()}/{total} done (complete)")

    # Restore natural sort order
    rows = [row for _, row in sorted(rows_unordered, key=lambda x: x[0])]

    if skipped_queries:
        print(
            f"Skipped {len(skipped_queries)} queries with no Calcite plan artifacts in {queries_dir_obj}",
            file=sys.stderr,
        )

    if output_file:
        _write_jsonl(Path(output_file), rows)

    if temp_dir is not None and not keep_work_dir:
        temp_dir.cleanup()

    return rows, skipped_queries


def main():
    parser = argparse.ArgumentParser(
        description="Build Calcite logical-token JSONL from a schema and query directory."
    )
    parser.add_argument("--schema-path", required=True, help="Path to the benchmark schema JSON file.")
    parser.add_argument("--queries-dir", required=True, help="Directory containing .sql query files.")
    parser.add_argument("--output-file", required=True, help="Destination JSONL file.")
    parser.add_argument("--plangen-bin", default=None, help="Path to the plangen binary.")
    parser.add_argument("--work-dir", default=None, help="Directory to persist Calcite plan artifacts.")
    parser.add_argument(
        "--keep-work-dir",
        action="store_true",
        help="Keep the auto-created work directory when --work-dir is not provided.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        metavar="N",
        help="Number of parallel JVM+postproc workers. Default: min(16, os.cpu_count()).",
    )
    args = parser.parse_args()

    build_calcite_training_records(
        schema_path=args.schema_path,
        queries_dir=args.queries_dir,
        output_file=args.output_file,
        plangen_bin=args.plangen_bin,
        work_dir=args.work_dir,
        keep_work_dir=args.keep_work_dir,
        workers=args.workers if args.workers is not None else min(16, os.cpu_count() or 1),
    )


if __name__ == "__main__":
    main()
