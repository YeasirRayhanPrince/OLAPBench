#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
import tempfile
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


def build_calcite_training_records(
    schema_path: str,
    queries_dir: str,
    output_file: str | None = None,
    plangen_bin: str | None = None,
    work_dir: str | None = None,
    keep_work_dir: bool = False,
) -> list[dict[str, Any]]:
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

    generator = PlanGenerator(jar_path=plangen_bin)
    generator.generate_plans(
        schema_path=str(schema_path_obj),
        queries_dir=str(queries_dir_obj),
        output_dir=str(work_dir_obj),
    )

    catalog_path = work_dir_obj / "global-column-catalog.json"
    rows: list[dict[str, Any]] = []
    skipped_queries: list[str] = []
    for query_file in natsorted(path.name for path in queries_dir_obj.iterdir() if path.suffix == ".sql"):
        query_path = queries_dir_obj / query_file
        query_key = query_file
        query_stem = query_path.stem
        if not _plan_artifacts_exist(work_dir_obj, query_stem):
            skipped_queries.append(query_key)
            continue
        plan = generator.load_plan(str(work_dir_obj / query_stem))

        logical_ir, pred_registry = _build_logical_token(plan.json_plan, table_ids)

        row = {
            "schema": schema_name,
            "query_key": query_key,
            "query_sql": query_path.read_text(),
            "calcite_plan": plan.text_plan,
            "logical_tokenized_plan": logical_ir,
            "pred_registry": pred_registry,
            "plan_json_path": str(work_dir_obj / f"{query_stem}.plan.json"),
            "plan_text_path": str(work_dir_obj / f"{query_stem}.plan.txt"),
            "global_mapping_path": str(work_dir_obj / f"{query_stem}.global-mapping.json"),
            "annotated_plan_path": str(work_dir_obj / f"{query_stem}.annotated.txt"),
            "catalog_path": str(catalog_path) if catalog_path.exists() else None,
        }
        rows.append(row)

    if skipped_queries:
        print(
            f"Skipped {len(skipped_queries)} queries with no Calcite plan artifacts in {queries_dir_obj}",
            file=sys.stderr,
        )

    if output_file:
        _write_jsonl(Path(output_file), rows)

    if temp_dir is not None and not keep_work_dir:
        temp_dir.cleanup()

    return rows


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
    args = parser.parse_args()

    build_calcite_training_records(
        schema_path=args.schema_path,
        queries_dir=args.queries_dir,
        output_file=args.output_file,
        plangen_bin=args.plangen_bin,
        work_dir=args.work_dir,
        keep_work_dir=args.keep_work_dir,
    )


if __name__ == "__main__":
    main()
