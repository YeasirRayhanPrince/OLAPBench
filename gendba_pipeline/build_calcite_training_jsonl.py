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


def _render_scalar_token(expr: dict[str, Any]) -> str:
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


def _render_expression_lines(expr: dict[str, Any], indent: str) -> list[str]:
    if "op" not in expr:
        return [f"{indent}{_render_scalar_token(expr)}"]

    kind = str(expr["op"].get("kind", expr["op"].get("name", "UNKNOWN"))).upper()
    operands = expr.get("operands", [])

    if kind in {"AND", "OR"}:
        lines = [f"{indent}[{kind}]"]
        for operand in operands:
            lines.extend(_render_expression_lines(operand, indent + "  "))
        return lines

    if kind == "NOT":
        child_lines = _render_expression_lines(operands[0], "")
        if len(child_lines) == 1:
            return [f"{indent}[NOT] {child_lines[0].lstrip().rstrip()} "]
        lines = [f"{indent}[NOT]"]
        for child_line in child_lines:
            lines.append(f"{indent}  {child_line.lstrip()}")
        return lines

    if kind in {"IS_NULL", "IS_NOT_NULL"} and operands:
        return [f"{indent}[{kind}] {_render_scalar_token(operands[0])} "]

    if len(operands) == 2:
        left = _render_scalar_token(operands[0])
        right = _render_scalar_token(operands[1])
        return [f"{indent}[{kind}] {left} {right} "]

    lines = [f"{indent}[{kind}]"]
    for operand in operands:
        lines.extend(_render_expression_lines(operand, indent + "  "))
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


def _build_logical_token(plan_json: dict[str, Any], table_ids: dict[str, int]) -> str:
    rels = plan_json.get("rels", [])
    ptr_ids = {str(rel.get("id")): index for index, rel in enumerate(rels)}
    token_lines = ["[LOGICAL_PLAN]"]

    for ptr_index, rel in enumerate(rels):
        operator = rel.get("relOp", "UNKNOWN")
        token_lines.append(f"  [PTR_{ptr_index}] [{operator}]" + (
            f" [T{table_ids.get(rel.get('table', ['',''])[-1], -1)}]"
            if operator == "LogicalTableScan"
            else (f" [{str(rel.get('joinType', '')).upper()}]" if operator == "LogicalJoin" else "")
        ))

        if operator == "LogicalJoin":
            condition_lines = _render_expression_lines(rel.get("condition", {}), "    ")
            if condition_lines:
                condition_lines[-1] = condition_lines[-1].rstrip()
                head = condition_lines[0].lstrip()
                token_lines.append(f"    [CONDITION] {head}")
                token_lines.extend(condition_lines[1:])
            input_tokens = _rel_inputs(rel, ptr_ids)
            token_lines.append(f"    [INPUT] {input_tokens}")
            continue

        if operator == "LogicalFilter":
            condition_lines = _render_expression_lines(rel.get("condition", {}), "    ")
            if condition_lines:
                condition_lines[-1] = condition_lines[-1].rstrip()
                head = condition_lines[0].lstrip()
                token_lines.append(f"    [CONDITION] {head}")
                token_lines.extend(condition_lines[1:])
            input_tokens = _rel_inputs(rel, ptr_ids, ptr_index - 1)
            token_lines.append(f"    [INPUT] {input_tokens}")
            continue

        if operator == "LogicalProject":
            token_lines.append(f"    [COL] {_render_project_cols(rel)}")
            input_tokens = _rel_inputs(rel, ptr_ids, ptr_index - 1)
            token_lines.append(f"    [INPUT] {input_tokens}")
            continue

        if operator == "LogicalAggregate":
            token_lines.extend(_render_aggregate_lines(rel, "    "))
            input_tokens = _rel_inputs(rel, ptr_ids, ptr_index - 1)
            token_lines.append(f"    [INPUT] {input_tokens}")

    token_lines.append("[/LOGICAL_PLAN]")
    return "\n".join(token_lines)


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

        row = {
            "schema": schema_name,
            "query_key": query_key,
            "query_sql": query_path.read_text(),
            "calcite_plan": plan.text_plan,
            "logical_tokenized_plan": _build_logical_token(plan.json_plan, table_ids),
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
