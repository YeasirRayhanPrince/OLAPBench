#!/usr/bin/env python3
import argparse
import csv
from pathlib import Path
from typing import Any

try:
    import simplejson as json
except ImportError:
    import json


def _loads(value: str) -> Any:
    try:
        return json.loads(value, allow_nan=True)
    except TypeError:
        return json.loads(value)


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
        # In encoded plans this field is often JSON-stringified twice.
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


def export_training_rows(input_csv: Path, output_jsonl: Path, only_success: bool):
    count_in = 0
    count_out = 0

    with input_csv.open("r", newline="") as in_file, output_jsonl.open("w") as out_file:
        reader = csv.DictReader(in_file)
        for row in reader:
            count_in += 1
            if only_success and row.get("state") != "success":
                continue

            plan_str = row.get("plan", "")
            if not plan_str:
                continue

            try:
                plan_doc = _loads(plan_str)
            except Exception:
                continue

            plan_root = plan_doc.get("queryPlan")
            if not isinstance(plan_root, dict):
                continue

            operators: list[dict[str, Any]] = []
            _flatten_plan(plan_root, operators)

            record = {
                "query_key": row.get("query"),
                "title": row.get("title"),
                "dbms": row.get("dbms"),
                "version": row.get("version"),
                "query_text": plan_doc.get("queryText"),
                "plan": plan_doc,
                "operators": operators,
            }

            try:
                line = json.dumps(record, allow_nan=True)
            except TypeError:
                line = json.dumps(record)
            out_file.write(line)
            out_file.write("\n")
            count_out += 1

    print(f"Read {count_in} CSV rows from {input_csv}")
    print(f"Wrote {count_out} training records to {output_jsonl}")


def main():
    parser = argparse.ArgumentParser(
        description="Export OLAPBench results CSV to GenDBA JSONL training format."
    )
    parser.add_argument("--input-csv", required=True, help="Path to benchmark result CSV")
    parser.add_argument("--output-jsonl", required=True, help="Path to output JSONL file")
    parser.add_argument(
        "--include-non-success",
        action="store_true",
        help="Include rows whose state is not success",
    )
    args = parser.parse_args()

    input_csv = Path(args.input_csv)
    output_jsonl = Path(args.output_jsonl)

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV does not exist: {input_csv}")

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    export_training_rows(input_csv, output_jsonl, only_success=not args.include_non_success)


if __name__ == "__main__":
    main()
