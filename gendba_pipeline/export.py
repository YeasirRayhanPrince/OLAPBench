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


def _read_text(path_str: str | None) -> str | None:
    if not path_str:
        return None
    path = Path(path_str)
    if not path.exists():
        return None
    return path.read_text().rstrip("\n")


def _read_json(path_str: str | None) -> Any:
    if not path_str:
        return None
    path = Path(path_str)
    if not path.exists():
        return None
    return _loads(path.read_text())


def export_training_manifest(run_manifest: Path, output_jsonl: Path, only_success: bool):
    manifest = _loads(run_manifest.read_text())
    targets = manifest.get("targets", [])
    count_out = 0
    count_in = 0

    with output_jsonl.open("w") as out_file:
        for target in targets:
            target_manifest_path = target.get("target_manifest_path")
            if not target_manifest_path:
                continue

            target_manifest = _read_json(target_manifest_path)
            if not isinstance(target_manifest, dict):
                continue

            for record in target_manifest.get("records", []):
                count_in += 1
                if only_success and record.get("state") != "success":
                    continue

                plan = _read_json(record.get("normalized_plan_path"))
                operators = _read_json(record.get("operators_path")) or []
                query_text = _read_text(record.get("query_sql_path"))
                physical_tokenized_plan = _read_text(record.get("tokenized_plan_path"))
                logical_plan = _read_text(record.get("logical_plan_path"))
                logical_tokenized_plan = _read_text(record.get("logical_tokenized_plan_path"))

                export_record = {
                    "dataset_id": manifest.get("dataset_id"),
                    "run_id": manifest.get("run_id"),
                    "target_id": target.get("target_id"),
                    "schema": record.get("schema_id"),
                    "workload": record.get("workload_id"),
                    "query_set": record.get("query_set_id"),
                    "query_key": record.get("query_id"),
                    "title": record.get("title"),
                    "dbms": record.get("dbms"),
                    "version": record.get("version"),
                    "state": record.get("state"),
                    "query_text": query_text,
                    "plan": plan,
                    "tokenized_plan": physical_tokenized_plan,
                    "physical_tokenized_plan": physical_tokenized_plan,
                    "logical_plan": logical_plan,
                    "logical_tokenized_plan": logical_tokenized_plan,
                    "operators": operators,
                    "metrics": record.get("metrics", {}),
                    "artifacts": {
                        "query_sql_path": record.get("query_sql_path"),
                        "raw_row_path": record.get("raw_row_path"),
                        "normalized_plan_path": record.get("normalized_plan_path"),
                        "operators_path": record.get("operators_path"),
                        "tokenized_plan_path": record.get("tokenized_plan_path"),
                        "logical_plan_path": record.get("logical_plan_path"),
                        "logical_tokenized_plan_path": record.get("logical_tokenized_plan_path"),
                    },
                }

                try:
                    line = json.dumps(export_record, allow_nan=True)
                except TypeError:
                    line = json.dumps(export_record)
                out_file.write(line)
                out_file.write("\n")
                count_out += 1

    print(f"Read {count_in} manifest-backed records from {run_manifest}")
    print(f"Wrote {count_out} training records to {output_jsonl}")


def main():
    parser = argparse.ArgumentParser(
        description="Export OLAPBench results CSV to GenDBA JSONL training format."
    )
    parser.add_argument("--input-csv", help="Path to benchmark result CSV")
    parser.add_argument(
        "--run-manifest",
        help="Path to a dataset pipeline run manifest.",
    )
    parser.add_argument("--output-jsonl", required=True, help="Path to output JSONL file")
    parser.add_argument(
        "--include-non-success",
        action="store_true",
        help="Include rows whose state is not success",
    )
    args = parser.parse_args()

    output_jsonl = Path(args.output_jsonl)

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)

    has_csv = args.input_csv is not None
    has_manifest = args.run_manifest is not None
    if has_csv == has_manifest:
        raise ValueError("Specify exactly one of --input-csv or --run-manifest")

    if has_csv:
        input_csv = Path(args.input_csv)
        if not input_csv.exists():
            raise FileNotFoundError(f"Input CSV does not exist: {input_csv}")
        export_training_rows(input_csv, output_jsonl, only_success=not args.include_non_success)
        return

    run_manifest = Path(args.run_manifest)
    if not run_manifest.exists():
        raise FileNotFoundError(f"Run manifest does not exist: {run_manifest}")
    export_training_manifest(run_manifest, output_jsonl, only_success=not args.include_non_success)


if __name__ == "__main__":
    main()
