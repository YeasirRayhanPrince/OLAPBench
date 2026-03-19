#!/usr/bin/env python3
"""
Inspect logical and physical tokens from a gendba training JSONL file.

Usage:
    python gendba_pipeline/inspect_tokens.py \
        --jsonl path/to/gendba_pool.training.jsonl \
        --queries exists.sql nested_047_kind_type_has_title_episode_of_id_tail_in.sql
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import simplejson as json
except ImportError:
    import json


# ── physical plan pretty-printer ─────────────────────────────────────────────

def _render_plan_node(node: dict, indent: int = 0) -> list[str]:
    pad = "  " * indent
    attrs = node.get("_attrs", {})
    label = node.get("_label", "?")

    # Pick the most informative name from attrs
    name_parts = []
    for key in ("name", "method", "table_name", "type"):
        v = attrs.get(key)
        if v:
            name_parts.append(str(v))

    est = attrs.get("estimated_cardinality", "?")
    exact = attrs.get("exact_cardinality", "?")
    op_id = attrs.get("operator_id", "")
    id_str = f"[op={op_id}] " if op_id is not None and op_id != "" else ""

    detail = f"  ({', '.join(name_parts)})" if name_parts else ""
    lines = [f"{pad}{id_str}[{label}]{detail}  est={est}  actual={exact}"]

    for child in node.get("_children", []):
        lines.extend(_render_plan_node(child, indent + 1))
    return lines


def _render_physical(physical: dict) -> str:
    blocks: list[str] = []
    for engine, payload in physical.items():
        blocks.append(f"  Engine: {engine}")

        # operator list
        operators = payload.get("operators", [])
        if operators:
            blocks.append("  Operators:")
            for op in operators:
                otype = op.get("operator_type", "?")
                oid   = op.get("operator_id", "?")
                loops = op.get("actual_loops", "?")
                rows  = op.get("actual_rows_total", "?")
                blocks.append(f"    op={str(oid):>3}  {str(otype):<20}  loops={loops}  total_rows={rows}")

        # plan tree
        plan = payload.get("plan", {})
        root = plan.get("queryPlan") or plan.get("query_plan")
        if root:
            blocks.append("  Plan tree:")
            for line in _render_plan_node(root, indent=2):
                blocks.append(line)

    return "\n".join(blocks)


# ── main ─────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Pretty-print logical/physical tokens from a training JSONL.")
    parser.add_argument("--jsonl", required=True, help="Path to the .training.jsonl file.")
    parser.add_argument(
        "--queries", nargs="+", required=True, metavar="SQL_FILE",
        help="One or more sql_file_name values to display (e.g. exists.sql 1a.sql).",
    )
    parser.add_argument(
        "--no-physical", action="store_true",
        help="Skip printing the physical token.",
    )
    parser.add_argument(
        "--no-logical", action="store_true",
        help="Skip printing the logical token.",
    )
    args = parser.parse_args()

    jsonl_path = Path(args.jsonl)
    if not jsonl_path.exists():
        sys.exit(f"File not found: {jsonl_path}")

    # Accept both "foo" and "foo.sql" — normalise to stem only.
    wanted = {q.removesuffix(".sql") for q in args.queries}
    found: set[str] = set()

    with jsonl_path.open() as fh:
        for raw_line in fh:
            raw_line = raw_line.strip()
            if not raw_line:
                continue
            row = json.loads(raw_line)
            key = row.get("sql_file_name", "")
            stem = key.removesuffix(".sql")
            if stem not in wanted:
                continue

            found.add(stem)
            sep = "=" * 72
            print(sep)
            print(f"  Query : {key}  (id={row.get('id', '?')})")
            print(f"  Schema: {row.get('schema', '?')}")
            print(sep)
            print()
            print("SQL:")
            print("  " + row.get("sql", "").replace("\n", "\n  ").rstrip())
            print()

            if not args.no_logical:
                print("LOGICAL TOKEN:")
                for line in row.get("ir_logical_token", "").splitlines():
                    print("  " + line)
                print()

            if not args.no_physical:
                physical = row.get("ir_physical_token", {})
                if physical:
                    print("PHYSICAL TOKEN:")
                    print(_render_physical(physical))
                    print()

                tokenized = row.get("ir_physical_plan_token", {})
                if tokenized:
                    print("TOKENIZED PHYSICAL PLAN:")
                    for engine, token_str in tokenized.items():
                        print(f"  Engine: {engine}")
                        for line in token_str.splitlines():
                            print(f"    {line}")
                    print()

    missing = wanted - found  # both sets use stems
    if missing:
        print(f"Warning: no records found for: {', '.join(sorted(missing))}", file=sys.stderr)


if __name__ == "__main__":
    main()
