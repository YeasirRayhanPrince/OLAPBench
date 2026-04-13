#!/usr/bin/env python3
"""Augment a base training JSONL with physical-plan tokens from a patch JSONL.

The three dict fields keyed by "dbms@version" in each training record:
    ir_physical_token
    ir_physical_plan_token
    ir_physical_plan_cardinalities

are extended with the entries from the patch JSONL. Records are matched by
sql_file_name. Records in the base with no matching patch entry are written
unchanged. Records in the patch with no matching base entry are skipped with
a warning.

Usage:
    python gendba_pipeline/augment_jsonl.py \\
        --base  <base.training.jsonl> \\
        --patch <duckdb.training.jsonl> \\
        --out   <augmented.training.jsonl>
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

try:
    import simplejson as json
except ImportError:
    import json

MERGE_FIELDS = [
    "ir_physical_token",
    "ir_physical_plan_token",
    "ir_physical_plan_cardinalities",
]


def _read_jsonl(path: Path) -> list[dict]:
    records = []
    with path.open() as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError as e:
                print(f"[augment] WARNING: skipping malformed line {lineno} in {path}: {e}", file=sys.stderr)
    return records


def _write_jsonl(path: Path, records: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")


def augment(base_path: Path, patch_path: Path, out_path: Path) -> None:
    print(f"[augment] Loading base:  {base_path}")
    base_records = _read_jsonl(base_path)
    print(f"[augment] Loading patch: {patch_path}")
    patch_records = _read_jsonl(patch_path)

    # Index base by sql_file_name for O(1) lookup
    base_index: dict[str, dict] = {}
    for rec in base_records:
        key = rec.get("sql_file_name")
        if key is None:
            continue
        base_index[key] = rec

    matched = 0
    skipped = 0
    for patch_rec in patch_records:
        key = patch_rec.get("sql_file_name")
        if key is None:
            print(f"[augment] WARNING: patch record missing sql_file_name, skipping", file=sys.stderr)
            skipped += 1
            continue

        base_rec = base_index.get(key)
        if base_rec is None:
            print(f"[augment] WARNING: no base record for '{key}', skipping", file=sys.stderr)
            skipped += 1
            continue

        for field in MERGE_FIELDS:
            patch_val = patch_rec.get(field)
            if not patch_val:
                continue
            base_val = base_rec.get(field)
            if isinstance(base_val, dict):
                base_val.update(patch_val)
            else:
                # Field missing from base — initialize from patch
                base_rec[field] = dict(patch_val)

        matched += 1

    print(f"[augment] Merged {matched} records, skipped {skipped}")
    print(f"[augment] Writing output: {out_path}")
    _write_jsonl(out_path, base_records)
    print(f"[augment] Done — {len(base_records)} records written")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base",  required=True, type=Path, help="Base training JSONL (e.g. postgres)")
    parser.add_argument("--patch", required=True, type=Path, help="Patch training JSONL (e.g. duckdb)")
    parser.add_argument("--out",   required=True, type=Path, help="Output path for the merged JSONL")
    args = parser.parse_args()

    for p in (args.base, args.patch):
        if not p.exists():
            print(f"[augment] ERROR: file not found: {p}", file=sys.stderr)
            sys.exit(1)

    augment(args.base, args.patch, args.out)


if __name__ == "__main__":
    main()
