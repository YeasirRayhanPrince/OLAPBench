"""
Patch ir_logical_token in an existing training JSONL using the manifest's
plan_json_path artifacts. Re-generates only ir_logical_token; all other
fields are preserved unchanged.

Usage:
    python patch_ir_logical_token.py \
        --training  output/.../foo.training.jsonl \
        --manifest  output/.../foo.manifest.jsonl \
        --schema    benchmarks/gendba_pool/job.dbschema.json \
        --output    output/.../foo.training.jsonl   # can overwrite in-place
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from gendba_pipeline.build_calcite_training_jsonl import (
    _build_logical_token,
    _load_schema_table_ids,
)
from gendba_pipeline.calcite_plangen_py import normalize_plan_inputs


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--training", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    _, table_ids = _load_schema_table_ids(Path(args.schema))

    # Build id -> plan_json_path from manifest
    plan_paths: dict[int, str] = {}
    with open(args.manifest) as f:
        for line in f:
            rec = json.loads(line)
            rec_id = rec["id"]
            path = rec.get("logical_source", {}).get("plan_json_path")
            if path:
                plan_paths[rec_id] = path

    patched = 0
    skipped = 0
    with open(args.training) as fin, open(args.output, "w") as fout:
        for line in fin:
            rec = json.loads(line)
            rec_id = rec["id"]
            plan_json_path = plan_paths.get(rec_id)

            if not plan_json_path or not Path(plan_json_path).exists():
                skipped += 1
                fout.write(line)
                continue

            plan_json = normalize_plan_inputs(
                json.loads(Path(plan_json_path).read_text())
            )
            new_token, _ = _build_logical_token(plan_json, table_ids)
            rec["ir_logical_token"] = new_token
            fout.write(json.dumps(rec, ensure_ascii=False) + "\n")
            patched += 1

    print(f"Patched: {patched}  Skipped: {skipped}")


if __name__ == "__main__":
    main()
