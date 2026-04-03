"""Analyze ghost nodes in physical plan tokenization.

A ghost node is a logical IR PTR that had no corresponding PG operator and
was auto-inserted by `_insert_missing_ptrs` (i.e., PTRs in
`ir_nodes.keys() - used_ir_ptrs` after `_traverse`).
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

# ---------------------------------------------------------------------------
# Add the project root to sys.path so we can import the tokenizer
# ---------------------------------------------------------------------------
PROJECT_ROOT = Path("/scratch1/yrayhan/OLAPBench")
sys.path.insert(0, str(PROJECT_ROOT))

from gendba_pipeline.phys_plan.tokenize_physical_plan import (
    _LOGICAL_TO_PHYS_DEFAULT,
    _build_ir_table_coverage,
    _build_matching_maps,
    _insert_missing_ptrs,
    _traverse,
    _TraversalCtx,
    _assign_predicates,
    load_table_name_to_id,
    parse_ir_nodes,
)

# ---------------------------------------------------------------------------
# Instrumented tokenize_physical_plan — returns ghost info alongside result
# ---------------------------------------------------------------------------

def tokenize_with_ghost_info(
    ir_logical_token: str,
    pg_physical: dict,
    table_name_to_id: dict[str, int],
    pred_registry=None,
):
    """
    Like tokenize_physical_plan but also returns:
        ghost_ptrs : list of (ptr, logical_op, phys_op)
        entries    : final list[Entry] after _insert_missing_ptrs
    Returns (token_string, cardinalities, ghost_info, entries) or None on failure.
    """
    try:
        tree = pg_physical.get("plan", {}).get("queryPlan")
        if tree is None:
            return None

        ir_nodes = parse_ir_nodes(ir_logical_token)
        if not ir_nodes:
            return None

        op_card: dict[int | None, float] = {}
        for op in pg_physical.get("operators", []):
            op_id = op.get("operator_id")
            if op_id not in op_card:
                op_card[op_id] = float(op.get("actual_rows_total", 0.0))

        scan_map, join_map, unary_map, subquery_filters = _build_matching_maps(ir_nodes)
        ir_table_coverage = _build_ir_table_coverage(ir_nodes)

        ptr_costs: dict[int, int] = {}
        used_ir_ptrs: set[int] = set()
        ctx = _TraversalCtx()

        entries, _, _ = _traverse(
            tree, op_card, ptr_costs, used_ir_ptrs,
            ir_nodes, scan_map, join_map, unary_map,
            table_name_to_id, subquery_filters, ctx,
            ir_table_coverage,
        )

        # ------------------------------------------------------------------
        # Ghost detection: PTRs in ir_nodes.keys() - used_ir_ptrs
        # ------------------------------------------------------------------
        ghost_set = set(ir_nodes.keys()) - used_ir_ptrs
        ghost_info: list[tuple[int, str, str]] = []  # (ptr, logical_op, phys_op)
        for gptr in sorted(ghost_set):
            node = ir_nodes[gptr]
            logical_op = node.op
            if logical_op == "LogicalTableScan":
                phys_op = "SeqScan"
            elif logical_op == "LogicalJoin":
                phys_op = "HashJoin"
            else:
                phys_op = _LOGICAL_TO_PHYS_DEFAULT.get(logical_op, "Filter")
            ghost_info.append((gptr, logical_op, phys_op))

        if ghost_set:
            entries = _insert_missing_ptrs(entries, ghost_set, ir_nodes, ptr_costs)

        if not entries:
            return None

        pred_annotations: dict = {}
        if pred_registry:
            pred_annotations = _assign_predicates(entries, pred_registry, ctx, table_name_to_id)

        cardinalities = [ptr_costs.get(e[0], 1) for e in entries]

        lines = ["[PHYSICAL_PLAN]"]
        for ir_ptr, phys_op, join_type, child_ptrs in entries:
            tok = f"  [PTR_{ir_ptr}] {phys_op}"
            if join_type:
                tok += f" {join_type}"
            for role, preds in sorted(pred_annotations.get(ir_ptr, {}).items()):
                tok += f" [{role} {' '.join(f'P_{p}' for p in preds)}]"
            lines.append(tok)
            if child_ptrs is not None:
                lines.append(f"    [INPUT] {' '.join(child_ptrs)}")
        lines.append("[/PHYSICAL_PLAN]")

        return ("\n".join(lines), cardinalities, ghost_info, entries)

    except Exception as e:
        return None


# ---------------------------------------------------------------------------
# Main analysis
# ---------------------------------------------------------------------------

VALIDATION_JSONL = Path(
    "/scratch1/yrayhan/OLAPBench/gendba_pipeline/output/"
    "gendba_local_postgres_preloaded/"
    "gendba_local_postgres_preloaded_job.validation.jsonl"
)

SCHEMA_PATH = Path("/scratch1/yrayhan/OLAPBench/benchmarks/job/job.dbschema.json")


def find_schema_json() -> dict[str, int]:
    """Load table_name_to_id from the JOB benchmark schema."""
    candidates = [
        SCHEMA_PATH,
        PROJECT_ROOT / "benchmarks" / "job" / "job.dbschema.json",
        PROJECT_ROOT / "benchmarks" / "gendba_pool" / "job.dbschema.json",
    ]
    for candidate in candidates:
        if candidate.exists():
            return load_table_name_to_id(candidate)
    print("WARNING: could not locate schema JSON; table names won't be resolved.")
    return {}


def main():
    # -----------------------------------------------------------------------
    # Load schema
    # -----------------------------------------------------------------------
    try:
        table_name_to_id = find_schema_json()
        print(f"Schema loaded: {len(table_name_to_id)} tables")
    except Exception as e:
        print(f"Schema load error: {e}")
        table_name_to_id = {}

    # -----------------------------------------------------------------------
    # Iterate over validation JSONL
    # -----------------------------------------------------------------------
    ENGINE_KEY = "postgres@12.5"

    total_rows = 0
    rows_with_ghost = 0
    ghost_op_counts: dict[str, int] = defaultdict(int)   # logical_op → count
    ghost_phys_counts: dict[str, int] = defaultdict(int) # phys_op assigned → count
    ghost_position_counts = {"end_only": 0, "interleaved": 0}  # position pattern

    # Collect examples for display
    examples: list[dict] = []  # dicts with row index, ghost_info, token_str

    with open(VALIDATION_JSONL) as f:
        for row_idx, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            total_rows += 1

            row = json.loads(line)
            ir_logical = row.get("ir_logical_token", "")
            phys_map = row.get("ir_physical_token", {})
            raw_phys = phys_map.get(ENGINE_KEY)
            pred_reg = row.get("pred_registry")

            if not (raw_phys and ir_logical):
                continue

            result = tokenize_with_ghost_info(
                ir_logical, raw_phys, table_name_to_id, pred_registry=pred_reg
            )
            if result is None:
                continue

            tok_str, cardinalities, ghost_info, entries = result

            if not ghost_info:
                continue

            rows_with_ghost += 1

            # Count per logical op type
            for gptr, logical_op, phys_op in ghost_info:
                ghost_op_counts[logical_op] += 1
                ghost_phys_counts[phys_op] += 1

            # Determine position: are all ghost ptrs at the END of entries list?
            ghost_ptrs_set = {g[0] for g in ghost_info}
            entry_ptrs = [e[0] for e in entries]

            # Find the index of the first ghost entry
            first_ghost_idx = next(
                (i for i, ptr in enumerate(entry_ptrs) if ptr in ghost_ptrs_set),
                len(entry_ptrs),
            )
            # Check if any non-ghost entry follows the first ghost entry
            non_ghost_after_first = any(
                ptr not in ghost_ptrs_set
                for ptr in entry_ptrs[first_ghost_idx:]
            )
            if non_ghost_after_first:
                ghost_position_counts["interleaved"] += 1
            else:
                ghost_position_counts["end_only"] += 1

            # Collect up to 5 examples
            if len(examples) < 5:
                examples.append({
                    "row_idx": row_idx,
                    "ghost_info": ghost_info,
                    "token_str": tok_str,
                    "query_id": row.get("query_id", row.get("id", f"row_{row_idx}")),
                })

    # -----------------------------------------------------------------------
    # Print summary
    # -----------------------------------------------------------------------
    print()
    print("=" * 70)
    print("GHOST NODE ANALYSIS SUMMARY")
    print("=" * 70)
    print(f"Total rows processed          : {total_rows}")
    print(f"Rows with ≥1 ghost node       : {rows_with_ghost} "
          f"({rows_with_ghost / total_rows * 100:.1f}%)" if total_rows else "")

    total_ghosts = sum(ghost_op_counts.values())
    print(f"Total ghost node occurrences  : {total_ghosts}")
    print()

    print("Ghost nodes by logical operator type:")
    print(f"  {'Logical Op':<30} {'Count':>8}  {'%':>6}")
    print(f"  {'-'*30}  {'-'*8}  {'-'*6}")
    for op, cnt in sorted(ghost_op_counts.items(), key=lambda x: -x[1]):
        pct = cnt / total_ghosts * 100 if total_ghosts else 0
        print(f"  {op:<30} {cnt:>8}  {pct:>5.1f}%")

    print()
    print("Ghost nodes by assigned physical operator:")
    print(f"  {'Phys Op':<25} {'Count':>8}  {'%':>6}")
    print(f"  {'-'*25}  {'-'*8}  {'-'*6}")
    for op, cnt in sorted(ghost_phys_counts.items(), key=lambda x: -x[1]):
        pct = cnt / total_ghosts * 100 if total_ghosts else 0
        print(f"  {op:<25} {cnt:>8}  {pct:>5.1f}%")

    print()
    print("Ghost node position (in entries list):")
    print(f"  Appended at end only    : {ghost_position_counts['end_only']}")
    print(f"  Interleaved (not at end): {ghost_position_counts['interleaved']}")

    print()
    print("=" * 70)
    print(f"EXAMPLE ROWS WITH GHOST NODES  ({len(examples)} shown)")
    print("=" * 70)
    for ex in examples:
        print(f"\n--- Row {ex['row_idx']}  query_id={ex['query_id']} ---")
        print(f"Ghost PTRs: {ex['ghost_info']}")
        print("Physical plan token:")
        # Print token but truncate if too long
        lines = ex["token_str"].splitlines()
        if len(lines) > 40:
            for l in lines[:20]:
                print("  " + l)
            print(f"  ... ({len(lines) - 25} lines omitted) ...")
            for l in lines[-5:]:
                print("  " + l)
        else:
            for l in lines:
                print("  " + l)


if __name__ == "__main__":
    main()
