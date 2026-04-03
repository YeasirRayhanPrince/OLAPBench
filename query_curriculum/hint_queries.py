#!/usr/bin/env python3
"""
Generate hint-annotated SQL variants for existing query files using PostBOUND
join order enumeration.

For each .sql file in --queries-dir this script:
  1. Parses the SQL with PostBOUND
  2. Enumerates join orders (bushy/left-deep/right-deep)
  3. Builds a pg_hint_plan Leading() comment for each join order
  4. Writes one .sql file per variant to --output-dir

Output files are named  {original_stem}_h{N:04d}.sql
  - _h0000.sql is always the unmodified original SQL (default plan, no hint)
  - _h0001.sql … _hNNNN.sql carry a /*+ Leading(…) */ comment

Requires Python 3.12+ (for PostBOUND). Run via hint_queries.sh which sets the
correct interpreter and PYTHONPATH.
"""
from __future__ import annotations

import argparse
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

try:
    from postbound.opt.enumeration import ExhaustiveJoinOrderEnumerator
    from postbound.parser import parse_query
    from postbound.postgres import _walk_join_order
except ImportError as exc:
    print(
        f"ERROR: PostBOUND not importable ({exc})\n"
        "Set PYTHONPATH to include the PostBOUND directory, e.g.:\n"
        "  PYTHONPATH=PostBOUND python query_curriculum/hint_queries.py ...\n"
        "Or use the provided hint_queries.sh wrapper.",
        file=sys.stderr,
    )
    sys.exit(1)


# ---------------------------------------------------------------------------
# Core logic
# ---------------------------------------------------------------------------

def _leading_hint(join_order) -> str:
    return f"/*+ Leading({_walk_join_order(join_order)}) */"


def enumerate_hints(sql: str, max_plans: int, tree_structure: str) -> list[str]:
    """Return hint comment strings for all enumerable join orders.

    The first element is always "" (no hint = default plan).
    """
    hints: list[str] = [""]  # slot 0: no hint → default plan

    try:
        parsed = parse_query(sql)
    except Exception:
        return hints  # unparseable SQL: only default plan

    if len(parsed.tables()) < 2:
        return hints  # single-table scan: nothing to reorder

    enumerator = ExhaustiveJoinOrderEnumerator(tree_structure)
    seen: set[str] = set()
    try:
        for join_order in enumerator.all_join_orders_for(parsed):
            if len(hints) > max_plans:
                break
            try:
                hint = _leading_hint(join_order)
            except Exception:
                continue
            if hint not in seen:
                seen.add(hint)
                hints.append(hint)
    except ValueError:
        # cross-product query: enumeration not supported, keep default only
        pass

    return hints


def _process_one(args: tuple) -> tuple[str, int]:
    """Worker: process a single SQL file. Returns (stem, variant_count)."""
    sql_file, output_dir, max_plans, tree_structure = args
    sql_file = Path(sql_file)
    output_dir = Path(output_dir)
    sql = sql_file.read_text()
    stem = sql_file.stem
    hints = enumerate_hints(sql, max_plans, tree_structure)
    for i, hint in enumerate(hints):
        content = f"{hint}\n{sql}" if hint else sql
        (output_dir / f"{stem}_h{i:04d}.sql").write_text(content)
    return stem, len(hints)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Write pg_hint_plan hinted SQL variants for join order enumeration"
    )
    ap.add_argument("--queries-dir", required=True, type=Path,
                    help="Directory of input .sql files")
    ap.add_argument("--output-dir", required=True, type=Path,
                    help="Directory to write hinted .sql variants")
    ap.add_argument("--max-plans", type=int, default=50,
                    help="Max hint variants per query, excluding the default (default: 50)")
    ap.add_argument("--tree-structure",
                    choices=["bushy", "left-deep", "right-deep"], default="bushy",
                    help="Join tree structure to enumerate (default: bushy)")
    ap.add_argument("--workers", type=int, default=1,
                    help="Parallel worker processes (default: 1)")
    args = ap.parse_args()

    queries_dir: Path = args.queries_dir
    output_dir: Path = args.output_dir

    if not queries_dir.exists():
        print(f"ERROR: --queries-dir not found: {queries_dir}", file=sys.stderr)
        sys.exit(1)

    sql_files = sorted(queries_dir.glob("*.sql"))
    if not sql_files:
        print(f"No .sql files in {queries_dir}", file=sys.stderr)
        sys.exit(1)

    output_dir.mkdir(parents=True, exist_ok=True)

    work_args = [
        (str(f), str(output_dir), args.max_plans, args.tree_structure)
        for f in sql_files
    ]

    total_variants = 0
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as pool:
            futures = {pool.submit(_process_one, a): a[0] for a in work_args}
            for fut in as_completed(futures):
                stem, n = fut.result()
                print(f"  {stem}: {n} variants")
                total_variants += n
    else:
        for a in work_args:
            stem, n = _process_one(a)
            print(f"  {stem}: {n} variants")
            total_variants += n

    print(f"\n{len(sql_files)} queries → {total_variants} hinted variants")
    print(f"Output: {output_dir}")


if __name__ == "__main__":
    main()
