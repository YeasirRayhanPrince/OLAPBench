"""Window functions curriculum generator.

Generates queries with ROW_NUMBER(), RANK(), SUM() OVER(...), LAG(), LEAD()
to exercise WindowAgg physical operators.
"""
from __future__ import annotations

import random
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from query_curriculum.core import (
    ColumnSchema,
    ColumnStats,
    GeneratorConfig,
    JoinEdge,
    QueryArtifact,
    StatsSnapshot,
    TableSchema,
    alias_map,
    bucket_for_selectivity,
    normalize_sql,
    sql_literal,
)
from query_curriculum.spj import (
    PredicateSpec,
    SpjBuildContext,
    average_selectivity,
    cached_projection_options,
    make_build_context,
    render_predicate,
    top_seed_pool,
    _hashable_value,
)
from query_curriculum.agg import _good_group_by_columns


WINDOW_FUNCS = [
    ("ROW_NUMBER", False),  # (func_name, needs_column)
    ("RANK", False),
    ("DENSE_RANK", False),
    ("SUM", True),
    ("AVG", True),
    ("COUNT", True),
    ("LAG", True),
    ("LEAD", True),
]


@dataclass
class WindowCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    window_func: str
    window_column: str | None  # column the function operates on (None for ROW_NUMBER etc.)
    partition_by: list[tuple[str, str, str]]  # (table, alias, column)
    order_by_col: tuple[str, str, str]  # (table, alias, column) for OVER(ORDER BY ...)
    order_direction: str
    predicates: list[PredicateSpec]
    projections: list[tuple[str, str, str]]
    projection_width: str
    predicate_families: list[str]
    target_selectivity_bucket: str
    estimated_selectivity: float | None
    rule_family: str
    limit: int | None = None
    calibration_source: str = "schema"
    calibrated: bool = False
    observed_rows: int | None = None
    observed_selectivity: float | None = None
    filename: str | None = None
    query_id: str | None = None
    _cached_signature: tuple[Any, ...] | None = field(default=None, init=False, repr=False, compare=False)

    def structural_signature(self) -> tuple[Any, ...]:
        if self._cached_signature is not None:
            return self._cached_signature
        sig = (
            "window",
            self.stage_id,
            self.base_table,
            self.window_func,
            self.window_column,
            tuple(self.partition_by),
            self.order_by_col,
            self.order_direction,
            self.rule_family,
            self.limit,
            tuple(
                (p.table, p.column, p.operator, _hashable_value(p.value), p.family)
                for p in self.predicates
            ),
            tuple(self.projections),
        )
        self._cached_signature = sig
        return sig

    def signature(self) -> str:
        return repr(self.structural_signature())

    def render_sql(self) -> str:
        parts: list[str] = []

        # Build window function expression
        _, ob_alias, ob_col = self.order_by_col
        order_clause = f"ORDER BY {ob_alias}.{ob_col} {self.order_direction}"

        partition_clause = ""
        if self.partition_by:
            part_cols = ", ".join(f"{a}.{c}" for _, a, c in self.partition_by)
            partition_clause = f"PARTITION BY {part_cols} "

        if self.window_column:
            # find alias for the column
            alias = self.projections[0][1] if self.projections else self.base_table[0]
            # For LAG/LEAD, use column directly
            if self.window_func in ("LAG", "LEAD"):
                win_expr = f"{self.window_func}({alias}.{self.window_column}) OVER({partition_clause}{order_clause})"
            else:
                win_expr = f"{self.window_func}({alias}.{self.window_column}) OVER({partition_clause}{order_clause})"
        else:
            win_expr = f"{self.window_func}() OVER({partition_clause}{order_clause})"

        # SELECT
        select_items = [f"{a}.{c}" for _, a, c in self.projections] if self.projections else ["*"]
        select_items.append(f"{win_expr} AS win_val")
        parts.append(f"SELECT {', '.join(select_items)}")

        # FROM
        aliases = alias_map([self.base_table])
        parts.append(f"FROM {self.base_table} AS {aliases[self.base_table]}")

        # WHERE
        where_preds = [render_predicate(p) for p in self.predicates if p.placement == "where"]
        if where_preds:
            parts.append("WHERE " + "\n  AND ".join(where_preds))

        # LIMIT
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")

        parts.append(";")
        return "\n".join(parts) + "\n"

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "window",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "window_func": self.window_func,
            "window_column": self.window_column,
            "partition_by": [{"table": t, "column": c} for t, _, c in self.partition_by],
            "order_by_col": {"table": self.order_by_col[0], "column": self.order_by_col[2]},
            "order_direction": self.order_direction,
            "rule_family": self.rule_family,
            "limit": self.limit,
            "target_selectivity_bucket": self.target_selectivity_bucket,
            "calibrated": self.calibrated,
            "calibration_source": self.calibration_source,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "projection_columns": [{"table": t, "column": c} for t, _, c in self.projections],
            "predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator}
                for p in self.predicates
            ],
        }


# ---------------------------------------------------------------------------
# Builder
# ---------------------------------------------------------------------------

def _build_window_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[WindowCandidate]:
    candidates: list[WindowCandidate] = []

    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        table_alias = aliases[table_name]
        col_stats = snapshot.column_stats.get(table_name, {})
        table_count = snapshot.table_counts.get(table_name, 1000)

        seeds = top_seed_pool(table, table_alias, snapshot, limit=3, build_context=build_context)
        proj_options = cached_projection_options([table_name], aliases, catalog, build_context)

        # Find partition-by columns (moderate n_distinct)
        partition_cols = _good_group_by_columns(table, col_stats, table_count)

        # Find order-by columns (prefer numeric with good spread)
        order_cols: list[ColumnSchema] = []
        for col in table.columns.values():
            stats = col_stats.get(col.name)
            if stats and stats.histogram_bounds and len(stats.histogram_bounds) >= 4:
                order_cols.append(col)
        if not order_cols:
            order_cols = [col for col in table.columns.values() if col.is_numeric][:2]
        if not order_cols:
            order_cols = list(table.columns.values())[:1]

        # Find numeric columns for aggregating window functions
        numeric_cols = [col for col in table.columns.values() if col.is_numeric and not col.is_primary_key]

        for func_name, needs_col in WINDOW_FUNCS:
            if needs_col and not numeric_cols:
                continue
            win_col = numeric_cols[0].name if needs_col else None

            for ob_col in order_cols[:2]:
                ob = (table_name, table_alias, ob_col.name)
                for direction in ("ASC", "DESC"):
                    # Without partition
                    for pred_group in [[], seeds[:1]] if seeds else [[]]:
                        estimated = average_selectivity(pred_group)
                        bucket = bucket_for_selectivity(estimated)
                        families = sorted({p.family for p in pred_group}) or ["scan"]
                        for pw, pcols in proj_options[:1]:
                            candidates.append(WindowCandidate(
                                stage_id="1_table_window",
                                tables=[table_name],
                                base_table=table_name,
                                window_func=func_name,
                                window_column=win_col,
                                partition_by=[],
                                order_by_col=ob,
                                order_direction=direction,
                                predicates=list(pred_group),
                                projections=pcols,
                                projection_width=pw,
                                predicate_families=families,
                                target_selectivity_bucket=bucket,
                                estimated_selectivity=estimated,
                                rule_family=f"window_{func_name.lower()}",
                                limit=50 if func_name in ("ROW_NUMBER", "RANK", "DENSE_RANK") else None,
                                calibration_source="pg_stats" if snapshot.column_stats else "schema",
                            ))

                    # With partition
                    for part_col in partition_cols[:2]:
                        part = [(table_name, table_alias, part_col.name)]
                        for pw, pcols in proj_options[:1]:
                            candidates.append(WindowCandidate(
                                stage_id="1_table_window",
                                tables=[table_name],
                                base_table=table_name,
                                window_func=func_name,
                                window_column=win_col,
                                partition_by=part,
                                order_by_col=ob,
                                order_direction=direction,
                                predicates=[],
                                projections=pcols,
                                projection_width=pw,
                                predicate_families=["scan"],
                                target_selectivity_bucket="medium",
                                estimated_selectivity=0.10,
                                rule_family=f"window_{func_name.lower()}_partitioned",
                                calibration_source="pg_stats" if snapshot.column_stats else "schema",
                            ))

    return candidates


# ---------------------------------------------------------------------------
# Selection & output
# ---------------------------------------------------------------------------

def unique_window_candidates(candidates: list[WindowCandidate]) -> list[WindowCandidate]:
    deduped: dict[tuple[Any, ...], WindowCandidate] = {}
    for c in candidates:
        deduped.setdefault(c.structural_signature(), c)
    return list(deduped.values())


def select_window_candidates(
    candidates: list[WindowCandidate],
    budget: int,
    seed: int,
) -> list[WindowCandidate]:
    rng = random.Random(seed)
    unique = unique_window_candidates(candidates)
    rng.shuffle(unique)
    groups: dict[str, list[WindowCandidate]] = defaultdict(list)
    for c in unique:
        groups[c.window_func].append(c)
    selected: list[WindowCandidate] = []
    seen_sql: set[str] = set()
    keys = sorted(groups)
    rng.shuffle(keys)
    while keys and len(selected) < budget:
        next_round: list[str] = []
        for key in keys:
            if len(selected) >= budget:
                break
            if groups[key]:
                c = groups[key].pop(0)
                sql_sig = normalize_sql(c.render_sql())
                if sql_sig not in seen_sql:
                    seen_sql.add(sql_sig)
                    selected.append(c)
            if groups[key]:
                next_round.append(key)
        keys = next_round
    return selected


def emit_window_artifacts(
    selected: list[WindowCandidate],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"win_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(
            filename=candidate.filename,
            sql=candidate.render_sql(),
            metadata=metadata,
        ))
    manifest = {
        "branch": "window",
        "benchmark": benchmark,
        "schema_path": schema_path,
        "suffix": suffix,
        "seed": seed,
        "query_count": len(artifacts),
        "queries": manifest_queries,
    }
    if diagnostics:
        manifest["generation_diagnostics"] = diagnostics
    return manifest, artifacts


def default_window_stage_budgets() -> dict[str, int]:
    return {"1_table_window": 24}


def generate_window_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    diagnostics: dict[str, dict[str, int]] = defaultdict(lambda: {"generated": 0, "selected": 0})

    all_candidates = _build_window_candidates(catalog, snapshot, config, build_context)
    budgets = default_window_stage_budgets()
    for stage_id, budget in config.stage_budgets.items():
        if stage_id in budgets:
            budgets[stage_id] = budget

    by_stage: dict[str, list[WindowCandidate]] = defaultdict(list)
    for c in all_candidates:
        by_stage[c.stage_id].append(c)

    selected: list[WindowCandidate] = []
    for stage_id in sorted(budgets):
        budget = budgets[stage_id]
        if budget <= 0:
            continue
        available = by_stage.get(stage_id, [])
        diagnostics[stage_id]["generated"] = len(available)
        if not available:
            continue
        chosen = select_window_candidates(available, min(budget, len(available)), config.seed)
        diagnostics[stage_id]["selected"] = len(chosen)
        selected.extend(chosen)

    selected = sorted(
        unique_window_candidates(selected),
        key=lambda item: (item.stage_id, item.window_func, item.signature()),
    )
    return emit_window_artifacts(
        selected, config.benchmark, schema_path, config.suffix, config.seed,
        diagnostics=dict(diagnostics),
    )
