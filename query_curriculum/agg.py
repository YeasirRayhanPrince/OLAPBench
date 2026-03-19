"""Aggregation / ORDER BY / DISTINCT curriculum generator.

Follows the same stats-driven, diversity-selected pattern as spj.py but
targets GROUP BY, HAVING, ORDER BY, LIMIT and DISTINCT SQL features to
exercise HashAggregate, SortAggregate, Sort, Limit and related physical
operators.
"""
from __future__ import annotations

import itertools
import random
from collections import defaultdict
from dataclasses import dataclass, field
from math import ceil
from numbers import Real
from typing import Any

from query_curriculum.core import (
    BUCKET_ORDER,
    BUCKET_TARGETS,
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
    validate_manifest_entry,
)
from query_curriculum.spj import (
    JoinSpec,
    PredicateSpec,
    SeedTemplate,
    SpjBuildContext,
    average_selectivity,
    bind_seed_template,
    build_pair_candidates_for_edge,
    build_projection_options,
    build_projection_templates,
    build_seed_templates,
    cached_projection_options,
    expand_predicate_groups,
    make_build_context,
    render_predicate,
    top_seed_pool,
    unique_predicate_groups,
    _hashable_value,
)


# ---------------------------------------------------------------------------
# Aggregate specification
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AggSpec:
    """A single aggregate function application, e.g. SUM(t.col)."""
    func: str          # SUM, AVG, COUNT, MIN, MAX, COUNT_DISTINCT
    table: str
    alias: str         # table alias
    column: str
    output_alias: str  # alias for the aggregate result, e.g. "sum_revenue"


# ---------------------------------------------------------------------------
# AggCandidate
# ---------------------------------------------------------------------------

@dataclass
class AggCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    join_type: str
    join_topology: str
    joins: list[JoinSpec]
    predicates: list[PredicateSpec]
    projections: list[tuple[str, str, str]]  # (table, alias, column)
    projection_width: str
    predicate_families: list[str]
    target_selectivity_bucket: str
    estimated_selectivity: float | None
    rule_family: str

    # Aggregation-specific fields
    group_by: list[tuple[str, str, str]]  # (table, alias, column)
    agg_functions: list[AggSpec]
    having_predicates: list[str]          # rendered SQL fragments
    order_by: list[tuple[str, str]]       # (expr, direction)
    limit: int | None = None
    distinct: bool = False

    render_mode: str = "ansi"
    template_pack: str | None = None
    calibrated: bool = False
    calibration_source: str = "schema"
    observed_rows: int | None = None
    observed_selectivity: float | None = None
    filename: str | None = None
    query_id: str | None = None
    _cached_signature: tuple[Any, ...] | None = field(default=None, init=False, repr=False, compare=False)

    def structural_signature(self) -> tuple[Any, ...]:
        if self._cached_signature is not None:
            return self._cached_signature
        sig = (
            "agg",
            self.stage_id,
            tuple(self.tables),
            self.base_table,
            self.join_type,
            self.join_topology,
            self.rule_family,
            self.projection_width,
            self.distinct,
            self.limit,
            tuple(self.group_by),
            tuple((a.func, a.table, a.column) for a in self.agg_functions),
            tuple(self.having_predicates),
            tuple(self.order_by),
            tuple(
                (j.join_type, j.left_table, j.left_alias, j.left_column,
                 j.right_table, j.right_alias, j.right_column, j.edge.key)
                for j in self.joins
            ),
            tuple(
                (p.table, p.alias, p.column, p.operator,
                 _hashable_value(p.value), p.family, p.placement)
                for p in self.predicates
            ),
            tuple(self.projections),
        )
        self._cached_signature = sig
        return sig

    def signature(self) -> str:
        return repr(self.structural_signature())

    # ---- SQL rendering ----------------------------------------------------

    def render_sql(self) -> str:
        parts: list[str] = []

        # SELECT clause
        select_items: list[str] = []
        if self.distinct and not self.group_by and not self.agg_functions:
            select_items = [f"{alias}.{col}" for _, alias, col in self.projections] or ["*"]
            parts.append(f"SELECT DISTINCT {', '.join(select_items)}")
        else:
            # group by columns
            for _, alias, col in self.group_by:
                select_items.append(f"{alias}.{col}")
            # aggregate functions
            for agg in self.agg_functions:
                if agg.func == "COUNT_DISTINCT":
                    select_items.append(f"COUNT(DISTINCT {agg.alias}.{agg.column}) AS {agg.output_alias}")
                elif agg.func == "COUNT_STAR":
                    select_items.append(f"COUNT(*) AS {agg.output_alias}")
                else:
                    select_items.append(f"{agg.func}({agg.alias}.{agg.column}) AS {agg.output_alias}")
            if not select_items:
                select_items = [f"{alias}.{col}" for _, alias, col in self.projections] or ["*"]
            parts.append(f"SELECT {', '.join(select_items)}")

        # FROM clause
        aliases = self.aliases()
        parts.append(f"FROM {self.base_table} AS {aliases[self.base_table]}")

        # JOIN clauses
        for join in self.joins:
            on_parts = [f"{join.left_alias}.{join.left_column} = {join.right_alias}.{join.right_column}"]
            for pred in join.on_predicates:
                on_parts.append(render_predicate(pred))
            parts.append(f"{join.join_type.upper()} JOIN {join.right_table} AS {join.right_alias}")
            parts.append("  ON " + "\n  AND ".join(on_parts))

        # WHERE clause
        where_preds = [render_predicate(p) for p in self.predicates if p.placement == "where"]
        if where_preds:
            parts.append("WHERE " + "\n  AND ".join(where_preds))

        # GROUP BY clause
        if self.group_by:
            gb_cols = [f"{alias}.{col}" for _, alias, col in self.group_by]
            parts.append(f"GROUP BY {', '.join(gb_cols)}")

        # HAVING clause
        if self.having_predicates:
            parts.append("HAVING " + "\n  AND ".join(self.having_predicates))

        # ORDER BY clause
        if self.order_by:
            ob_items = [f"{expr} {direction}" for expr, direction in self.order_by]
            parts.append(f"ORDER BY {', '.join(ob_items)}")

        # LIMIT clause
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")

        parts.append(";")
        return "\n".join(parts) + "\n"

    def aliases(self) -> dict[str, str]:
        aliases: dict[str, str] = {}
        if self.projections:
            aliases[self.base_table] = self.projections[0][1]
        aliases[self.base_table] = aliases.get(self.base_table) or self.base_table[0]
        for join in self.joins:
            aliases[join.left_table] = join.left_alias
            aliases[join.right_table] = join.right_alias
        for table_name, alias, _ in self.projections:
            aliases[table_name] = alias
        for table_name, alias, _ in self.group_by:
            aliases[table_name] = alias
        return aliases

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "agg",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "join_edges": [join.edge.as_manifest() for join in self.joins],
            "join_type": self.join_type,
            "join_topology": self.join_topology,
            "predicate_count": len(self.predicates) + sum(len(j.on_predicates) for j in self.joins),
            "predicate_families": self.predicate_families,
            "projection_width": self.projection_width,
            "target_selectivity_bucket": self.target_selectivity_bucket,
            "calibrated": self.calibrated,
            "calibration_source": self.calibration_source,
            "rule_family": self.rule_family,
            "template_pack": self.template_pack,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "group_by_columns": [{"table": t, "column": c} for t, _, c in self.group_by],
            "agg_functions": [{"func": a.func, "table": a.table, "column": a.column} for a in self.agg_functions],
            "having": self.having_predicates,
            "order_by": [{"expr": e, "direction": d} for e, d in self.order_by],
            "limit": self.limit,
            "distinct": self.distinct,
            "projection_columns": [{"table": t, "column": c} for t, _, c in self.projections],
            "predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator, "placement": p.placement}
                for p in self.predicates
            ] + [
                {"table": p.table, "column": p.column, "operator": p.operator, "placement": p.placement}
                for j in self.joins for p in j.on_predicates
            ],
        }


# ---------------------------------------------------------------------------
# Stats-driven helpers
# ---------------------------------------------------------------------------

def _good_group_by_columns(
    table: TableSchema,
    column_stats: dict[str, ColumnStats],
    table_count: int,
) -> list[ColumnSchema]:
    """Pick columns with moderate n_distinct -- not unique, not constant."""
    candidates: list[tuple[float, ColumnSchema]] = []
    for col in table.columns.values():
        if col.is_primary_key:
            continue
        stats = column_stats.get(col.name)
        if stats is None or stats.n_distinct is None:
            continue
        nd = stats.n_distinct
        # n_distinct < 0 means fraction of rows; > 0 means absolute count
        # We want moderate cardinality: not 1, not total unique
        if nd == 0 or nd == 1:
            continue
        if nd < 0:
            # fraction of table rows; -1.0 means every row unique
            if nd <= -0.95:
                continue
            score = abs(nd + 0.5)  # best near -0.5 (half unique)
        else:
            # absolute count
            if nd <= 1:
                continue
            if nd > 10000 and table_count > 100000:
                continue
            score = abs(nd - min(50, table_count * 0.1)) / max(1, table_count)
        candidates.append((score, col))
    candidates.sort(key=lambda x: (x[0], x[1].name))
    return [col for _, col in candidates]


def _agg_functions_for_table(
    table: TableSchema,
    table_alias: str,
    column_stats: dict[str, ColumnStats],
) -> list[AggSpec]:
    """Build a set of applicable aggregate functions from schema + stats."""
    aggs: list[AggSpec] = []
    seen: set[tuple[str, str]] = set()
    for col in sorted(table.columns.values(), key=lambda c: c.name):
        if col.is_primary_key:
            continue
        stats = column_stats.get(col.name)
        if col.is_numeric:
            for func in ("SUM", "AVG", "MIN", "MAX"):
                key = (func, col.name)
                if key not in seen:
                    seen.add(key)
                    aggs.append(AggSpec(
                        func=func, table=table.name, alias=table_alias,
                        column=col.name, output_alias=f"{func.lower()}_{col.name}",
                    ))
        # COUNT for any column
        key = ("COUNT", col.name)
        if key not in seen:
            seen.add(key)
            aggs.append(AggSpec(
                func="COUNT", table=table.name, alias=table_alias,
                column=col.name, output_alias=f"cnt_{col.name}",
            ))
        # COUNT(DISTINCT ...) for moderate-cardinality columns
        if stats and stats.n_distinct is not None and stats.n_distinct != 0:
            nd = stats.n_distinct
            if (nd < 0 and nd > -0.95) or (0 < nd <= 5000):
                key = ("COUNT_DISTINCT", col.name)
                if key not in seen:
                    seen.add(key)
                    aggs.append(AggSpec(
                        func="COUNT_DISTINCT", table=table.name, alias=table_alias,
                        column=col.name, output_alias=f"cntd_{col.name}",
                    ))
    # COUNT(*)
    aggs.append(AggSpec(
        func="COUNT_STAR", table=table.name, alias=table_alias,
        column="*", output_alias="cnt_star",
    ))
    return aggs


def _having_thresholds(
    agg: AggSpec,
    column_stats: dict[str, ColumnStats],
    table_count: int,
) -> list[str]:
    """Generate HAVING predicates from stats for a given aggregate."""
    thresholds: list[str] = []
    if agg.func in ("COUNT", "COUNT_STAR"):
        # use table-size derived thresholds
        for divisor in (10, 100):
            threshold = max(1, table_count // divisor)
            thresholds.append(f"{agg.output_alias} > {threshold}")
        thresholds.append(f"{agg.output_alias} >= 1")
        return thresholds

    if agg.func == "COUNT_DISTINCT":
        thresholds.append(f"{agg.output_alias} > 1")
        return thresholds

    stats = column_stats.get(agg.column)
    if stats is None:
        return thresholds

    # For SUM/AVG/MIN/MAX use histogram-derived values
    histogram = [b for b in stats.histogram_bounds if isinstance(b, Real)]
    if len(histogram) >= 4:
        mid_idx = len(histogram) // 2
        mid_val = histogram[mid_idx]
        if agg.func == "AVG":
            thresholds.append(f"{agg.output_alias} > {sql_literal(mid_val)}")
            thresholds.append(f"{agg.output_alias} < {sql_literal(mid_val)}")
        elif agg.func == "SUM":
            # SUM is usually much larger; use a scaled value
            scaled = mid_val * max(1, table_count // 100)
            thresholds.append(f"{agg.output_alias} > {sql_literal(scaled)}")
        elif agg.func in ("MIN", "MAX"):
            quarter = len(histogram) // 4
            thresholds.append(f"{agg.output_alias} > {sql_literal(histogram[quarter])}")
            thresholds.append(f"{agg.output_alias} < {sql_literal(histogram[-quarter - 1])}")
    return thresholds


def _limit_values(table_count: int) -> list[int]:
    """Stats-driven LIMIT values."""
    values = [1, 5, 10, 50]
    if table_count > 0:
        for divisor in (10, 100):
            v = max(1, table_count // divisor)
            if v not in values:
                values.append(v)
    return sorted(set(values))


# ---------------------------------------------------------------------------
# Candidate builders
# ---------------------------------------------------------------------------

def _build_single_table_agg_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[AggCandidate]:
    """Stages: 1_table_agg, 1_table_agg+, 1_table_orderby, 1_table_distinct."""
    candidates: list[AggCandidate] = []

    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        table_alias = aliases[table_name]
        col_stats = snapshot.column_stats.get(table_name, {})
        table_count = snapshot.table_counts.get(table_name, 1000)

        # Seed predicates
        seeds = top_seed_pool(table, table_alias, snapshot, limit=4, build_context=build_context)

        # Group-by columns
        gb_cols = _good_group_by_columns(table, col_stats, table_count)
        if not gb_cols:
            # fallback: use any non-pk non-unique column
            gb_cols = [c for c in table.columns.values() if not c.is_primary_key][:2]

        # Aggregate functions
        all_aggs = _agg_functions_for_table(table, table_alias, col_stats)
        if not all_aggs:
            continue

        # Projection options (used for order-by and distinct)
        proj_options = cached_projection_options([table_name], aliases, catalog, build_context)

        # --- Stage: 1_table_agg (single GROUP BY, 1 agg function, no HAVING) ---
        for gb_col in gb_cols[:3]:
            gb = [(table_name, table_alias, gb_col.name)]
            for agg in all_aggs[:4]:
                for pred_group in [[], seeds[:1]] if seeds else [[]]:
                    estimated = average_selectivity(pred_group)
                    bucket = bucket_for_selectivity(estimated)
                    families = sorted({p.family for p in pred_group}) or ["scan"]
                    candidates.append(AggCandidate(
                        stage_id="1_table_agg",
                        tables=[table_name],
                        base_table=table_name,
                        join_type="scan",
                        join_topology="scan",
                        joins=[],
                        predicates=list(pred_group),
                        projections=proj_options[0][1] if proj_options else [],
                        projection_width=proj_options[0][0] if proj_options else "narrow",
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="single_group_by",
                        group_by=gb,
                        agg_functions=[agg],
                        having_predicates=[],
                        order_by=[],
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

        # --- Stage: 1_table_agg+ (GROUP BY with 2+ agg functions + HAVING) ---
        for gb_col in gb_cols[:2]:
            gb = [(table_name, table_alias, gb_col.name)]
            # pick 2-3 agg functions
            for agg_combo in itertools.combinations(all_aggs[:6], min(2, len(all_aggs[:6]))):
                agg_list = list(agg_combo)
                # generate HAVING predicates from first agg
                havings = _having_thresholds(agg_list[0], col_stats, table_count)
                for having in havings[:2]:
                    for pred_group in [[], seeds[:1]] if seeds else [[]]:
                        estimated = average_selectivity(pred_group)
                        bucket = bucket_for_selectivity(estimated)
                        families = sorted({p.family for p in pred_group}) or ["scan"]
                        candidates.append(AggCandidate(
                            stage_id="1_table_agg+",
                            tables=[table_name],
                            base_table=table_name,
                            join_type="scan",
                            join_topology="scan",
                            joins=[],
                            predicates=list(pred_group),
                            projections=proj_options[0][1] if proj_options else [],
                            projection_width=proj_options[0][0] if proj_options else "narrow",
                            predicate_families=families,
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family="having_filter",
                            group_by=gb,
                            agg_functions=agg_list,
                            having_predicates=[having],
                            order_by=[],
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

        # Multi-column GROUP BY
        if len(gb_cols) >= 2:
            gb = [(table_name, table_alias, gb_cols[0].name),
                  (table_name, table_alias, gb_cols[1].name)]
            for agg in all_aggs[:3]:
                estimated = average_selectivity([])
                bucket = bucket_for_selectivity(estimated)
                candidates.append(AggCandidate(
                    stage_id="1_table_agg",
                    tables=[table_name],
                    base_table=table_name,
                    join_type="scan",
                    join_topology="scan",
                    joins=[],
                    predicates=[],
                    projections=proj_options[0][1] if proj_options else [],
                    projection_width=proj_options[0][0] if proj_options else "narrow",
                    predicate_families=["scan"],
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family="multi_group_by",
                    group_by=gb,
                    agg_functions=[agg],
                    having_predicates=[],
                    order_by=[],
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

        # --- Stage: 1_table_orderby (ORDER BY + LIMIT, no GROUP BY) ---
        limit_vals = _limit_values(table_count)
        for pw, pcols in proj_options[:2]:
            # order by first projected column
            if not pcols:
                continue
            _, first_alias, first_col = pcols[0]
            for direction in ("ASC", "DESC"):
                for lim in limit_vals[:3]:
                    for pred_group in [[], seeds[:1]] if seeds else [[]]:
                        estimated = average_selectivity(pred_group)
                        bucket = bucket_for_selectivity(estimated)
                        families = sorted({p.family for p in pred_group}) or ["scan"]
                        candidates.append(AggCandidate(
                            stage_id="1_table_orderby",
                            tables=[table_name],
                            base_table=table_name,
                            join_type="scan",
                            join_topology="scan",
                            joins=[],
                            predicates=list(pred_group),
                            projections=pcols,
                            projection_width=pw,
                            predicate_families=families,
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family="order_limit",
                            group_by=[],
                            agg_functions=[],
                            having_predicates=[],
                            order_by=[(f"{first_alias}.{first_col}", direction)],
                            limit=lim,
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

        # ORDER BY + GROUP BY + agg (sorted aggregates)
        if gb_cols and all_aggs:
            gb_col = gb_cols[0]
            gb = [(table_name, table_alias, gb_col.name)]
            agg = all_aggs[0]
            for direction in ("ASC", "DESC"):
                for lim in limit_vals[:2]:
                    candidates.append(AggCandidate(
                        stage_id="1_table_agg+",
                        tables=[table_name],
                        base_table=table_name,
                        join_type="scan",
                        join_topology="scan",
                        joins=[],
                        predicates=[],
                        projections=proj_options[0][1] if proj_options else [],
                        projection_width=proj_options[0][0] if proj_options else "narrow",
                        predicate_families=["scan"],
                        target_selectivity_bucket="medium",
                        estimated_selectivity=0.10,
                        rule_family="order_limit",
                        group_by=gb,
                        agg_functions=[agg],
                        having_predicates=[],
                        order_by=[(agg.output_alias, direction)],
                        limit=lim,
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

        # --- Stage: 1_table_distinct ---
        for pw, pcols in proj_options[:3]:
            if not pcols:
                continue
            for pred_group in [[], seeds[:1]] if seeds else [[]]:
                estimated = average_selectivity(pred_group)
                bucket = bucket_for_selectivity(estimated)
                families = sorted({p.family for p in pred_group}) or ["scan"]
                candidates.append(AggCandidate(
                    stage_id="1_table_distinct",
                    tables=[table_name],
                    base_table=table_name,
                    join_type="scan",
                    join_topology="scan",
                    joins=[],
                    predicates=list(pred_group),
                    projections=pcols,
                    projection_width=pw,
                    predicate_families=families,
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family="distinct_scan",
                    group_by=[],
                    agg_functions=[],
                    having_predicates=[],
                    order_by=[],
                    distinct=True,
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

    return candidates


def _build_two_table_agg_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[AggCandidate]:
    """Stages: 2_table_agg, 2_table_agg+."""
    candidates: list[AggCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        child = catalog[edge.child_table]
        parent = catalog[edge.parent_table]
        child_alias, parent_alias = alias_map([child.name, parent.name]).values()
        aliases = {child.name: child_alias, parent.name: parent_alias}

        child_stats = snapshot.column_stats.get(child.name, {})
        parent_stats = snapshot.column_stats.get(parent.name, {})
        child_count = snapshot.table_counts.get(child.name, 1000)
        parent_count = snapshot.table_counts.get(parent.name, 1000)

        # Build join spec
        join = JoinSpec(
            join_type="inner",
            left_table=child.name,
            left_alias=child_alias,
            left_column=edge.child_column,
            right_table=parent.name,
            right_alias=parent_alias,
            right_column=edge.parent_column,
            edge=edge,
        )

        # Group by columns from parent table (dimension table pattern)
        parent_gb_cols = _good_group_by_columns(parent, parent_stats, parent_count)
        child_gb_cols = _good_group_by_columns(child, child_stats, child_count)

        # Aggregate on child table (fact table pattern)
        child_aggs = _agg_functions_for_table(child, child_alias, child_stats)
        parent_aggs = _agg_functions_for_table(parent, parent_alias, parent_stats)

        # Projections
        proj_options = cached_projection_options(
            [child.name, parent.name], aliases, catalog, build_context,
        )

        # Seed predicates
        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)
        parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=3, build_context=build_context)

        # --- Stage: 2_table_agg (join + GROUP BY) ---
        # Pattern: GROUP BY parent columns, aggregate child columns
        for gb_col in parent_gb_cols[:2]:
            gb = [(parent.name, parent_alias, gb_col.name)]
            for agg in child_aggs[:3]:
                for pred_group in [[], child_seeds[:1], parent_seeds[:1]] if (child_seeds or parent_seeds) else [[]]:
                    estimated = average_selectivity(pred_group)
                    bucket = bucket_for_selectivity(estimated)
                    families = sorted({p.family for p in pred_group}) or ["join"]
                    candidates.append(AggCandidate(
                        stage_id="2_table_agg",
                        tables=[child.name, parent.name],
                        base_table=child.name,
                        join_type="inner",
                        join_topology="pair",
                        joins=[join],
                        predicates=list(pred_group),
                        projections=proj_options[0][1] if proj_options else [],
                        projection_width=proj_options[0][0] if proj_options else "narrow",
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="agg_after_join",
                        group_by=gb,
                        agg_functions=[agg],
                        having_predicates=[],
                        order_by=[],
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

        # --- Stage: 2_table_agg+ (join + GROUP BY + HAVING + ORDER BY + LIMIT) ---
        for gb_col in parent_gb_cols[:2]:
            gb = [(parent.name, parent_alias, gb_col.name)]
            for agg in child_aggs[:2]:
                havings = _having_thresholds(agg, child_stats, child_count)
                for having in havings[:1]:
                    limit_vals = _limit_values(parent_count)
                    for lim in limit_vals[:2]:
                        estimated = average_selectivity([])
                        bucket = bucket_for_selectivity(estimated)
                        candidates.append(AggCandidate(
                            stage_id="2_table_agg+",
                            tables=[child.name, parent.name],
                            base_table=child.name,
                            join_type="inner",
                            join_topology="pair",
                            joins=[join],
                            predicates=[],
                            projections=proj_options[0][1] if proj_options else [],
                            projection_width=proj_options[0][0] if proj_options else "narrow",
                            predicate_families=["join"],
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family="having_filter",
                            group_by=gb,
                            agg_functions=[agg],
                            having_predicates=[having],
                            order_by=[(agg.output_alias, "DESC")],
                            limit=lim,
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

        # Aggregation pushdown pattern: aggregate child before join
        for gb_col in child_gb_cols[:1]:
            gb = [(child.name, child_alias, gb_col.name)]
            for agg in child_aggs[:2]:
                estimated = average_selectivity([])
                bucket = bucket_for_selectivity(estimated)
                candidates.append(AggCandidate(
                    stage_id="2_table_agg",
                    tables=[child.name, parent.name],
                    base_table=child.name,
                    join_type="inner",
                    join_topology="pair",
                    joins=[join],
                    predicates=[],
                    projections=proj_options[0][1] if proj_options else [],
                    projection_width=proj_options[0][0] if proj_options else "narrow",
                    predicate_families=["join"],
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family="agg_pushdown",
                    group_by=gb,
                    agg_functions=[agg],
                    having_predicates=[],
                    order_by=[],
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

    return candidates


# ---------------------------------------------------------------------------
# Deduplication & diversity (mirrors spj.py patterns)
# ---------------------------------------------------------------------------

def unique_agg_candidates(candidates: list[AggCandidate]) -> list[AggCandidate]:
    deduped: dict[tuple[Any, ...], AggCandidate] = {}
    for c in candidates:
        deduped.setdefault(c.structural_signature(), c)
    return list(deduped.values())


def agg_diversity_key(candidate: AggCandidate) -> tuple[Any, ...]:
    return (
        candidate.rule_family,
        candidate.target_selectivity_bucket,
        len(candidate.group_by),
        len(candidate.agg_functions),
        candidate.distinct,
        candidate.limit is not None,
    )


def retain_diverse_agg_candidates(
    candidates: list[AggCandidate],
    *,
    limit: int,
    seed: int,
) -> list[AggCandidate]:
    if limit <= 0 or len(candidates) <= limit:
        return unique_agg_candidates(candidates)
    groups: dict[tuple[Any, ...], list[AggCandidate]] = defaultdict(list)
    for c in sorted(candidates, key=lambda item: (agg_diversity_key(item), item.signature())):
        groups[agg_diversity_key(c)].append(c)
    rng = random.Random(seed)
    ordered_keys = sorted(groups)
    rng.shuffle(ordered_keys)
    for key in ordered_keys:
        rng.shuffle(groups[key])
    retained: list[AggCandidate] = []
    while ordered_keys and len(retained) < limit:
        next_round: list[tuple[Any, ...]] = []
        for key in ordered_keys:
            if len(retained) >= limit:
                break
            if groups[key]:
                retained.append(groups[key].pop(0))
            if groups[key]:
                next_round.append(key)
        ordered_keys = next_round
    return unique_agg_candidates(retained)


def ordered_agg_candidates(candidates: list[AggCandidate], seed: int) -> list[AggCandidate]:
    if not candidates:
        return []
    rng = random.Random(seed)
    groups: dict[tuple[Any, ...], list[AggCandidate]] = defaultdict(list)
    for c in sorted(candidates, key=lambda item: (item.stage_id, item.rule_family, item.signature())):
        key = (
            tuple(c.tables),
            c.join_topology,
            c.rule_family,
            len(c.group_by),
            c.distinct,
            c.limit is not None,
        )
        groups[key].append(c)
    ordered_keys = sorted(groups)
    for key in ordered_keys:
        rng.shuffle(groups[key])
    selected: list[AggCandidate] = []
    while ordered_keys:
        next_round: list[tuple[Any, ...]] = []
        for key in ordered_keys:
            if groups[key]:
                selected.append(groups[key].pop(0))
            if groups[key]:
                next_round.append(key)
        ordered_keys = next_round
    return selected


# ---------------------------------------------------------------------------
# Selection (simplified version of spj.select_candidates_for_stage)
# ---------------------------------------------------------------------------

def select_agg_candidates_for_stage(
    candidates: list[AggCandidate],
    budget: int,
    seed: int,
    snapshot: StatsSnapshot,
    *,
    stage_id: str,
) -> list[AggCandidate]:
    """Select up to *budget* diverse candidates for a stage."""
    ordered = ordered_agg_candidates(candidates, seed)
    selected: list[AggCandidate] = []
    seen_sql: set[str] = set()
    for candidate in ordered:
        sql_sig = normalize_sql(candidate.render_sql())
        if sql_sig in seen_sql:
            continue
        seen_sql.add(sql_sig)
        selected.append(candidate)
        if len(selected) >= budget:
            break
    return selected


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def emit_agg_artifacts(
    selected: list[AggCandidate],
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    generation_diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"agg_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(
            filename=candidate.filename,
            sql=candidate.render_sql(),
            metadata=metadata,
        ))
    manifest = {
        "branch": "agg",
        "benchmark": benchmark,
        "schema_path": schema_path,
        "suffix": suffix,
        "seed": seed,
        "query_count": len(artifacts),
        "queries": manifest_queries,
    }
    if generation_diagnostics:
        manifest["generation_diagnostics"] = generation_diagnostics
    return manifest, artifacts


# ---------------------------------------------------------------------------
# Default stage budgets for agg branch
# ---------------------------------------------------------------------------

def default_agg_stage_budgets() -> dict[str, int]:
    return {
        "1_table_agg": 24,
        "1_table_agg+": 18,
        "1_table_orderby": 18,
        "1_table_distinct": 12,
        "2_table_agg": 24,
        "2_table_agg+": 18,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_agg_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    diagnostics: dict[str, dict[str, int]] = defaultdict(lambda: {
        "generated": 0, "selected": 0,
    })

    # Build all candidates
    single_table = _build_single_table_agg_candidates(catalog, snapshot, config, build_context)
    two_table = _build_two_table_agg_candidates(join_edges, catalog, snapshot, config, build_context)

    # Group by stage
    by_stage: dict[str, list[AggCandidate]] = defaultdict(list)
    for c in single_table + two_table:
        by_stage[c.stage_id].append(c)

    # Resolve budgets
    agg_budgets = default_agg_stage_budgets()
    # Allow config overrides via stage_budgets
    for stage_id, budget in config.stage_budgets.items():
        if stage_id in agg_budgets:
            agg_budgets[stage_id] = budget

    selected: list[AggCandidate] = []
    stage_order = [
        "1_table_agg", "1_table_agg+", "1_table_orderby", "1_table_distinct",
        "2_table_agg", "2_table_agg+",
    ]

    for stage_id in stage_order:
        budget = agg_budgets.get(stage_id, 0)
        if budget <= 0:
            continue
        available = unique_agg_candidates(by_stage.get(stage_id, []))
        diagnostics[stage_id]["generated"] = len(available)

        if not available:
            continue

        # Don't fail if we have fewer candidates than budget -- just take what we have
        actual_budget = min(budget, len(available))
        chosen = select_agg_candidates_for_stage(
            available, actual_budget, config.seed, snapshot, stage_id=stage_id,
        )
        diagnostics[stage_id]["selected"] = len(chosen)
        selected.extend(chosen)

    selected = sorted(
        unique_agg_candidates(selected),
        key=lambda item: (item.stage_id, item.rule_family, item.signature()),
    )
    return emit_agg_artifacts(
        selected, catalog, join_edges,
        config.benchmark, schema_path, config.suffix, config.seed,
        generation_diagnostics=dict(diagnostics),
    )
