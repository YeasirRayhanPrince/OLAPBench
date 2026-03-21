"""Subquery curriculum generator.

Generates EXISTS, NOT EXISTS, IN, NOT IN, scalar aggregate, quantified (ALL/ANY),
and derived table queries to exercise SemiJoin, AntiJoin, NullAwareAntiJoin,
SubqueryScan, and NestedLoop physical operators.
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
    SeedTemplate,
    SpjBuildContext,
    average_selectivity,
    bind_seed_template,
    build_seed_templates,
    cached_projection_options,
    make_build_context,
    render_predicate,
    top_seed_pool,
    _hashable_value,
)


# ---------------------------------------------------------------------------
# SubqueryCandidate
# ---------------------------------------------------------------------------

@dataclass
class SubqueryCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    subquery_type: str  # "exists", "not_exists", "in_subquery", "not_in_subquery", "scalar", "quantified", "derived"
    outer_predicates: list[PredicateSpec]
    inner_predicates: list[PredicateSpec]
    # FK correlation: outer_table.outer_col = inner_table.inner_col
    correlation_outer_table: str
    correlation_outer_alias: str
    correlation_outer_col: str
    correlation_inner_table: str
    correlation_inner_alias: str
    correlation_inner_col: str
    # For IN-subquery: the column projected by the inner query
    in_column: str | None = None
    in_alias: str | None = None
    # For scalar subquery: the aggregate in the inner query
    scalar_agg_func: str | None = None
    scalar_agg_col: str | None = None
    # For quantified comparisons: operator and outer comparison column
    quantified_op: str | None = None  # e.g., "> ALL", "< ANY"
    quantified_compare_col: str | None = None
    # For derived tables: the derived alias and columns
    derived_alias: str | None = None
    derived_columns: list[tuple[str, str]] | None = None  # (alias, col)
    # Projection for outer query
    projections: list[tuple[str, str, str]] = field(default_factory=list)
    projection_width: str = "narrow"
    predicate_families: list[str] = field(default_factory=list)
    target_selectivity_bucket: str = "medium"
    estimated_selectivity: float | None = None
    rule_family: str = "exists"
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
            "subquery",
            self.stage_id,
            tuple(self.tables),
            self.subquery_type,
            self.rule_family,
            self.correlation_outer_table, self.correlation_outer_col,
            self.correlation_inner_table, self.correlation_inner_col,
            self.in_column,
            self.scalar_agg_func, self.scalar_agg_col,
            self.quantified_op, self.quantified_compare_col,
            tuple(
                (p.table, p.column, p.operator, _hashable_value(p.value), p.family)
                for p in self.outer_predicates
            ),
            tuple(
                (p.table, p.column, p.operator, _hashable_value(p.value), p.family)
                for p in self.inner_predicates
            ),
            tuple(self.projections),
        )
        self._cached_signature = sig
        return sig

    def signature(self) -> str:
        return repr(self.structural_signature())

    def render_sql(self) -> str:
        if self.subquery_type == "derived":
            return self._render_derived()
        parts: list[str] = []

        # SELECT
        select_items = [f"{alias}.{col}" for _, alias, col in self.projections] or ["*"]
        parts.append(f"SELECT {', '.join(select_items)}")

        # FROM
        parts.append(f"FROM {self.correlation_outer_table} AS {self.correlation_outer_alias}")

        # WHERE (outer predicates)
        where_parts: list[str] = []
        for p in self.outer_predicates:
            where_parts.append(render_predicate(p))

        # Subquery clause
        if self.subquery_type in ("exists", "not_exists"):
            prefix = "NOT EXISTS" if self.subquery_type == "not_exists" else "EXISTS"
            inner_where = [
                f"{self.correlation_inner_alias}.{self.correlation_inner_col} = "
                f"{self.correlation_outer_alias}.{self.correlation_outer_col}"
            ]
            for p in self.inner_predicates:
                inner_where.append(render_predicate(p))
            sub = (
                f"{prefix} (SELECT 1 FROM {self.correlation_inner_table} AS {self.correlation_inner_alias} "
                f"WHERE {' AND '.join(inner_where)})"
            )
            where_parts.append(sub)

        elif self.subquery_type == "in_subquery":
            in_col = self.in_column or self.correlation_outer_col
            in_alias = self.in_alias or self.correlation_inner_alias
            inner_where: list[str] = []
            for p in self.inner_predicates:
                inner_where.append(render_predicate(p))
            inner_where_sql = f" WHERE {' AND '.join(inner_where)}" if inner_where else ""
            sub = (
                f"{self.correlation_outer_alias}.{self.correlation_outer_col} IN "
                f"(SELECT {in_alias}.{in_col} FROM {self.correlation_inner_table} AS {in_alias}{inner_where_sql})"
            )
            where_parts.append(sub)

        elif self.subquery_type == "not_in_subquery":
            in_col = self.in_column or self.correlation_outer_col
            in_alias = self.in_alias or self.correlation_inner_alias
            inner_where: list[str] = []
            for p in self.inner_predicates:
                inner_where.append(render_predicate(p))
            inner_where_sql = f" WHERE {' AND '.join(inner_where)}" if inner_where else ""
            sub = (
                f"{self.correlation_outer_alias}.{self.correlation_outer_col} NOT IN "
                f"(SELECT {in_alias}.{in_col} FROM {self.correlation_inner_table} AS {in_alias}{inner_where_sql})"
            )
            where_parts.append(sub)

        elif self.subquery_type == "scalar":
            func = self.scalar_agg_func or "MAX"
            col = self.scalar_agg_col or self.correlation_inner_col
            inner_where = [
                f"{self.correlation_inner_alias}.{self.correlation_inner_col} = "
                f"{self.correlation_outer_alias}.{self.correlation_outer_col}"
            ]
            for p in self.inner_predicates:
                inner_where.append(render_predicate(p))
            # Scalar subquery used as a filter: outer.col OP (scalar subquery)
            sub = (
                f"{self.correlation_outer_alias}.{self.correlation_outer_col} = "
                f"(SELECT {func}({self.correlation_inner_alias}.{col}) "
                f"FROM {self.correlation_inner_table} AS {self.correlation_inner_alias} "
                f"WHERE {' AND '.join(inner_where)})"
            )
            where_parts.append(sub)

        elif self.subquery_type == "quantified":
            op = self.quantified_op or "> ALL"
            compare_col = self.quantified_compare_col or self.correlation_outer_col
            inner_where: list[str] = []
            for p in self.inner_predicates:
                inner_where.append(render_predicate(p))
            inner_where_sql = f" WHERE {' AND '.join(inner_where)}" if inner_where else ""
            sub = (
                f"{self.correlation_outer_alias}.{compare_col} {op} "
                f"(SELECT {self.correlation_inner_alias}.{self.correlation_inner_col} "
                f"FROM {self.correlation_inner_table} AS {self.correlation_inner_alias}{inner_where_sql})"
            )
            where_parts.append(sub)

        if where_parts:
            parts.append("WHERE " + "\n  AND ".join(where_parts))
        parts.append(";")
        return "\n".join(parts) + "\n"

    def _render_derived(self) -> str:
        """Render derived table (inline view) query."""
        parts: list[str] = []

        # outer projections
        select_items = [f"{alias}.{col}" for _, alias, col in self.projections] or ["*"]
        parts.append(f"SELECT {', '.join(select_items)}")

        # derived subquery
        d_alias = self.derived_alias or "sub"
        d_cols = self.derived_columns or [(self.correlation_inner_alias, self.correlation_inner_col)]
        d_select = ", ".join(f"{a}.{c}" for a, c in d_cols)
        inner_where: list[str] = []
        for p in self.inner_predicates:
            inner_where.append(render_predicate(p))
        inner_where_sql = f" WHERE {' AND '.join(inner_where)}" if inner_where else ""
        parts.append(
            f"FROM (SELECT {d_select} FROM {self.correlation_inner_table} "
            f"AS {self.correlation_inner_alias}{inner_where_sql}) AS {d_alias}"
        )

        # join outer table
        parts.append(
            f"INNER JOIN {self.correlation_outer_table} AS {self.correlation_outer_alias}"
        )
        parts.append(
            f"  ON {self.correlation_outer_alias}.{self.correlation_outer_col} = "
            f"{d_alias}.{self.correlation_inner_col}"
        )

        # outer predicates
        where_parts = [render_predicate(p) for p in self.outer_predicates]
        if where_parts:
            parts.append("WHERE " + "\n  AND ".join(where_parts))

        parts.append(";")
        return "\n".join(parts) + "\n"

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "subquery",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "subquery_type": self.subquery_type,
            "rule_family": self.rule_family,
            "target_selectivity_bucket": self.target_selectivity_bucket,
            "calibrated": self.calibrated,
            "calibration_source": self.calibration_source,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "projection_columns": [{"table": t, "column": c} for t, _, c in self.projections],
            "outer_predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator}
                for p in self.outer_predicates
            ],
            "inner_predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator}
                for p in self.inner_predicates
            ],
            "correlation": {
                "outer_table": self.correlation_outer_table,
                "outer_column": self.correlation_outer_col,
                "inner_table": self.correlation_inner_table,
                "inner_column": self.correlation_inner_col,
            },
            **({"scalar_agg_func": self.scalar_agg_func} if self.scalar_agg_func else {}),
            **({"quantified_op": self.quantified_op} if self.quantified_op else {}),
        }


# ---------------------------------------------------------------------------
# Candidate builders
# ---------------------------------------------------------------------------

def _build_exists_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """EXISTS and NOT EXISTS correlated subqueries using FK edges."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        # Pattern: SELECT ... FROM parent WHERE [NOT] EXISTS (SELECT 1 FROM child WHERE child.fk = parent.pk)
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]
        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        # Ensure distinct aliases
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=3, build_context=build_context)
        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for subq_type in ("exists", "not_exists"):
            rule_family = "correlated_exists" if subq_type == "exists" else "correlated_not_exists"
            for outer_preds in [[], parent_seeds[:1]] if parent_seeds else [[]]:
                for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
                    estimated = average_selectivity(list(outer_preds) + list(inner_preds))
                    bucket = bucket_for_selectivity(estimated)
                    families = sorted({p.family for p in list(outer_preds) + list(inner_preds)}) or ["subquery"]
                    for pw, pcols in proj_options[:2]:
                        candidates.append(SubqueryCandidate(
                            stage_id="2_table_semi" if subq_type == "exists" else "2_table_anti",
                            tables=[parent.name, child.name],
                            base_table=parent.name,
                            subquery_type=subq_type,
                            outer_predicates=list(outer_preds),
                            inner_predicates=list(inner_preds),
                            correlation_outer_table=parent.name,
                            correlation_outer_alias=parent_alias,
                            correlation_outer_col=edge.parent_column,
                            correlation_inner_table=child.name,
                            correlation_inner_alias=child_alias,
                            correlation_inner_col=edge.child_column,
                            projections=pcols,
                            projection_width=pw,
                            predicate_families=families,
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family=rule_family,
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

    return candidates


def _build_in_subquery_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """IN (subquery) candidates using FK edges."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]
        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)
        parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=3, build_context=build_context)

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
            for outer_preds in [[], parent_seeds[:1]] if parent_seeds else [[]]:
                estimated = average_selectivity(list(outer_preds) + list(inner_preds))
                bucket = bucket_for_selectivity(estimated)
                families = sorted({p.family for p in list(outer_preds) + list(inner_preds)}) or ["subquery"]
                for pw, pcols in proj_options[:2]:
                    candidates.append(SubqueryCandidate(
                        stage_id="1_table_sub",
                        tables=[parent.name, child.name],
                        base_table=parent.name,
                        subquery_type="in_subquery",
                        outer_predicates=list(outer_preds),
                        inner_predicates=list(inner_preds),
                        correlation_outer_table=parent.name,
                        correlation_outer_alias=parent_alias,
                        correlation_outer_col=edge.parent_column,
                        correlation_inner_table=child.name,
                        correlation_inner_alias=child_alias,
                        correlation_inner_col=edge.child_column,
                        in_column=edge.child_column,
                        in_alias=child_alias,
                        projections=pcols,
                        projection_width=pw,
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="in_subquery",
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

    return candidates


def _build_derived_table_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """Derived table (inline view) candidates."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]
        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)

        # Derive a few columns from child table
        child_cols = [(child_alias, col.name) for col in list(child.columns.values())[:4]]

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
            estimated = average_selectivity(list(inner_preds))
            bucket = bucket_for_selectivity(estimated)
            families = sorted({p.family for p in inner_preds}) or ["subquery"]
            for pw, pcols in proj_options[:1]:
                candidates.append(SubqueryCandidate(
                    stage_id="2_table_derived",
                    tables=[parent.name, child.name],
                    base_table=parent.name,
                    subquery_type="derived",
                    outer_predicates=[],
                    inner_predicates=list(inner_preds),
                    correlation_outer_table=parent.name,
                    correlation_outer_alias=parent_alias,
                    correlation_outer_col=edge.parent_column,
                    correlation_inner_table=child.name,
                    correlation_inner_alias=child_alias,
                    correlation_inner_col=edge.child_column,
                    derived_alias="sub",
                    derived_columns=child_cols,
                    projections=pcols,
                    projection_width=pw,
                    predicate_families=families,
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family="derived_table",
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

    return candidates


def _pick_scalar_agg_column(
    table: TableSchema, exclude_cols: set[str],
) -> ColumnSchema | None:
    """Pick a numeric column suitable for aggregation, preferring non-key columns."""
    best: ColumnSchema | None = None
    for col in table.columns.values():
        if not col.is_numeric or col.name in exclude_cols:
            continue
        if not col.is_primary_key and not col.is_foreign_key:
            return col  # ideal: non-key numeric column
        if best is None:
            best = col  # fallback: key numeric column
    return best


def _build_scalar_subquery_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """Correlated scalar aggregate subqueries: WHERE outer.col = (SELECT AGG(inner.col) FROM inner WHERE correlation)."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]
        agg_col = _pick_scalar_agg_column(child, {edge.child_column})
        if agg_col is None:
            continue

        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=3, build_context=build_context)
        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for func in ("MIN", "MAX", "AVG", "SUM"):
            for outer_preds in [[], parent_seeds[:1]] if parent_seeds else [[]]:
                for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
                    estimated = average_selectivity(list(outer_preds) + list(inner_preds))
                    bucket = bucket_for_selectivity(estimated)
                    families = sorted({p.family for p in list(outer_preds) + list(inner_preds)}) or ["subquery"]
                    for pw, pcols in proj_options[:2]:
                        candidates.append(SubqueryCandidate(
                            stage_id="2_table_scalar",
                            tables=[parent.name, child.name],
                            base_table=parent.name,
                            subquery_type="scalar",
                            outer_predicates=list(outer_preds),
                            inner_predicates=list(inner_preds),
                            correlation_outer_table=parent.name,
                            correlation_outer_alias=parent_alias,
                            correlation_outer_col=edge.parent_column,
                            correlation_inner_table=child.name,
                            correlation_inner_alias=child_alias,
                            correlation_inner_col=edge.child_column,
                            scalar_agg_func=func,
                            scalar_agg_col=agg_col.name,
                            projections=pcols,
                            projection_width=pw,
                            predicate_families=families,
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family="scalar_subquery",
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

    return candidates


def _build_not_in_subquery_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """NOT IN (subquery) candidates — exercises NullAwareAntiJoin operators."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]
        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)
        parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=3, build_context=build_context)

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
            for outer_preds in [[], parent_seeds[:1]] if parent_seeds else [[]]:
                estimated = average_selectivity(list(outer_preds) + list(inner_preds))
                bucket = bucket_for_selectivity(estimated)
                families = sorted({p.family for p in list(outer_preds) + list(inner_preds)}) or ["subquery"]
                for pw, pcols in proj_options[:2]:
                    candidates.append(SubqueryCandidate(
                        stage_id="2_table_not_in",
                        tables=[parent.name, child.name],
                        base_table=parent.name,
                        subquery_type="not_in_subquery",
                        outer_predicates=list(outer_preds),
                        inner_predicates=list(inner_preds),
                        correlation_outer_table=parent.name,
                        correlation_outer_alias=parent_alias,
                        correlation_outer_col=edge.parent_column,
                        correlation_inner_table=child.name,
                        correlation_inner_alias=child_alias,
                        correlation_inner_col=edge.child_column,
                        in_column=edge.child_column,
                        in_alias=child_alias,
                        projections=pcols,
                        projection_width=pw,
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="not_in_subquery",
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

    return candidates


def _build_quantified_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SubqueryCandidate]:
    """Quantified comparison subqueries: WHERE outer.col > ALL / < ANY (SELECT inner.col ...)."""
    candidates: list[SubqueryCandidate] = []

    for edge in join_edges:
        if edge.child_table not in catalog or edge.parent_table not in catalog:
            continue
        parent = catalog[edge.parent_table]
        child = catalog[edge.child_table]

        # Need numeric columns on both sides for meaningful comparisons
        outer_num = _pick_scalar_agg_column(parent, set())
        inner_num = _pick_scalar_agg_column(child, set())
        if outer_num is None or inner_num is None:
            continue

        parent_alias = alias_map([parent.name])[parent.name]
        child_alias = alias_map([child.name])[child.name]
        if parent_alias == child_alias:
            child_alias = child_alias + "2"

        child_seeds = top_seed_pool(child, child_alias, snapshot, limit=3, build_context=build_context)

        proj_options = cached_projection_options(
            [parent.name], {parent.name: parent_alias}, catalog, build_context,
        )

        for op in ("> ALL", "< ALL", ">= ANY", "<= ANY"):
            rule_family = "quantified_all" if "ALL" in op else "quantified_any"
            for inner_preds in [[], child_seeds[:1]] if child_seeds else [[]]:
                estimated = average_selectivity(list(inner_preds))
                bucket = bucket_for_selectivity(estimated)
                families = sorted({p.family for p in inner_preds}) or ["subquery"]
                for pw, pcols in proj_options[:1]:
                    candidates.append(SubqueryCandidate(
                        stage_id="2_table_quantified",
                        tables=[parent.name, child.name],
                        base_table=parent.name,
                        subquery_type="quantified",
                        outer_predicates=[],
                        inner_predicates=list(inner_preds),
                        correlation_outer_table=parent.name,
                        correlation_outer_alias=parent_alias,
                        correlation_outer_col=edge.parent_column,
                        correlation_inner_table=child.name,
                        correlation_inner_alias=child_alias,
                        correlation_inner_col=inner_num.name,
                        quantified_op=op,
                        quantified_compare_col=outer_num.name,
                        projections=pcols,
                        projection_width=pw,
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family=rule_family,
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    ))

    return candidates


# ---------------------------------------------------------------------------
# Deduplication & selection
# ---------------------------------------------------------------------------

def unique_subquery_candidates(candidates: list[SubqueryCandidate]) -> list[SubqueryCandidate]:
    deduped: dict[tuple[Any, ...], SubqueryCandidate] = {}
    for c in candidates:
        deduped.setdefault(c.structural_signature(), c)
    return list(deduped.values())


def select_subquery_candidates(
    candidates: list[SubqueryCandidate],
    budget: int,
    seed: int,
) -> list[SubqueryCandidate]:
    rng = random.Random(seed)
    unique = unique_subquery_candidates(candidates)
    rng.shuffle(unique)
    # Round-robin by rule_family
    groups: dict[str, list[SubqueryCandidate]] = defaultdict(list)
    for c in unique:
        groups[c.rule_family].append(c)
    selected: list[SubqueryCandidate] = []
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


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

def emit_subquery_artifacts(
    selected: list[SubqueryCandidate],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"sub_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(
            filename=candidate.filename,
            sql=candidate.render_sql(),
            metadata=metadata,
        ))
    manifest = {
        "branch": "subquery",
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


# ---------------------------------------------------------------------------
# Default budgets
# ---------------------------------------------------------------------------

def default_subquery_stage_budgets() -> dict[str, int]:
    return {
        "1_table_sub": 18,
        "2_table_semi": 18,
        "2_table_anti": 18,
        "2_table_derived": 12,
        "2_table_scalar": 18,
        "2_table_not_in": 12,
        "2_table_quantified": 12,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_subquery_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    diagnostics: dict[str, dict[str, int]] = defaultdict(lambda: {"generated": 0, "selected": 0})

    all_candidates: list[SubqueryCandidate] = []
    all_candidates.extend(_build_exists_candidates(join_edges, catalog, snapshot, config, build_context))
    all_candidates.extend(_build_in_subquery_candidates(join_edges, catalog, snapshot, config, build_context))
    all_candidates.extend(_build_derived_table_candidates(join_edges, catalog, snapshot, config, build_context))
    all_candidates.extend(_build_scalar_subquery_candidates(join_edges, catalog, snapshot, config, build_context))
    all_candidates.extend(_build_not_in_subquery_candidates(join_edges, catalog, snapshot, config, build_context))
    all_candidates.extend(_build_quantified_candidates(join_edges, catalog, snapshot, config, build_context))

    budgets = default_subquery_stage_budgets()
    for stage_id, budget in config.stage_budgets.items():
        if stage_id in budgets:
            budgets[stage_id] = budget

    by_stage: dict[str, list[SubqueryCandidate]] = defaultdict(list)
    for c in all_candidates:
        by_stage[c.stage_id].append(c)

    selected: list[SubqueryCandidate] = []
    for stage_id in sorted(budgets):
        budget = budgets[stage_id]
        if budget <= 0:
            continue
        available = by_stage.get(stage_id, [])
        diagnostics[stage_id]["generated"] = len(available)
        if not available:
            continue
        chosen = select_subquery_candidates(available, min(budget, len(available)), config.seed)
        diagnostics[stage_id]["selected"] = len(chosen)
        selected.extend(chosen)

    selected = sorted(
        unique_subquery_candidates(selected),
        key=lambda item: (item.stage_id, item.rule_family, item.signature()),
    )
    return emit_subquery_artifacts(
        selected, config.benchmark, schema_path, config.suffix, config.seed,
        diagnostics=dict(diagnostics),
    )
