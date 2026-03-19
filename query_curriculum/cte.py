"""CTE (Common Table Expression) curriculum generator.

Generates non-recursive CTEs wrapping existing query patterns to exercise
CTEScan and materialization decision boundaries.
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


def _schema_fallback_seeds(table: TableSchema, table_alias: str) -> list[PredicateSpec]:
    """Generate basic predicates from schema alone when stats are unavailable."""
    seeds: list[PredicateSpec] = []
    for col in table.columns.values():
        if col.nullable and not col.is_primary_key:
            seeds.append(PredicateSpec(
                table=table.name, alias=table_alias, column=col.name,
                operator="is_not_null", family="null", selectivity=0.95, placement="where",
            ))
            if len(seeds) >= 4:
                break
    return seeds
from query_curriculum.agg import _good_group_by_columns, _agg_functions_for_table


@dataclass
class CteCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    cte_name: str
    cte_body_sql: str  # rendered SQL for the CTE body (without WITH...AS wrapper)
    outer_sql: str  # rendered SQL for the outer query referencing the CTE
    rule_family: str
    predicates: list[PredicateSpec]
    projections: list[tuple[str, str, str]]
    projection_width: str
    predicate_families: list[str]
    target_selectivity_bucket: str
    estimated_selectivity: float | None
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
            "cte",
            self.stage_id,
            self.base_table,
            self.rule_family,
            self.cte_body_sql,
            self.outer_sql,
        )
        self._cached_signature = sig
        return sig

    def signature(self) -> str:
        return repr(self.structural_signature())

    def render_sql(self) -> str:
        return f"WITH {self.cte_name} AS (\n{self.cte_body_sql}\n)\n{self.outer_sql}\n;\n"

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "cte",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "rule_family": self.rule_family,
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

def _build_cte_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[CteCandidate]:
    """Generate CTE candidates wrapping single-table scans and aggregates."""
    candidates: list[CteCandidate] = []

    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        table_alias = aliases[table_name]
        col_stats = snapshot.column_stats.get(table_name, {})
        table_count = snapshot.table_counts.get(table_name, 1000)

        seeds = top_seed_pool(table, table_alias, snapshot, limit=4, build_context=build_context)
        if not seeds:
            seeds = _schema_fallback_seeds(table, table_alias)
        proj_options = cached_projection_options([table_name], aliases, catalog, build_context)

        # Pattern 1: CTE wrapping a filtered scan, outer query selects all from CTE
        for pred_group in [seeds[:1], seeds[:2]] if len(seeds) >= 2 else ([seeds[:1]] if seeds else []):
            estimated = average_selectivity(pred_group)
            bucket = bucket_for_selectivity(estimated)
            families = sorted({p.family for p in pred_group}) or ["scan"]

            for pw, pcols in proj_options[:2]:
                col_list = ", ".join(f"{a}.{c}" for _, a, c in pcols) or "*"
                where_parts = [render_predicate(p) for p in pred_group]
                where_sql = f"\n  WHERE {' AND '.join(where_parts)}" if where_parts else ""

                cte_body = f"  SELECT {col_list}\n  FROM {table_name} AS {table_alias}{where_sql}"
                cte_name = "filtered_data"
                outer_cols = ", ".join(c for _, _, c in pcols) or "*"
                outer_sql = f"SELECT {outer_cols}\nFROM {cte_name}"

                candidates.append(CteCandidate(
                    stage_id="1_table_cte",
                    tables=[table_name],
                    base_table=table_name,
                    cte_name=cte_name,
                    cte_body_sql=cte_body,
                    outer_sql=outer_sql,
                    rule_family="cte_filter_scan",
                    predicates=list(pred_group),
                    projections=pcols,
                    projection_width=pw,
                    predicate_families=families,
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

        # Pattern 2: CTE wrapping an aggregate, outer query filters on aggregate result
        gb_cols = _good_group_by_columns(table, col_stats, table_count)
        all_aggs = _agg_functions_for_table(table, table_alias, col_stats)
        if gb_cols and all_aggs:
            gb_col = gb_cols[0]
            agg = all_aggs[0]

            # CTE: aggregate query
            if agg.func == "COUNT_STAR":
                agg_expr = f"COUNT(*) AS {agg.output_alias}"
            elif agg.func == "COUNT_DISTINCT":
                agg_expr = f"COUNT(DISTINCT {table_alias}.{agg.column}) AS {agg.output_alias}"
            else:
                agg_expr = f"{agg.func}({table_alias}.{agg.column}) AS {agg.output_alias}"

            cte_body = (
                f"  SELECT {table_alias}.{gb_col.name}, {agg_expr}\n"
                f"  FROM {table_name} AS {table_alias}\n"
                f"  GROUP BY {table_alias}.{gb_col.name}"
            )
            cte_name = "agg_data"
            # Outer: filter on aggregate result
            threshold = max(1, table_count // 100)
            outer_sql = f"SELECT {gb_col.name}, {agg.output_alias}\nFROM {cte_name}\nWHERE {agg.output_alias} > {threshold}"

            for pw, pcols in proj_options[:1]:
                candidates.append(CteCandidate(
                    stage_id="1_table_cte",
                    tables=[table_name],
                    base_table=table_name,
                    cte_name=cte_name,
                    cte_body_sql=cte_body,
                    outer_sql=outer_sql,
                    rule_family="cte_aggregate",
                    predicates=[],
                    projections=pcols,
                    projection_width=pw,
                    predicate_families=["scan"],
                    target_selectivity_bucket="medium",
                    estimated_selectivity=0.10,
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

        # Pattern 3: CTE referenced multiple times (materialization boundary)
        if seeds:
            for pw, pcols in proj_options[:1]:
                col_list = ", ".join(f"{a}.{c}" for _, a, c in pcols) or "*"
                where_parts = [render_predicate(seeds[0])]
                cte_body = f"  SELECT {col_list}\n  FROM {table_name} AS {table_alias}\n  WHERE {where_parts[0]}"
                cte_name = "reused_data"
                first_col = pcols[0][2] if pcols else "*"
                outer_sql = (
                    f"SELECT a.{first_col}, COUNT(*) AS cnt\n"
                    f"FROM {cte_name} AS a\n"
                    f"INNER JOIN {cte_name} AS b ON a.{first_col} = b.{first_col}\n"
                    f"GROUP BY a.{first_col}"
                )
                candidates.append(CteCandidate(
                    stage_id="1_table_cte",
                    tables=[table_name],
                    base_table=table_name,
                    cte_name=cte_name,
                    cte_body_sql=cte_body,
                    outer_sql=outer_sql,
                    rule_family="cte_multi_ref",
                    predicates=[seeds[0]],
                    projections=pcols,
                    projection_width=pw,
                    predicate_families=[seeds[0].family],
                    target_selectivity_bucket=bucket_for_selectivity(seeds[0].selectivity),
                    estimated_selectivity=seeds[0].selectivity,
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                ))

    return candidates


# ---------------------------------------------------------------------------
# Selection & output
# ---------------------------------------------------------------------------

def unique_cte_candidates(candidates: list[CteCandidate]) -> list[CteCandidate]:
    deduped: dict[tuple[Any, ...], CteCandidate] = {}
    for c in candidates:
        deduped.setdefault(c.structural_signature(), c)
    return list(deduped.values())


def select_cte_candidates(
    candidates: list[CteCandidate],
    budget: int,
    seed: int,
) -> list[CteCandidate]:
    rng = random.Random(seed)
    unique = unique_cte_candidates(candidates)
    rng.shuffle(unique)
    groups: dict[str, list[CteCandidate]] = defaultdict(list)
    for c in unique:
        groups[c.rule_family].append(c)
    selected: list[CteCandidate] = []
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


def emit_cte_artifacts(
    selected: list[CteCandidate],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"cte_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(
            filename=candidate.filename,
            sql=candidate.render_sql(),
            metadata=metadata,
        ))
    manifest = {
        "branch": "cte",
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


def default_cte_stage_budgets() -> dict[str, int]:
    return {"1_table_cte": 18}


def generate_cte_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    diagnostics: dict[str, dict[str, int]] = defaultdict(lambda: {"generated": 0, "selected": 0})

    all_candidates = _build_cte_candidates(catalog, snapshot, config, build_context)
    budgets = default_cte_stage_budgets()
    for stage_id, budget in config.stage_budgets.items():
        if stage_id in budgets:
            budgets[stage_id] = budget

    by_stage: dict[str, list[CteCandidate]] = defaultdict(list)
    for c in all_candidates:
        by_stage[c.stage_id].append(c)

    selected: list[CteCandidate] = []
    for stage_id in sorted(budgets):
        budget = budgets[stage_id]
        if budget <= 0:
            continue
        available = by_stage.get(stage_id, [])
        diagnostics[stage_id]["generated"] = len(available)
        if not available:
            continue
        chosen = select_cte_candidates(available, min(budget, len(available)), config.seed)
        diagnostics[stage_id]["selected"] = len(chosen)
        selected.extend(chosen)

    selected = sorted(
        unique_cte_candidates(selected),
        key=lambda item: (item.stage_id, item.rule_family, item.signature()),
    )
    return emit_cte_artifacts(
        selected, config.benchmark, schema_path, config.suffix, config.seed,
        diagnostics=dict(diagnostics),
    )
