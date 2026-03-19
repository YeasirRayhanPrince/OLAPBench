"""Set operations curriculum generator.

Generates UNION / UNION ALL / INTERSECT / EXCEPT queries to exercise
Append, SetOp, and MergeAppend physical operators.
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
    cached_projection_options,
    make_build_context,
    render_predicate,
    top_seed_pool,
    _hashable_value,
)


SET_OPS = ("UNION", "UNION ALL", "INTERSECT", "EXCEPT")


@dataclass
class SetOpCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    set_op: str  # UNION, UNION ALL, INTERSECT, EXCEPT
    left_predicates: list[PredicateSpec]
    right_predicates: list[PredicateSpec]
    projections: list[tuple[str, str, str]]  # shared projection columns
    projection_width: str
    predicate_families: list[str]
    target_selectivity_bucket: str
    estimated_selectivity: float | None
    rule_family: str
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
            "setop",
            self.stage_id,
            self.base_table,
            self.set_op,
            self.rule_family,
            tuple(
                (p.table, p.column, p.operator, _hashable_value(p.value), p.family)
                for p in self.left_predicates
            ),
            tuple(
                (p.table, p.column, p.operator, _hashable_value(p.value), p.family)
                for p in self.right_predicates
            ),
            tuple(self.projections),
        )
        self._cached_signature = sig
        return sig

    def signature(self) -> str:
        return repr(self.structural_signature())

    def render_sql(self) -> str:
        aliases = alias_map([self.base_table])
        table_alias = aliases[self.base_table]
        proj_sql = ", ".join(f"{table_alias}.{col}" for _, _, col in self.projections) or "*"

        # Left branch
        left_parts = [f"SELECT {proj_sql}", f"FROM {self.base_table} AS {table_alias}"]
        left_where = [render_predicate(p) for p in self.left_predicates]
        if left_where:
            left_parts.append("WHERE " + " AND ".join(left_where))
        left_sql = "\n".join(left_parts)

        # Right branch (same table, different predicates)
        right_parts = [f"SELECT {proj_sql}", f"FROM {self.base_table} AS {table_alias}"]
        right_where = [render_predicate(p) for p in self.right_predicates]
        if right_where:
            right_parts.append("WHERE " + " AND ".join(right_where))
        right_sql = "\n".join(right_parts)

        return f"{left_sql}\n{self.set_op}\n{right_sql}\n;\n"

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "setop",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "set_op": self.set_op,
            "rule_family": self.rule_family,
            "target_selectivity_bucket": self.target_selectivity_bucket,
            "calibrated": self.calibrated,
            "calibration_source": self.calibration_source,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "projection_columns": [{"table": t, "column": c} for t, _, c in self.projections],
            "left_predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator}
                for p in self.left_predicates
            ],
            "right_predicates": [
                {"table": p.table, "column": p.column, "operator": p.operator}
                for p in self.right_predicates
            ],
        }


# ---------------------------------------------------------------------------
# Candidate builder
# ---------------------------------------------------------------------------

def _build_setop_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SetOpCandidate]:
    """Generate set operation candidates from same-table query pairs."""
    candidates: list[SetOpCandidate] = []

    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        table_alias = aliases[table_name]

        seeds = top_seed_pool(table, table_alias, snapshot, limit=6, build_context=build_context)
        # Schema-only fallback: generate IS NULL / IS NOT NULL predicates for nullable columns
        if len(seeds) < 2:
            nullable_cols = [c for c in table.columns.values() if c.nullable and not c.is_primary_key]
            for col in nullable_cols[:3]:
                seeds.append(PredicateSpec(
                    table=table_name, alias=table_alias, column=col.name,
                    operator="is_null", family="null", selectivity=0.05, placement="where",
                ))
                seeds.append(PredicateSpec(
                    table=table_name, alias=table_alias, column=col.name,
                    operator="is_not_null", family="null", selectivity=0.95, placement="where",
                ))
        if len(seeds) < 2:
            continue

        proj_options = cached_projection_options([table_name], aliases, catalog, build_context)

        # Generate pairs of different predicates
        for i in range(min(len(seeds), 4)):
            for j in range(i + 1, min(len(seeds), 5)):
                left_preds = [seeds[i]]
                right_preds = [seeds[j]]
                estimated = average_selectivity(left_preds + right_preds)
                bucket = bucket_for_selectivity(estimated)
                families = sorted({p.family for p in left_preds + right_preds})

                for set_op in SET_OPS:
                    for pw, pcols in proj_options[:2]:
                        candidates.append(SetOpCandidate(
                            stage_id="1_table_setop",
                            tables=[table_name],
                            base_table=table_name,
                            set_op=set_op,
                            left_predicates=left_preds,
                            right_predicates=right_preds,
                            projections=pcols,
                            projection_width=pw,
                            predicate_families=families,
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family=set_op.lower().replace(" ", "_"),
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        ))

    return candidates


# ---------------------------------------------------------------------------
# Selection & output
# ---------------------------------------------------------------------------

def unique_setop_candidates(candidates: list[SetOpCandidate]) -> list[SetOpCandidate]:
    deduped: dict[tuple[Any, ...], SetOpCandidate] = {}
    for c in candidates:
        deduped.setdefault(c.structural_signature(), c)
    return list(deduped.values())


def select_setop_candidates(
    candidates: list[SetOpCandidate],
    budget: int,
    seed: int,
) -> list[SetOpCandidate]:
    rng = random.Random(seed)
    unique = unique_setop_candidates(candidates)
    rng.shuffle(unique)
    groups: dict[str, list[SetOpCandidate]] = defaultdict(list)
    for c in unique:
        groups[c.set_op].append(c)
    selected: list[SetOpCandidate] = []
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


def emit_setop_artifacts(
    selected: list[SetOpCandidate],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    diagnostics: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"setop_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(
            filename=candidate.filename,
            sql=candidate.render_sql(),
            metadata=metadata,
        ))
    manifest = {
        "branch": "setop",
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


def default_setop_stage_budgets() -> dict[str, int]:
    return {"1_table_setop": 24}


def generate_setop_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    diagnostics: dict[str, dict[str, int]] = defaultdict(lambda: {"generated": 0, "selected": 0})

    all_candidates = _build_setop_candidates(catalog, snapshot, config, build_context)
    budgets = default_setop_stage_budgets()
    for stage_id, budget in config.stage_budgets.items():
        if stage_id in budgets:
            budgets[stage_id] = budget

    by_stage: dict[str, list[SetOpCandidate]] = defaultdict(list)
    for c in all_candidates:
        by_stage[c.stage_id].append(c)

    selected: list[SetOpCandidate] = []
    for stage_id in sorted(budgets):
        budget = budgets[stage_id]
        if budget <= 0:
            continue
        available = by_stage.get(stage_id, [])
        diagnostics[stage_id]["generated"] = len(available)
        if not available:
            continue
        chosen = select_setop_candidates(available, min(budget, len(available)), config.seed)
        diagnostics[stage_id]["selected"] = len(chosen)
        selected.extend(chosen)

    selected = sorted(
        unique_setop_candidates(selected),
        key=lambda item: (item.stage_id, item.set_op, item.signature()),
    )
    return emit_setop_artifacts(
        selected, config.benchmark, schema_path, config.suffix, config.seed,
        diagnostics=dict(diagnostics),
    )
