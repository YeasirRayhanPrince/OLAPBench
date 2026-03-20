from __future__ import annotations

import itertools
import random
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field, replace
from math import ceil
from numbers import Real
from typing import Any

from query_curriculum.core import (
    BUCKET_ORDER,
    BUCKET_TARGETS,
    ColumnSchema,
    ColumnStats,
    alias_map,
    bucket_for_selectivity,
    GeneratorConfig,
    JoinEdge,
    normalize_sql,
    QueryArtifact,
    sql_literal,
    StatsSnapshot,
    TableSchema,
    validate_manifest_entry,
)
from query_curriculum.templates import apply_template_packs, register_template_pack


JOB_LIKE_TEMPLATE_PACK = "job_like_implicit_joins"
SEED_BUCKET_PRIORITY = ("medium", "high", "low", "very_high", "very_low")
SEED_FAMILY_PRIORITY = ("range", "membership", "equality", "null")
MAX_MCV_PREDICATES_PER_COLUMN = 6
MEMBERSHIP_WIDTHS = (2, 3, 4)
RANGE_QUANTILES = (0.15, 0.35, 0.50, 0.70, 0.85)
MAX_RANGE_PREDICATES_PER_COLUMN = 6
MIN_TOPOLOGY_RETAIN = 8
PROBE_BATCH_MULTIPLIER = 2
MAX_TOPOLOGIES = 64


@dataclass(frozen=True)
class SeedTemplate:
    table: str
    column: str
    operator: str
    value: Any = None
    family: str = "generic"
    selectivity: float | None = None
    placement: str = "where"


@dataclass
class SpjBuildContext:
    catalog: dict[str, TableSchema]
    snapshot: StatsSnapshot
    config: GeneratorConfig
    seed_templates_by_table: dict[str, list[SeedTemplate]]
    top_seed_templates_by_table_limit: dict[tuple[str, int], list[SeedTemplate]]
    projection_templates_by_tables: dict[tuple[str, ...], list[tuple[str, list[tuple[str, str]]]]]
    paths_by_length: dict[int, list[list[JoinEdge]]]
    stars_by_size: dict[int, list[list[JoinEdge]]]
    diagnostics: dict[str, dict[str, int]]


@dataclass(frozen=True)
class PredicateSpec:
    table: str
    alias: str
    column: str
    operator: str
    value: Any = None
    family: str = "generic"
    selectivity: float | None = None
    placement: str = "where"


def bind_seed_template(seed: SeedTemplate, alias: str) -> PredicateSpec:
    return PredicateSpec(
        table=seed.table,
        alias=alias,
        column=seed.column,
        operator=seed.operator,
        value=seed.value,
        family=seed.family,
        selectivity=seed.selectivity,
        placement=seed.placement,
    )


@dataclass(frozen=True)
class JoinSpec:
    join_type: str
    left_table: str
    left_alias: str
    left_column: str
    right_table: str
    right_alias: str
    right_column: str
    edge: JoinEdge
    on_predicates: tuple[PredicateSpec, ...] = ()


@dataclass
class SpjCandidate:
    stage_id: str
    tables: list[str]
    base_table: str
    join_type: str
    join_topology: str
    joins: list[JoinSpec]
    predicates: list[PredicateSpec]
    projections: list[tuple[str, str, str]]
    projection_width: str
    predicate_families: list[str]
    target_selectivity_bucket: str
    estimated_selectivity: float | None
    rule_family: str
    render_mode: str = "ansi"
    template_pack: str | None = None
    calibrated: bool = False
    calibration_source: str = "schema"
    observed_rows: int | None = None
    observed_selectivity: float | None = None
    filename: str | None = None
    query_id: str | None = None
    _cached_signature: tuple[Any, ...] | None = field(default=None, init=False, repr=False, compare=False)
    _cached_sql: str | None = field(default=None, init=False, repr=False, compare=False)

    def structural_signature(self) -> tuple[Any, ...]:
        if self._cached_signature is not None:
            return self._cached_signature
        sig = (
            self.stage_id,
            tuple(self.tables),
            self.base_table,
            self.join_type,
            self.join_topology,
            self.rule_family,
            self.projection_width,
            self.render_mode,
            self.template_pack,
            tuple(
                (
                    join.join_type,
                    join.left_table,
                    join.left_alias,
                    join.left_column,
                    join.right_table,
                    join.right_alias,
                    join.right_column,
                    join.edge.key,
                    tuple(
                        (
                            predicate.table,
                            predicate.alias,
                            predicate.column,
                            predicate.operator,
                            _hashable_value(predicate.value),
                            predicate.family,
                            predicate.placement,
                        )
                        for predicate in join.on_predicates
                    ),
                )
                for join in self.joins
            ),
            tuple(
                (
                    predicate.table,
                    predicate.alias,
                    predicate.column,
                    predicate.operator,
                    _hashable_value(predicate.value),
                    predicate.family,
                    predicate.placement,
                )
                for predicate in self.predicates
            ),
            tuple(self.projections),
        )
        self._cached_signature = sig
        return sig

    def _invalidate_signature_cache(self) -> None:
        self._cached_signature = None
        self._cached_sql = None

    def signature(self) -> str:
        return repr(self.structural_signature())

    def render_sql(self) -> str:
        if self._cached_sql is not None:
            return self._cached_sql
        if self.render_mode == "job_like":
            sql = render_job_like_sql(self)
        else:
            projection_sql = ", ".join(f"{alias}.{column}" for _, alias, column in self.projections) or "*"
            lines = [f"SELECT {projection_sql}", f"FROM {self.base_table} AS {self.aliases()[self.base_table]}"]
            for join in self.joins:
                on_parts = [f"{join.left_alias}.{join.left_column} = {join.right_alias}.{join.right_column}"]
                for predicate in join.on_predicates:
                    on_parts.append(render_predicate(predicate))
                lines.append(f"{join.join_type.upper()} JOIN {join.right_table} AS {join.right_alias}")
                lines.append("  ON " + "\n  AND ".join(on_parts))
            where_predicates = [render_predicate(predicate) for predicate in self.predicates if predicate.placement == "where"]
            if where_predicates:
                lines.append("WHERE " + "\n  AND ".join(where_predicates))
            lines.append(";")
            sql = "\n".join(lines) + "\n"
        self._cached_sql = sql
        return sql

    def aliases(self) -> dict[str, str]:
        aliases = {self.base_table: self.projections[0][1]} if self.projections else {}
        aliases[self.base_table] = aliases.get(self.base_table) or self.base_table[0]
        for join in self.joins:
            aliases[join.left_table] = join.left_alias
            aliases[join.right_table] = join.right_alias
        for table_name, alias, _ in self.projections:
            aliases[table_name] = alias
        return aliases

    def to_manifest(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "branch": "spj",
            "stage_id": self.stage_id,
            "tables": self.tables,
            "join_edges": [join.edge.as_manifest() for join in self.joins],
            "join_type": self.join_type,
            "join_topology": self.join_topology,
            "predicate_count": len(self.predicates) + sum(len(join.on_predicates) for join in self.joins),
            "predicate_families": self.predicate_families,
            "projection_width": self.projection_width,
            "target_selectivity_bucket": self.target_selectivity_bucket,
            "calibrated": self.calibrated,
            "calibration_source": self.calibration_source,
            "rule_family": self.rule_family,
            "template_pack": self.template_pack,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "projection_columns": [{"table": table, "column": column} for table, _, column in self.projections],
            "predicates": [
                {"table": predicate.table, "column": predicate.column, "operator": predicate.operator, "placement": predicate.placement}
                for predicate in self.predicates
            ]
            + [
                {"table": predicate.table, "column": predicate.column, "operator": predicate.operator, "placement": predicate.placement}
                for join in self.joins
                for predicate in join.on_predicates
            ],
        }


def render_predicate(predicate: PredicateSpec) -> str:
    qualified = f"{predicate.alias}.{predicate.column}"
    if predicate.operator == "is_null":
        return f"{qualified} IS NULL"
    if predicate.operator == "is_not_null":
        return f"{qualified} IS NOT NULL"
    if predicate.operator == "between":
        lower, upper = predicate.value
        return f"{qualified} BETWEEN {sql_literal(lower)} AND {sql_literal(upper)}"
    if predicate.operator == "in":
        values = ", ".join(sql_literal(value) for value in predicate.value)
        return f"{qualified} IN ({values})"
    if predicate.operator == "like":
        return f"{qualified} LIKE {sql_literal(predicate.value)}"
    return f"{qualified} {predicate.operator} {sql_literal(predicate.value)}"


def render_job_like_sql(candidate: SpjCandidate) -> str:
    aliases = candidate.aliases()
    projection_sql = ", ".join(f"{alias}.{column}" for _, alias, column in candidate.projections) or "*"
    from_sql = ", ".join(f"{table_name} AS {aliases[table_name]}" for table_name in candidate.tables)
    where_parts = [f"{join.left_alias}.{join.left_column} = {join.right_alias}.{join.right_column}" for join in candidate.joins]
    where_parts.extend(render_predicate(predicate) for join in candidate.joins for predicate in join.on_predicates)
    where_parts.extend(render_predicate(predicate) for predicate in candidate.predicates if predicate.placement == "where")

    lines = [f"SELECT {projection_sql}", f"FROM {from_sql}"]
    if where_parts:
        lines.append("WHERE " + "\n  AND ".join(where_parts))
    lines.append(";")
    return "\n".join(lines) + "\n"


def stage_key(table_count: int) -> str:
    return f"{table_count}_table"


def _hashable_value(value: Any) -> Any:
    if isinstance(value, list):
        return tuple(_hashable_value(item) for item in value)
    if isinstance(value, tuple):
        return tuple(_hashable_value(item) for item in value)
    return value


def _diagnostic_bucket() -> dict[str, int]:
    return {
        "generated_before_prune": 0,
        "retained_before_probe": 0,
        "probed": 0,
        "selected": 0,
        "probe_calls": 0,
    }


def make_build_context(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
) -> SpjBuildContext:
    return SpjBuildContext(
        catalog=catalog,
        snapshot=snapshot,
        config=config,
        seed_templates_by_table={},
        top_seed_templates_by_table_limit={},
        projection_templates_by_tables={},
        paths_by_length={},
        stars_by_size={},
        diagnostics=defaultdict(_diagnostic_bucket),
    )


def build_projection_templates(table_specs: list[TableSchema]) -> list[tuple[str, list[tuple[str, str]]]]:
    all_columns = [(table.name, column.name) for table in table_specs for column in table.columns.values()]
    preferred: list[tuple[str, str]] = []
    for table in table_specs:
        ordered = sorted(
            table.columns.values(),
            key=lambda column: (
                0 if column.is_primary_key else 1,
                0 if column.is_foreign_key else 1,
                column.name,
            ),
        )
        preferred.extend((table.name, column.name) for column in ordered)
    options = []
    if preferred:
        options.append(("narrow", preferred[:1]))
        options.append(("medium", preferred[: min(3, len(preferred))]))
        options.append(("wide", preferred[: min(5, len(preferred))]))
        options.append(("star", all_columns))
    return options


def bind_projection_templates(
    projection_templates: list[tuple[str, list[tuple[str, str]]]],
    aliases: dict[str, str],
) -> list[tuple[str, list[tuple[str, str, str]]]]:
    return [
        (projection_width, [(table_name, aliases[table_name], column_name) for table_name, column_name in projection_columns])
        for projection_width, projection_columns in projection_templates
    ]


def build_projection_options(table_specs: list[TableSchema], aliases: dict[str, str]) -> list[tuple[str, list[tuple[str, str, str]]]]:
    return bind_projection_templates(build_projection_templates(table_specs), aliases)


def estimate_from_stats(column: ColumnSchema, stats: ColumnStats | None, family: str) -> float | None:
    if stats is None:
        return None
    if family == "null" and stats.null_frac is not None and 0.0 < stats.null_frac < 1.0:
        return max(0.001, min(0.999, float(stats.null_frac)))
    if family == "range":
        histogram = [bound for bound in stats.histogram_bounds if isinstance(bound, Real)]
        if len(histogram) >= 4:
            return max(0.01, 1.0 / max(4, len(histogram) - 1))
    if family == "membership" and stats.most_common_freqs:
        return max(0.001, min(0.95, sum(float(value) for value in stats.most_common_freqs[:2])))
    if family == "equality" and stats.most_common_freqs:
        return max(0.001, min(0.95, float(stats.most_common_freqs[0])))
    return None


def _clamp_selectivity(value: float | None) -> float | None:
    if value is None:
        return None
    return max(0.0001, min(0.99, float(value)))


def _meaningful_null_frac(stats: ColumnStats | None) -> bool:
    return stats is not None and stats.null_frac is not None and 0.0 < stats.null_frac < 1.0


def _spread_sample_indices(count: int, limit: int) -> list[int]:
    if count <= 0 or limit <= 0:
        return []
    if count <= limit:
        return list(range(count))
    if limit == 1:
        return [0]
    indices: list[int] = []
    for position in range(limit):
        index = round(position * (count - 1) / (limit - 1))
        if index not in indices:
            indices.append(index)
    next_index = 0
    while len(indices) < limit and next_index < count:
        if next_index not in indices:
            indices.append(next_index)
        next_index += 1
    return sorted(indices)


def _sampled_mcv_entries(stats: ColumnStats | None, limit: int = MAX_MCV_PREDICATES_PER_COLUMN) -> list[tuple[Any, float]]:
    if stats is None:
        return []
    entries = [
        (value, float(freq))
        for value, freq in zip(stats.most_common_vals, stats.most_common_freqs)
        if freq is not None
    ]
    if not entries:
        return []
    return [entries[index] for index in _spread_sample_indices(len(entries), min(limit, len(entries)))]


def _membership_windows(entries: list[tuple[Any, float]]) -> list[tuple[list[Any], float]]:
    groups: list[tuple[list[Any], float]] = []
    seen: set[tuple[Any, ...]] = set()
    for width in MEMBERSHIP_WIDTHS:
        if len(entries) < width:
            continue
        candidate_windows = [entries[:width]]
        if len(entries) > width:
            candidate_windows.append(entries[-width:])
        if len(entries) > width + 1:
            start = max(0, (len(entries) - width) // 2)
            candidate_windows.append(entries[start : start + width])
        for window in candidate_windows:
            values = tuple(value for value, _ in window)
            if values in seen:
                continue
            seen.add(values)
            groups.append((list(values), _clamp_selectivity(sum(freq for _, freq in window)) or 0.0))
    return groups


def _range_intervals(stats: ColumnStats | None) -> list[tuple[str, Any, float]]:
    if stats is None:
        return []
    histogram = [bound for bound in stats.histogram_bounds if isinstance(bound, Real)]
    if len(histogram) < 4:
        return []
    cut_points: list[tuple[float, Any]] = []
    for quantile in sorted(set(RANGE_QUANTILES)):
        index = min(len(histogram) - 2, max(1, int((len(histogram) - 1) * quantile)))
        value = histogram[index]
        if cut_points and cut_points[-1][1] == value:
            continue
        cut_points.append((quantile, value))
    if len(cut_points) < 2:
        return []

    intervals: list[tuple[str, Any, float]] = [("<=", cut_points[0][1], _clamp_selectivity(cut_points[0][0]) or 0.0)]
    for (left_quantile, left_value), (right_quantile, right_value) in zip(cut_points, cut_points[1:]):
        if left_value == right_value:
            continue
        intervals.append(("between", [left_value, right_value], _clamp_selectivity(max(0.01, right_quantile - left_quantile)) or 0.0))
    intervals.append((">=", cut_points[-1][1], _clamp_selectivity(max(0.01, 1.0 - cut_points[-1][0])) or 0.0))
    return intervals[:MAX_RANGE_PREDICATES_PER_COLUMN]


def _build_null_predicates(table: TableSchema, column: ColumnSchema, stats: ColumnStats | None) -> list[SeedTemplate]:
    if not column.nullable or not _meaningful_null_frac(stats):
        return []
    assert stats is not None and stats.null_frac is not None
    return [
        SeedTemplate(
            table=table.name,
            column=column.name,
            operator="is_null",
            family="null",
            selectivity=_clamp_selectivity(stats.null_frac),
        ),
        SeedTemplate(
            table=table.name,
            column=column.name,
            operator="is_not_null",
            family="null",
            selectivity=_clamp_selectivity(1.0 - stats.null_frac),
        ),
    ]


def _build_mcv_predicates(table: TableSchema, column: ColumnSchema, stats: ColumnStats | None) -> list[SeedTemplate]:
    predicates: list[SeedTemplate] = []
    sampled_entries = _sampled_mcv_entries(stats)
    for value, estimate in sampled_entries:
        predicates.append(
            SeedTemplate(
                table=table.name,
                column=column.name,
                operator="=",
                value=value,
                family="equality",
                selectivity=_clamp_selectivity(estimate),
            )
        )
    for values, estimate in _membership_windows(sampled_entries):
        predicates.append(
            SeedTemplate(
                table=table.name,
                column=column.name,
                operator="in",
                value=values,
                family="membership",
                selectivity=estimate,
            )
        )
    return predicates


def _build_range_predicates(table: TableSchema, column: ColumnSchema, stats: ColumnStats | None) -> list[SeedTemplate]:
    if not column.is_numeric:
        return []
    predicates: list[SeedTemplate] = []
    for operator, value, estimate in _range_intervals(stats):
        predicates.append(
            SeedTemplate(
                table=table.name,
                column=column.name,
                operator=operator,
                value=value,
                family="range",
                selectivity=estimate,
            )
        )
    return predicates


def predicate_selection_score(predicate: PredicateSpec) -> float:
    if predicate.selectivity is None:
        return 0.0
    bucket = bucket_for_selectivity(predicate.selectivity)
    target = BUCKET_TARGETS[bucket]
    closeness = max(0.0, 1.0 - abs(predicate.selectivity - target))
    richness = 0.05 if predicate.family in {"membership", "range"} else 0.0
    return closeness + richness


def build_seed_templates(
    table: TableSchema,
    column_stats: dict[str, ColumnStats],
    *,
    llm_fallback: bool = False,
    benchmark: str = "job",
    llm_seed_model: str = "gpt-4o-mini",
) -> list[SeedTemplate]:
    predicates: list[SeedTemplate] = []
    for column in sorted(table.columns.values(), key=lambda item: (item.role, item.name)):
        stats = column_stats.get(column.name)
        predicates.extend(_build_null_predicates(table, column, stats))
        predicates.extend(_build_mcv_predicates(table, column, stats))
        predicates.extend(_build_range_predicates(table, column, stats))
    if not predicates and llm_fallback:
        from query_curriculum.llm_seeds import build_llm_seed_templates
        predicates = build_llm_seed_templates(table, benchmark=benchmark, model=llm_seed_model)
    deduped: dict[tuple[str, str, str, str], SeedTemplate] = {}
    for predicate in predicates:
        key = (predicate.column, predicate.operator, repr(predicate.value), predicate.family)
        deduped.setdefault(key, predicate)
    return list(deduped.values())


def build_predicate_seeds(
    table: TableSchema,
    alias: str,
    column_stats: dict[str, ColumnStats],
    *,
    llm_fallback: bool = False,
    benchmark: str = "job",
    llm_seed_model: str = "gpt-4o-mini",
) -> list[PredicateSpec]:
    return [bind_seed_template(seed, alias) for seed in build_seed_templates(
        table, column_stats, llm_fallback=llm_fallback, benchmark=benchmark, llm_seed_model=llm_seed_model,
    )]


def top_seed_pool(
    table: TableSchema,
    alias: str,
    snapshot: StatsSnapshot,
    *,
    limit: int = 6,
    build_context: SpjBuildContext | None = None,
    llm_fallback: bool = False,
    benchmark: str = "job",
    llm_seed_model: str = "gpt-4o-mini",
) -> list[PredicateSpec]:
    llm_kw = dict(llm_fallback=llm_fallback, benchmark=benchmark, llm_seed_model=llm_seed_model)
    if build_context is not None:
        cache_key = (table.name, limit)
        if cache_key not in build_context.top_seed_templates_by_table_limit:
            build_context.top_seed_templates_by_table_limit[cache_key] = _top_seed_templates(
                table,
                build_context.seed_templates_by_table.setdefault(
                    table.name,
                    build_seed_templates(table, snapshot.column_stats.get(table.name, {}), **llm_kw),
                ),
                limit=limit,
            )
        return [bind_seed_template(seed, alias) for seed in build_context.top_seed_templates_by_table_limit[cache_key]]
    seeds = build_seed_templates(table, snapshot.column_stats.get(table.name, {}), **llm_kw)
    return [bind_seed_template(seed, alias) for seed in _top_seed_templates(table, seeds, limit=limit)]


def _top_seed_templates(
    table: TableSchema,
    seeds: list[SeedTemplate],
    *,
    limit: int = 6,
) -> list[SeedTemplate]:
    ranked = sorted(
        seeds,
        key=lambda predicate: (
            -predicate_selection_score(predicate),
            predicate.column,
            predicate.operator,
            repr(predicate.value),
        ),
    )
    groups: dict[tuple[str, str], list[PredicateSpec]] = defaultdict(list)
    for predicate in ranked:
        if predicate.selectivity is None:
            continue
        groups[(bucket_for_selectivity(predicate.selectivity), predicate.family)].append(predicate)

    ordered_keys = [
        (bucket, family)
        for bucket in SEED_BUCKET_PRIORITY
        for family in SEED_FAMILY_PRIORITY
        if groups.get((bucket, family))
    ]
    selected: list[PredicateSpec] = []
    seen: set[tuple[str, str, str, str]] = set()
    while ordered_keys and len(selected) < limit:
        next_round: list[tuple[str, str]] = []
        for key in ordered_keys:
            if len(selected) >= limit:
                break
            while groups[key]:
                candidate = groups[key].pop(0)
                signature = (candidate.column, candidate.operator, repr(candidate.value), candidate.family)
                if signature in seen:
                    continue
                seen.add(signature)
                selected.append(candidate)
                break
            if groups[key]:
                next_round.append(key)
        ordered_keys = next_round

    if len(selected) < limit:
        for predicate in ranked:
            signature = (predicate.column, predicate.operator, repr(predicate.value), predicate.family)
            if signature in seen:
                continue
            seen.add(signature)
            selected.append(predicate)
            if len(selected) >= limit:
                break
    return selected[:limit]


def unique_predicate_groups(groups: list[list[PredicateSpec]]) -> list[list[PredicateSpec]]:
    deduped: dict[tuple[tuple[str, str, str, str], ...], list[PredicateSpec]] = {}
    for group in groups:
        key = tuple(sorted((predicate.table, predicate.column, predicate.operator, repr(predicate.value)) for predicate in group))
        deduped.setdefault(key, group)
    return list(deduped.values())


def expand_predicate_groups(
    table_names: list[str],
    aliases: dict[str, str],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    *,
    per_table_seed_limit: int,
    max_predicates_per_query: int,
    build_context: SpjBuildContext | None = None,
    config: GeneratorConfig | None = None,
) -> list[list[PredicateSpec]]:
    llm_kw: dict = {}
    if config is not None:
        llm_kw = dict(
            llm_fallback=snapshot.mode == "schema_plus_llm",
            benchmark=config.benchmark,
            llm_seed_model=config.llm_seed_model,
        )
    seed_pools = {
        table_name: top_seed_pool(
            catalog[table_name],
            aliases[table_name],
            snapshot,
            limit=per_table_seed_limit,
            build_context=build_context,
            **llm_kw,
        )
        for table_name in table_names
    }
    groups: list[list[PredicateSpec]] = [[]]
    lightweight_multi_table = len(table_names) >= 3

    def preferred_pool(pool: list[PredicateSpec], *, limit: int) -> list[PredicateSpec]:
        if not lightweight_multi_table:
            return pool[:limit]
        preferred = [
            predicate
            for predicate in pool
            if bucket_for_selectivity(predicate.selectivity) in {"very_high", "high", "medium"}
        ]
        if preferred:
            return preferred[:limit]
        return pool[: min(1, len(pool))]

    for table_name in table_names:
        pool = preferred_pool(seed_pools[table_name], limit=2 if lightweight_multi_table else len(seed_pools[table_name]))
        groups.extend([[predicate] for predicate in pool])
        if not lightweight_multi_table and max_predicates_per_query >= 2:
            groups.extend([list(combo) for combo in itertools.combinations(pool[: min(4, len(pool))], 2)])

    if max_predicates_per_query >= 2:
        for left_table, right_table in itertools.combinations(table_names, 2):
            left_pool = preferred_pool(seed_pools[left_table], limit=1 if lightweight_multi_table else 3)
            right_pool = preferred_pool(seed_pools[right_table], limit=1 if lightweight_multi_table else 3)
            for left in left_pool:
                for right in right_pool:
                    groups.append([left, right])

    if not lightweight_multi_table and max_predicates_per_query >= 3:
        limited_tables = table_names[: min(4, len(table_names))]
        for table_combo in itertools.combinations(limited_tables, 3):
            if all(seed_pools[table_name] for table_name in table_combo):
                groups.append([seed_pools[table_name][0] for table_name in table_combo])
                if all(len(seed_pools[table_name]) > 1 for table_name in table_combo):
                    groups.append([seed_pools[table_name][1] for table_name in table_combo])

    return unique_predicate_groups(groups)


def cached_projection_options(
    table_names: list[str],
    aliases: dict[str, str],
    catalog: dict[str, TableSchema],
    build_context: SpjBuildContext | None = None,
) -> list[tuple[str, list[tuple[str, str, str]]]]:
    table_specs = [catalog[table_name] for table_name in table_names]
    if build_context is None:
        return build_projection_options(table_specs, aliases)
    cache_key = tuple(table_names)
    if cache_key not in build_context.projection_templates_by_tables:
        build_context.projection_templates_by_tables[cache_key] = build_projection_templates(table_specs)
    return bind_projection_templates(build_context.projection_templates_by_tables[cache_key], aliases)


def probe_oversubscribe_factor(snapshot: StatsSnapshot) -> int:
    if snapshot.provider is not None:
        if snapshot.mode == "stats_plus_estimated_probes":
            return 6
        if snapshot.mode == "stats_plus_selective_probes":
            return 4
    return 2


def topology_retain_limit(stage_budget: int, topology_count: int, snapshot: StatsSnapshot) -> int:
    if stage_budget <= 0 or topology_count <= 0:
        return 0
    return max(MIN_TOPOLOGY_RETAIN, ceil(stage_budget * probe_oversubscribe_factor(snapshot) / topology_count))


def stage_pool_limit(stage_id: str, stage_budget: int, snapshot: StatsSnapshot) -> int:
    if stage_budget <= 0:
        return 0
    if snapshot.provider is not None and stage_id in {"1_table", "2_table"}:
        if snapshot.mode == "stats_plus_estimated_probes":
            return max(stage_budget * 6, 48)
        if snapshot.mode == "stats_plus_selective_probes":
            return max(stage_budget * 4, 32)
    if stage_id == "1_table":
        return max(stage_budget * 2, 24)
    return max(stage_budget * 2, 24)


def merge_candidate_pool(
    current: list[SpjCandidate],
    incoming: list[SpjCandidate],
    *,
    limit: int,
    seed: int,
) -> list[SpjCandidate]:
    merged = unique_candidates_by_signature(current + incoming)
    if limit <= 0 or len(merged) <= limit:
        return merged
    return retain_diverse_candidates(merged, limit=limit, seed=seed)


def apply_template_packs_to_chunk(
    candidates: list[SpjCandidate],
    context: dict[str, Any],
) -> list[SpjCandidate]:
    return unique_candidates_by_signature(apply_template_packs(list(context["config"].template_packs), candidates, context))


def _record_generation_stats(
    build_context: SpjBuildContext | None,
    stage_id: str,
    *,
    generated_before_prune: int,
    retained_before_probe: int,
) -> None:
    if build_context is None:
        return
    bucket = build_context.diagnostics[stage_id]
    bucket["generated_before_prune"] += generated_before_prune
    bucket["retained_before_probe"] += retained_before_probe


def unique_candidates_by_signature(candidates: list[SpjCandidate]) -> list[SpjCandidate]:
    deduped: dict[tuple[Any, ...], SpjCandidate] = {}
    for candidate in candidates:
        deduped.setdefault(candidate.structural_signature(), candidate)
    return list(deduped.values())


def diversity_key(candidate: SpjCandidate) -> tuple[Any, ...]:
    return (
        candidate.projection_width,
        candidate.target_selectivity_bucket,
        len(candidate.predicates) + sum(len(join.on_predicates) for join in candidate.joins),
        candidate.rule_family,
    )


def retain_diverse_candidates(
    candidates: list[SpjCandidate],
    *,
    limit: int,
    seed: int,
) -> list[SpjCandidate]:
    if limit <= 0 or len(candidates) <= limit:
        return unique_candidates_by_signature(candidates)
    groups: dict[tuple[Any, ...], list[SpjCandidate]] = defaultdict(list)
    for candidate in sorted(candidates, key=lambda item: (diversity_key(item), item.signature())):
        groups[diversity_key(candidate)].append(candidate)
    rng = random.Random(seed)
    ordered_keys = sorted(groups)
    rng.shuffle(ordered_keys)
    for key in ordered_keys:
        rng.shuffle(groups[key])
    retained: list[SpjCandidate] = []
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
    return unique_candidates_by_signature(retained)


def predicate_combinations(predicates: list[PredicateSpec], max_count: int) -> list[list[PredicateSpec]]:
    combinations: list[list[PredicateSpec]] = [[]]
    singles = sorted(predicates, key=lambda item: (item.column, item.family, item.operator))
    for count in range(1, max_count + 1):
        for combo in itertools.combinations(singles, count):
            columns = {predicate.column for predicate in combo}
            if len(columns) != len(combo):
                continue
            combinations.append(list(combo))
    return combinations


def average_selectivity(predicates: list[PredicateSpec]) -> float | None:
    if not predicates:
        return 0.90
    estimates = [predicate.selectivity for predicate in predicates if predicate.selectivity is not None]
    if not estimates:
        return None
    value = 1.0
    for estimate in estimates:
        value *= estimate
    return max(0.0001, min(0.99, value))


def _numeric_literal_from_stats(stats: ColumnStats | None) -> int | float | None:
    histogram = list(stats.histogram_bounds) if stats else []
    numeric_bounds = [bound for bound in histogram if isinstance(bound, Real)]
    if len(numeric_bounds) >= 2:
        return numeric_bounds[1]
    if numeric_bounds:
        return numeric_bounds[0]
    if stats:
        for value in stats.most_common_vals:
            if isinstance(value, Real):
                return value
    return None


def _job_like_filter_column(
    table: TableSchema,
    join_columns: set[str],
    column_stats: dict[str, ColumnStats],
) -> ColumnSchema | None:
    numeric_columns = [
        column
        for column in table.columns.values()
        if column.is_numeric and _numeric_literal_from_stats(column_stats.get(column.name)) is not None
    ]
    if not numeric_columns:
        return None

    def sort_key(column: ColumnSchema) -> tuple[int, int, str]:
        return (0 if column.is_foreign_key else 1, 0 if column.is_primary_key else 1, column.name)

    non_join = sorted(
        [column for column in numeric_columns if column.name not in join_columns and not column.is_primary_key],
        key=sort_key,
    )
    if non_join:
        return non_join[0]

    join_key_columns = sorted(
        [column for column in numeric_columns if column.name in join_columns or column.is_foreign_key],
        key=sort_key,
    )
    if join_key_columns:
        return join_key_columns[0]

    primary_keys = sorted([column for column in numeric_columns if column.is_primary_key], key=sort_key)
    if primary_keys:
        return primary_keys[0]

    return sorted(numeric_columns, key=sort_key)[0]


def build_job_like_predicates(
    candidate: SpjCandidate,
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
) -> list[PredicateSpec]:
    join_columns: dict[str, set[str]] = defaultdict(set)
    for join in candidate.joins:
        join_columns[join.left_table].add(join.left_column)
        join_columns[join.right_table].add(join.right_column)

    aliases = candidate.aliases()
    predicates: list[PredicateSpec] = []
    for table_name in candidate.tables:
        table = catalog[table_name]
        stats_map = snapshot.column_stats.get(table_name, {})
        column = _job_like_filter_column(table, join_columns.get(table_name, set()), stats_map)
        if column is None:
            return []
        stats = stats_map.get(column.name)
        value = _numeric_literal_from_stats(stats)
        if value is None:
            return []
        predicates.append(
            PredicateSpec(
                table=table_name,
                alias=aliases[table_name],
                column=column.name,
                operator=">",
                value=value,
                family="job_like_range",
                selectivity=estimate_from_stats(column, stats, "range"),
            )
        )
    return predicates


def append_job_like_join_templates(candidates: list[SpjCandidate], context: dict[str, Any]) -> list[SpjCandidate]:
    catalog: dict[str, TableSchema] = context["catalog"]
    snapshot: StatsSnapshot = context["snapshot"]
    appended: list[SpjCandidate] = []

    for candidate in candidates:
        if candidate.join_type != "inner" or len(candidate.tables) < 2 or candidate.render_mode != "ansi":
            continue
        extra_predicates = build_job_like_predicates(candidate, catalog, snapshot)
        if len(extra_predicates) != len(candidate.tables):
            continue
        all_predicates = list(candidate.predicates) + extra_predicates
        appended.append(
            replace(
                candidate,
                predicates=all_predicates,
                predicate_families=sorted(set(candidate.predicate_families) | {"job_like_range"}),
                target_selectivity_bucket=bucket_for_selectivity(average_selectivity(all_predicates)),
                estimated_selectivity=average_selectivity(all_predicates),
                rule_family=f"job_like_{candidate.rule_family}",
                render_mode="job_like",
                template_pack=JOB_LIKE_TEMPLATE_PACK,
            )
        )
    return list(candidates) + appended


def build_stage_one_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext | None = None,
) -> list[SpjCandidate]:
    candidates: list[SpjCandidate] = []
    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        alias = aliases[table_name]
        llm_kw = dict(
            llm_fallback=snapshot.mode == "schema_plus_llm",
            benchmark=config.benchmark,
            llm_seed_model=config.llm_seed_model,
        )
        if build_context is not None:
            seed_templates = build_context.seed_templates_by_table.setdefault(
                table_name,
                build_seed_templates(table, snapshot.column_stats.get(table_name, {}), **llm_kw),
            )
            seeds = [bind_seed_template(seed, alias) for seed in seed_templates]
        else:
            seeds = build_predicate_seeds(table, alias, snapshot.column_stats.get(table_name, {}), **llm_kw)
        projections = cached_projection_options([table_name], aliases, catalog, build_context)
        for predicate_group in predicate_combinations(seeds, config.max_predicates_per_table):
            estimated = average_selectivity(predicate_group)
            bucket = bucket_for_selectivity(estimated)
            families = sorted({predicate.family for predicate in predicate_group}) or ["scan"]
            rule_family = "scan" if not predicate_group else ("single_predicate" if len(predicate_group) == 1 else "multi_predicate")
            calibration_source = "pg_stats" if snapshot.column_stats else "schema"
            for projection_width, projection_columns in projections:
                candidates.append(
                    SpjCandidate(
                        stage_id="1_table",
                        tables=[table_name],
                        base_table=table_name,
                        join_type="scan",
                        join_topology="scan",
                        joins=[],
                        predicates=predicate_group,
                        projections=projection_columns,
                        projection_width=projection_width,
                        predicate_families=families,
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family=rule_family,
                        calibration_source=calibration_source,
                    )
                )
    return candidates


def build_pair_candidates_for_edge(
    edge: JoinEdge,
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    *,
    build_context: SpjBuildContext | None = None,
    retain_limit: int | None = None,
    seed_offset: int = 0,
) -> list[SpjCandidate]:
    child = catalog[edge.child_table]
    parent = catalog[edge.parent_table]
    child_alias, parent_alias = alias_map([child.name, parent.name]).values()
    llm_kw = dict(
        llm_fallback=snapshot.mode == "schema_plus_llm",
        benchmark=config.benchmark,
        llm_seed_model=config.llm_seed_model,
    )
    child_seeds = top_seed_pool(child, child_alias, snapshot, limit=6, build_context=build_context, **llm_kw)
    parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=6, build_context=build_context, **llm_kw)
    projections = cached_projection_options([child.name, parent.name], {child.name: child_alias, parent.name: parent_alias}, catalog, build_context)
    candidates: list[SpjCandidate] = []

    if "inner" in config.join_types:
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
        predicate_plans: list[tuple[str, list[PredicateSpec]]] = [("join_only", [])]
        predicate_plans.extend(("predicate_pushdown_left", [predicate]) for predicate in child_seeds)
        predicate_plans.extend(("predicate_pushdown_right", [predicate]) for predicate in parent_seeds)
        predicate_plans.extend(
            ("predicate_pushdown_both", [left, right])
            for left in child_seeds[:4]
            for right in parent_seeds[:4]
        )
        predicate_plans.extend(
            ("predicate_pushdown_left", list(combo))
            for combo in itertools.combinations(child_seeds[: min(4, len(child_seeds))], 2)
        )
        predicate_plans.extend(
            ("predicate_pushdown_right", list(combo))
            for combo in itertools.combinations(parent_seeds[: min(4, len(parent_seeds))], 2)
        )
        for rule_family, predicates in predicate_plans:
            estimated = average_selectivity(predicates)
            bucket = bucket_for_selectivity(estimated)
            for projection_width, projection_columns in projections:
                candidates.append(
                    SpjCandidate(
                        stage_id="2_table",
                        tables=[child.name, parent.name],
                        base_table=child.name,
                        join_type="inner",
                        join_topology="pair",
                        joins=[join],
                        predicates=list(predicates),
                        projections=projection_columns,
                        projection_width=projection_width,
                        predicate_families=sorted({predicate.family for predicate in predicates}) or ["join"],
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family=rule_family if projection_width != "narrow" else "projection_pruning",
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    )
                )

    if "left" in config.join_types:
        join = JoinSpec(
            join_type="left",
            left_table=parent.name,
            left_alias=parent_alias,
            left_column=edge.parent_column,
            right_table=child.name,
            right_alias=child_alias,
            right_column=edge.child_column,
            edge=edge,
        )
        left_predicates = parent_seeds[:4]
        right_predicates = child_seeds[:4]
        for projection_width, projection_columns in projections:
            candidates.append(
                SpjCandidate(
                    stage_id="2_table",
                    tables=[parent.name, child.name],
                    base_table=parent.name,
                    join_type="left",
                    join_topology="pair",
                    joins=[join],
                    predicates=[],
                    projections=projection_columns,
                    projection_width=projection_width,
                    predicate_families=["join"],
                    target_selectivity_bucket="high",
                    estimated_selectivity=0.30,
                    rule_family="left_join_preserving",
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                )
            )
        for predicate in left_predicates:
            estimated = average_selectivity([predicate])
            bucket = bucket_for_selectivity(estimated)
            for projection_width, projection_columns in projections:
                candidates.append(
                    SpjCandidate(
                        stage_id="2_table",
                        tables=[parent.name, child.name],
                        base_table=parent.name,
                        join_type="left",
                        join_topology="pair",
                        joins=[join],
                        predicates=[predicate],
                        projections=projection_columns,
                        projection_width=projection_width,
                        predicate_families=[predicate.family],
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="left_join_preserving",
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    )
                )
        for predicate in right_predicates:
            estimated = average_selectivity([predicate])
            bucket = bucket_for_selectivity(estimated)
            for projection_width, projection_columns in projections:
                candidates.append(
                    SpjCandidate(
                        stage_id="2_table",
                        tables=[parent.name, child.name],
                        base_table=parent.name,
                        join_type="left",
                        join_topology="pair",
                        joins=[join],
                        predicates=[predicate],
                        projections=projection_columns,
                        projection_width=projection_width,
                        predicate_families=[predicate.family],
                        target_selectivity_bucket=bucket,
                        estimated_selectivity=estimated,
                        rule_family="outer_to_inner_candidate",
                        calibration_source="pg_stats" if snapshot.column_stats else "schema",
                    )
                )
        for left_predicate in left_predicates[:3]:
            for right_predicate in right_predicates[:3]:
                predicates = [left_predicate, right_predicate]
                estimated = average_selectivity(predicates)
                bucket = bucket_for_selectivity(estimated)
                for projection_width, projection_columns in projections:
                    candidates.append(
                        SpjCandidate(
                            stage_id="2_table",
                            tables=[parent.name, child.name],
                            base_table=parent.name,
                            join_type="left",
                            join_topology="pair",
                            joins=[join],
                            predicates=predicates,
                            projections=projection_columns,
                            projection_width=projection_width,
                            predicate_families=sorted({predicate.family for predicate in predicates}),
                            target_selectivity_bucket=bucket,
                            estimated_selectivity=estimated,
                            rule_family="left_join_preserving",
                            calibration_source="pg_stats" if snapshot.column_stats else "schema",
                        )
                    )
    unique = unique_candidates_by_signature(candidates)
    if retain_limit is None:
        return unique
    retained = retain_diverse_candidates(unique, limit=retain_limit, seed=config.seed + seed_offset)
    _record_generation_stats(
        build_context,
        stage_key(2),
        generated_before_prune=len(unique),
        retained_before_probe=len(retained),
    )
    return retained


def undirected_neighbors(join_edges: list[JoinEdge]) -> dict[str, list[JoinEdge]]:
    neighbors: dict[str, list[JoinEdge]] = defaultdict(list)
    for edge in join_edges:
        neighbors[edge.child_table].append(edge)
        neighbors[edge.parent_table].append(edge)
    return neighbors


def build_path_candidates(
    path_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    join_type: str,
    config: GeneratorConfig,
    *,
    build_context: SpjBuildContext | None = None,
    retain_limit: int | None = None,
    seed_offset: int = 0,
) -> list[SpjCandidate]:
    ordered_tables = [path_edges[0].child_table, path_edges[0].parent_table]
    for edge in path_edges[1:]:
        if edge.child_table not in ordered_tables:
            ordered_tables.append(edge.child_table)
        if edge.parent_table not in ordered_tables:
            ordered_tables.append(edge.parent_table)
    aliases = alias_map(ordered_tables)
    joins: list[JoinSpec] = []
    seen_tables = {ordered_tables[0]}
    for edge in path_edges:
        if edge.child_table in seen_tables and edge.parent_table not in seen_tables:
            left_table, left_column = edge.child_table, edge.child_column
            right_table, right_column = edge.parent_table, edge.parent_column
        elif edge.parent_table in seen_tables and edge.child_table not in seen_tables:
            left_table, left_column = edge.parent_table, edge.parent_column
            right_table, right_column = edge.child_table, edge.child_column
        else:
            left_table, left_column = edge.child_table, edge.child_column
            right_table, right_column = edge.parent_table, edge.parent_column
        joins.append(
            JoinSpec(
                join_type=join_type,
                left_table=left_table,
                left_alias=aliases[left_table],
                left_column=left_column,
                right_table=right_table,
                right_alias=aliases[right_table],
                right_column=right_column,
                edge=edge,
            )
        )
        seen_tables.add(right_table)

    all_projections = cached_projection_options(ordered_tables, aliases, catalog, build_context)
    many_tables = len(ordered_tables) >= 4
    projections = [(w, c) for w, c in all_projections if w in ("narrow", "medium")] if many_tables else all_projections
    predicate_groups = expand_predicate_groups(
        ordered_tables,
        aliases,
        catalog,
        snapshot,
        per_table_seed_limit=2 if many_tables else 4,
        max_predicates_per_query=min(3, max(1, config.max_predicates_per_table)),
        build_context=build_context,
        config=config,
    )
    rule_family = "join_order_chain" if join_type == "inner" else "left_chain"
    candidates: list[SpjCandidate] = []
    for predicates in predicate_groups:
        estimated = average_selectivity(predicates)
        bucket = bucket_for_selectivity(estimated)
        for projection_width, projection_columns in projections:
            candidates.append(
                SpjCandidate(
                    stage_id=stage_key(len(ordered_tables)),
                    tables=ordered_tables,
                    base_table=ordered_tables[0],
                    join_type=join_type,
                    join_topology="chain",
                    joins=joins,
                    predicates=predicates,
                    projections=projection_columns,
                    projection_width=projection_width,
                    predicate_families=sorted({predicate.family for predicate in predicates}) or ["join"],
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family=rule_family,
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                )
            )
    unique = unique_candidates_by_signature(candidates)
    if retain_limit is None:
        return unique
    retained = retain_diverse_candidates(unique, limit=retain_limit, seed=config.seed + seed_offset)
    _record_generation_stats(
        build_context,
        stage_key(len(ordered_tables)),
        generated_before_prune=len(unique),
        retained_before_probe=len(retained),
    )
    return retained


def enumerate_paths(join_edges: list[JoinEdge], length: int, *, max_topologies: int = MAX_TOPOLOGIES) -> list[list[JoinEdge]]:
    neighbors = undirected_neighbors(join_edges)
    deduped: dict[tuple[str, ...], list[JoinEdge]] = {}
    _early_stop = False

    def walk(current_table: str, remaining: int, path: list[JoinEdge], used_tables: set[str]) -> None:
        nonlocal _early_stop
        if _early_stop:
            return
        if remaining == 0:
            key = tuple(sorted(edge.key for edge in path))
            if key not in deduped:
                deduped[key] = list(path)
                if max_topologies > 0 and len(deduped) >= max_topologies:
                    _early_stop = True
            return
        for edge in sorted(neighbors[current_table], key=lambda item: item.key):
            if _early_stop:
                return
            next_table = edge.parent_table if edge.child_table == current_table else edge.child_table
            if next_table in used_tables:
                continue
            path.append(edge)
            used_tables.add(next_table)
            walk(next_table, remaining - 1, path, used_tables)
            used_tables.remove(next_table)
            path.pop()

    for edge in join_edges:
        if _early_stop:
            break
        for start in [edge.child_table, edge.parent_table]:
            if _early_stop:
                break
            used_tables = {start}
            walk(start, length, [], used_tables)

    return list(deduped.values())


def enumerate_star_edges(join_edges: list[JoinEdge], size: int, *, max_topologies: int = MAX_TOPOLOGIES) -> list[list[JoinEdge]]:
    neighbors = undirected_neighbors(join_edges)
    deduped: dict[tuple[str, ...], list[JoinEdge]] = {}
    for table_name, edges in sorted(neighbors.items()):
        if len(edges) < size - 1:
            continue
        for combo in itertools.combinations(sorted(edges, key=lambda item: item.key), size - 1):
            key = tuple(sorted(edge.key for edge in combo))
            if key not in deduped:
                deduped[key] = list(combo)
                if max_topologies > 0 and len(deduped) >= max_topologies:
                    return list(deduped.values())
    return list(deduped.values())


def build_star_candidates(
    star_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    *,
    build_context: SpjBuildContext | None = None,
    retain_limit: int | None = None,
    seed_offset: int = 0,
) -> list[SpjCandidate]:
    tables = sorted({edge.child_table for edge in star_edges} | {edge.parent_table for edge in star_edges})
    aliases = alias_map(tables)
    root_table = min(tables)
    joins: list[JoinSpec] = []
    used = {root_table}
    for edge in sorted(star_edges, key=lambda item: item.key):
        if edge.parent_table in used:
            left_table, left_column = edge.parent_table, edge.parent_column
            right_table, right_column = edge.child_table, edge.child_column
        else:
            left_table, left_column = edge.child_table, edge.child_column
            right_table, right_column = edge.parent_table, edge.parent_column
        joins.append(
            JoinSpec(
                join_type="inner",
                left_table=left_table,
                left_alias=aliases[left_table],
                left_column=left_column,
                right_table=right_table,
                right_alias=aliases[right_table],
                right_column=right_column,
                edge=edge,
            )
        )
        used.add(right_table)
    all_projections = cached_projection_options(tables, aliases, catalog, build_context)
    many_tables = len(tables) >= 4
    projections = [(w, c) for w, c in all_projections if w in ("narrow", "medium")] if many_tables else all_projections
    predicate_groups = expand_predicate_groups(
        tables,
        aliases,
        catalog,
        snapshot,
        per_table_seed_limit=2 if many_tables else 3,
        max_predicates_per_query=min(3, max(1, config.max_predicates_per_table)),
        build_context=build_context,
        config=config,
    )
    candidates: list[SpjCandidate] = []
    for predicates in predicate_groups:
        estimated = average_selectivity(predicates)
        bucket = bucket_for_selectivity(estimated)
        for projection_width, projection_columns in projections:
            candidates.append(
                SpjCandidate(
                    stage_id=stage_key(len(tables)),
                    tables=tables,
                    base_table=root_table,
                    join_type="inner",
                    join_topology="star",
                    joins=joins,
                    predicates=predicates,
                    projections=projection_columns,
                    projection_width=projection_width,
                    predicate_families=sorted({predicate.family for predicate in predicates}) or ["join"],
                    target_selectivity_bucket=bucket,
                    estimated_selectivity=estimated,
                    rule_family="join_order_star",
                    calibration_source="pg_stats" if snapshot.column_stats else "schema",
                )
            )
    unique = unique_candidates_by_signature(candidates)
    if retain_limit is None:
        return unique
    retained = retain_diverse_candidates(unique, limit=retain_limit, seed=config.seed + seed_offset)
    _record_generation_stats(
        build_context,
        stage_key(len(tables)),
        generated_before_prune=len(unique),
        retained_before_probe=len(retained),
    )
    return retained


def build_multi_table_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    *,
    build_context: SpjBuildContext | None = None,
) -> list[SpjCandidate]:
    candidates: list[SpjCandidate] = []
    for table_count in range(3, config.max_join_tables + 1):
        current_stage = stage_key(table_count)
        stage_budget = config.stage_budgets.get(current_stage, 0)
        if stage_budget <= 0:
            continue
        if build_context is not None:
            path_edges_list = build_context.paths_by_length.setdefault(table_count - 1, enumerate_paths(join_edges, table_count - 1))
            star_edges_list = build_context.stars_by_size.setdefault(table_count, enumerate_star_edges(join_edges, table_count))
        else:
            path_edges_list = enumerate_paths(join_edges, table_count - 1)
            star_edges_list = enumerate_star_edges(join_edges, table_count)
        topology_count = len(path_edges_list) + len(star_edges_list)
        if "left" in config.join_types and table_count == 3:
            topology_count += len(path_edges_list)
        retain_limit = topology_retain_limit(stage_budget, topology_count, snapshot)
        for index, path_edges in enumerate(path_edges_list):
            candidates.extend(
                build_path_candidates(
                    path_edges,
                    catalog,
                    snapshot,
                    "inner",
                    config,
                    build_context=build_context,
                    retain_limit=retain_limit,
                    seed_offset=(table_count * 1000) + index,
                )
            )
            if "left" in config.join_types and table_count == 3:
                candidates.extend(
                    build_path_candidates(
                        path_edges,
                        catalog,
                        snapshot,
                        "left",
                        config,
                        build_context=build_context,
                        retain_limit=retain_limit,
                        seed_offset=(table_count * 2000) + index,
                    )
                )
        for index, star_edges in enumerate(star_edges_list):
            candidates.extend(
                build_star_candidates(
                    star_edges,
                    catalog,
                    snapshot,
                    config,
                    build_context=build_context,
                    retain_limit=retain_limit,
                    seed_offset=(table_count * 3000) + index,
                )
            )
    return unique_candidates_by_signature(candidates)


def build_stage_two_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext,
) -> list[SpjCandidate]:
    stage_id = "2_table"
    stage_budget = config.stage_budgets.get(stage_id, 0)
    if stage_budget <= 0:
        return []
    topology_count = len(join_edges) * len(config.join_types)
    per_topology_limit = topology_retain_limit(stage_budget, topology_count, snapshot)
    reservoir_limit = stage_pool_limit(stage_id, stage_budget, snapshot)
    context = {"catalog": catalog, "join_edges": join_edges, "config": config, "snapshot": snapshot}
    all_chunks: list[SpjCandidate] = []
    for edge_index, edge in enumerate(join_edges):
        chunk = build_pair_candidates_for_edge(
            edge,
            catalog,
            snapshot,
            config,
            build_context=build_context,
            retain_limit=per_topology_limit,
            seed_offset=20000 + edge_index,
        )
        chunk = apply_template_packs_to_chunk(chunk, context)
        all_chunks.extend(chunk)
    stage_pool = unique_candidates_by_signature(all_chunks)
    if reservoir_limit > 0 and len(stage_pool) > reservoir_limit:
        stage_pool = retain_diverse_candidates(stage_pool, limit=reservoir_limit, seed=config.seed + 30000)
    build_context.diagnostics[stage_id]["retained_before_probe"] = len(stage_pool)
    return stage_pool


def build_stage_multi_table_candidates(
    table_count: int,
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
    build_context: SpjBuildContext,
) -> list[SpjCandidate]:
    stage_id = stage_key(table_count)
    stage_budget = config.stage_budgets.get(stage_id, 0)
    if stage_budget <= 0:
        return []

    path_edges_list = build_context.paths_by_length.setdefault(table_count - 1, enumerate_paths(join_edges, table_count - 1))
    star_edges_list = build_context.stars_by_size.setdefault(table_count, enumerate_star_edges(join_edges, table_count))
    topology_count = len(path_edges_list) + len(star_edges_list)
    if "left" in config.join_types and table_count == 3:
        topology_count += len(path_edges_list)

    per_topology_limit = topology_retain_limit(stage_budget, topology_count, snapshot)
    reservoir_limit = stage_pool_limit(stage_id, stage_budget, snapshot)
    context = {"catalog": catalog, "join_edges": join_edges, "config": config, "snapshot": snapshot}
    all_chunks: list[SpjCandidate] = []

    for index, path_edges in enumerate(path_edges_list):
        inner_chunk = build_path_candidates(
            path_edges,
            catalog,
            snapshot,
            "inner",
            config,
            build_context=build_context,
            retain_limit=per_topology_limit,
            seed_offset=(table_count * 1000) + index,
        )
        inner_chunk = apply_template_packs_to_chunk(inner_chunk, context)
        all_chunks.extend(inner_chunk)
        if "left" in config.join_types and table_count == 3:
            left_chunk = build_path_candidates(
                path_edges,
                catalog,
                snapshot,
                "left",
                config,
                build_context=build_context,
                retain_limit=per_topology_limit,
                seed_offset=(table_count * 2000) + index,
            )
            left_chunk = apply_template_packs_to_chunk(left_chunk, context)
            all_chunks.extend(left_chunk)

    for index, star_edges in enumerate(star_edges_list):
        star_chunk = build_star_candidates(
            star_edges,
            catalog,
            snapshot,
            config,
            build_context=build_context,
            retain_limit=per_topology_limit,
            seed_offset=(table_count * 3000) + index,
        )
        star_chunk = apply_template_packs_to_chunk(star_chunk, context)
        all_chunks.extend(star_chunk)

    stage_pool = unique_candidates_by_signature(all_chunks)
    if reservoir_limit > 0 and len(stage_pool) > reservoir_limit:
        stage_pool = retain_diverse_candidates(stage_pool, limit=reservoir_limit, seed=config.seed + (table_count * 5000))
    build_context.diagnostics[stage_id]["retained_before_probe"] = len(stage_pool)
    return stage_pool


def ordered_candidates(candidates: list[SpjCandidate], seed: int) -> list[SpjCandidate]:
    if not candidates:
        return []
    rng = random.Random(seed)
    groups: dict[tuple[Any, ...], list[SpjCandidate]] = defaultdict(list)
    for candidate in sorted(candidates, key=lambda item: (item.stage_id, item.rule_family, item.join_type, item.signature())):
        key = (
            tuple(candidate.tables),
            candidate.join_topology,
            candidate.join_type,
            candidate.projection_width,
            candidate.target_selectivity_bucket,
            len(candidate.predicates) + sum(len(join.on_predicates) for join in candidate.joins),
        )
        groups[key].append(candidate)
    ordered_keys = sorted(groups)
    for key in ordered_keys:
        rng.shuffle(groups[key])
    selected: list[SpjCandidate] = []
    while ordered_keys:
        next_round: list[tuple[Any, ...]] = []
        for key in ordered_keys:
            if groups[key]:
                selected.append(groups[key].pop(0))
            if groups[key]:
                next_round.append(key)
        ordered_keys = next_round
    return selected


def calibrate_candidate(
    candidate: SpjCandidate,
    snapshot: StatsSnapshot,
    provider: Any | None = None,
) -> None:
    effective_provider = provider or snapshot.provider
    if effective_provider is None:
        return
    if snapshot.mode == "stats_plus_selective_probes":
        row_count = effective_provider.probe_count(candidate.render_sql())
    elif snapshot.mode == "stats_plus_estimated_probes":
        try:
            row_count = effective_provider.probe_estimate(candidate.render_sql())
        except Exception:
            return
    else:
        return
    base_table_count = snapshot.table_counts.get(candidate.base_table, 0)
    candidate.observed_rows = int(row_count)
    candidate.observed_selectivity = None if base_table_count == 0 else row_count / base_table_count
    candidate.target_selectivity_bucket = bucket_for_selectivity(candidate.observed_selectivity)
    candidate.calibrated = True
    candidate.calibration_source = "probe" if snapshot.mode == "stats_plus_selective_probes" else "estimate"


def select_candidates_for_stage(
    candidates: list[SpjCandidate],
    budget: int,
    seed: int,
    snapshot: StatsSnapshot,
    *,
    stage_id: str,
    probe_workers: int,
) -> tuple[list[SpjCandidate], dict[str, int]]:
    ordered = ordered_candidates(candidates, seed)
    diagnostics = _diagnostic_bucket()
    diagnostics["retained_before_probe"] = len(ordered)

    # --- Deduplicate by SQL signature BEFORE probing ---
    seen_sql: set[str] = set()
    unique_ordered: list[SpjCandidate] = []
    for candidate in ordered:
        sql_signature = normalize_sql(candidate.render_sql())
        if sql_signature not in seen_sql:
            seen_sql.add(sql_signature)
            unique_ordered.append(candidate)
    diagnostics["unique_before_probe"] = len(unique_ordered)
    ordered = unique_ordered

    probe_modes = {"stats_plus_selective_probes", "stats_plus_estimated_probes"}
    selective_stages = {"1_table", "2_table"}
    estimated_stages = {"1_table", "2_table", "3_table", "4_table"}
    probe_stages = estimated_stages if snapshot.mode == "stats_plus_estimated_probes" else selective_stages
    should_probe = (
        snapshot.mode in probe_modes
        and snapshot.provider is not None
        and stage_id in probe_stages
    )
    if not should_probe:
        selected = ordered[:budget]
        diagnostics["selected"] = len(selected)
        return selected, diagnostics

    provider = snapshot.provider
    if budget <= 0:
        return [], diagnostics

    workers = max(1, probe_workers)
    if workers <= 1 or not hasattr(provider, "clone"):
        start_calls = getattr(provider, "probe_calls", 0)
        selected: list[SpjCandidate] = []
        for candidate in ordered:
            calibrate_candidate(candidate, snapshot, provider)
            diagnostics["probed"] += 1
            if candidate.observed_rows == 0:
                continue
            selected.append(candidate)
            if len(selected) >= budget:
                break
        diagnostics["probe_calls"] = max(0, getattr(provider, "probe_calls", 0) - start_calls)
        diagnostics["selected"] = len(selected)
        return selected, diagnostics

    thread_local = threading.local()
    created_providers: list[Any] = []
    created_lock = threading.Lock()

    def worker(candidate: SpjCandidate) -> SpjCandidate:
        worker_provider = getattr(thread_local, "provider", None)
        if worker_provider is None:
            worker_provider = provider.clone()
            thread_local.provider = worker_provider
            with created_lock:
                created_providers.append(worker_provider)
        calibrate_candidate(candidate, snapshot, worker_provider)
        return candidate

    selected: list[SpjCandidate] = []
    batch_size = max(workers, min(len(ordered), budget * 4))
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            for offset in range(0, len(ordered), batch_size):
                batch = ordered[offset : offset + batch_size]
                futures = [executor.submit(worker, candidate) for candidate in batch]
                for future in futures:
                    if len(selected) >= budget:
                        future.cancel()
                        continue
                    candidate = future.result()
                    diagnostics["probed"] += 1
                    if candidate.observed_rows == 0:
                        continue
                    selected.append(candidate)
                if len(selected) >= budget:
                    break
    finally:
        for worker_provider in created_providers:
            diagnostics["probe_calls"] += getattr(worker_provider, "probe_calls", 0)
            if hasattr(worker_provider, "close"):
                worker_provider.close()
    diagnostics["selected"] = len(selected)
    return selected, diagnostics


def emit_artifacts(
    selected: list[SpjCandidate],
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
    generation_diagnostics: dict[str, dict[str, int]] | None = None,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    artifacts: list[QueryArtifact] = []
    manifest_queries: list[dict[str, Any]] = []
    for index, candidate in enumerate(selected, start=1):
        candidate.query_id = f"spj_{index:04d}"
        candidate.filename = f"{candidate.query_id}_{candidate.stage_id}_{candidate.rule_family}.sql"
        metadata = candidate.to_manifest()
        validate_manifest_entry(catalog, join_edges, metadata)
        manifest_queries.append(metadata)
        artifacts.append(QueryArtifact(filename=candidate.filename, sql=candidate.render_sql(), metadata=metadata))
    manifest = {
        "branch": "spj",
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


def generate_spj_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    build_context = make_build_context(catalog, snapshot, config)
    selected: list[SpjCandidate] = []
    for table_count in range(1, config.max_join_tables + 1):
        current_stage = stage_key(table_count)
        budget = config.stage_budgets.get(current_stage, 0)
        if budget <= 0:
            continue
        if table_count == 1:
            context = {"catalog": catalog, "join_edges": join_edges, "config": config, "snapshot": snapshot}
            available = apply_template_packs_to_chunk(
                build_stage_one_candidates(catalog, snapshot, config, build_context=build_context),
                context,
            )
            build_context.diagnostics[current_stage]["generated_before_prune"] = len(available)
            build_context.diagnostics[current_stage]["retained_before_probe"] = len(available)
        elif table_count == 2:
            available = build_stage_two_candidates(join_edges, catalog, snapshot, config, build_context)
        else:
            available = build_stage_multi_table_candidates(table_count, join_edges, catalog, snapshot, config, build_context)
        if budget > len(available):
            raise ValueError(
                f"Stage {current_stage} requested {budget} queries but only {len(available)} unique candidates were generated "
                f"from stats-backed predicates (literal fallbacks are disabled)"
            )
        chosen, probe_diagnostics = select_candidates_for_stage(
            available,
            budget,
            config.seed + table_count,
            snapshot,
            stage_id=current_stage,
            probe_workers=config.probe_workers,
        )
        build_context.diagnostics[current_stage]["probed"] = probe_diagnostics["probed"]
        build_context.diagnostics[current_stage]["probe_calls"] = probe_diagnostics["probe_calls"]
        build_context.diagnostics[current_stage]["selected"] = probe_diagnostics["selected"]
        if budget > len(chosen):
            raise ValueError(
                f"Stage {current_stage} requested {budget} non-empty queries but only {len(chosen)} remained after "
                f"zero-row filtering and stats-backed predicate selection"
            )
        selected.extend(chosen)

    selected = sorted(unique_candidates_by_signature(selected), key=lambda item: (item.stage_id, item.join_topology, item.join_type, item.signature()))
    return emit_artifacts(
        selected,
        catalog,
        join_edges,
        config.benchmark,
        schema_path,
        config.suffix,
        config.seed,
        generation_diagnostics={stage_id: dict(values) for stage_id, values in sorted(build_context.diagnostics.items())},
    )


register_template_pack(JOB_LIKE_TEMPLATE_PACK, append_job_like_join_templates)
