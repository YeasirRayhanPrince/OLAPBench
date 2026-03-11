from __future__ import annotations

import itertools
import random
from collections import defaultdict
from dataclasses import dataclass, field
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
from query_curriculum.templates import apply_template_packs


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
    template_pack: str | None = None
    calibrated: bool = False
    calibration_source: str = "schema"
    observed_rows: int | None = None
    observed_selectivity: float | None = None
    filename: str | None = None
    query_id: str | None = None

    def signature(self) -> str:
        return normalize_sql(self.render_sql())

    def render_sql(self) -> str:
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
        return "\n".join(lines) + "\n"

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


def stage_key(table_count: int) -> str:
    return f"{table_count}_table"


def build_projection_options(table_specs: list[TableSchema], aliases: dict[str, str]) -> list[tuple[str, list[tuple[str, str, str]]]]:
    all_columns = [(table.name, aliases[table.name], column.name) for table in table_specs for column in table.columns.values()]
    preferred = []
    for table in table_specs:
        ordered = sorted(
            table.columns.values(),
            key=lambda column: (
                0 if column.is_primary_key else 1,
                0 if column.is_foreign_key else 1,
                column.name,
            ),
        )
        preferred.extend((table.name, aliases[table.name], column.name) for column in ordered)
    options = []
    if preferred:
        options.append(("narrow", preferred[:1]))
        options.append(("medium", preferred[: min(3, len(preferred))]))
        options.append(("wide", preferred[: min(5, len(preferred))]))
        options.append(("star", all_columns))
    return options


def estimate_from_stats(column: ColumnSchema, stats: ColumnStats | None, family: str) -> float | None:
    if stats is None:
        if family == "null":
            return 0.10
        if family == "range":
            return 0.15 if column.is_numeric else 0.10
        if family == "membership":
            return 0.12
        if family == "like":
            return 0.08
        return 0.05
    if family == "null" and stats.null_frac is not None:
        return max(0.001, stats.null_frac)
    if family == "range":
        return 0.10 if len(stats.histogram_bounds) < 4 else max(0.02, 1.0 / max(4, len(stats.histogram_bounds)))
    if family == "membership" and stats.most_common_freqs:
        return max(0.001, min(0.95, sum(stats.most_common_freqs[:2])))
    if family == "equality" and stats.most_common_freqs:
        return max(0.001, min(0.95, float(stats.most_common_freqs[0])))
    if family == "like":
        return 0.08
    return 0.05


def literal_from_stats(column: ColumnSchema, stats: ColumnStats | None) -> Any:
    if stats and stats.most_common_vals:
        return stats.most_common_vals[0]
    if column.is_numeric:
        return 1
    if column.is_text:
        return "A"
    return 1


def build_predicate_seeds(
    table: TableSchema,
    alias: str,
    column_stats: dict[str, ColumnStats],
) -> list[PredicateSpec]:
    predicates: list[PredicateSpec] = []
    for column in sorted(table.columns.values(), key=lambda item: (item.role, item.name)):
        stats = column_stats.get(column.name)
        if column.nullable:
            predicates.append(
                PredicateSpec(
                    table=table.name,
                    alias=alias,
                    column=column.name,
                    operator="is_null",
                    family="null",
                    selectivity=estimate_from_stats(column, stats, "null"),
                )
            )
        if column.is_numeric:
            histogram = stats.histogram_bounds if stats else []
            if len(histogram) >= 4:
                predicates.append(
                    PredicateSpec(
                        table=table.name,
                        alias=alias,
                        column=column.name,
                        operator="between",
                        value=[histogram[1], histogram[-2]],
                        family="range",
                        selectivity=estimate_from_stats(column, stats, "range"),
                    )
                )
            else:
                predicates.append(
                    PredicateSpec(
                        table=table.name,
                        alias=alias,
                        column=column.name,
                        operator="between",
                        value=[1, 100],
                        family="range",
                        selectivity=estimate_from_stats(column, stats, "range"),
                    )
                )
        if stats and stats.most_common_vals:
            predicates.append(
                PredicateSpec(
                    table=table.name,
                    alias=alias,
                    column=column.name,
                    operator="=",
                    value=stats.most_common_vals[0],
                    family="equality",
                    selectivity=estimate_from_stats(column, stats, "equality"),
                )
            )
            if len(stats.most_common_vals) >= 2:
                predicates.append(
                    PredicateSpec(
                        table=table.name,
                        alias=alias,
                        column=column.name,
                        operator="in",
                        value=stats.most_common_vals[:2],
                        family="membership",
                        selectivity=estimate_from_stats(column, stats, "membership"),
                    )
                )
        else:
            predicates.append(
                PredicateSpec(
                    table=table.name,
                    alias=alias,
                    column=column.name,
                    operator="=",
                    value=literal_from_stats(column, stats),
                    family="equality" if not column.is_text else "like",
                    selectivity=estimate_from_stats(column, stats, "equality" if not column.is_text else "like"),
                )
            )
        if column.is_text:
            predicates.append(
                PredicateSpec(
                    table=table.name,
                    alias=alias,
                    column=column.name,
                    operator="like",
                    value="A%",
                    family="like",
                    selectivity=estimate_from_stats(column, stats, "like"),
                )
            )
    deduped: dict[tuple[str, str, str, str], PredicateSpec] = {}
    for predicate in predicates:
        key = (predicate.column, predicate.operator, repr(predicate.value), predicate.family)
        deduped.setdefault(key, predicate)
    return list(deduped.values())


def top_seed_pool(
    table: TableSchema,
    alias: str,
    snapshot: StatsSnapshot,
    *,
    limit: int = 6,
) -> list[PredicateSpec]:
    seeds = build_predicate_seeds(table, alias, snapshot.column_stats.get(table.name, {}))
    family_rank = {"equality": 0, "membership": 1, "range": 2, "null": 3, "like": 4}
    return sorted(
        seeds,
        key=lambda predicate: (
            family_rank.get(predicate.family, 99),
            abs((predicate.selectivity or 0.10) - BUCKET_TARGETS["medium"]),
            predicate.column,
            predicate.operator,
        ),
    )[:limit]


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
) -> list[list[PredicateSpec]]:
    seed_pools = {
        table_name: top_seed_pool(catalog[table_name], aliases[table_name], snapshot, limit=per_table_seed_limit)
        for table_name in table_names
    }
    groups: list[list[PredicateSpec]] = [[]]

    for table_name in table_names:
        pool = seed_pools[table_name]
        groups.extend([[predicate] for predicate in pool])
        if max_predicates_per_query >= 2:
            groups.extend([list(combo) for combo in itertools.combinations(pool[: min(4, len(pool))], 2)])

    if max_predicates_per_query >= 2:
        for left_table, right_table in itertools.combinations(table_names, 2):
            for left in seed_pools[left_table][:3]:
                for right in seed_pools[right_table][:3]:
                    groups.append([left, right])

    if max_predicates_per_query >= 3:
        limited_tables = table_names[: min(4, len(table_names))]
        for table_combo in itertools.combinations(limited_tables, 3):
            if all(seed_pools[table_name] for table_name in table_combo):
                groups.append([seed_pools[table_name][0] for table_name in table_combo])
                if all(len(seed_pools[table_name]) > 1 for table_name in table_combo):
                    groups.append([seed_pools[table_name][1] for table_name in table_combo])

    return unique_predicate_groups(groups)


def unique_candidates_by_signature(candidates: list[SpjCandidate]) -> list[SpjCandidate]:
    deduped: dict[str, SpjCandidate] = {}
    for candidate in candidates:
        deduped.setdefault(candidate.signature(), candidate)
    return list(deduped.values())


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


def build_stage_one_candidates(
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
) -> list[SpjCandidate]:
    candidates: list[SpjCandidate] = []
    for table_name in sorted(catalog):
        table = catalog[table_name]
        aliases = alias_map([table_name])
        alias = aliases[table_name]
        stats = snapshot.column_stats.get(table_name, {})
        seeds = build_predicate_seeds(table, alias, stats)
        projections = build_projection_options([table], aliases)
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
) -> list[SpjCandidate]:
    child = catalog[edge.child_table]
    parent = catalog[edge.parent_table]
    child_alias, parent_alias = alias_map([child.name, parent.name]).values()
    child_seeds = top_seed_pool(child, child_alias, snapshot, limit=6)
    parent_seeds = top_seed_pool(parent, parent_alias, snapshot, limit=6)
    projections = build_projection_options([child, parent], {child.name: child_alias, parent.name: parent_alias})
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
    return unique_candidates_by_signature(candidates)


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

    table_specs = [catalog[table_name] for table_name in ordered_tables]
    projections = build_projection_options(table_specs, aliases)
    predicate_groups = expand_predicate_groups(
        ordered_tables,
        aliases,
        catalog,
        snapshot,
        per_table_seed_limit=4,
        max_predicates_per_query=min(3, max(1, config.max_predicates_per_table)),
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
    return unique_candidates_by_signature(candidates)


def enumerate_paths(join_edges: list[JoinEdge], length: int) -> list[list[JoinEdge]]:
    neighbors = undirected_neighbors(join_edges)
    paths: list[list[JoinEdge]] = []

    def walk(current_table: str, remaining: int, path: list[JoinEdge], used_tables: set[str]) -> None:
        if remaining == 0:
            paths.append(list(path))
            return
        for edge in sorted(neighbors[current_table], key=lambda item: item.key):
            next_table = edge.parent_table if edge.child_table == current_table else edge.child_table
            if next_table in used_tables:
                continue
            path.append(edge)
            used_tables.add(next_table)
            walk(next_table, remaining - 1, path, used_tables)
            used_tables.remove(next_table)
            path.pop()

    for edge in join_edges:
        for start in [edge.child_table, edge.parent_table]:
            used_tables = {start}
            walk(start, length, [], used_tables)

    deduped: dict[tuple[str, ...], list[JoinEdge]] = {}
    for path in paths:
        key = tuple(sorted(edge.key for edge in path))
        deduped.setdefault(key, path)
    return list(deduped.values())


def enumerate_star_edges(join_edges: list[JoinEdge], size: int) -> list[list[JoinEdge]]:
    neighbors = undirected_neighbors(join_edges)
    stars: list[list[JoinEdge]] = []
    for table_name, edges in sorted(neighbors.items()):
        if len(edges) < size - 1:
            continue
        for combo in itertools.combinations(sorted(edges, key=lambda item: item.key), size - 1):
            stars.append(list(combo))
    deduped: dict[tuple[str, ...], list[JoinEdge]] = {}
    for star in stars:
        key = tuple(sorted(edge.key for edge in star))
        deduped.setdefault(key, star)
    return list(deduped.values())


def build_star_candidates(
    star_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
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
    projections = build_projection_options([catalog[table] for table in tables], aliases)
    predicate_groups = expand_predicate_groups(
        tables,
        aliases,
        catalog,
        snapshot,
        per_table_seed_limit=3,
        max_predicates_per_query=min(3, max(1, config.max_predicates_per_table)),
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
    return unique_candidates_by_signature(candidates)


def build_multi_table_candidates(
    join_edges: list[JoinEdge],
    catalog: dict[str, TableSchema],
    snapshot: StatsSnapshot,
    config: GeneratorConfig,
) -> list[SpjCandidate]:
    candidates: list[SpjCandidate] = []
    for table_count in range(3, config.max_join_tables + 1):
        if config.stage_budgets.get(stage_key(table_count), 0) <= 0:
            continue
        path_edges_list = enumerate_paths(join_edges, table_count - 1)
        for path_edges in path_edges_list:
            candidates.extend(build_path_candidates(path_edges, catalog, snapshot, "inner", config))
            if "left" in config.join_types and table_count == 3:
                candidates.extend(build_path_candidates(path_edges, catalog, snapshot, "left", config))
        for star_edges in enumerate_star_edges(join_edges, table_count):
            candidates.extend(build_star_candidates(star_edges, catalog, snapshot, config))
    return unique_candidates_by_signature(candidates)


def balanced_select(candidates: list[SpjCandidate], budget: int, seed: int) -> list[SpjCandidate]:
    if budget <= 0 or not candidates:
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
    while len(selected) < budget and ordered_keys:
        next_round: list[tuple[Any, ...]] = []
        for key in ordered_keys:
            if groups[key]:
                selected.append(groups[key].pop(0))
                if len(selected) >= budget:
                    break
            if groups[key]:
                next_round.append(key)
        ordered_keys = next_round
    return selected


def calibrate_selected(
    candidates: list[SpjCandidate],
    snapshot: StatsSnapshot,
) -> None:
    if snapshot.mode != "stats_plus_selective_probes" or snapshot.provider is None:
        return
    for candidate in candidates:
        row_count = snapshot.provider.probe_count(candidate.render_sql())
        base_table_count = snapshot.table_counts.get(candidate.base_table, 0)
        candidate.observed_rows = row_count
        candidate.observed_selectivity = None if base_table_count == 0 else row_count / base_table_count
        candidate.target_selectivity_bucket = bucket_for_selectivity(candidate.observed_selectivity)
        candidate.calibrated = True
        candidate.calibration_source = "probe"


def emit_artifacts(
    selected: list[SpjCandidate],
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
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
    return manifest, artifacts


def generate_spj_workload(
    catalog: dict[str, TableSchema],
    join_edges: list[JoinEdge],
    config: GeneratorConfig,
    schema_path: str,
    snapshot: StatsSnapshot,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    stage_candidates: dict[str, list[SpjCandidate]] = {
        "1_table": build_stage_one_candidates(catalog, snapshot, config),
        "2_table": [candidate for edge in join_edges for candidate in build_pair_candidates_for_edge(edge, catalog, snapshot, config)],
    }
    for candidate in build_multi_table_candidates(join_edges, catalog, snapshot, config):
        stage_candidates.setdefault(candidate.stage_id, []).append(candidate)

    context = {"catalog": catalog, "join_edges": join_edges, "config": config, "snapshot": snapshot}
    for stage_id, candidates in list(stage_candidates.items()):
        stage_candidates[stage_id] = unique_candidates_by_signature(
            apply_template_packs(list(config.template_packs), candidates, context)
        )

    selected: list[SpjCandidate] = []
    for table_count in range(1, config.max_join_tables + 1):
        current_stage = stage_key(table_count)
        budget = config.stage_budgets.get(current_stage, 0)
        available = stage_candidates.get(current_stage, [])
        if budget > len(available):
            raise ValueError(
                f"Stage {current_stage} requested {budget} queries but only {len(available)} unique candidates were generated"
            )
        chosen = balanced_select(available, budget, config.seed + table_count)
        selected.extend(chosen)

    calibrate_selected(selected, snapshot)
    selected = sorted(unique_candidates_by_signature(selected), key=lambda item: (item.stage_id, item.join_topology, item.join_type, item.signature()))
    return emit_artifacts(selected, catalog, join_edges, config.benchmark, schema_path, config.suffix, config.seed)
