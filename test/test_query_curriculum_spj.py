from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from query_curriculum.templates import apply_template_packs
from query_curriculum.core import (
    ColumnStats,
    GeneratorConfig,
    PgConnectionConfig,
    StatsSnapshot,
    build_join_edges,
    load_stats_snapshot,
    load_schema_catalog,
)
from query_curriculum.spj import (
    JOB_LIKE_TEMPLATE_PACK,
    build_predicate_seeds,
    build_projection_templates,
    bind_projection_templates,
    bind_seed_template,
    build_seed_templates,
    build_multi_table_candidates,
    build_pair_candidates_for_edge,
    generate_spj_workload,
    make_build_context,
    retain_diverse_candidates,
    top_seed_pool,
    unique_candidates_by_signature,
)


TOY_SCHEMA = {
    "tables": [
        {
            "name": "customers",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "segment", "type": "varchar(16)"},
                {"name": "region", "type": "varchar(16)"},
            ],
            "primary key": {"column": "id"},
        },
        {
            "name": "orders",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "customer_id", "type": "integer not null"},
                {"name": "total_price", "type": "numeric"},
                {"name": "order_status", "type": "char(1)"},
            ],
            "primary key": {"column": "id"},
            "foreign keys": [{"column": "customer_id", "foreign table": "customers", "foreign column": "id"}],
        },
        {
            "name": "lineitem",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "order_id", "type": "integer not null"},
                {"name": "part_id", "type": "integer not null"},
                {"name": "quantity", "type": "integer"},
            ],
            "primary key": {"column": "id"},
            "foreign keys": [{"column": "order_id", "foreign table": "orders", "foreign column": "id"}],
        },
        {
            "name": "part",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "category", "type": "varchar(20)"},
                {"name": "retail_price", "type": "numeric"},
            ],
            "primary key": {"column": "id"},
        },
    ]
}

STAR_SCHEMA = {
    "tables": [
        {
            "name": "customers",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "segment_id", "type": "integer"},
            ],
            "primary key": {"column": "id"},
        },
        {
            "name": "orders",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "customer_id", "type": "integer not null"},
                {"name": "total_price", "type": "numeric"},
            ],
            "primary key": {"column": "id"},
            "foreign keys": [{"column": "customer_id", "foreign table": "customers", "foreign column": "id"}],
        },
        {
            "name": "payments",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "customer_id", "type": "integer not null"},
                {"name": "amount", "type": "numeric"},
            ],
            "primary key": {"column": "id"},
            "foreign keys": [{"column": "customer_id", "foreign table": "customers", "foreign column": "id"}],
        },
    ]
}


class FakeStatsProvider:
    def __init__(self) -> None:
        self.probe_calls = 0

    def probe_count(self, sql: str) -> int:
        self.probe_calls += 1
        return 10

    def clone(self):
        return FakeStatsProvider()

    def close(self) -> None:
        return None


class FakeSkippingStatsProvider:
    def __init__(self, zero_calls: int) -> None:
        self.zero_calls = zero_calls
        self.probe_calls = 0

    def probe_count(self, sql: str) -> int:
        self.probe_calls += 1
        if self.probe_calls <= self.zero_calls:
            return 0
        return 10

    def clone(self):
        return FakeSkippingStatsProvider(self.zero_calls)

    def close(self) -> None:
        return None


class CountingProbeProvider:
    instances: list["CountingProbeProvider"] = []

    def __init__(self) -> None:
        self.probe_calls = 0
        CountingProbeProvider.instances.append(self)

    def probe_count(self, sql: str) -> int:
        self.probe_calls += 1
        return 10

    def clone(self):
        return CountingProbeProvider()

    def close(self) -> None:
        return None


class QueryCurriculumSpjTest(unittest.TestCase):
    def setUp(self) -> None:
        CountingProbeProvider.instances = []
        self.temp_dir = tempfile.TemporaryDirectory()
        self.schema_path = Path(self.temp_dir.name) / "toy.dbschema.json"
        self.schema_path.write_text(json.dumps(TOY_SCHEMA))
        self.catalog = load_schema_catalog(self.schema_path)
        self.join_edges = build_join_edges(self.catalog)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _pair_candidates(self, snapshot: StatsSnapshot, config: GeneratorConfig) -> list:
        return [candidate for edge in self.join_edges for candidate in build_pair_candidates_for_edge(edge, self.catalog, snapshot, config)]

    def _template_context(self, snapshot: StatsSnapshot, config: GeneratorConfig, catalog=None, join_edges=None) -> dict:
        return {
            "catalog": catalog or self.catalog,
            "join_edges": join_edges or self.join_edges,
            "config": config,
            "snapshot": snapshot,
        }

    def _job_like_variant(self, baseline_candidate, candidates: list):
        for candidate in candidates:
            if candidate.template_pack != JOB_LIKE_TEMPLATE_PACK:
                continue
            if candidate.rule_family != f"job_like_{baseline_candidate.rule_family}":
                continue
            if candidate.join_type != baseline_candidate.join_type or candidate.join_topology != baseline_candidate.join_topology:
                continue
            if candidate.projection_width != baseline_candidate.projection_width:
                continue
            if candidate.projections != baseline_candidate.projections:
                continue
            if candidate.joins != baseline_candidate.joins:
                continue
            if candidate.predicates[: len(baseline_candidate.predicates)] != baseline_candidate.predicates:
                continue
            return candidate
        raise AssertionError("Matching JOB-like variant not found")

    def _rich_snapshot(self) -> StatsSnapshot:
        return StatsSnapshot(
            mode="stats_only",
            table_counts={"customers": 500, "orders": 2000, "lineitem": 8000, "part": 400},
            column_stats={
                "customers": {
                    "id": ColumnStats(histogram_bounds=[1, 50, 100, 200, 350, 500]),
                    "segment": ColumnStats(
                        most_common_vals=["AUTO", "BUILD", "HOME", "TECH", "TOYS", "GARDEN"],
                        most_common_freqs=[0.28, 0.19, 0.13, 0.09, 0.06, 0.04],
                    ),
                    "region": ColumnStats(
                        null_frac=0.18,
                        most_common_vals=["N", "S", "E", "W", "C"],
                        most_common_freqs=[0.22, 0.18, 0.14, 0.09, 0.05],
                    ),
                },
                "orders": {
                    "customer_id": ColumnStats(histogram_bounds=[1, 50, 200, 700, 1400, 2000], n_distinct=2000),
                    "total_price": ColumnStats(histogram_bounds=[5, 25, 75, 150, 300, 600, 1200]),
                    "order_status": ColumnStats(
                        most_common_vals=["F", "O", "P", "R", "S", "X"],
                        most_common_freqs=[0.31, 0.22, 0.14, 0.09, 0.05, 0.03],
                    ),
                },
                "lineitem": {
                    "order_id": ColumnStats(histogram_bounds=[1, 100, 500, 1500, 4000, 8000], n_distinct=8000),
                    "part_id": ColumnStats(histogram_bounds=[1, 40, 100, 300, 1000, 3000], n_distinct=3000),
                    "quantity": ColumnStats(
                        histogram_bounds=[1, 2, 5, 10, 20, 40, 80],
                        most_common_vals=[1, 2, 5, 10, 20],
                        most_common_freqs=[0.16, 0.12, 0.09, 0.06, 0.04],
                    ),
                },
                "part": {
                    "category": ColumnStats(
                        null_frac=0.05,
                        most_common_vals=["A", "B", "C", "D", "E", "F"],
                        most_common_freqs=[0.24, 0.18, 0.13, 0.08, 0.05, 0.03],
                    ),
                    "retail_price": ColumnStats(histogram_bounds=[10, 20, 40, 80, 160, 320, 640]),
                },
            },
        )

    def test_balanced_generation_and_reproducibility(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="spjtest",
            max_join_tables=3,
            stage_budgets={"1_table": 4, "2_table": 4, "3_table": 2},
            seed=7,
        )
        snapshot = StatsSnapshot(mode="schema_only")
        manifest_a, artifacts_a = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)
        manifest_b, artifacts_b = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual([artifact.sql for artifact in artifacts_a], [artifact.sql for artifact in artifacts_b])
        self.assertEqual(len({artifact.sql for artifact in artifacts_a}), len(artifacts_a))

        stage_counts: dict[str, int] = {}
        for query in manifest_a["queries"]:
            stage_counts[query["stage_id"]] = stage_counts.get(query["stage_id"], 0) + 1
            self.assertFalse(query["calibrated"])
            for edge in query["join_edges"]:
                self.assertIn(edge["child_table"], self.catalog)
                self.assertIn(edge["parent_table"], self.catalog)

        self.assertEqual(stage_counts.get("1_table"), 4)
        self.assertEqual(stage_counts.get("2_table"), 4)
        self.assertEqual(stage_counts.get("3_table"), 2)

    def test_selective_probing_only_selected_queries(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="probe",
            max_join_tables=2,
            stage_budgets={"1_table": 3, "2_table": 3},
            seed=11,
            probe_workers=1,
            stats_mode="stats_plus_selective_probes",
        )
        provider = FakeStatsProvider()
        snapshot = StatsSnapshot(
            mode="stats_plus_selective_probes",
            table_counts={"customers": 100, "orders": 100, "lineitem": 100, "part": 100},
            column_stats={
                "customers": {"segment": ColumnStats(most_common_vals=["AUTO"], most_common_freqs=[0.2])},
                "orders": {"order_status": ColumnStats(most_common_vals=["F"], most_common_freqs=[0.3])},
            },
            provider=provider,
        )
        manifest, artifacts = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual(provider.probe_calls, len(artifacts))
        self.assertEqual(manifest["query_count"], len(artifacts))
        for query in manifest["queries"]:
            self.assertTrue(query["calibrated"])
            self.assertEqual(query["calibration_source"], "probe")

    def test_pg_enabled_requires_psycopg2(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="probe",
            pg=PgConnectionConfig(enabled=True),
        )
        with mock.patch("query_curriculum.core.psycopg2", None):
            with self.assertRaisesRegex(RuntimeError, "psycopg2 is required"):
                load_stats_snapshot(config, self.catalog)

    def test_probe_mode_skips_zero_row_queries(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="probe",
            max_join_tables=1,
            stage_budgets={"1_table": 2},
            seed=3,
            probe_workers=1,
            stats_mode="stats_plus_selective_probes",
        )
        provider = FakeSkippingStatsProvider(zero_calls=2)
        snapshot = StatsSnapshot(
            mode="stats_plus_selective_probes",
            table_counts={"customers": 100, "orders": 100, "lineitem": 100, "part": 100},
            provider=provider,
        )
        manifest, artifacts = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual(len(artifacts), 2)
        self.assertGreater(provider.probe_calls, len(artifacts))
        for query in manifest["queries"]:
            self.assertTrue(query["calibrated"])
            self.assertGreater(query["observed_rows"], 0)

    def test_no_fallback_predicates_without_stats(self) -> None:
        seeds = build_predicate_seeds(self.catalog["customers"], "c", {})

        self.assertEqual(seeds, [])

    def test_stats_backed_predicates_are_diverse_without_like_fallbacks(self) -> None:
        snapshot = self._rich_snapshot()
        seeds = build_predicate_seeds(self.catalog["customers"], "c", snapshot.column_stats["customers"])
        segment_predicates = [predicate for predicate in seeds if predicate.column == "segment"]
        region_predicates = [predicate for predicate in seeds if predicate.column == "region"]

        self.assertGreaterEqual(len([predicate for predicate in segment_predicates if predicate.operator == "="]), 4)
        self.assertGreaterEqual(len([predicate for predicate in segment_predicates if predicate.operator == "in"]), 2)
        self.assertFalse(any(predicate.operator == "like" for predicate in seeds))
        self.assertEqual(sorted(predicate.operator for predicate in region_predicates if predicate.family == "null"), ["is_not_null", "is_null"])

    def test_histogram_ranges_replace_fk_equality_without_mcv(self) -> None:
        snapshot = self._rich_snapshot()
        seeds = build_predicate_seeds(self.catalog["orders"], "o", snapshot.column_stats["orders"])
        customer_id_predicates = [predicate for predicate in seeds if predicate.column == "customer_id"]
        total_price_predicates = [predicate for predicate in seeds if predicate.column == "total_price"]

        self.assertFalse(any(predicate.operator == "=" for predicate in customer_id_predicates))
        self.assertGreaterEqual(len(total_price_predicates), 3)
        self.assertTrue(any(predicate.operator == "<=" for predicate in total_price_predicates))
        self.assertTrue(any(predicate.operator == ">=" for predicate in total_price_predicates))
        self.assertTrue(any(predicate.operator == "between" for predicate in total_price_predicates))

    def test_top_seed_pool_prefers_bucket_and_family_diversity(self) -> None:
        snapshot = self._rich_snapshot()
        pool = top_seed_pool(self.catalog["orders"], "o", snapshot, limit=6)

        self.assertGreaterEqual(len({predicate.family for predicate in pool}), 2)
        self.assertIn("range", {predicate.family for predicate in pool})
        self.assertFalse(any(predicate.operator == "like" for predicate in pool))
        self.assertGreaterEqual(len({predicate.column for predicate in pool}), 2)

    def test_cached_seed_binding_matches_direct_generation(self) -> None:
        snapshot = self._rich_snapshot()
        build_context = make_build_context(self.catalog, snapshot, GeneratorConfig(benchmark="toy", suffix="cache"))
        table = self.catalog["orders"]

        direct = build_predicate_seeds(table, "o", snapshot.column_stats["orders"])
        cached_templates = build_context.seed_templates_by_table.setdefault(
            "orders",
            build_seed_templates(table, snapshot.column_stats["orders"]),
        )
        rebound = [bind_seed_template(seed, "o") for seed in cached_templates]

        self.assertEqual(
            [(predicate.column, predicate.operator, predicate.value, predicate.family) for predicate in direct],
            [(predicate.column, predicate.operator, predicate.value, predicate.family) for predicate in rebound],
        )

    def test_cached_projection_binding_matches_direct_generation(self) -> None:
        aliases = {"customers": "c", "orders": "o"}
        direct = [
            (width, [(table, alias, column) for table, alias, column in columns])
            for width, columns in bind_projection_templates(
                build_projection_templates([self.catalog["customers"], self.catalog["orders"]]),
                aliases,
            )
        ]
        rebound = build_projection_templates([self.catalog["customers"], self.catalog["orders"]])

        self.assertEqual(
            direct,
            bind_projection_templates(rebound, aliases),
        )

    def test_schema_only_generation_fails_when_budget_exceeds_no_fallback_pool(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="schema_only_fail",
            max_join_tables=1,
            stage_budgets={"1_table": 17},
        )

        with self.assertRaisesRegex(ValueError, "literal fallbacks are disabled"):
            generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), StatsSnapshot(mode="schema_only"))

    def test_structural_dedup_preserves_sql_uniqueness(self) -> None:
        snapshot = self._rich_snapshot()
        config = GeneratorConfig(benchmark="toy", suffix="dedup", max_join_tables=2)

        candidates = self._pair_candidates(snapshot, config)
        sql_texts = {candidate.render_sql() for candidate in candidates}
        structural_signatures = {candidate.signature() for candidate in candidates}

        self.assertEqual(len(sql_texts), len(structural_signatures))

    def test_retain_diverse_candidates_is_deterministic(self) -> None:
        snapshot = self._rich_snapshot()
        config = GeneratorConfig(benchmark="toy", suffix="retain", max_join_tables=2)
        candidates = self._pair_candidates(snapshot, config)

        retained_a = retain_diverse_candidates(candidates, limit=10, seed=17)
        retained_b = retain_diverse_candidates(candidates, limit=10, seed=17)

        self.assertEqual([candidate.signature() for candidate in retained_a], [candidate.signature() for candidate in retained_b])
        self.assertGreaterEqual(len({candidate.projection_width for candidate in retained_a}), 2)
        self.assertGreaterEqual(len({candidate.target_selectivity_bucket for candidate in retained_a}), 2)

    def test_parallel_probing_uses_cloned_providers_and_records_diagnostics(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="parallel_probe",
            max_join_tables=2,
            stage_budgets={"1_table": 3, "2_table": 3},
            seed=19,
            probe_workers=3,
            stats_mode="stats_plus_selective_probes",
        )
        provider = CountingProbeProvider()
        snapshot = StatsSnapshot(
            mode="stats_plus_selective_probes",
            table_counts={"customers": 100, "orders": 100, "lineitem": 100, "part": 100},
            column_stats={
                "customers": {"segment": ColumnStats(most_common_vals=["AUTO"], most_common_freqs=[0.2])},
                "orders": {"order_status": ColumnStats(most_common_vals=["F"], most_common_freqs=[0.3])},
            },
            provider=provider,
        )

        manifest, artifacts = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual(len(artifacts), 6)
        self.assertGreater(len(CountingProbeProvider.instances), 1)
        diagnostics = manifest["generation_diagnostics"]
        self.assertGreater(diagnostics["1_table"]["probe_calls"], 0)
        self.assertGreater(diagnostics["2_table"]["probe_calls"], 0)

    def test_multi_table_stages_skip_live_probing(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="stage_probe_policy",
            max_join_tables=3,
            stage_budgets={"1_table": 2, "2_table": 2, "3_table": 2},
            seed=23,
            probe_workers=3,
            stats_mode="stats_plus_selective_probes",
        )
        provider = CountingProbeProvider()
        snapshot = StatsSnapshot(
            mode="stats_plus_selective_probes",
            table_counts={"customers": 100, "orders": 100, "lineitem": 100, "part": 100},
            column_stats={
                "customers": {"segment": ColumnStats(most_common_vals=["AUTO"], most_common_freqs=[0.2])},
                "orders": {"order_status": ColumnStats(most_common_vals=["F"], most_common_freqs=[0.3])},
                "lineitem": {"quantity": ColumnStats(histogram_bounds=[1, 2, 5, 10, 20, 40])},
            },
            provider=provider,
        )

        manifest, artifacts = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual(len(artifacts), 6)
        diagnostics = manifest["generation_diagnostics"]
        self.assertGreater(diagnostics["1_table"]["probe_calls"], 0)
        self.assertGreater(diagnostics["2_table"]["probe_calls"], 0)
        self.assertEqual(diagnostics["3_table"]["probe_calls"], 0)
        stage_three_queries = [query for query in manifest["queries"] if query["stage_id"] == "3_table"]
        self.assertTrue(stage_three_queries)
        self.assertTrue(all(not query["calibrated"] for query in stage_three_queries))

    def test_multi_table_candidates_use_lightweight_predicate_groups(self) -> None:
        snapshot = self._rich_snapshot()
        config = GeneratorConfig(benchmark="toy", suffix="light_multi", max_join_tables=3, join_types=("inner",))

        candidates = [candidate for candidate in build_multi_table_candidates(self.join_edges, self.catalog, snapshot, config) if candidate.stage_id == "3_table"]

        self.assertTrue(candidates)
        self.assertTrue(all(len(candidate.predicates) <= 2 for candidate in candidates))

    def test_job_like_template_pack_appends_candidates_and_preserves_left_joins(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="joblike",
            max_join_tables=2,
            join_types=("inner", "left"),
        )
        snapshot = self._rich_snapshot()
        baseline_pairs = self._pair_candidates(snapshot, config)
        enriched_pairs = apply_template_packs(
            [JOB_LIKE_TEMPLATE_PACK],
            baseline_pairs,
            self._template_context(snapshot, config),
        )

        self.assertGreater(len(enriched_pairs), len(baseline_pairs))
        self.assertEqual(
            len([candidate for candidate in enriched_pairs if candidate.join_type == "left"]),
            len([candidate for candidate in baseline_pairs if candidate.join_type == "left"]),
        )
        self.assertTrue(any(candidate.template_pack == JOB_LIKE_TEMPLATE_PACK for candidate in enriched_pairs))
        self.assertTrue(all(candidate.template_pack is None for candidate in baseline_pairs))
        self.assertTrue(
            all(candidate.template_pack is None for candidate in enriched_pairs if candidate.join_type == "left")
        )

    def test_job_like_pair_sql_uses_implicit_join_and_reuses_projection(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="joblike",
            max_join_tables=2,
            join_types=("inner",),
        )
        snapshot = self._rich_snapshot()
        baseline_pairs = self._pair_candidates(snapshot, config)
        enriched_pairs = apply_template_packs(
            [JOB_LIKE_TEMPLATE_PACK],
            baseline_pairs,
            self._template_context(snapshot, config),
        )
        baseline_candidate = next(
            candidate
            for candidate in baseline_pairs
            if candidate.join_type == "inner" and candidate.projection_width == "medium" and len(candidate.predicates) >= 1
        )
        job_like_candidate = self._job_like_variant(baseline_candidate, enriched_pairs)
        sql = job_like_candidate.render_sql()

        self.assertEqual(job_like_candidate.projections, baseline_candidate.projections)
        self.assertIn(", ", sql.splitlines()[1])
        self.assertNotIn("JOIN", sql)
        self.assertIn("WHERE ", sql)
        self.assertIn(
            f"{baseline_candidate.joins[0].left_alias}.{baseline_candidate.joins[0].left_column} = "
            f"{baseline_candidate.joins[0].right_alias}.{baseline_candidate.joins[0].right_column}",
            sql,
        )
        self.assertGreaterEqual(sql.count(" > "), len(job_like_candidate.tables))

    def test_job_like_chain_sql_uses_where_join_predicates(self) -> None:
        config = GeneratorConfig(
            benchmark="toy",
            suffix="joblike",
            max_join_tables=3,
            join_types=("inner",),
        )
        snapshot = self._rich_snapshot()
        baseline_multi = [candidate for candidate in build_multi_table_candidates(self.join_edges, self.catalog, snapshot, config) if candidate.join_topology == "chain"]
        enriched_multi = apply_template_packs(
            [JOB_LIKE_TEMPLATE_PACK],
            baseline_multi,
            self._template_context(snapshot, config),
        )
        baseline_candidate = next(candidate for candidate in baseline_multi if candidate.projection_width == "wide" and len(candidate.predicates) >= 1)
        job_like_candidate = self._job_like_variant(baseline_candidate, enriched_multi)
        sql = job_like_candidate.render_sql()

        self.assertEqual(job_like_candidate.projections, baseline_candidate.projections)
        self.assertIn(", ", sql.splitlines()[1])
        self.assertNotIn("JOIN", sql)
        self.assertGreaterEqual(sql.count(" = "), len(job_like_candidate.joins))
        self.assertGreaterEqual(sql.count(" > "), len(job_like_candidate.tables))

    def test_job_like_star_sql_uses_where_join_predicates(self) -> None:
        star_path = Path(self.temp_dir.name) / "star.dbschema.json"
        star_path.write_text(json.dumps(STAR_SCHEMA))
        star_catalog = load_schema_catalog(star_path)
        star_join_edges = build_join_edges(star_catalog)
        config = GeneratorConfig(
            benchmark="star",
            suffix="joblike",
            max_join_tables=3,
            join_types=("inner",),
        )
        snapshot = StatsSnapshot(
            mode="stats_only",
            table_counts={"customers": 200, "orders": 1200, "payments": 1200},
            column_stats={
                "customers": {"id": ColumnStats(histogram_bounds=[1, 10, 30, 70, 120, 200])},
                "orders": {
                    "customer_id": ColumnStats(histogram_bounds=[1, 25, 100, 400, 800, 1200]),
                    "total_price": ColumnStats(histogram_bounds=[5, 20, 60, 120, 240, 480]),
                },
                "payments": {
                    "customer_id": ColumnStats(histogram_bounds=[1, 25, 100, 400, 800, 1200]),
                    "amount": ColumnStats(histogram_bounds=[1, 10, 25, 50, 100, 250]),
                },
            },
        )
        baseline_multi = [candidate for candidate in build_multi_table_candidates(star_join_edges, star_catalog, snapshot, config) if candidate.join_topology == "star"]
        enriched_multi = apply_template_packs(
            [JOB_LIKE_TEMPLATE_PACK],
            baseline_multi,
            self._template_context(snapshot, config, star_catalog, star_join_edges),
        )
        baseline_candidate = next(candidate for candidate in baseline_multi if candidate.projection_width == "medium")
        job_like_candidate = self._job_like_variant(baseline_candidate, enriched_multi)
        sql = job_like_candidate.render_sql()

        self.assertEqual(job_like_candidate.projections, baseline_candidate.projections)
        self.assertIn(", ", sql.splitlines()[1])
        self.assertNotIn("JOIN", sql)
        self.assertGreaterEqual(sql.count(" = "), len(job_like_candidate.joins))
        self.assertGreaterEqual(sql.count(" > "), len(job_like_candidate.tables))

    def test_job_like_generation_is_reproducible_and_marks_manifest(self) -> None:
        base_config = GeneratorConfig(
            benchmark="toy",
            suffix="joblike",
            max_join_tables=2,
            join_types=("inner",),
            template_packs=(JOB_LIKE_TEMPLATE_PACK,),
        )
        snapshot = self._rich_snapshot()
        available_pairs = unique_candidates_by_signature(apply_template_packs(
            [JOB_LIKE_TEMPLATE_PACK],
            self._pair_candidates(snapshot, base_config),
            self._template_context(snapshot, base_config),
        ))
        config = GeneratorConfig(
            benchmark="toy",
            suffix="joblike",
            max_join_tables=2,
            join_types=("inner",),
            template_packs=(JOB_LIKE_TEMPLATE_PACK,),
            stage_budgets={"1_table": 0, "2_table": len(available_pairs)},
        )

        manifest_a, artifacts_a = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)
        manifest_b, artifacts_b = generate_spj_workload(self.catalog, self.join_edges, config, str(self.schema_path), snapshot)

        self.assertEqual([artifact.sql for artifact in artifacts_a], [artifact.sql for artifact in artifacts_b])
        self.assertTrue(any(query["template_pack"] == JOB_LIKE_TEMPLATE_PACK for query in manifest_a["queries"]))
        self.assertTrue(any(query["template_pack"] is None for query in manifest_a["queries"]))
        self.assertTrue(
            all(query["rule_family"].startswith("job_like_") for query in manifest_a["queries"] if query["template_pack"] == JOB_LIKE_TEMPLATE_PACK)
        )


if __name__ == "__main__":
    unittest.main()
