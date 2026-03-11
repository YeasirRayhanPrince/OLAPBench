from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from query_curriculum.core import (
    ColumnStats,
    GeneratorConfig,
    PgConnectionConfig,
    StatsSnapshot,
    build_join_edges,
    load_stats_snapshot,
    load_schema_catalog,
)
from query_curriculum.spj import generate_spj_workload


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


class FakeStatsProvider:
    def __init__(self) -> None:
        self.probe_calls = 0

    def probe_count(self, sql: str) -> int:
        self.probe_calls += 1
        return 10

    def close(self) -> None:
        return None


class QueryCurriculumSpjTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.schema_path = Path(self.temp_dir.name) / "toy.dbschema.json"
        self.schema_path.write_text(json.dumps(TOY_SCHEMA))
        self.catalog = load_schema_catalog(self.schema_path)
        self.join_edges = build_join_edges(self.catalog)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

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


if __name__ == "__main__":
    unittest.main()
