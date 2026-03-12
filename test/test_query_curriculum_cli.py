from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from query_curriculum import cli


TOY_SCHEMA = {
    "tables": [
        {
            "name": "customers",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "segment", "type": "varchar(16)"},
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
            "name": "lineitem",
            "columns": [
                {"name": "id", "type": "integer not null"},
                {"name": "order_id", "type": "integer not null"},
                {"name": "quantity", "type": "integer"},
            ],
            "primary key": {"column": "id"},
            "foreign keys": [{"column": "order_id", "foreign table": "orders", "foreign column": "id"}],
        },
    ]
}


class QueryCurriculumCliTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.benchmarks_root = self.root / "benchmarks"
        self.benchmark_dir = self.benchmarks_root / "toy"
        self.benchmark_dir.mkdir(parents=True)
        (self.benchmark_dir / "toy.dbschema.json").write_text(json.dumps(TOY_SCHEMA))

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_generate_writes_queries_suffix_directory(self) -> None:
        args = cli.parse_args(
            [
                "generate",
                "--benchmarks-root",
                str(self.benchmarks_root),
                "--benchmark",
                "toy",
                "--suffix",
                "unit",
                "--stage-budget",
                "1_table=3",
                "--stage-budget",
                "2_table=2",
                "--stage-budget",
                "3_table=1",
                "--max-join-tables",
                "3",
            ]
        )
        result = cli.run_generate(args)

        output_dir = self.benchmark_dir / "queries_unit"
        self.assertEqual(result["output_dir"], str(output_dir))
        self.assertTrue(output_dir.exists())
        self.assertTrue((output_dir / "manifest.json").exists())
        sql_files = sorted(path.name for path in output_dir.glob("*.sql"))
        self.assertGreater(len(sql_files), 0)

    def test_overwrite_requires_replace_output(self) -> None:
        args = cli.parse_args(
            [
                "generate",
                "--benchmarks-root",
                str(self.benchmarks_root),
                "--benchmark",
                "toy",
                "--suffix",
                "collision",
                "--stage-budget",
                "1_table=2",
                "--max-join-tables",
                "1",
            ]
        )
        cli.run_generate(args)
        with self.assertRaises(FileExistsError):
            cli.run_generate(args)

        replace_args = cli.parse_args(
            [
                "generate",
                "--benchmarks-root",
                str(self.benchmarks_root),
                "--benchmark",
                "toy",
                "--suffix",
                "collision",
                "--replace-output",
                "--stage-budget",
                "1_table=2",
                "--max-join-tables",
                "1",
            ]
        )
        cli.run_generate(replace_args)

    def test_probe_workers_flows_into_config(self) -> None:
        args = cli.parse_args(
            [
                "generate",
                "--benchmarks-root",
                str(self.benchmarks_root),
                "--benchmark",
                "toy",
                "--suffix",
                "workers",
                "--probe-workers",
                "6",
            ]
        )

        config = cli.build_config(args)

        self.assertEqual(config.probe_workers, 6)


if __name__ == "__main__":
    unittest.main()
