import argparse
import os
import pathlib

import natsort

from benchmarks import benchmark
from benchmarks.job.job import JOB


class SQLStorm(JOB):
    @property
    def nice_name(self) -> str:
        return "SQLStorm"

    @property
    def fullname(self) -> str:
        return "SQLStorm v1.0 JOB workload"

    @property
    def description(self) -> str:
        return "SQLStorm v1.0 JOB queries on the IMDB schema"

    @property
    def unique_name(self) -> str:
        return "sqlstorm"

    @property
    def queries_path(self) -> str:
        repo_root = pathlib.Path(__file__).resolve().parents[2]
        query_dir_name = "queries" if self.query_dir is None else f"queries_{self.query_dir}"
        query_dir = repo_root / "SQLStorm" / "v1.0" / "job" / query_dir_name
        if not query_dir.is_dir():
            raise FileNotFoundError(f"SQLStorm query directory not found: {query_dir}")
        return str(query_dir)

    def queries(self, dbms_name: str) -> tuple[list[tuple[str, str]], dict[str, dict[str, str]]]:
        result = []
        overrides: dict[str, dict[str, str]] = {}
        base_queries: list[str] = []

        dbms_name_map = {
            "umbradev": "umbra",
            "apollo": "sqlserver"
        }
        dbms_name = dbms_name_map.get(dbms_name, dbms_name)

        queries_dir = pathlib.Path(self.queries_path)
        for query_file in natsort.natsorted(os.listdir(queries_dir)):
            query_path = queries_dir / query_file
            if not query_path.is_file():
                continue

            suffixes = pathlib.Path(query_file).suffixes
            if suffixes == [".sql"]:
                base_queries.append(query_file)
                continue

            # Collect specialized variants in one pass to avoid an expensive per-file glob.
            if len(suffixes) >= 2 and suffixes[-2] == ".sql":
                specialized_dbms = suffixes[-1][1:]
                base_query_file = query_file[: -len(suffixes[-1])]
                overrides.setdefault(specialized_dbms, {})[base_query_file] = query_path.read_text().strip()

        for query_file in base_queries:
            if self._included_queries and query_file not in self._included_queries:
                continue
            if self._excluded_queries and query_file in self._excluded_queries:
                continue

            query = (queries_dir / query_file).read_text().strip()
            query = overrides.get(dbms_name, {}).get(query_file, query)
            result.append((query_file, query))

        return result, overrides


class SQLStormDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "sqlstorm"

    @staticmethod
    def get_description() -> str:
        return "SQLStorm JOB workload"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return SQLStorm(base_dir, args, included_queries, excluded_queries)
