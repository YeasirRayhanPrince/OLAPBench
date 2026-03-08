import argparse
import os
import pathlib

from benchmarks import benchmark
from benchmarks.job.job import JOB


class GenDBAPool(JOB):
    @property
    def path(self) -> pathlib.Path:
        return pathlib.Path(__file__).parent.resolve()

    @property
    def nice_name(self) -> str:
        return "GenDBA Pool"

    @property
    def fullname(self) -> str:
        return "GenDBA Pool JOB workload"

    @property
    def description(self) -> str:
        return "GenDBA Pool (Empty)" if self.zero else "GenDBA Pool queries on the IMDB schema"

    @property
    def unique_name(self) -> str:
        return "gendba_pool" + ("_zero" if self.zero else "")

    @property
    def queries_path(self) -> str:
        return os.path.join(self.path, "queries" + ("" if self.query_dir is None else f"_{self.query_dir}"))


class GenDBAPoolDescription(benchmark.BenchmarkDescription):
    @staticmethod
    def get_name() -> str:
        return "gendba-pool"

    @staticmethod
    def get_description() -> str:
        return "GenDBA Pool JOB workload"

    @staticmethod
    def add_arguments(parser: argparse.ArgumentParser):
        benchmark.BenchmarkDescription.add_arguments(parser)
        parser.add_argument("-z", "--zero", dest="zero", default=False, action="store_true", help="empty tables")

    @staticmethod
    def instantiate(base_dir: str, args: dict, included_queries: list[str] = None, excluded_queries: list[str] = None) -> benchmark.Benchmark:
        return GenDBAPool(base_dir, args, included_queries, excluded_queries)
