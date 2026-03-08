#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import urllib.error
import urllib.request


BUCKET_TARGETS = {
    "very_high": 0.75,
    "high": 0.30,
    "medium": 0.10,
    "low": 0.02,
    "very_low": 0.002,
}

BUCKET_ORDER = ["very_high", "high", "medium", "low", "very_low"]
NESTED_SHAPES = ["in", "exists", "scalar_count", "derived_join", "cte_in", "cte_exists"]
DEFAULT_POSTGRES_VERSION = "18"
DEFAULT_PROBE_DBMS = "postgres_probe"
PROBE_QUERY_TIMEOUT_SECONDS = 30
MANAGED_PROBE_STARTUP_TIMEOUT_SECONDS = 900


@dataclass
class QueryArtifact:
    filename: str
    sql: str
    metadata: dict[str, Any]


@dataclass
class ColumnSchema:
    name: str
    type: str


@dataclass
class ForeignKey:
    column: str
    foreign_table: str
    foreign_column: str


@dataclass
class TableSchema:
    name: str
    columns: dict[str, ColumnSchema]
    primary_key: str | None
    foreign_keys: list[ForeignKey]


@dataclass
class ProfileCandidate:
    id: str
    family: str
    table: str
    name: str
    bucket: str
    predicates: list[dict[str, Any]]
    estimated_selectivity: float | None = None
    observed_rows: int | None = None
    observed_selectivity: float | None = None
    source: str = "schema"
    score: float = 0.0
    metadata: dict[str, Any] = field(default_factory=dict)

    def summary(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "family": self.family,
            "table": self.table,
            "name": self.name,
            "bucket": self.bucket,
            "estimated_selectivity": self.estimated_selectivity,
            "observed_rows": self.observed_rows,
            "observed_selectivity": self.observed_selectivity,
            "source": self.source,
            "predicates": self.predicates,
            "score": round(self.score, 6),
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class WorkloadPreset:
    name: str
    single_cases_per_table: int
    nested_cases_per_family: int
    max_nested_families: int
    dimension_table_max_rows: int
    mcv_value_limit: int
    membership_widths: tuple[int, ...]
    range_quantiles: tuple[float, ...]
    max_range_candidates_per_column: int
    conjunction_pool_size: int
    max_conjunctions_per_table: int
    conjunctions_fill_only: bool
    outer_predicate_limit: int
    allow_hybrid_dimension_cases: bool
    hybrid_cases_per_family: int


WORKLOAD_PRESETS = {
    "standard": WorkloadPreset(
        name="standard",
        single_cases_per_table=6,
        nested_cases_per_family=3,
        max_nested_families=12,
        dimension_table_max_rows=50_000,
        mcv_value_limit=3,
        membership_widths=(2,),
        range_quantiles=(0.20, 0.50, 0.80),
        max_range_candidates_per_column=4,
        conjunction_pool_size=6,
        max_conjunctions_per_table=6,
        conjunctions_fill_only=True,
        outer_predicate_limit=1,
        allow_hybrid_dimension_cases=False,
        hybrid_cases_per_family=0,
    ),
    "exhaustive": WorkloadPreset(
        name="exhaustive",
        single_cases_per_table=10,
        nested_cases_per_family=20,
        max_nested_families=24,
        dimension_table_max_rows=1_000_000,
        mcv_value_limit=10,
        membership_widths=(2, 3, 4, 5),
        range_quantiles=(0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95),
        max_range_candidates_per_column=12,
        conjunction_pool_size=20,
        max_conjunctions_per_table=20,
        conjunctions_fill_only=False,
        outer_predicate_limit=10,
        allow_hybrid_dimension_cases=True,
        hybrid_cases_per_family=20,
    ),
}


class ServerProbeAdapter:
    def __init__(self, url: str, dbms: str, timeout: int = 30):
        self.url = url.rstrip("/") + "/query"
        self.dbms = dbms
        self.timeout = timeout

    def _request(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = urllib.request.Request(
            self.url,
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout + 5) as response:
                response_payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"probe request failed: {exc}") from exc

        if response_payload.get("status") != "success":
            raise RuntimeError(response_payload.get("error", "unknown probe failure"))
        return response_payload

    def query_rows(self, query: str, fetch_result_limit: int = 1000) -> list[list[Any]]:
        payload = {
            "dbms": self.dbms,
            "query": query,
            "timeout": self.timeout,
            "fetch_result": True,
            "fetch_result_limit": fetch_result_limit,
        }
        return self._request(payload).get("result") or []

    def query_scalar(self, query: str) -> Any:
        rows = self.query_rows(query, fetch_result_limit=1)
        if not rows or not rows[0]:
            raise RuntimeError("probe query returned no rows")
        return rows[0][0]

    def measure_count(self, query: str) -> int:
        return int(self.query_scalar(query))


class ManagedProbeServer:
    def __init__(
        self,
        benchmark: str,
        postgres_version: str,
        port: int,
        base_port: int,
        db_dir: Path,
        data_dir: Path,
        host: str = "127.0.0.1",
    ):
        self.benchmark = benchmark
        self.postgres_version = postgres_version
        self.port = port
        self.base_port = base_port
        self.db_dir = db_dir
        self.data_dir = data_dir
        self.host = host
        self.process: subprocess.Popen[str] | None = None
        self.config_path: Path | None = None
        self.log_path: Path | None = None

    def __enter__(self) -> ServerProbeAdapter:
        config_payload = "\n".join(
            [
                "benchmark:",
                f"  name: {self.benchmark}",
                "systems:",
                f"  - title: {DEFAULT_PROBE_DBMS}",
                "    dbms: postgres",
                "    parameter:",
                f"      version: \"{self.postgres_version}\"",
                "    settings: {}",
                "",
            ]
        )
        config_file = tempfile.NamedTemporaryFile(prefix="gendba-pool-probe-", suffix=".yaml", delete=False)
        config_file.write(config_payload.encode("utf-8"))
        config_file.close()
        self.config_path = Path(config_file.name)

        log_file = tempfile.NamedTemporaryFile(prefix="gendba-pool-probe-", suffix=".log", delete=False)
        log_file.close()
        self.log_path = Path(log_file.name)

        command = [
            sys.executable,
            str(repo_root() / "server.py"),
            "-j",
            str(self.config_path),
            "--host",
            self.host,
            "--port",
            str(self.port),
            "--base-port",
            str(self.base_port),
            "--db-dir",
            str(self.db_dir),
            "--data-dir",
            str(self.data_dir),
            self.benchmark,
        ]
        with self.log_path.open("w") as handle:
            self.process = subprocess.Popen(
                command,
                cwd=repo_root(),
                stdout=handle,
                stderr=subprocess.STDOUT,
                text=True,
            )

        health_url = f"http://{self.host}:{self.port}/health"
        deadline = time.time() + MANAGED_PROBE_STARTUP_TIMEOUT_SECONDS
        try:
            while time.time() < deadline:
                if self.process and self.process.poll() is not None:
                    raise RuntimeError(self._startup_failure("probe server exited early"))
                try:
                    with urllib.request.urlopen(health_url, timeout=2) as response:
                        payload = json.loads(response.read().decode("utf-8"))
                    if payload.get("status") == "ok":
                        return ServerProbeAdapter(
                            f"http://{self.host}:{self.port}",
                            DEFAULT_PROBE_DBMS,
                            PROBE_QUERY_TIMEOUT_SECONDS,
                        )
                except Exception:
                    time.sleep(1)
        except Exception:
            self.__exit__(None, None, None)
            raise

        failure = self._startup_failure("timed out waiting for probe server")
        self.__exit__(None, None, None)
        raise RuntimeError(failure)

    def _startup_failure(self, message: str) -> str:
        log_tail = ""
        if self.log_path and self.log_path.exists():
            lines = self.log_path.read_text().splitlines()
            log_tail = "\n".join(lines[-40:])
        return f"{message}\n{log_tail}".strip()

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.process and self.process.poll() is None:
            # Prefer SIGINT so server.py reaches its graceful shutdown path and
            # calls cleanup_dbms(), which stops the Docker-backed Postgres
            # container and releases the fixed host port.
            self.process.send_signal(signal.SIGINT)
            try:
                self.process.wait(timeout=30)
            except subprocess.TimeoutExpired:
                self.process.terminate()
                try:
                    self.process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.process.kill()
                    self.process.wait(timeout=5)
        if self.config_path and self.config_path.exists():
            self.config_path.unlink()
        if self.log_path and self.log_path.exists():
            self.log_path.unlink()


class OpenAICompatibleClient:
    def __init__(self, base_url: str, model: str, api_key: str | None, timeout: int = 60):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.api_key = api_key
        self.timeout = timeout

    def chat_json(self, system_prompt: str, user_payload: dict[str, Any]) -> dict[str, Any]:
        body = {
            "model": self.model,
            "temperature": 0.2,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(user_payload, indent=2, sort_keys=True)},
            ],
        }
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        request = urllib.request.Request(
            self.base_url + "/chat/completions",
            data=json.dumps(body).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except urllib.error.URLError as exc:
            raise RuntimeError(f"llm request failed: {exc}") from exc

        choices = payload.get("choices") or []
        if not choices:
            raise RuntimeError("llm returned no choices")
        content = choices[0].get("message", {}).get("content")
        if not content:
            raise RuntimeError("llm returned empty content")
        return json.loads(extract_json_object(content))


class ProfileBuilder:
    def __init__(
        self,
        schema: dict[str, Any],
        adapter: ServerProbeAdapter | None,
        preset: WorkloadPreset,
        final_query_cap: int | None = None,
    ):
        self.schema = schema
        self.adapter = adapter
        self.preset = preset
        self.final_query_cap = final_query_cap
        self.tables = load_schema_catalog(schema)
        self.aliases = build_alias_map(self.tables)
        self.table_counts = self._load_table_counts()
        self.column_stats = self._load_column_stats()

    def build(self, use_llm: bool = False, llm_client: OpenAICompatibleClient | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
        # Heuristic pipeline:
        # 1. propose single-table predicates from schema/pg_stats and schema fallback
        # 2. retain all valid unique exhaustive candidates or a compact standard subset
        # 3. derive nested families from the FK graph using those retained predicates
        # 4. apply the final emitted-query cap only after semantic candidate generation
        single_candidates = self._build_single_table_candidates()
        single_nonzero = self._retained_candidates(single_candidates)
        if self.preset.name == "exhaustive":
            single_selected = self._select_single_candidates_exhaustive(single_candidates)
        else:
            single_selected = self._select_single_candidates(single_candidates)
        nested_candidates = self._build_nested_candidates(single_selected)
        nested_nonzero = self._retained_candidates(nested_candidates)
        if self.preset.name == "exhaustive":
            nested_selected = self._select_nested_candidates_exhaustive(nested_candidates)
            single_selected, nested_selected = self._apply_final_query_cap(single_selected, nested_selected)
        else:
            nested_selected = self._select_nested_candidates(nested_candidates)

        llm_io: dict[str, Any] = {}
        if use_llm and llm_client is not None:
            llm_input = self._build_llm_payload(single_selected, nested_selected)
            llm_output = llm_client.chat_json(llm_system_prompt(), llm_input)
            single_selected, nested_selected = self._apply_llm_selection(single_selected, nested_selected, llm_output)
            llm_io = {"input": llm_input, "output": llm_output}

        profile = assemble_profile(single_selected, nested_selected, self.aliases, self.preset.name)
        profile.update(
            {
                "final_query_cap": self.final_query_cap,
                "selected_single_case_count": len(single_selected),
                "selected_nested_semantic_case_count": len(nested_selected),
                "emitted_query_count": len(single_selected) + len(nested_selected) * len(NESTED_SHAPES),
            }
        )
        candidates = {
            "table_counts": self.table_counts,
            "summary": self._candidate_summary(single_candidates, nested_candidates, single_nonzero, nested_nonzero),
            "single_table": [candidate.summary() for candidate in single_candidates],
            "nested": [candidate.summary() for candidate in nested_candidates],
            "llm": llm_io,
        }
        return profile, candidates

    def _load_table_counts(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table_name in sorted(self.tables):
            if self.adapter is None:
                counts[table_name] = 0
                continue
            counts[table_name] = int(self.adapter.query_scalar(f"SELECT COUNT(*) AS row_count FROM {table_name};"))
        return counts

    def _load_column_stats(self) -> dict[str, dict[str, dict[str, Any]]]:
        if self.adapter is None:
            return {}

        rows = self.adapter.query_rows(
            """
            SELECT
              tablename,
              attname,
              null_frac,
              n_distinct,
              correlation,
              CASE WHEN most_common_vals IS NULL THEN NULL ELSE array_to_json(most_common_vals)::text END AS most_common_vals_json,
              CASE WHEN most_common_freqs IS NULL THEN NULL ELSE array_to_json(most_common_freqs)::text END AS most_common_freqs_json,
              CASE WHEN histogram_bounds IS NULL THEN NULL ELSE array_to_json(histogram_bounds)::text END AS histogram_bounds_json
            FROM pg_stats
            WHERE schemaname = 'public'
            ORDER BY tablename, attname;
            """,
            fetch_result_limit=10000,
        )
        stats: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for row in rows:
            table_name, attname, null_frac, n_distinct, correlation, mcv_json, freq_json, hist_json = row
            stats[str(table_name)][str(attname)] = {
                "null_frac": None if null_frac is None else float(null_frac),
                "n_distinct": None if n_distinct is None else float(n_distinct),
                "correlation": None if correlation is None else float(correlation),
                "most_common_vals": json.loads(mcv_json) if mcv_json else [],
                "most_common_freqs": json.loads(freq_json) if freq_json else [],
                "histogram_bounds": json.loads(hist_json) if hist_json else [],
            }
        return stats

    def _build_single_table_candidates(self) -> list[ProfileCandidate]:
        candidates: list[ProfileCandidate] = []
        for table_name in sorted(self.tables, key=lambda name: (-self.table_counts.get(name, 0), name)):
            table = self.tables[table_name]
            row_count = self.table_counts.get(table_name, 0)
            if self.adapter and row_count == 0:
                continue
            table_candidates: list[ProfileCandidate] = []
            for column in table.columns.values():
                stats = self.column_stats.get(table_name, {}).get(column.name, {})
                # Exhaustive generation broadens the grounded pool instead of relying only
                # on columns with rich pg_stats support. If a column yields no stats-based
                # candidates, fall back to schema-shaped probes for that column as well.
                column_candidates: list[ProfileCandidate] = []
                column_candidates.extend(self._null_candidates(table, column, stats, row_count))
                column_candidates.extend(self._mcv_candidates(table, column, stats, row_count))
                column_candidates.extend(self._range_candidates(table, column, stats, row_count))
                if (self._is_exhaustive() and not column_candidates) or (not stats and self.adapter is None):
                    column_candidates.extend(self._schema_fallback_candidates(table, column))
                table_candidates.extend(column_candidates)
            if self._is_exhaustive() and not table_candidates:
                for column in table.columns.values():
                    table_candidates.extend(self._schema_fallback_candidates(table, column))
            candidates.extend(table_candidates)

        return deduplicate_candidates(candidates)

    def _schema_fallback_candidates(self, table: TableSchema, column: ColumnSchema) -> list[ProfileCandidate]:
        candidates: list[ProfileCandidate] = []
        column_name = column.name
        # Exhaustive mode uses schema fallback even with probing enabled so tables with
        # weak pg_stats still contribute grounded candidate probes. The fallback remains
        # deterministic; probing decides which predicates survive.
        if "not null" not in column.type.lower():
            for operator, estimate in [("is_null", 0.10), ("is_not_null", 0.90)]:
                observed_rows, observed_selectivity = self._measure_single(
                    table.name,
                    [{"column": column_name, "operator": operator}],
                    self.table_counts.get(table.name, 0),
                    estimate,
                )
                candidates.append(
                    build_candidate(
                        family="single_table",
                        table=table.name,
                        name=f"{column_name}_{'missing' if operator == 'is_null' else 'present'}",
                        predicates=[{"column": column_name, "operator": operator}],
                        bucket=bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimate),
                        estimated_selectivity=estimate,
                        observed_rows=observed_rows,
                        observed_selectivity=observed_selectivity,
                        source="schema",
                        metadata={"column": column_name, "kind": operator},
                    )
                )
        if is_numeric_type(column.type):
            column_name_lower = column_name.lower()
            if "year" in column_name_lower:
                ranges = [
                    (f"{column_name}_classic", [{"column": column_name, "operator": "<=", "value": 1950}], 0.08),
                    (f"{column_name}_golden", [{"column": column_name, "operator": "between", "value": [1951, 1979]}], 0.12),
                    (f"{column_name}_modern", [{"column": column_name, "operator": "between", "value": [1980, 1999]}], 0.12),
                    (f"{column_name}_recent", [{"column": column_name, "operator": "between", "value": [2000, 2009]}], 0.08),
                    (f"{column_name}_current", [{"column": column_name, "operator": ">=", "value": 2010}], 0.03),
                ]
            elif column_name_lower == "id" or column_name_lower.endswith("_id"):
                ranges = [
                    (f"{column_name}_id_band_1", [{"column": column_name, "operator": "<=", "value": 1000}], 0.05),
                    (f"{column_name}_id_band_2", [{"column": column_name, "operator": "between", "value": [1001, 10000]}], 0.10),
                    (f"{column_name}_id_band_3", [{"column": column_name, "operator": "between", "value": [10001, 100000]}], 0.10),
                    (f"{column_name}_id_band_4", [{"column": column_name, "operator": "between", "value": [100001, 1000000]}], 0.10),
                    (f"{column_name}_id_tail", [{"column": column_name, "operator": ">=", "value": 1000001}], 0.05),
                ]
            else:
                ranges = [
                    (f"{column_name}_low_band", [{"column": column_name, "operator": "<=", "value": 10}], 0.05),
                    (f"{column_name}_band_1", [{"column": column_name, "operator": "between", "value": [11, 100]}], 0.08),
                    (f"{column_name}_band_2", [{"column": column_name, "operator": "between", "value": [101, 1000]}], 0.10),
                    (f"{column_name}_band_3", [{"column": column_name, "operator": "between", "value": [1001, 10000]}], 0.10),
                    (f"{column_name}_tail", [{"column": column_name, "operator": ">=", "value": 10001}], 0.05),
                ]
            for name, predicates, estimate in ranges:
                observed_rows, observed_selectivity = self._measure_single(
                    table.name,
                    predicates,
                    self.table_counts.get(table.name, 0),
                    estimate,
                )
                candidates.append(
                    build_candidate(
                        family="single_table",
                        table=table.name,
                        name=name,
                        predicates=predicates,
                        bucket=bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimate),
                        estimated_selectivity=estimate,
                        observed_rows=observed_rows,
                        observed_selectivity=observed_selectivity,
                        source="schema",
                        metadata={"column": column_name, "kind": "range"},
                    )
                )
        return candidates

    def _null_candidates(self, table: TableSchema, column: ColumnSchema, stats: dict[str, Any], row_count: int) -> list[ProfileCandidate]:
        null_frac = stats.get("null_frac")
        if null_frac is None or null_frac <= 0.0 or null_frac >= 1.0:
            return []
        candidates: list[ProfileCandidate] = []
        # Null predicates are cheap, broadly supported, and often expose strong selectivity
        # differences in IMDB-style schemas with sparse note/text columns.
        for operator, estimate in [("is_null", null_frac), ("is_not_null", 1.0 - null_frac)]:
            predicates = [{"column": column.name, "operator": operator}]
            observed_rows, observed_selectivity = self._measure_single(table.name, predicates, row_count, estimate)
            bucket = bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimate)
            candidates.append(
                build_candidate(
                    family="single_table",
                    table=table.name,
                    name=f"{column.name}_{'missing' if operator == 'is_null' else 'present'}",
                    predicates=predicates,
                    bucket=bucket,
                    estimated_selectivity=estimate,
                    observed_rows=observed_rows,
                    observed_selectivity=observed_selectivity,
                    source="pg_stats" if self.adapter else "schema",
                    metadata={"column": column.name, "kind": operator},
                )
            )
        return candidates

    def _mcv_candidates(self, table: TableSchema, column: ColumnSchema, stats: dict[str, Any], row_count: int) -> list[ProfileCandidate]:
        values = stats.get("most_common_vals") or []
        freqs = stats.get("most_common_freqs") or []
        if not values or not freqs:
            return []
        candidates: list[ProfileCandidate] = []
        # Equality and small IN predicates come directly from pg_stats MCVs. This gives us
        # grounded categorical filters without hardcoding domain values into the profile.
        zipped = list(zip(values, freqs))[: self.preset.mcv_value_limit]
        for value, estimate in zipped:
            predicates = [{"column": column.name, "operator": "=", "value": value}]
            observed_rows, observed_selectivity = self._measure_single(table.name, predicates, row_count, float(estimate))
            bucket = bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else float(estimate))
            candidates.append(
                build_candidate(
                    family="single_table",
                    table=table.name,
                    name=f"{column.name}_{slugify(str(value))[:32] or 'value'}",
                    predicates=predicates,
                    bucket=bucket,
                    estimated_selectivity=float(estimate),
                    observed_rows=observed_rows,
                    observed_selectivity=observed_selectivity,
                    source="pg_stats" if self.adapter else "schema",
                    metadata={"column": column.name, "kind": "equality"},
                )
            )

        for width in self.preset.membership_widths:
            if len(zipped) < width:
                continue
            values_only = [value for value, _ in zipped[:width]]
            estimate = float(sum(freq for _, freq in zipped[:width]))
            predicates = [{"column": column.name, "operator": "in", "value": values_only}]
            observed_rows, observed_selectivity = self._measure_single(table.name, predicates, row_count, estimate)
            bucket = bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimate)
            candidates.append(
                build_candidate(
                    family="single_table",
                    table=table.name,
                    name=f"{column.name}_top_{width}_values",
                    predicates=predicates,
                    bucket=bucket,
                    estimated_selectivity=estimate,
                    observed_rows=observed_rows,
                    observed_selectivity=observed_selectivity,
                    source="pg_stats" if self.adapter else "schema",
                    metadata={"column": column.name, "kind": "membership"},
                )
            )
        return candidates

    def _range_candidates(self, table: TableSchema, column: ColumnSchema, stats: dict[str, Any], row_count: int) -> list[ProfileCandidate]:
        if not is_numeric_type(column.type):
            return []
        histogram = stats.get("histogram_bounds") or []
        if len(histogram) < 4:
            return []
        # Numeric/date-ish range predicates are approximated from histogram cut points.
        # These are not optimizer-aware quantiles; they are simply a compact way to span
        # low/mid/high slices before live counts validate the actual selectivity.
        quantiles = sorted(set(self.preset.range_quantiles))
        cut_points: list[tuple[float, Any]] = []
        for quantile in quantiles:
            index = min(len(histogram) - 2, max(1, int((len(histogram) - 1) * quantile)))
            value = histogram[index]
            if cut_points and cut_points[-1][1] == value:
                continue
            cut_points.append((quantile, value))
        if len(cut_points) < 2:
            return []

        intervals: list[tuple[str, list[dict[str, Any]], float]] = []
        first_quantile, first_value = cut_points[0]
        intervals.append((f"{column.name}_low_band", [{"column": column.name, "operator": "<=", "value": first_value}], first_quantile))
        for band_index, ((left_quantile, left_value), (right_quantile, right_value)) in enumerate(zip(cut_points, cut_points[1:]), start=1):
            if left_value == right_value:
                continue
            intervals.append(
                (
                    f"{column.name}_band_{band_index}",
                    [{"column": column.name, "operator": "between", "value": [left_value, right_value]}],
                    max(0.01, right_quantile - left_quantile),
                )
            )
        last_quantile, last_value = cut_points[-1]
        intervals.append((f"{column.name}_tail", [{"column": column.name, "operator": ">=", "value": last_value}], max(0.01, 1.0 - last_quantile)))

        candidates: list[ProfileCandidate] = []
        for name, predicates, estimate in intervals[: self.preset.max_range_candidates_per_column]:
            observed_rows, observed_selectivity = self._measure_single(table.name, predicates, row_count, estimate)
            bucket = bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimate)
            candidates.append(
                build_candidate(
                    family="single_table",
                    table=table.name,
                    name=name,
                    predicates=predicates,
                    bucket=bucket,
                    estimated_selectivity=estimate,
                    observed_rows=observed_rows,
                    observed_selectivity=observed_selectivity,
                    source="probe" if self.adapter else "schema",
                    metadata={"column": column.name, "kind": "range"},
                )
            )
        return candidates

    def _measure_single(self, table_name: str, predicates: list[dict[str, Any]], row_count: int, estimate: float | None) -> tuple[int | None, float | None]:
        if self.adapter is None:
            return None, estimate
        # Every candidate is normalized to a COUNT(*) probe so later selection compares
        # heterogeneous predicates on one axis: observed selectivity over the base table.
        where_clause = flatten_predicates(render_predicates(predicates, 4))
        filtered = self.adapter.measure_count(f"SELECT COUNT(*) AS row_count FROM {table_name} WHERE {where_clause};")
        observed = None if row_count == 0 else filtered / row_count
        return filtered, observed

    def _is_exhaustive(self) -> bool:
        return self.preset.name == "exhaustive"

    def _max_nested_candidates_per_family(self) -> int:
        return 500 if self._is_exhaustive() else self.preset.nested_cases_per_family

    def _retained_candidates(self, candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
        return [candidate for candidate in candidates if candidate.observed_rows != 0]

    def _candidate_counts(
        self,
        candidates: list[ProfileCandidate],
        *,
        key_name: str,
        key_fn,
    ) -> dict[str, Any]:
        before: dict[str, int] = defaultdict(int)
        after: dict[str, int] = defaultdict(int)
        for candidate in candidates:
            key = str(key_fn(candidate))
            before[key] += 1
            if candidate.observed_rows != 0:
                after[key] += 1
        return {
            f"{key_name}_before_filter": dict(sorted(before.items())),
            f"{key_name}_after_filter": dict(sorted(after.items())),
        }

    def _candidate_summary(
        self,
        single_candidates: list[ProfileCandidate],
        nested_candidates: list[ProfileCandidate],
        single_nonzero: list[ProfileCandidate],
        nested_nonzero: list[ProfileCandidate],
    ) -> dict[str, Any]:
        summary = {
            "single_candidate_count_before_filter": len(single_candidates),
            "single_candidate_count_after_filter": len(single_nonzero),
            "nested_candidate_count_before_filter": len(nested_candidates),
            "nested_candidate_count_after_filter": len(nested_nonzero),
        }
        summary.update(self._candidate_counts(single_candidates, key_name="single_per_table", key_fn=lambda candidate: candidate.table))
        summary.update(
            self._candidate_counts(
                nested_candidates,
                key_name="nested_per_family",
                key_fn=lambda candidate: candidate.metadata.get("family_name", ""),
            )
        )
        return summary

    def _select_single_candidates_exhaustive(self, candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
        selected = [candidate for candidate in candidates if candidate.observed_rows != 0]
        for candidate in selected:
            candidate.score = candidate_score(candidate)
        return sorted(selected, key=lambda item: (-item.score, item.table, item.name, item.id))

    def _select_single_candidates(self, candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
        by_table: dict[str, list[ProfileCandidate]] = defaultdict(list)
        for candidate in candidates:
            candidate.score = candidate_score(candidate)
            if candidate.observed_rows == 0:
                continue
            by_table[candidate.table].append(candidate)

        selected: list[ProfileCandidate] = []
        for table_name in sorted(by_table, key=lambda name: (-self.table_counts.get(name, 0), name)):
            table_candidates = sorted(by_table[table_name], key=lambda item: (-item.score, item.name))
            picked: list[ProfileCandidate] = []
            seen_signatures: set[tuple[Any, ...]] = set()
            bucket_counts: dict[str, int] = defaultdict(int)
            # Selection priority for base-table cases:
            # 1. cover target selectivity buckets in a stable order
            # 2. avoid duplicate predicate signatures
            # 3. cap per-bucket repetition so one easy bucket does not dominate a table
            for bucket in BUCKET_ORDER:
                for candidate in table_candidates:
                    signature = predicate_signature(candidate.predicates)
                    if candidate.bucket != bucket or signature in seen_signatures:
                        continue
                    picked.append(candidate)
                    seen_signatures.add(signature)
                    bucket_counts[bucket] += 1
                    if len(picked) >= self.preset.single_cases_per_table:
                        break
                    if bucket_counts[bucket] >= 2:
                        break
                if len(picked) >= self.preset.single_cases_per_table:
                    break
            if len(picked) < self.preset.single_cases_per_table:
                for candidate in table_candidates:
                    signature = predicate_signature(candidate.predicates)
                    if signature in seen_signatures:
                        continue
                    picked.append(candidate)
                    seen_signatures.add(signature)
                    if len(picked) >= self.preset.single_cases_per_table:
                        break
            # If the table still has budget left, add a few two-predicate conjunctions.
            # This is the only place where multi-filter single-table cases are synthesized.
            selected.extend(self._build_conjunctions(table_name, picked, table_candidates, seen_signatures))
            selected.extend(picked)
        return sorted(selected, key=lambda item: (item.table, item.name, item.id))

    def _build_conjunctions(
        self,
        table_name: str,
        picked: list[ProfileCandidate],
        table_candidates: list[ProfileCandidate],
        seen_signatures: set[tuple[Any, ...]],
    ) -> list[ProfileCandidate]:
        if self.preset.conjunctions_fill_only and len(picked) >= self.preset.single_cases_per_table:
            return []
        row_count = self.table_counts.get(table_name, 0)
        conjunctions: list[ProfileCandidate] = []
        # Conjunctions are built only from the strongest single-predicate cases and only
        # across different columns. This keeps the search space small and avoids redundant
        # variants like two predicates on the same attribute family.
        base_candidates = [candidate for candidate in table_candidates if len(candidate.predicates) == 1][: self.preset.conjunction_pool_size]
        for left_index, left in enumerate(base_candidates):
            left_column = left.predicates[0]["column"]
            for right in base_candidates[left_index + 1:]:
                right_column = right.predicates[0]["column"]
                if left_column == right_column:
                    continue
                predicates = left.predicates + right.predicates
                signature = predicate_signature(predicates)
                if signature in seen_signatures:
                    continue
                estimated = None
                if left.observed_selectivity is not None and right.observed_selectivity is not None:
                    estimated = left.observed_selectivity * right.observed_selectivity
                elif left.estimated_selectivity is not None and right.estimated_selectivity is not None:
                    estimated = left.estimated_selectivity * right.estimated_selectivity
                observed_rows, observed_selectivity = self._measure_single(table_name, predicates, row_count, estimated)
                if observed_rows == 0:
                    continue
                candidate = build_candidate(
                    family="single_table",
                    table=table_name,
                    name=f"{left.name}_{right.name}",
                    predicates=predicates,
                    bucket=bucket_for_selectivity(observed_selectivity if observed_selectivity is not None else estimated),
                    estimated_selectivity=estimated,
                    observed_rows=observed_rows,
                    observed_selectivity=observed_selectivity,
                    source="probe" if self.adapter else "schema",
                    metadata={"kind": "conjunction"},
                )
                candidate.score = candidate_score(candidate) + 0.05
                conjunctions.append(candidate)
                seen_signatures.add(signature)
                if self.preset.conjunctions_fill_only:
                    if len(conjunctions) + len(picked) >= self.preset.single_cases_per_table:
                        return conjunctions
                elif len(conjunctions) >= self.preset.max_conjunctions_per_table:
                    return conjunctions
        return conjunctions

    def _qualified_outer_shortlist(self, outer_cases: list[ProfileCandidate], outer_alias: str) -> list[list[dict[str, Any]]]:
        shortlist = []
        for outer_case in sorted(outer_cases, key=lambda item: (-item.score, item.name)):
            if len(outer_case.predicates) != 1:
                continue
            shortlist.append(qualify_predicates(outer_case.predicates, outer_alias))
            if len(shortlist) >= self.preset.outer_predicate_limit:
                break
        return shortlist

    def _build_nested_candidates(self, single_selected: list[ProfileCandidate]) -> list[ProfileCandidate]:
        by_table: dict[str, list[ProfileCandidate]] = defaultdict(list)
        for candidate in single_selected:
            by_table[candidate.table].append(candidate)

        nested: list[ProfileCandidate] = []
        # Nested generation reuses already-selected base-table predicates instead of
        # starting from scratch. That keeps nested families grounded in filters that
        # already showed useful selectivity behavior on their own.
        for child_table in sorted(self.tables.values(), key=lambda item: item.name):
            if self.adapter and self.table_counts.get(child_table.name, 0) == 0:
                continue
            for foreign_key in child_table.foreign_keys:
                if foreign_key.foreign_table not in self.tables:
                    continue
                outer_table = self.tables[foreign_key.foreign_table]
                if not outer_table.primary_key:
                    continue
                child_alias = self.aliases[child_table.name]
                outer_alias = self.aliases[outer_table.name]
                family_name = f"{outer_table.name}_has_{child_table.name}"
                inner_from = f"{child_table.name} AS {child_alias}"
                inner_key = f"{child_alias}.{foreign_key.column}"
                direct_cases = self._nested_cases_from_child(
                    family_name,
                    outer_table,
                    outer_alias,
                    inner_from,
                    inner_key,
                    by_table.get(child_table.name, []),
                    by_table.get(outer_table.name, []),
                    child_alias,
                )
                nested.extend(direct_cases)

                for child_dim_fk in child_table.foreign_keys:
                    if child_dim_fk.foreign_table == outer_table.name:
                        continue
                    if child_dim_fk.foreign_table not in self.tables:
                        continue
                    dimension_table = self.tables[child_dim_fk.foreign_table]
                    if self.adapter and self.table_counts.get(dimension_table.name, 0) > self.preset.dimension_table_max_rows:
                        continue
                    # Dimension-join families are restricted to smaller lookup-style tables
                    # so nested generation stays readable and does not explode on large facts.
                    dimension_cases = [candidate for candidate in by_table.get(dimension_table.name, []) if len(candidate.predicates) == 1]
                    if not dimension_cases:
                        continue
                    if not dimension_table.primary_key:
                        continue
                    dim_alias = self.aliases[dimension_table.name]
                    family_name = f"{outer_table.name}_has_{dimension_table.name}_via_{child_table.name}"
                    inner_from = (
                        f"{child_table.name} AS {child_alias} "
                        f"JOIN {dimension_table.name} AS {dim_alias} "
                        f"ON {child_alias}.{child_dim_fk.column} = {dim_alias}.{dimension_table.primary_key}"
                    )
                    nested.extend(
                        self._nested_cases_from_dimension(
                            family_name,
                            outer_table,
                            outer_alias,
                            inner_from,
                            inner_key,
                            dimension_cases,
                            by_table.get(child_table.name, []),
                            by_table.get(outer_table.name, []),
                            dim_alias,
                            {child_table.name, dimension_table.name},
                        )
                    )
        return deduplicate_candidates(nested)

    def _nested_cases_from_child(
        self,
        family_name: str,
        outer_table: TableSchema,
        outer_alias: str,
        inner_from: str,
        inner_key: str,
        child_cases: list[ProfileCandidate],
        outer_cases: list[ProfileCandidate],
        child_alias: str,
    ) -> list[ProfileCandidate]:
        nested: list[ProfileCandidate] = []
        # Exhaustive mode keeps direct FK families generation-heavy: use all child cases,
        # alternate plain and outer-filtered forms, and cap only with a broad family-wide
        # safety limit before the final emitted-query cap applies.
        outer_shortlist = self._qualified_outer_shortlist(outer_cases, outer_alias)
        sorted_child_cases = sorted(child_cases, key=lambda item: (-item.score, item.name))
        if not self._is_exhaustive():
            sorted_child_cases = sorted_child_cases[: self.preset.nested_cases_per_family]
        family_limit = self._max_nested_candidates_per_family()
        for index, child_case in enumerate(sorted_child_cases):
            outer_predicates = outer_shortlist[index % len(outer_shortlist)] if outer_shortlist and index % 2 == 1 else []
            inner_predicates = qualify_predicates(child_case.predicates, child_alias)
            nested.append(
                self._build_nested_candidate(
                    family_name,
                    outer_table,
                    outer_alias,
                    inner_from,
                    inner_key,
                    inner_predicates,
                    outer_predicates,
                    {outer_table.name, family_name.split("_has_")[-1]},
                    child_case.name,
                )
            )
            if len(nested) >= family_limit:
                break
        return nested

    def _nested_cases_from_dimension(
        self,
        family_name: str,
        outer_table: TableSchema,
        outer_alias: str,
        inner_from: str,
        inner_key: str,
        dimension_cases: list[ProfileCandidate],
        child_cases: list[ProfileCandidate],
        outer_cases: list[ProfileCandidate],
        dim_alias: str,
        related_tables: set[str],
    ) -> list[ProfileCandidate]:
        nested: list[ProfileCandidate] = []
        # Exhaustive dimension families use all one-predicate dimension seeds and then add
        # broader child+dimension hybrids, bounded only by a broad per-family safety cap.
        outer_shortlist = self._qualified_outer_shortlist(outer_cases, outer_alias)
        top_dimension_cases = sorted(dimension_cases, key=lambda item: (-item.score, item.name))
        if not self._is_exhaustive():
            top_dimension_cases = top_dimension_cases[: self.preset.nested_cases_per_family]
        family_limit = self._max_nested_candidates_per_family()
        for index, dimension_case in enumerate(top_dimension_cases):
            outer_predicates = []
            if outer_shortlist and index % 2 == 1:
                outer_predicates = outer_shortlist[index % len(outer_shortlist)]
            inner_predicates = qualify_predicates(dimension_case.predicates, dim_alias)
            nested.append(
                self._build_nested_candidate(
                    family_name,
                    outer_table,
                    outer_alias,
                    inner_from,
                    inner_key,
                    inner_predicates,
                    outer_predicates,
                    related_tables | {outer_table.name},
                    dimension_case.name,
                )
            )
            if len(nested) >= family_limit:
                return nested
        if self.preset.allow_hybrid_dimension_cases:
            top_child_cases = [candidate for candidate in sorted(child_cases, key=lambda item: (-item.score, item.name)) if len(candidate.predicates) == 1]
            hybrid_added = 0
            child_alias = inner_key.split(".")[0]
            for dimension_index, dimension_case in enumerate(top_dimension_cases):
                for child_index, child_case in enumerate(top_child_cases):
                    if hybrid_added >= self.preset.hybrid_cases_per_family or len(nested) >= family_limit:
                        return nested
                    if child_case.predicates[0]["column"] == dimension_case.predicates[0]["column"]:
                        continue
                    outer_predicates = []
                    if outer_shortlist and (dimension_index + child_index) % 2 == 0:
                        outer_predicates = outer_shortlist[(dimension_index + child_index) % len(outer_shortlist)]
                    hybrid_predicates = qualify_predicates(dimension_case.predicates, dim_alias) + qualify_predicates(child_case.predicates, child_alias)
                    nested.append(
                        self._build_nested_candidate(
                            family_name,
                            outer_table,
                            outer_alias,
                            inner_from,
                            inner_key,
                            hybrid_predicates,
                            outer_predicates,
                            related_tables | {outer_table.name},
                            f"{dimension_case.name}_{child_case.name}",
                        )
                    )
                    hybrid_added += 1
        return nested

    def _build_nested_candidate(
        self,
        family_name: str,
        outer_table: TableSchema,
        outer_alias: str,
        inner_from: str,
        inner_key: str,
        inner_predicates: list[dict[str, Any]],
        outer_predicates: list[dict[str, Any]],
        related_tables: set[str],
        case_name: str,
    ) -> ProfileCandidate:
        predicate_selectivity = None
        observed_rows = None
        observed_selectivity = None
        if self.adapter is not None:
            # Probe every logical nested candidate through one canonical EXISTS form, even
            # though the renderer later emits multiple rewrite-equivalent shapes. That gives
            # one measured outer cardinality per semantic case.
            from_clause, where_clause = nested_probe_query(
                {
                    "outer_table": outer_table.name,
                    "outer_alias": outer_alias,
                    "outer_key": f"{outer_alias}.{outer_table.primary_key}",
                    "inner_from": inner_from,
                    "inner_key": inner_key,
                },
                {
                    "outer_predicates": outer_predicates,
                    "inner_predicates": inner_predicates,
                },
            )
            observed_rows = self.adapter.measure_count(f"SELECT COUNT(*) AS row_count FROM {from_clause} WHERE {where_clause};")
            total = self.table_counts.get(outer_table.name, 0)
            observed_selectivity = None if total == 0 else observed_rows / total
            predicate_selectivity = observed_selectivity
        bucket = bucket_for_selectivity(predicate_selectivity or 0.1)
        candidate = build_candidate(
            family="nested",
            table=outer_table.name,
            name=case_name,
            predicates=inner_predicates,
            bucket=bucket,
            estimated_selectivity=predicate_selectivity,
            observed_rows=observed_rows,
            observed_selectivity=observed_selectivity,
            source="probe" if self.adapter else "schema",
            metadata={
                "family_name": family_name,
                "outer_table": outer_table.name,
                "outer_alias": outer_alias,
                "outer_key": f"{outer_alias}.{outer_table.primary_key}",
                "inner_from": inner_from,
                "inner_key": inner_key,
                "inner_predicates": inner_predicates,
                "outer_predicates": outer_predicates,
                "related_tables": sorted(related_tables),
            },
        )
        candidate.score = candidate_score(candidate) + (0.1 if len(related_tables) > 2 else 0.0)
        return candidate

    def _select_nested_candidates(self, candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
        by_family: dict[str, list[ProfileCandidate]] = defaultdict(list)
        for candidate in candidates:
            if candidate.observed_rows == 0:
                continue
            family_name = str(candidate.metadata["family_name"])
            by_family[family_name].append(candidate)

        family_order = sorted(
            by_family,
            key=lambda family_name: (
                -max(candidate.score for candidate in by_family[family_name]),
                family_name,
            ),
        )[: self.preset.max_nested_families]

        selected: list[ProfileCandidate] = []
        for family_name in family_order:
            family_candidates = sorted(by_family[family_name], key=lambda item: (-item.score, item.name))
            picked: list[ProfileCandidate] = []
            seen_names: set[str] = set()
            # Nested selection prefers family breadth first, then bucket spread within a
            # family. The later SQL renderer expands each selected semantic case into the
            # rewrite zoo (IN / EXISTS / scalar / derived / CTE).
            for bucket in BUCKET_ORDER:
                for candidate in family_candidates:
                    if candidate.bucket != bucket or candidate.name in seen_names:
                        continue
                    picked.append(candidate)
                    seen_names.add(candidate.name)
                    break
                if len(picked) >= self.preset.nested_cases_per_family:
                    break
            if len(picked) < self.preset.nested_cases_per_family:
                for candidate in family_candidates:
                    if candidate.name in seen_names:
                        continue
                    picked.append(candidate)
                    seen_names.add(candidate.name)
                    if len(picked) >= self.preset.nested_cases_per_family:
                        break
            selected.extend(picked)
        return sorted(selected, key=lambda item: (str(item.metadata["family_name"]), item.name, item.id))

    def _select_nested_candidates_exhaustive(self, candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
        selected = [candidate for candidate in candidates if candidate.observed_rows != 0]
        for candidate in selected:
            candidate.score = candidate_score(candidate) + (0.1 if len(candidate.metadata.get("related_tables", [])) > 2 else 0.0)
        return sorted(
            selected,
            key=lambda item: (
                -item.score,
                str(item.metadata.get("family_name", "")),
                item.name,
                item.id,
            ),
        )

    def _apply_final_query_cap(
        self,
        single_candidates: list[ProfileCandidate],
        nested_candidates: list[ProfileCandidate],
    ) -> tuple[list[ProfileCandidate], list[ProfileCandidate]]:
        if self.final_query_cap is None:
            return single_candidates, nested_candidates

        nested_cost = len(NESTED_SHAPES)
        selected_single = single_candidates[: self.final_query_cap]
        remaining = self.final_query_cap - len(selected_single)
        if remaining < nested_cost:
            return selected_single, []

        nested_budget = remaining // nested_cost
        return selected_single, nested_candidates[:nested_budget]

    def _build_llm_payload(self, single_candidates: list[ProfileCandidate], nested_candidates: list[ProfileCandidate]) -> dict[str, Any]:
        return {
            "task": "Select a diverse, insightful workload profile from grounded SQL candidates. Keep coverage across tables, predicate kinds, selectivity buckets, and nested rewrite families. Use only provided ids.",
                "budgets": {
                "max_single_cases_per_table": self.preset.single_cases_per_table,
                "max_nested_cases_per_family": self.preset.nested_cases_per_family,
                "max_nested_families": self.preset.max_nested_families,
            },
            "single_candidates": [candidate.summary() for candidate in single_candidates],
            "nested_candidates": [candidate.summary() for candidate in nested_candidates],
            "response_schema": {
                "single_case_ids": ["candidate ids"],
                "nested_case_ids": ["candidate ids"],
                "case_name_overrides": {"candidate_id": "new_case_name"},
                "family_name_overrides": {"existing_family_name": "new_family_name"},
            },
        }

    def _apply_llm_selection(
        self,
        single_candidates: list[ProfileCandidate],
        nested_candidates: list[ProfileCandidate],
        llm_output: dict[str, Any],
    ) -> tuple[list[ProfileCandidate], list[ProfileCandidate]]:
        single_index = {candidate.id: candidate for candidate in single_candidates}
        nested_index = {candidate.id: candidate for candidate in nested_candidates}
        picked_single_ids = [candidate_id for candidate_id in llm_output.get("single_case_ids", []) if candidate_id in single_index]
        picked_nested_ids = [candidate_id for candidate_id in llm_output.get("nested_case_ids", []) if candidate_id in nested_index]

        picked_single = [single_index[candidate_id] for candidate_id in picked_single_ids] or single_candidates
        picked_nested = [nested_index[candidate_id] for candidate_id in picked_nested_ids] or nested_candidates

        case_name_overrides = llm_output.get("case_name_overrides", {}) or {}
        family_name_overrides = llm_output.get("family_name_overrides", {}) or {}
        for candidate in picked_single + picked_nested:
            if candidate.id in case_name_overrides:
                candidate.name = sanitize_case_name(str(case_name_overrides[candidate.id]))
            family_name = str(candidate.metadata.get("family_name", ""))
            if family_name in family_name_overrides:
                candidate.metadata["family_name"] = sanitize_case_name(str(family_name_overrides[family_name]))
        return picked_single, picked_nested


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def default_output_dir() -> Path:
    return Path(__file__).resolve().with_name("queries")


def default_profile_path() -> Path:
    return Path(__file__).resolve().with_name("job.profile.json")


def default_schema_path() -> Path:
    return repo_root() / "benchmarks" / "job" / "job.dbschema.json"


def default_candidates_path(profile_path: Path) -> Path:
    return profile_path.with_name(profile_path.stem + ".candidates.json")


def default_llm_input_path(profile_path: Path) -> Path:
    return profile_path.with_name(profile_path.stem + ".llm_input.json")


def default_llm_output_path(profile_path: Path) -> Path:
    return profile_path.with_name(profile_path.stem + ".llm_output.json")


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text())


def dump_json(path: Path, payload: dict[str, Any]):
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def load_schema_catalog(schema: dict[str, Any]) -> dict[str, TableSchema]:
    catalog: dict[str, TableSchema] = {}
    for table in schema["tables"]:
        columns = {column["name"]: ColumnSchema(column["name"], column["type"]) for column in table["columns"]}
        foreign_keys = [
            ForeignKey(foreign_key["column"], foreign_key["foreign table"], foreign_key["foreign column"])
            for foreign_key in table.get("foreign keys", [])
        ]
        catalog[table["name"]] = TableSchema(
            name=table["name"],
            columns=columns,
            primary_key=table.get("primary key", {}).get("column"),
            foreign_keys=foreign_keys,
        )
    return catalog


def build_alias_map(tables: dict[str, TableSchema]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    used: set[str] = set()
    for table_name in sorted(tables):
        base = "".join(part[0] for part in table_name.split("_")) or table_name[0]
        alias = base
        suffix = 2
        while alias in used:
            alias = f"{base}{suffix}"
            suffix += 1
        aliases[table_name] = alias
        used.add(alias)
    return aliases


def is_numeric_type(column_type: str) -> bool:
    column_type = column_type.lower()
    return any(token in column_type for token in ["int", "numeric", "decimal", "real", "double"])


def sql_literal(value: Any) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    return str(value)


def render_predicate(predicate: dict[str, Any]) -> str:
    column = predicate["column"]
    operator = predicate["operator"]
    value = predicate.get("value")

    if operator == "is_null":
        return f"{column} IS NULL"
    if operator == "is_not_null":
        return f"{column} IS NOT NULL"
    if operator == "between":
        lower, upper = value
        return f"{column} BETWEEN {sql_literal(lower)} AND {sql_literal(upper)}"
    if operator == "in":
        values = ", ".join(sql_literal(item) for item in value)
        return f"{column} IN ({values})"
    if operator == "like":
        return f"{column} LIKE {sql_literal(value)}"
    if operator in {"=", "!=", "<", "<=", ">", ">="}:
        return f"{column} {operator} {sql_literal(value)}"
    raise ValueError(f"Unsupported operator: {operator}")


def render_predicates(predicates: list[dict[str, Any]], indent: int = 2) -> str:
    rendered = [render_predicate(predicate) for predicate in predicates]
    if not rendered:
        return ""
    spacer = " " * indent
    return rendered[0] + "".join(f"\n{spacer}AND {predicate}" for predicate in rendered[1:])


def indent_block(text: str, spaces: int) -> str:
    prefix = " " * spaces
    return "\n".join(prefix + line if line else line for line in text.splitlines())


def format_from_clause(clause: str) -> str:
    parts = clause.split(" JOIN ")
    lines = [parts[0]]
    for part in parts[1:]:
        if " ON " in part:
            table_part, on_part = part.split(" ON ", 1)
            lines.append(f"JOIN {table_part}")
            lines.append(f"  ON {on_part}")
        else:
            lines.append(f"JOIN {part}")
    return "\n".join(lines)


def render_select_match(inner_key: str, inner_from: str, inner_filter: str, distinct: bool = False) -> str:
    select_target = f"DISTINCT {inner_key} AS match_key" if distinct else inner_key
    return "\n".join(
        [
            f"SELECT {select_target}",
            "FROM " + format_from_clause(inner_from).replace("\n", "\n  "),
            "WHERE " + inner_filter.replace("\n", "\n  "),
        ]
    )


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def sanitize_case_name(value: str) -> str:
    return slugify(value) or "case"


def flatten_predicates(text: str) -> str:
    return text.replace("\n", " ")


def qualify_predicates(predicates: list[dict[str, Any]], alias: str) -> list[dict[str, Any]]:
    qualified = []
    for predicate in predicates:
        column = predicate["column"]
        if "." not in column:
            column = f"{alias}.{column}"
        qualified.append({**predicate, "column": column})
    return qualified


def bucket_for_selectivity(selectivity: float | None) -> str:
    if selectivity is None:
        return "medium"
    return min(BUCKET_TARGETS, key=lambda bucket: abs(BUCKET_TARGETS[bucket] - selectivity))


def predicate_signature(predicates: list[dict[str, Any]]) -> tuple[Any, ...]:
    signature = []
    for predicate in predicates:
        value = predicate.get("value")
        if isinstance(value, list):
            value = tuple(value)
        signature.append((predicate["column"], predicate["operator"], value))
    return tuple(signature)


def candidate_score(candidate: ProfileCandidate) -> float:
    selectivity = candidate.observed_selectivity if candidate.observed_selectivity is not None else candidate.estimated_selectivity
    if selectivity is None:
        return 0.0
    # Score means "how representative is this candidate for its assigned bucket", not
    # "how hard is this query". Richer predicates get only a small tie-break bonus.
    target = BUCKET_TARGETS[candidate.bucket]
    closeness = max(0.0, 1.0 - abs(selectivity - target))
    richness = 0.05 * max(0, len(candidate.predicates) - 1)
    return closeness + richness


def build_candidate(
    family: str,
    table: str,
    name: str,
    predicates: list[dict[str, Any]],
    bucket: str,
    estimated_selectivity: float | None,
    observed_rows: int | None,
    observed_selectivity: float | None,
    source: str,
    metadata: dict[str, Any],
) -> ProfileCandidate:
    signature_text = json.dumps(predicate_signature(predicates), sort_keys=True)
    signature_hash = hashlib.md5(signature_text.encode("utf-8")).hexdigest()[:8]
    candidate_id = f"{family}::{table}::{sanitize_case_name(name)}::{signature_hash}"
    return ProfileCandidate(
        id=candidate_id,
        family=family,
        table=table,
        name=sanitize_case_name(name),
        bucket=bucket,
        predicates=predicates,
        estimated_selectivity=estimated_selectivity,
        observed_rows=observed_rows,
        observed_selectivity=observed_selectivity,
        source=source,
        metadata=metadata,
    )


def deduplicate_candidates(candidates: list[ProfileCandidate]) -> list[ProfileCandidate]:
    deduped: dict[tuple[Any, ...], ProfileCandidate] = {}
    for candidate in candidates:
        # Nested candidates must retain outer predicate structure and family identity in the
        # dedupe key; otherwise exhaustive generation collapses distinct semantic cases that
        # share the same inner predicate.
        if candidate.family == "nested":
            key = (
                candidate.family,
                candidate.table,
                str(candidate.metadata.get("family_name", "")),
                str(candidate.metadata.get("inner_from", "")),
                predicate_signature(candidate.predicates),
                predicate_signature(candidate.metadata.get("outer_predicates", [])),
            )
        else:
            key = (candidate.family, candidate.table, predicate_signature(candidate.predicates))
        current = deduped.get(key)
        if current is None or candidate_score(candidate) > candidate_score(current):
            deduped[key] = candidate
    return list(deduped.values())


def render_outer_filters(case: dict[str, Any]) -> str:
    predicates = case.get("outer_predicates", [])
    return render_predicates(predicates) if predicates else ""


def render_inner_filters(case: dict[str, Any]) -> str:
    return render_predicates(case["inner_predicates"])


def nested_sql(family: dict[str, Any], case: dict[str, Any], shape: str) -> str:
    outer_table = family["outer_table"]
    outer_alias = family["outer_alias"]
    outer_key = family["outer_key"]
    inner_from = family["inner_from"]
    inner_key = family["inner_key"]
    outer_filter = render_outer_filters(case)
    inner_filter = render_inner_filters(case)
    match_select = render_select_match(inner_key, inner_from, inner_filter, distinct=True)
    membership_select = render_select_match(inner_key, inner_from, inner_filter)

    where_parts: list[str] = []
    if outer_filter:
        where_parts.append(outer_filter)

    if shape == "in":
        where_parts.append("\n".join([f"{outer_key} IN (", indent_block(membership_select, 2), ")"]))
        return render_outer_query(outer_table, outer_alias, where_parts)

    if shape == "exists":
        where_parts.append(
            "\n".join(
                [
                    "EXISTS (",
                    indent_block(render_select_match("1", inner_from, f"{inner_filter}\nAND {inner_key} = {outer_key}"), 2),
                    ")",
                ]
            )
        )
        return render_outer_query(outer_table, outer_alias, where_parts)

    if shape == "scalar_count":
        where_parts.append(
            "\n".join(
                [
                    "(",
                    indent_block(render_select_match("COUNT(*)", inner_from, f"{inner_filter}\nAND {inner_key} = {outer_key}"), 2),
                    ") > 0",
                ]
            )
        )
        return render_outer_query(outer_table, outer_alias, where_parts)

    if shape == "derived_join":
        query_lines = [
            "SELECT COUNT(*) AS row_count",
            f"FROM {outer_table} AS {outer_alias}",
            "JOIN (",
            indent_block(match_select, 2),
            f") AS matched ON matched.match_key = {outer_key}",
        ]
        if outer_filter:
            query_lines.append("WHERE " + outer_filter.replace("\n", "\n  "))
        query_lines.append(";")
        return "\n".join(query_lines) + "\n"

    if shape == "cte_in":
        cte = ["WITH matched AS (", indent_block(match_select, 2), ")"]
        where_parts.append("{0} IN (SELECT match_key FROM matched)".format(outer_key))
        return render_cte_query(cte, outer_table, outer_alias, where_parts)

    if shape == "cte_exists":
        cte = ["WITH matched AS (", indent_block(match_select, 2), ")"]
        where_parts.append(f"EXISTS (SELECT 1 FROM matched WHERE matched.match_key = {outer_key})")
        return render_cte_query(cte, outer_table, outer_alias, where_parts)

    raise ValueError(f"Unsupported nested shape: {shape}")


def render_outer_query(outer_table: str, outer_alias: str, where_parts: list[str]) -> str:
    query_lines = ["SELECT COUNT(*) AS row_count", f"FROM {outer_table} AS {outer_alias}"]
    where_clause = where_parts[0]
    for part in where_parts[1:]:
        where_clause += "\n  AND " + part.replace("\n", "\n  ")
    query_lines.append("WHERE " + where_clause.replace("\n", "\n  "))
    query_lines.append(";")
    return "\n".join(query_lines) + "\n"


def render_cte_query(cte: list[str], outer_table: str, outer_alias: str, where_parts: list[str]) -> str:
    query_lines = cte + ["SELECT COUNT(*) AS row_count", f"FROM {outer_table} AS {outer_alias}"]
    where_clause = where_parts[0]
    for part in where_parts[1:]:
        where_clause += "\n  AND " + part.replace("\n", "\n  ")
    query_lines.append("WHERE " + where_clause.replace("\n", "\n  "))
    query_lines.append(";")
    return "\n".join(query_lines) + "\n"


def nested_probe_query(family: dict[str, Any], case: dict[str, Any]) -> tuple[str, str]:
    outer_table = family["outer_table"]
    outer_alias = family["outer_alias"]
    inner_from = family["inner_from"]
    outer_key = family["outer_key"]
    inner_key = family["inner_key"]
    outer_filter = render_outer_filters(case)
    inner_filter = render_inner_filters(case)

    where_parts: list[str] = []
    if outer_filter:
        where_parts.append(flatten_predicates(outer_filter))
    # Canonical probe shape for nested semantics. The emitted workload may later use IN,
    # EXISTS, scalar subqueries, derived joins, or CTEs, but selectivity is measured once
    # through this stable EXISTS-based form.
    where_parts.append(f"EXISTS (SELECT 1 FROM {inner_from} WHERE {flatten_predicates(inner_filter)} AND {inner_key} = {outer_key})")
    return f"{outer_table} AS {outer_alias}", " AND ".join(where_parts)


def single_case_payload(candidate: ProfileCandidate) -> dict[str, Any]:
    metadata = candidate.metadata.copy()
    metadata["candidate_id"] = candidate.id
    metadata["source"] = candidate.source
    metadata["estimated_selectivity"] = candidate.estimated_selectivity
    metadata["observed_rows"] = candidate.observed_rows
    metadata["observed_selectivity"] = candidate.observed_selectivity
    return {
        "name": candidate.name,
        "bucket": candidate.bucket,
        "predicates": candidate.predicates,
        **metadata,
    }


def nested_case_payload(candidate: ProfileCandidate) -> dict[str, Any]:
    metadata = candidate.metadata.copy()
    metadata["candidate_id"] = candidate.id
    metadata["source"] = candidate.source
    metadata["estimated_selectivity"] = candidate.estimated_selectivity
    metadata["observed_rows"] = candidate.observed_rows
    metadata["observed_selectivity"] = candidate.observed_selectivity
    return {
        "name": candidate.name,
        "bucket": candidate.bucket,
        "inner_predicates": metadata.pop("inner_predicates"),
        "outer_predicates": metadata.pop("outer_predicates"),
        **metadata,
    }


def assemble_profile(
    single_candidates: list[ProfileCandidate],
    nested_candidates: list[ProfileCandidate],
    aliases: dict[str, str],
    preset_name: str,
) -> dict[str, Any]:
    # The persisted profile stores selected semantic cases, not the expanded SQL workload.
    # Nested cases remain one logical unit here and are expanded into multiple rewrite
    # shapes only when `build_*_queries` renders the final .sql files.
    single_table: list[dict[str, Any]] = []
    single_by_table: dict[str, list[ProfileCandidate]] = defaultdict(list)
    for candidate in single_candidates:
        single_by_table[candidate.table].append(candidate)
    for table_name in sorted(single_by_table):
        cases = [single_case_payload(candidate) for candidate in sorted(single_by_table[table_name], key=lambda item: item.name)]
        single_table.append({"table": table_name, "alias": aliases[table_name], "cases": cases})

    nested: list[dict[str, Any]] = []
    nested_by_family: dict[str, list[ProfileCandidate]] = defaultdict(list)
    family_metadata: dict[str, dict[str, Any]] = {}
    for candidate in nested_candidates:
        family_name = str(candidate.metadata["family_name"])
        nested_by_family[family_name].append(candidate)
        family_metadata[family_name] = {
            "name": family_name,
            "outer_table": candidate.metadata["outer_table"],
            "outer_alias": candidate.metadata["outer_alias"],
            "outer_key": candidate.metadata["outer_key"],
            "inner_from": candidate.metadata["inner_from"],
            "inner_key": candidate.metadata["inner_key"],
        }

    for family_name in sorted(nested_by_family):
        family = family_metadata[family_name].copy()
        family["cases"] = [nested_case_payload(candidate) for candidate in sorted(nested_by_family[family_name], key=lambda item: item.name)]
        nested.append(family)

    return {
        "benchmark": "job",
        "dataset_name": "gendba_pool",
        "preset": preset_name,
        "generation_mode": "auto_profile",
        "nested_shapes": NESTED_SHAPES,
        "single_table": single_table,
        "nested": nested,
    }


def parse_inner_aliases(inner_from: str) -> dict[str, str]:
    aliases = {}
    for table_name, alias in re.findall(r"(\w+)\s+AS\s+(\w+)", inner_from):
        aliases[alias] = table_name
    return aliases


def validate_profile(schema: dict[str, Any], profile: dict[str, Any]):
    schema_tables = {table["name"]: {column["name"] for column in table["columns"]} for table in schema["tables"]}

    for table_profile in profile["single_table"]:
        table_name = table_profile["table"]
        if table_name not in schema_tables:
            raise ValueError(f"Unknown table in profile: {table_name}")
        for case in table_profile["cases"]:
            for predicate in case["predicates"]:
                column_name = predicate["column"].split(".")[-1]
                if column_name not in schema_tables[table_name]:
                    raise ValueError(f"Unknown column {column_name} for table {table_name}")

    for family in profile["nested"]:
        outer_table = family["outer_table"]
        if outer_table not in schema_tables:
            raise ValueError(f"Unknown outer table in nested profile: {outer_table}")
        inner_aliases = parse_inner_aliases(family["inner_from"])
        for case in family["cases"]:
            for predicate in case.get("outer_predicates", []):
                column_name = predicate["column"].split(".")[-1]
                if column_name not in schema_tables[outer_table]:
                    raise ValueError(f"Unknown outer column {column_name} for table {outer_table}")
            for predicate in case["inner_predicates"]:
                if "." not in predicate["column"]:
                    raise ValueError(f"Nested predicate must use an alias-qualified column: {predicate['column']}")
                alias, column_name = predicate["column"].split(".", 1)
                if alias not in inner_aliases:
                    raise ValueError(f"Unknown alias {alias} in nested predicate {predicate['column']}")
                if column_name not in schema_tables[inner_aliases[alias]]:
                    raise ValueError(f"Unknown inner column {column_name} for alias {alias}")


def build_single_table_queries(profile: dict[str, Any], adapter: ServerProbeAdapter | None) -> list[QueryArtifact]:
    artifacts: list[QueryArtifact] = []
    index = 1
    for table_profile in profile["single_table"]:
        table_name = table_profile["table"]
        alias = table_profile.get("alias") or table_name[0]
        for case in table_profile["cases"]:
            where_clause = render_predicates(case["predicates"])
            sql = "\n".join(
                [
                    "SELECT COUNT(*) AS row_count",
                    f"FROM {table_name} AS {alias}",
                    "WHERE " + where_clause.replace("\n", "\n  "),
                    ";",
                ]
            ) + "\n"
            observed_rows = case.get("observed_rows")
            observed_selectivity = case.get("observed_selectivity")
            generation_mode = case.get("source", profile.get("generation_mode", "profile"))
            if adapter is not None:
                # External probe mode can re-measure an already-built profile against some
                # other live server without regenerating the candidate set.
                observed_rows, observed_selectivity = estimate_selectivity(adapter, f"{table_name} AS {alias}", flatten_predicates(render_predicates(case["predicates"], 4)))
                generation_mode = "live_probe"
            filename = f"st_{index:03d}_{slugify(table_name)}_{slugify(case['name'])}.sql"
            artifacts.append(
                QueryArtifact(
                    filename=filename,
                    sql=sql,
                    metadata={
                        "filename": filename,
                        "family": "single_table",
                        "semantic_family": f"single_table::{table_name}",
                        "table": table_name,
                        "predicate_columns": [predicate["column"].split(".")[-1] for predicate in case["predicates"]],
                        "predicate_kinds": [predicate["operator"] for predicate in case["predicates"]],
                        "target_bucket": case["bucket"],
                        "target_selectivity": BUCKET_TARGETS.get(case["bucket"]),
                        "observed_rows": observed_rows,
                        "observed_selectivity": observed_selectivity,
                        "generation_mode": generation_mode,
                        "candidate_id": case.get("candidate_id"),
                        "equivalence_group": None,
                    },
                )
            )
            index += 1
    return artifacts


def estimate_selectivity(adapter: ServerProbeAdapter | None, from_clause: str, where_clause: str) -> tuple[int | None, float | None]:
    if adapter is None:
        return None, None
    total = adapter.measure_count(f"SELECT COUNT(*) AS row_count FROM {from_clause};")
    filtered = adapter.measure_count(f"SELECT COUNT(*) AS row_count FROM {from_clause} WHERE {where_clause};")
    return filtered, None if total == 0 else filtered / total


def build_nested_queries(profile: dict[str, Any], adapter: ServerProbeAdapter | None, starting_index: int) -> list[QueryArtifact]:
    artifacts: list[QueryArtifact] = []
    index = starting_index
    shapes = profile.get("nested_shapes", NESTED_SHAPES)

    for family in profile["nested"]:
        for case in family["cases"]:
            observed_rows = case.get("observed_rows")
            observed_selectivity = case.get("observed_selectivity")
            generation_mode = case.get("source", profile.get("generation_mode", "profile"))
            if adapter is not None:
                from_clause, where_clause = nested_probe_query(family, case)
                observed_rows, observed_selectivity = estimate_selectivity(adapter, from_clause, where_clause)
                generation_mode = "live_probe"
            equivalence_group = f"nested::{family['name']}::{case['name']}"
            for shape in shapes:
                # One selected nested semantic case becomes several SQL rewrites here.
                filename = f"nested_{index:03d}_{slugify(family['name'])}_{slugify(case['name'])}_{shape}.sql"
                artifacts.append(
                    QueryArtifact(
                        filename=filename,
                        sql=nested_sql(family, case, shape),
                        metadata={
                            "filename": filename,
                            "family": "nested",
                            "semantic_family": family["name"],
                            "table": family["outer_table"],
                            "predicate_columns": [predicate["column"] for predicate in case.get("outer_predicates", []) + case["inner_predicates"]],
                            "predicate_kinds": [predicate["operator"] for predicate in case.get("outer_predicates", []) + case["inner_predicates"]],
                            "target_bucket": case["bucket"],
                            "target_selectivity": BUCKET_TARGETS.get(case["bucket"]),
                            "observed_rows": observed_rows,
                            "observed_selectivity": observed_selectivity,
                            "generation_mode": generation_mode,
                            "candidate_id": case.get("candidate_id"),
                            "equivalence_group": equivalence_group,
                            "nested_shape": shape,
                        },
                    )
                )
                index += 1
    return artifacts


def clean_output_dir(output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)
    for path in output_dir.iterdir():
        if path.is_file() and (path.suffix == ".sql" or path.name == "manifest.json"):
            path.unlink()


def write_artifacts(output_dir: Path, profile: dict[str, Any], artifacts: list[QueryArtifact], replace: bool = False):
    if output_dir.exists():
        if not replace:
            raise FileExistsError(f"Output directory already exists: {output_dir}")
        clean_output_dir(output_dir)
    else:
        output_dir.mkdir(parents=True, exist_ok=True)

    manifest = {
        "dataset_name": profile["dataset_name"],
        "benchmark": profile["benchmark"],
        "query_count": len(artifacts),
        "queries": [artifact.metadata for artifact in artifacts],
    }
    for artifact in artifacts:
        output_dir.joinpath(artifact.filename).write_text(artifact.sql.replace("\n;", ";"))
    output_dir.joinpath("manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def extract_json_object(text: str) -> str:
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end < start:
        raise ValueError("Expected a JSON object in LLM response")
    return text[start : end + 1]


def llm_system_prompt() -> str:
    return (
        "You are selecting SQL workload profile candidates. Return JSON only. "
        "Use only candidate ids from the input. Prefer diversity across tables, predicate kinds, selectivity buckets, and nested rewrite opportunities. "
        "Do not invent new tables, columns, or predicates."
    )


def resolve_llm_client(args: argparse.Namespace) -> OpenAICompatibleClient | None:
    base_url = args.llm_base_url or os.getenv("GENDBA_LLM_BASE_URL")
    model = args.llm_model or os.getenv("GENDBA_LLM_MODEL")
    api_key = args.llm_api_key or os.getenv("GENDBA_LLM_API_KEY")
    if not base_url or not model:
        return None
    return OpenAICompatibleClient(base_url, model, api_key, timeout=args.llm_timeout)


def resolve_preset(args: argparse.Namespace) -> WorkloadPreset:
    if args.preset not in WORKLOAD_PRESETS:
        raise ValueError(f"Unknown preset: {args.preset}")
    if args.final_query_cap is not None and args.preset != "exhaustive":
        raise ValueError("--final-query-cap is only supported with --preset exhaustive")
    preset = WORKLOAD_PRESETS[args.preset]
    overrides = {}
    for field_name in [
        "single_cases_per_table",
        "nested_cases_per_family",
        "max_nested_families",
        "dimension_table_max_rows",
    ]:
        value = getattr(args, field_name)
        if value is not None:
            overrides[field_name] = value
    return replace(preset, **overrides) if overrides else preset


def resolve_adapter(args: argparse.Namespace):
    if args.probe_mode == "none":
        return nullcontext(), None
    if args.probe_mode == "external":
        if not args.probe_url or not args.probe_dbms:
            raise ValueError("--probe-url and --probe-dbms are required with --probe-mode external")
        return nullcontext(), ServerProbeAdapter(args.probe_url, args.probe_dbms, PROBE_QUERY_TIMEOUT_SECONDS)
    managed = ManagedProbeServer(
        benchmark="job",
        postgres_version=args.postgres_version,
        port=args.probe_port,
        base_port=args.probe_base_port,
        db_dir=args.db_dir,
        data_dir=args.data_dir,
    )
    return managed, None


class nullcontext:
    def __enter__(self):
        return None

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False


def add_common_args(parser: argparse.ArgumentParser):
    parser.add_argument("--profile", type=Path, default=default_profile_path())
    parser.add_argument("--schema", type=Path, default=default_schema_path())
    parser.add_argument("--output-dir", type=Path, default=default_output_dir())
    parser.add_argument("--replace-output", action="store_true", help="overwrite generated SQL files in the output directory")
    parser.add_argument("--preset", choices=sorted(WORKLOAD_PRESETS), default="standard")
    parser.add_argument("--final-query-cap", type=int, default=None)
    parser.add_argument("--probe-mode", choices=["managed", "external", "none"], default="none")
    parser.add_argument("--probe-url", type=str, default=None)
    parser.add_argument("--probe-dbms", type=str, default=DEFAULT_PROBE_DBMS)
    parser.add_argument("--probe-port", type=int, default=5060)
    parser.add_argument("--probe-base-port", type=int, default=55100)
    parser.add_argument("--postgres-version", type=str, default=DEFAULT_POSTGRES_VERSION)
    parser.add_argument("--db-dir", type=Path, default=repo_root() / "db")
    parser.add_argument("--data-dir", type=Path, default=repo_root() / "data")
    parser.add_argument("--single-cases-per-table", type=int, default=None)
    parser.add_argument("--nested-cases-per-family", type=int, default=None)
    parser.add_argument("--max-nested-families", type=int, default=None)
    parser.add_argument("--dimension-table-max-rows", type=int, default=None)
    parser.add_argument("--llm-mode", choices=["off", "select"], default="off")
    parser.add_argument("--llm-base-url", type=str, default=None)
    parser.add_argument("--llm-model", type=str, default=None)
    parser.add_argument("--llm-api-key", type=str, default=None)
    parser.add_argument("--llm-timeout", type=int, default=60)



def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the GenDBA Pool profile and query set")
    subparsers = parser.add_subparsers(dest="command")

    profile_parser = subparsers.add_parser("profile", help="build job.profile.json from schema, pg_stats, probes, and optional LLM selection")
    add_common_args(profile_parser)
    profile_parser.add_argument("--candidates-output", type=Path, default=None)
    profile_parser.add_argument("--llm-input-output", type=Path, default=None)
    profile_parser.add_argument("--llm-output-output", type=Path, default=None)

    queries_parser = subparsers.add_parser("queries", help="render SQL files from an existing profile")
    add_common_args(queries_parser)

    all_parser = subparsers.add_parser("all", help="build the profile, then render SQL files")
    add_common_args(all_parser)
    all_parser.add_argument("--candidates-output", type=Path, default=None)
    all_parser.add_argument("--llm-input-output", type=Path, default=None)
    all_parser.add_argument("--llm-output-output", type=Path, default=None)

    args = parser.parse_args()
    args.command = args.command or "queries"
    return args


def build_profile_command(args: argparse.Namespace) -> dict[str, Any]:
    schema = load_json(args.schema)
    preset = resolve_preset(args)
    candidates_output = args.candidates_output or default_candidates_path(args.profile)
    llm_input_output = args.llm_input_output or default_llm_input_path(args.profile)
    llm_output_output = args.llm_output_output or default_llm_output_path(args.profile)
    llm_client = resolve_llm_client(args) if args.llm_mode == "select" else None

    probe_context, adapter = resolve_adapter(args)
    with probe_context as managed_adapter:
        adapter = adapter or managed_adapter
        builder = ProfileBuilder(
            schema=schema,
            adapter=adapter,
            preset=preset,
            final_query_cap=args.final_query_cap,
        )
        profile, candidates = builder.build(use_llm=args.llm_mode == "select", llm_client=llm_client)

    validate_profile(schema, profile)
    dump_json(args.profile, profile)
    dump_json(candidates_output, candidates)
    if candidates.get("llm", {}).get("input"):
        dump_json(llm_input_output, candidates["llm"]["input"])
    if candidates.get("llm", {}).get("output"):
        dump_json(llm_output_output, candidates["llm"]["output"])
    return profile


def build_queries_command(args: argparse.Namespace, profile: dict[str, Any] | None = None) -> list[QueryArtifact]:
    profile = profile or load_json(args.profile)
    schema = load_json(args.schema)
    validate_profile(schema, profile)

    adapter = None
    if args.probe_mode == "external":
        if not args.probe_url or not args.probe_dbms:
            raise ValueError("--probe-url and --probe-dbms are required with --probe-mode external")
        adapter = ServerProbeAdapter(args.probe_url, args.probe_dbms, PROBE_QUERY_TIMEOUT_SECONDS)

    artifacts = build_single_table_queries(profile, adapter)
    artifacts.extend(build_nested_queries(profile, adapter, len(artifacts) + 1))
    write_artifacts(args.output_dir, profile, artifacts, replace=args.replace_output)
    return artifacts


def main():
    args = parse_args()
    if args.command == "profile":
        profile = build_profile_command(args)
        print(json.dumps({"profile": str(args.profile), "single_tables": len(profile["single_table"]), "nested_families": len(profile["nested"])} , indent=2))
        return

    if args.command == "all":
        profile = build_profile_command(args)
        artifacts = build_queries_command(args, profile)
        print(json.dumps({"profile": str(args.profile), "output_dir": str(args.output_dir), "query_count": len(artifacts)}, indent=2))
        return

    artifacts = build_queries_command(args)
    print(json.dumps({"output_dir": str(args.output_dir), "query_count": len(artifacts)}, indent=2))


if __name__ == "__main__":
    main()
