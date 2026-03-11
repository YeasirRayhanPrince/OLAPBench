from __future__ import annotations

import json
import re
import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

try:
    import psycopg2
except ImportError:  # pragma: no cover - dependency is optional at runtime
    psycopg2 = None


BUCKET_TARGETS = {
    "very_high": 0.75,
    "high": 0.30,
    "medium": 0.10,
    "low": 0.02,
    "very_low": 0.002,
}
BUCKET_ORDER = ["very_high", "high", "medium", "low", "very_low"]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_stage_budgets(max_join_tables: int = 4) -> dict[str, int]:
    budgets = {"1_table": 24, "2_table": 36, "3_table": 36, "4_table": 24}
    for table_count in range(5, max_join_tables + 1):
        budgets[f"{table_count}_table"] = 16
    return budgets


@dataclass(frozen=True)
class ForeignKey:
    column: str
    foreign_table: str
    foreign_column: str


@dataclass
class ColumnSchema:
    name: str
    type: str
    nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False

    @property
    def is_numeric(self) -> bool:
        value = self.type.lower()
        return any(token in value for token in ["int", "numeric", "decimal", "real", "double", "float"])

    @property
    def is_text(self) -> bool:
        value = self.type.lower()
        return any(token in value for token in ["char", "text", "string"])

    @property
    def role(self) -> str:
        if self.is_primary_key:
            return "primary_key"
        if self.is_foreign_key:
            return "foreign_key"
        if self.is_numeric:
            return "numeric"
        if self.is_text:
            return "text"
        if self.nullable:
            return "nullable"
        return "generic"


@dataclass
class TableSchema:
    name: str
    columns: dict[str, ColumnSchema]
    primary_keys: tuple[str, ...]
    foreign_keys: list[ForeignKey]


@dataclass(frozen=True)
class JoinEdge:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str

    @property
    def key(self) -> str:
        return f"{self.child_table}.{self.child_column}->{self.parent_table}.{self.parent_column}"

    def as_manifest(self) -> dict[str, str]:
        return {
            "child_table": self.child_table,
            "child_column": self.child_column,
            "parent_table": self.parent_table,
            "parent_column": self.parent_column,
        }


@dataclass
class ColumnStats:
    null_frac: float | None = None
    n_distinct: float | None = None
    most_common_vals: list[Any] = field(default_factory=list)
    most_common_freqs: list[float] = field(default_factory=list)
    histogram_bounds: list[Any] = field(default_factory=list)


@dataclass
class PgConnectionConfig:
    enabled: bool = False
    host: str = "127.0.0.1"
    port: int = 5432
    user: str = "postgres"
    password: str = ""
    database: str = "postgres"


@dataclass
class GeneratorConfig:
    benchmark: str
    suffix: str
    branch: str = "spj"
    schema_path: Path | None = None
    benchmarks_root: Path = field(default_factory=lambda: repo_root() / "benchmarks")
    replace_output: bool = False
    seed: int = 0
    max_join_tables: int = 4
    join_types: tuple[str, ...] = ("inner", "left")
    stage_budgets: dict[str, int] = field(default_factory=default_stage_budgets)
    max_predicates_per_table: int = 3
    stats_mode: str = "auto"
    pg: PgConnectionConfig | None = None
    template_packs: tuple[str, ...] = ()


@dataclass
class StatsSnapshot:
    mode: str
    table_counts: dict[str, int] = field(default_factory=dict)
    column_stats: dict[str, dict[str, ColumnStats]] = field(default_factory=dict)
    provider: Any | None = None


@dataclass
class QueryArtifact:
    filename: str
    sql: str
    metadata: dict[str, Any]


def load_config_file(path: Path) -> dict[str, Any]:
    payload = path.read_text()
    if path.suffix.lower() == ".json":
        return json.loads(payload)
    return yaml.safe_load(payload) or {}


def resolve_stats_mode(config: GeneratorConfig) -> str:
    if config.stats_mode == "auto":
        return "stats_plus_selective_probes" if config.pg and config.pg.enabled else "schema_only"
    return config.stats_mode


def resolve_benchmark_dir(benchmarks_root: Path, benchmark: str) -> Path:
    benchmark_dir = benchmarks_root / benchmark
    if not benchmark_dir.exists():
        raise FileNotFoundError(f"Unknown benchmark directory: {benchmark_dir}")
    return benchmark_dir


def resolve_schema_path(benchmark_dir: Path, schema_override: Path | None = None) -> Path:
    if schema_override is not None:
        if not schema_override.exists():
            raise FileNotFoundError(f"Schema file does not exist: {schema_override}")
        return schema_override
    expected = benchmark_dir / f"{benchmark_dir.name}.dbschema.json"
    if expected.exists():
        return expected
    matches = sorted(benchmark_dir.glob("*.dbschema.json"))
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise FileNotFoundError(f"No schema file found under {benchmark_dir}")
    raise FileNotFoundError(f"Multiple schema files found under {benchmark_dir}; specify --schema-path")


def resolve_output_dir(benchmark_dir: Path, suffix: str) -> Path:
    return benchmark_dir / f"queries_{suffix}"


def load_schema_catalog(path: Path) -> dict[str, TableSchema]:
    schema_doc = json.loads(path.read_text())
    catalog: dict[str, TableSchema] = {}

    for table_doc in schema_doc["tables"]:
        primary_key_doc = table_doc.get("primary key", {})
        primary_keys = tuple(primary_key_doc.get("columns") or ([primary_key_doc["column"]] if "column" in primary_key_doc else []))
        foreign_keys = [
            ForeignKey(
                column=foreign_key["column"],
                foreign_table=foreign_key["foreign table"],
                foreign_column=foreign_key["foreign column"],
            )
            for foreign_key in table_doc.get("foreign keys", [])
        ]
        foreign_key_columns = {foreign_key.column for foreign_key in foreign_keys}
        columns: dict[str, ColumnSchema] = {}
        for column_doc in table_doc["columns"]:
            column_name = column_doc["name"]
            if column_name in columns:
                continue
            column_type = column_doc["type"]
            columns[column_name] = ColumnSchema(
                name=column_name,
                type=column_type,
                nullable="not null" not in column_type.lower(),
                is_primary_key=column_name in primary_keys,
                is_foreign_key=column_name in foreign_key_columns,
            )
        catalog[table_doc["name"]] = TableSchema(
            name=table_doc["name"],
            columns=columns,
            primary_keys=primary_keys,
            foreign_keys=foreign_keys,
        )
    return catalog


def build_join_edges(catalog: dict[str, TableSchema]) -> list[JoinEdge]:
    edges: list[JoinEdge] = []
    for table in sorted(catalog.values(), key=lambda item: item.name):
        for foreign_key in table.foreign_keys:
            if foreign_key.foreign_table not in catalog:
                continue
            edges.append(
                JoinEdge(
                    child_table=table.name,
                    child_column=foreign_key.column,
                    parent_table=foreign_key.foreign_table,
                    parent_column=foreign_key.foreign_column,
                )
            )
    return edges


def alias_map(table_names: list[str]) -> dict[str, str]:
    aliases: dict[str, str] = {}
    used: set[str] = set()
    for table_name in table_names:
        base = "".join(part[0] for part in table_name.split("_")) or table_name[0]
        alias = base
        suffix = 2
        while alias in used:
            alias = f"{base}{suffix}"
            suffix += 1
        aliases[table_name] = alias
        used.add(alias)
    return aliases


def sql_literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(value)


def bucket_for_selectivity(selectivity: float | None) -> str:
    if selectivity is None:
        return "medium"
    return min(BUCKET_TARGETS, key=lambda bucket: abs(BUCKET_TARGETS[bucket] - selectivity))


def normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip())


def deduplicate_artifacts(artifacts: list[QueryArtifact]) -> list[QueryArtifact]:
    deduped: dict[str, QueryArtifact] = {}
    for artifact in artifacts:
        key = normalize_sql(artifact.sql)
        if key not in deduped:
            deduped[key] = artifact
    return list(deduped.values())


def clean_output_dir(output_dir: Path) -> None:
    if not output_dir.exists():
        return
    for path in output_dir.iterdir():
        if path.is_file() and (path.suffix == ".sql" or path.name == "manifest.json"):
            path.unlink()
        elif path.is_dir():
            shutil.rmtree(path)


def write_output(output_dir: Path, manifest: dict[str, Any], artifacts: list[QueryArtifact], replace_output: bool) -> None:
    if output_dir.exists():
        if not replace_output:
            raise FileExistsError(f"Output directory already exists: {output_dir}")
        clean_output_dir(output_dir)
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
    for artifact in artifacts:
        output_dir.joinpath(artifact.filename).write_text(artifact.sql)
    output_dir.joinpath("manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")


def validate_manifest_entry(catalog: dict[str, TableSchema], join_edges: list[JoinEdge], entry: dict[str, Any]) -> None:
    edge_keys = {edge.key for edge in join_edges}
    for table_name in entry.get("tables", []):
        if table_name not in catalog:
            raise ValueError(f"Unknown table in manifest: {table_name}")
    for projection in entry.get("projection_columns", []):
        table_name = projection["table"]
        column_name = projection["column"]
        if table_name not in catalog or column_name not in catalog[table_name].columns:
            raise ValueError(f"Unknown projection column {table_name}.{column_name}")
    for predicate in entry.get("predicates", []):
        table_name = predicate["table"]
        column_name = predicate["column"]
        if table_name not in catalog or column_name not in catalog[table_name].columns:
            raise ValueError(f"Unknown predicate column {table_name}.{column_name}")
    for edge in entry.get("join_edges", []):
        key = f"{edge['child_table']}.{edge['child_column']}->{edge['parent_table']}.{edge['parent_column']}"
        if key not in edge_keys:
            raise ValueError(f"Unknown join edge {key}")


class PostgresStatsProvider:
    def __init__(self, connection: PgConnectionConfig):
        self.connection = connection
        self._handle = None
        self.probe_calls = 0

    def _connect(self):
        if self._handle is None:
            if psycopg2 is None:
                raise RuntimeError("psycopg2 is required for PostgreSQL stats mode")
            self._handle = psycopg2.connect(
                host=self.connection.host,
                port=self.connection.port,
                user=self.connection.user,
                password=self.connection.password,
                dbname=self.connection.database,
            )
            self._handle.autocommit = True
        return self._handle

    def close(self) -> None:
        if self._handle is not None:
            self._handle.close()
            self._handle = None

    def query_rows(self, sql: str, params: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
        handle = self._connect()
        with handle.cursor() as cursor:
            cursor.execute(sql, params)
            return list(cursor.fetchall())

    def load_table_counts(self, table_names: list[str]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for table_name in table_names:
            rows = self.query_rows(f"SELECT COUNT(*) FROM {table_name};")
            counts[table_name] = int(rows[0][0]) if rows else 0
        return counts

    def load_column_stats(self, table_names: list[str]) -> dict[str, dict[str, ColumnStats]]:
        rows = self.query_rows(
            """
            SELECT
              tablename,
              attname,
              null_frac,
              n_distinct,
              CASE WHEN most_common_vals IS NULL THEN NULL ELSE array_to_json(most_common_vals)::text END,
              CASE WHEN most_common_freqs IS NULL THEN NULL ELSE array_to_json(most_common_freqs)::text END,
              CASE WHEN histogram_bounds IS NULL THEN NULL ELSE array_to_json(histogram_bounds)::text END
            FROM pg_stats
            WHERE schemaname = 'public' AND tablename = ANY(%s)
            ORDER BY tablename, attname;
            """,
            (table_names,),
        )
        stats: dict[str, dict[str, ColumnStats]] = {}
        for table_name, column_name, null_frac, n_distinct, mcv_json, freq_json, hist_json in rows:
            table_stats = stats.setdefault(str(table_name), {})
            table_stats[str(column_name)] = ColumnStats(
                null_frac=None if null_frac is None else float(null_frac),
                n_distinct=None if n_distinct is None else float(n_distinct),
                most_common_vals=json.loads(mcv_json) if mcv_json else [],
                most_common_freqs=[float(value) for value in json.loads(freq_json)] if freq_json else [],
                histogram_bounds=json.loads(hist_json) if hist_json else [],
            )
        return stats

    def probe_count(self, sql: str) -> int:
        self.probe_calls += 1
        query = sql.strip().rstrip(";")
        rows = self.query_rows(f"SELECT COUNT(*) FROM ({query}) AS curriculum_probe;")
        return int(rows[0][0]) if rows else 0


def load_stats_snapshot(config: GeneratorConfig, catalog: dict[str, TableSchema]) -> StatsSnapshot:
    mode = resolve_stats_mode(config)
    if mode == "schema_only":
        return StatsSnapshot(mode=mode)
    if config.pg is None or not config.pg.enabled:
        if mode == "stats_only":
            raise ValueError("stats_only mode requires PostgreSQL connection settings")
        return StatsSnapshot(mode="schema_only")
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required when PostgreSQL-backed generation is enabled")
    provider = PostgresStatsProvider(config.pg)
    table_names = sorted(catalog)
    try:
        return StatsSnapshot(
            mode=mode,
            table_counts=provider.load_table_counts(table_names),
            column_stats=provider.load_column_stats(table_names),
            provider=provider,
        )
    except Exception:
        provider.close()
        if mode == "stats_only":
            raise
        return StatsSnapshot(mode="schema_only")


def close_stats_snapshot(snapshot: StatsSnapshot) -> None:
    provider = snapshot.provider
    if provider is not None and hasattr(provider, "close"):
        provider.close()
