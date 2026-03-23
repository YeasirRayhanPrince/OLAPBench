#!/usr/bin/env python3
"""
collect_stats.py — Collect PostgreSQL optimizer statistics for a dbschema.json file.

Reads the database connection from a pipeline YAML config (same format as run.py),
queries pg_class, pg_stats, and pg_stats_ext, and writes an enriched
*.dbschema.stats.json file alongside the input schema.

Usage:
    python gendba_pipeline/collect_stats.py \
        --config gendba_pipeline/configs/postgres_local_preloaded.dataset.yaml \
        --schema benchmarks/job/job.dbschema.json
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

import yaml


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _load_yaml(path: Path) -> dict[str, Any]:
    with path.open("r") as fh:
        return yaml.safe_load(fh)


def _extract_conn_params(config: dict[str, Any]) -> dict[str, Any]:
    """Extract psycopg2 connection kwargs from the pipeline YAML."""
    systems = config.get("systems", [])
    if not systems:
        raise ValueError("No 'systems' entries found in config YAML.")

    # Use the first system that has local: enabled: true
    for system in systems:
        local = system.get("local", {}) or {}
        if local.get("enabled", False):
            if system.get("dbms", "").lower() != "postgres":
                raise ValueError(
                    f"Local mode is only supported for postgres, not {system.get('dbms')}"
                )
            return {
                "host": local.get("host", "localhost"),
                "port": local.get("port", 5432),
                "user": local.get("user"),
                "password": local.get("password") or "",
                "dbname": local.get("database", "postgres"),
            }

    raise ValueError(
        "No system with 'local.enabled: true' found in config YAML. "
        "Ensure at least one system has a local: block with enabled: true."
    )


# ---------------------------------------------------------------------------
# Statistics queries
# ---------------------------------------------------------------------------

def _collect_table_sizes(
    cur: Any, table_names: list[str]
) -> dict[str, dict[str, Any]]:
    """Query pg_class for reltuples and relpages per table."""
    result: dict[str, dict[str, Any]] = {t: {"reltuples": None, "relpages": None} for t in table_names}
    for table in table_names:
        cur.execute(
            """
            SELECT reltuples::bigint, relpages::bigint
            FROM pg_class
            WHERE relname = %s
              AND relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')
            """,
            (table,),
        )
        row = cur.fetchone()
        if row:
            result[table]["reltuples"] = row[0]
            result[table]["relpages"] = row[1]
    return result


def _collect_column_stats(
    cur: Any, table_names: list[str]
) -> dict[str, dict[str, dict[str, Any]]]:
    """Query pg_stats for per-column statistics."""
    cur.execute(
        """
        SELECT
            tablename,
            attname,
            null_frac,
            avg_width,
            n_distinct,
            CASE WHEN most_common_vals  IS NULL THEN NULL
                 ELSE array_to_json(most_common_vals)::text END,
            CASE WHEN most_common_freqs IS NULL THEN NULL
                 ELSE array_to_json(most_common_freqs)::text END,
            CASE WHEN histogram_bounds  IS NULL THEN NULL
                 ELSE array_to_json(histogram_bounds)::text END,
            correlation
        FROM pg_stats
        WHERE schemaname = 'public'
          AND tablename = ANY(%s)
        ORDER BY tablename, attname;
        """,
        (table_names,),
    )
    stats: dict[str, dict[str, dict[str, Any]]] = {}
    for (
        tablename,
        attname,
        null_frac,
        avg_width,
        n_distinct,
        mcv_json,
        freq_json,
        hist_json,
        correlation,
    ) in cur.fetchall():
        col_stats = {
            "null_frac": float(null_frac) if null_frac is not None else None,
            "avg_width": int(avg_width) if avg_width is not None else None,
            "n_distinct": float(n_distinct) if n_distinct is not None else None,
            "most_common_vals": json.loads(mcv_json) if mcv_json else [],
            "most_common_freqs": [float(v) for v in json.loads(freq_json)] if freq_json else [],
            "histogram_bounds": json.loads(hist_json) if hist_json else [],
            "correlation": float(correlation) if correlation is not None else None,
        }
        stats.setdefault(str(tablename), {})[str(attname)] = col_stats
    return stats


def _collect_extended_stats(
    cur: Any, table_names: list[str]
) -> dict[str, list[dict[str, Any]]]:
    """
    Query pg_stats_ext (and pg_stats_ext_exprs) for extended statistics.
    Returns an empty list per table if none exist.
    """
    result: dict[str, list[dict[str, Any]]] = {t: [] for t in table_names}
    try:
        cur.execute(
            """
            SELECT
                s.stxname,
                s.stxrelid::regclass::text AS tablename,
                array(
                    SELECT a.attname
                    FROM pg_attribute a
                    WHERE a.attrelid = s.stxrelid
                      AND a.attnum = ANY(s.stxkeys)
                    ORDER BY a.attnum
                ) AS key_columns,
                e.stxdndistinct  AS ndistinct,
                e.stxddependencies AS dependencies,
                e.stxdmcv AS mcv
            FROM pg_statistic_ext s
            LEFT JOIN pg_statistic_ext_data e ON e.stxoid = s.oid
            WHERE s.stxrelid::regclass::text = ANY(%s)
               OR split_part(s.stxrelid::regclass::text, '.', 2) = ANY(%s)
            ORDER BY s.stxrelid, s.stxname;
            """,
            (table_names, table_names),
        )
        rows = cur.fetchall()
    except Exception:
        # pg_statistic_ext_data may not exist in older PG versions
        return result

    for stxname, tablename, key_columns, ndistinct, dependencies, mcv in rows:
        # tablename may be schema-qualified; strip schema prefix
        bare = tablename.split(".")[-1] if "." in tablename else tablename
        if bare not in result:
            continue
        entry: dict[str, Any] = {
            "stxname": stxname,
            "stxkeys": list(key_columns) if key_columns else [],
        }
        if ndistinct is not None:
            try:
                entry["ndistinct"] = json.loads(ndistinct) if isinstance(ndistinct, str) else ndistinct
            except Exception:
                entry["ndistinct"] = ndistinct
        if dependencies is not None:
            try:
                entry["dependencies"] = json.loads(dependencies) if isinstance(dependencies, str) else dependencies
            except Exception:
                entry["dependencies"] = dependencies
        if mcv is not None:
            try:
                entry["mcv"] = json.loads(mcv) if isinstance(mcv, str) else mcv
            except Exception:
                entry["mcv"] = str(mcv)
        result[bare].append(entry)

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Collect PostgreSQL optimizer stats and enrich a dbschema.json file."
    )
    parser.add_argument(
        "--config",
        required=True,
        type=Path,
        help="Path to the pipeline YAML config (same as used by run.py / run_explain.sh).",
    )
    parser.add_argument(
        "--schema",
        required=True,
        type=Path,
        help="Path to the input dbschema.json file.",
    )
    args = parser.parse_args()

    config_path: Path = args.config.resolve()
    schema_path: Path = args.schema.resolve()

    # Derive output path: foo.dbschema.json -> foo.dbschema.stats.json
    stem = schema_path.name  # e.g. "job.dbschema.json"
    if stem.endswith(".dbschema.json"):
        out_name = stem[: -len(".dbschema.json")] + ".dbschema.stats.json"
    else:
        out_name = stem.rsplit(".", 1)[0] + ".stats.json"
    output_path = schema_path.parent / out_name

    print(f"Config : {config_path}")
    print(f"Schema : {schema_path}")
    print(f"Output : {output_path}")

    # Load config
    config = _load_yaml(config_path)
    conn_params = _extract_conn_params(config)
    print(
        f"Connecting to PostgreSQL: {conn_params['host']}:{conn_params['port']}"
        f" db={conn_params['dbname']} user={conn_params['user']}"
    )

    # Load input schema
    with schema_path.open("r") as fh:
        schema = json.load(fh)

    table_names: list[str] = [t["name"] for t in schema.get("tables", [])]
    print(f"Tables  : {', '.join(table_names)}")

    # Connect
    try:
        import psycopg2
    except ImportError:
        print("ERROR: psycopg2 is not installed. Run: pip install psycopg2-binary", file=sys.stderr)
        sys.exit(1)

    conn = psycopg2.connect(**conn_params)
    conn.autocommit = True

    try:
        with conn.cursor() as cur:
            print("Querying pg_class for table sizes...")
            sizes = _collect_table_sizes(cur, table_names)

            print("Querying pg_stats for column statistics...")
            col_stats = _collect_column_stats(cur, table_names)

            print("Querying pg_stats_ext for extended statistics...")
            ext_stats = _collect_extended_stats(cur, table_names)
    finally:
        conn.close()

    # Enrich schema
    output_schema = {k: v for k, v in schema.items() if k != "tables"}
    enriched_tables = []
    for table in schema.get("tables", []):
        tname = table["name"]
        enriched = dict(table)  # keep all original fields (name, columns, primary key, foreign keys)

        # Table-level stats from pg_class
        size_info = sizes.get(tname, {})
        enriched["reltuples"] = size_info.get("reltuples")
        enriched["relpages"] = size_info.get("relpages")

        # Per-column stats from pg_stats
        table_col_stats = col_stats.get(tname, {})
        enriched["columns"] = [
            {**col, "stats": table_col_stats.get(col["name"])}
            for col in table.get("columns", [])
        ]

        # Extended statistics from pg_stats_ext
        enriched["extended_stats"] = ext_stats.get(tname, [])

        enriched_tables.append(enriched)

    output_schema["tables"] = enriched_tables

    with output_path.open("w") as fh:
        json.dump(output_schema, fh, indent=2)
        fh.write("\n")

    print(f"Done. Written to: {output_path}")


if __name__ == "__main__":
    main()
