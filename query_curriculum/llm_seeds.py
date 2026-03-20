"""LLM-based seed generation for schema_plus_llm mode.

Uses an OpenAI-compatible API to generate realistic predicate values
for tables when pg_stats are unavailable.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from query_curriculum.core import ColumnSchema, TableSchema
from query_curriculum.spj import SeedTemplate

logger = logging.getLogger(__name__)

VALID_OPERATORS = {"=", "!=", "<", "<=", ">", ">=", "between", "in", "is_null", "is_not_null"}
VALID_FAMILIES = {"equality", "membership", "range", "null"}


def build_llm_seed_templates(
    table: TableSchema,
    *,
    benchmark: str = "job",
    cache_dir: Path | None = None,
    model: str = "gpt-4o-mini",
) -> list[SeedTemplate]:
    """Generate SeedTemplate objects for a table using an LLM."""
    if cache_dir is None:
        from query_curriculum.core import repo_root
        cache_dir = repo_root() / "benchmarks" / benchmark / ".llm_seed_cache"

    if not _filterable_columns(table):
        logger.debug("Skipping LLM seeds for %s: no filterable columns (all PK/FK)", table.name)
        return []

    cached = _load_cache(cache_dir, table.name)
    if cached is not None:
        seeds = _entries_to_seeds(table, cached)
        logger.info("LLM seeds for %s: %d from cache", table.name, len(seeds))
        return seeds

    try:
        import openai  # noqa: F401
    except ImportError:
        raise RuntimeError(
            "openai package required for schema_plus_llm mode. "
            "Install with: pip install openai"
        )

    entries = _call_llm(table, model=model)
    if entries is None:
        return []

    _write_cache(cache_dir, table.name, entries, model=model)
    seeds = _entries_to_seeds(table, entries)
    logger.info("LLM seeds for %s: %d from API (%s)", table.name, len(seeds), model)
    return seeds


def _filterable_columns(table: TableSchema) -> list[ColumnSchema]:
    """Return columns suitable for predicates (skip PK/FK)."""
    return [
        col for col in table.columns.values()
        if not col.is_primary_key and not col.is_foreign_key
    ]


def _build_prompt(table: TableSchema) -> tuple[str, str]:
    """Build system and user prompts for the LLM call."""
    system = (
        "You are a database expert. Given a table schema from the IMDb dataset "
        "(used in the Join Order Benchmark), produce realistic SQL predicate values. "
        "Return JSON only — no markdown fences, no explanation."
    )

    columns_desc = []
    for col in _filterable_columns(table):
        columns_desc.append(
            f"  - {col.name}: {col.type}"
            + (", nullable" if col.nullable else ", NOT NULL")
        )

    user = (
        f"Table: {table.name}\n"
        f"Columns:\n" + "\n".join(columns_desc) + "\n\n"
        "Generate realistic predicate seeds as a JSON array. For each entry:\n"
        '  {"column": "<name>", "operator": "<op>", "value": <val>, '
        '"family": "<fam>", "selectivity": <float>}\n\n'
        "Rules:\n"
        "- equality: 3-6 '=' values per filterable column, selectivity 0.001-0.1\n"
        "- membership: 1-3 'in' lists of 2-4 values per column, selectivity 0.01-0.3\n"
        "- range: 2-4 'between'/'>='/'<=' predicates for numeric columns, selectivity 0.05-0.75\n"
        "- null: 'is_null' and 'is_not_null' for each nullable column, "
        "estimate selectivity based on typical IMDb data\n"
        "- Use realistic IMDb values (e.g., production_year around 1900-2020, "
        "kind_id 1-7, etc.)\n"
        "- 'between' value is a 2-element list [lo, hi]\n"
        "- 'in' value is a list of 2-4 elements\n"
        "- 'is_null'/'is_not_null' value should be null\n"
        "- Spread selectivities across buckets: some very selective (0.002), "
        "some medium (0.05-0.2), some broad (0.3-0.75)\n"
    )
    return system, user


def _call_llm(table: TableSchema, *, model: str, max_retries: int = 2) -> list[dict] | None:
    """Call the OpenAI API and parse the response."""
    import openai

    system_msg, user_msg = _build_prompt(table)
    client = openai.OpenAI()

    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system_msg},
                    {"role": "user", "content": user_msg},
                ],
                temperature=0.7,
                max_tokens=4096,
            )
            content = response.choices[0].message.content or ""
            entries = _parse_llm_response(content, table)
            if entries:
                return entries
            logger.warning(
                "LLM returned no valid entries for %s (attempt %d/%d)",
                table.name, attempt + 1, max_retries,
            )
        except (json.JSONDecodeError, Exception) as exc:
            logger.warning(
                "LLM call failed for %s (attempt %d/%d): %s",
                table.name, attempt + 1, max_retries, exc,
            )

    logger.warning("All LLM attempts failed for %s, returning empty seeds", table.name)
    return None


def _parse_llm_response(content: str, table: TableSchema) -> list[dict]:
    """Parse and validate the LLM JSON response."""
    # Strip markdown fences if present
    text = content.strip()
    if text.startswith("```"):
        lines = text.split("\n")
        lines = lines[1:]  # drop opening fence
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines)

    entries = json.loads(text)
    if not isinstance(entries, list):
        return []

    valid_columns = set(table.columns.keys())
    filterable = {col.name for col in _filterable_columns(table)}
    validated = []

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        col = entry.get("column")
        op = entry.get("operator")
        family = entry.get("family")
        value = entry.get("value")
        sel = entry.get("selectivity")

        # Reject invalid column names
        if col not in valid_columns or col not in filterable:
            continue
        if op not in VALID_OPERATORS:
            continue
        if family not in VALID_FAMILIES:
            continue

        # Validate value shape
        if op == "in":
            if not isinstance(value, list) or len(value) < 2:
                continue
        elif op == "between":
            if not isinstance(value, list) or len(value) != 2:
                continue
        elif op in ("is_null", "is_not_null"):
            value = None
        elif value is None:
            continue

        # Clamp selectivity
        if sel is None:
            sel = 0.05
        sel = max(0.0001, min(0.99, float(sel)))

        validated.append({
            "column": col,
            "operator": op,
            "value": value,
            "family": family,
            "selectivity": sel,
        })

    return validated


def _entries_to_seeds(table: TableSchema, entries: list[dict]) -> list[SeedTemplate]:
    """Convert validated entry dicts to SeedTemplate objects."""
    valid_columns = {col.name for col in _filterable_columns(table)}
    seeds = []
    for entry in entries:
        col = entry.get("column")
        if col not in valid_columns:
            continue  # stale cache entry
        op = entry.get("operator")
        if op not in VALID_OPERATORS:
            continue
        value = entry.get("value")
        if op in ("is_null", "is_not_null"):
            value = None
        seeds.append(SeedTemplate(
            table=table.name,
            column=col,
            operator=op,
            value=value,
            family=entry.get("family", "generic"),
            selectivity=entry.get("selectivity"),
        ))
    return seeds


def _load_cache(cache_dir: Path, table_name: str) -> list[dict] | None:
    """Load cached LLM seeds from disk."""
    path = cache_dir / f"{table_name}.json"
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
        return data.get("seeds", [])
    except (json.JSONDecodeError, OSError) as exc:
        logger.warning("Failed to read LLM seed cache for %s: %s", table_name, exc)
        return None


def _write_cache(cache_dir: Path, table_name: str, entries: list[dict], *, model: str) -> None:
    """Write validated seeds to disk cache."""
    cache_dir.mkdir(parents=True, exist_ok=True)
    path = cache_dir / f"{table_name}.json"
    data = {
        "model": model,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seeds": entries,
    }
    path.write_text(json.dumps(data, indent=2))
