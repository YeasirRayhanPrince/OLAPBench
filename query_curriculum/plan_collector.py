"""EXPLAIN plan collection infrastructure.

Collects (logical plan, physical plan) pairs from PostgreSQL for training
data. Supports default plans and alternative plans via pg_hint_plan and GUC
toggles.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

try:
    import psycopg2
except ImportError:
    psycopg2 = None

from query_curriculum.core import PgConnectionConfig


@dataclass
class PlanResult:
    """A single EXPLAIN plan for a query."""
    sql: str
    plan_json: dict[str, Any]
    hint: str | None = None  # the hint used, or None for default plan
    total_cost: float = 0.0
    plan_type: str = "default"  # "default", "hint_join", "hint_scan", "hint_agg"


@dataclass
class QueryPlans:
    """All collected plans for a single query."""
    query_id: str
    sql: str
    default_plan: PlanResult | None = None
    alternative_plans: list[PlanResult] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "query_id": self.query_id,
            "sql": self.sql,
        }
        if self.default_plan:
            result["default_plan"] = {
                "plan": self.default_plan.plan_json,
                "total_cost": self.default_plan.total_cost,
            }
        if self.alternative_plans:
            result["alternative_plans"] = [
                {
                    "hint": p.hint,
                    "plan_type": p.plan_type,
                    "plan": p.plan_json,
                    "total_cost": p.total_cost,
                }
                for p in self.alternative_plans
            ]
        return result


# ---------------------------------------------------------------------------
# Core EXPLAIN functions
# ---------------------------------------------------------------------------

def _get_connection(pg_config: PgConnectionConfig):
    """Create a new psycopg2 connection."""
    if psycopg2 is None:
        raise RuntimeError("psycopg2 is required for plan collection")
    conn = psycopg2.connect(
        host=pg_config.host,
        port=pg_config.port,
        user=pg_config.user,
        password=pg_config.password,
        dbname=pg_config.database,
    )
    conn.autocommit = True
    return conn


def explain_plan(conn, sql: str) -> PlanResult:
    """Run EXPLAIN (FORMAT JSON) and return the parsed plan."""
    clean_sql = sql.strip().rstrip(";")
    with conn.cursor() as cur:
        cur.execute(f"EXPLAIN (FORMAT JSON) {clean_sql}")
        rows = cur.fetchall()
    plan_json = rows[0][0]
    if isinstance(plan_json, str):
        plan_json = json.loads(plan_json)
    # plan_json is a list with one element
    plan_data = plan_json[0] if isinstance(plan_json, list) else plan_json
    plan_node = plan_data.get("Plan", plan_data)
    total_cost = float(plan_node.get("Total Cost", 0.0))
    return PlanResult(
        sql=sql,
        plan_json=plan_data,
        total_cost=total_cost,
        plan_type="default",
    )


def _extract_table_aliases(sql: str) -> list[str]:
    """Extract table aliases from SQL (best-effort regex)."""
    # Match patterns like: FROM table AS alias, JOIN table AS alias
    pattern = r'(?:FROM|JOIN)\s+(\w+)\s+AS\s+(\w+)'
    matches = re.findall(pattern, sql, re.IGNORECASE)
    return [(table, alias) for table, alias in matches]


def _extract_join_pairs(sql: str) -> list[tuple[str, str]]:
    """Extract pairs of table aliases involved in joins."""
    table_aliases = _extract_table_aliases(sql)
    if len(table_aliases) < 2:
        return []
    aliases = [alias for _, alias in table_aliases]
    pairs = []
    for i in range(len(aliases)):
        for j in range(i + 1, len(aliases)):
            pairs.append((aliases[i], aliases[j]))
    return pairs


def explain_with_hint(conn, sql: str, hint_comment: str, plan_type: str = "hint") -> PlanResult | None:
    """Run EXPLAIN with a pg_hint_plan hint comment prepended."""
    clean_sql = sql.strip().rstrip(";")
    hinted_sql = f"{hint_comment}\n{clean_sql}"
    try:
        with conn.cursor() as cur:
            cur.execute(f"EXPLAIN (FORMAT JSON) {hinted_sql}")
            rows = cur.fetchall()
        plan_json = rows[0][0]
        if isinstance(plan_json, str):
            plan_json = json.loads(plan_json)
        plan_data = plan_json[0] if isinstance(plan_json, list) else plan_json
        plan_node = plan_data.get("Plan", plan_data)
        total_cost = float(plan_node.get("Total Cost", 0.0))
        return PlanResult(
            sql=hinted_sql,
            plan_json=plan_data,
            hint=hint_comment,
            total_cost=total_cost,
            plan_type=plan_type,
        )
    except Exception:
        return None


def explain_with_guc(conn, sql: str, guc_settings: dict[str, str], plan_type: str = "guc") -> PlanResult | None:
    """Run EXPLAIN after setting GUC parameters, then reset them."""
    clean_sql = sql.strip().rstrip(";")
    try:
        with conn.cursor() as cur:
            for param, value in guc_settings.items():
                cur.execute(f"SET {param} = {value}")
            cur.execute(f"EXPLAIN (FORMAT JSON) {clean_sql}")
            rows = cur.fetchall()
            # Reset GUCs
            for param in guc_settings:
                cur.execute(f"RESET {param}")
        plan_json = rows[0][0]
        if isinstance(plan_json, str):
            plan_json = json.loads(plan_json)
        plan_data = plan_json[0] if isinstance(plan_json, list) else plan_json
        plan_node = plan_data.get("Plan", plan_data)
        total_cost = float(plan_node.get("Total Cost", 0.0))
        hint_desc = ", ".join(f"{k}={v}" for k, v in guc_settings.items())
        return PlanResult(
            sql=sql,
            plan_json=plan_data,
            hint=hint_desc,
            total_cost=total_cost,
            plan_type=plan_type,
        )
    except Exception:
        # Reset GUCs on error
        try:
            with conn.cursor() as cur:
                for param in guc_settings:
                    cur.execute(f"RESET {param}")
        except Exception:
            pass
        return None


def explain_with_alternatives(
    conn,
    sql: str,
    *,
    use_pg_hint_plan: bool = True,
    max_alternatives: int = 5,
) -> QueryPlans:
    """Collect the default plan plus alternative plans via hints/GUCs.

    Strategies:
    1. pg_hint_plan: force join methods (HashJoin, MergeJoin, NestLoop)
    2. pg_hint_plan: force scan types (SeqScan, IndexScan)
    3. GUC toggles: enable_hashagg, enable_sort, enable_mergejoin, etc.
    """
    query_plans = QueryPlans(query_id="", sql=sql)

    # Default plan
    try:
        query_plans.default_plan = explain_plan(conn, sql)
    except Exception:
        return query_plans

    default_cost = query_plans.default_plan.total_cost
    seen_costs: set[float] = {default_cost}
    alternatives: list[PlanResult] = []

    # Strategy 1: pg_hint_plan join method hints
    if use_pg_hint_plan:
        join_pairs = _extract_join_pairs(sql)
        join_methods = ["HashJoin", "MergeJoin", "NestLoop"]
        for alias1, alias2 in join_pairs[:3]:
            for method in join_methods:
                if len(alternatives) >= max_alternatives:
                    break
                hint = f"/*+ {method}({alias1} {alias2}) */"
                result = explain_with_hint(conn, sql, hint, plan_type="hint_join")
                if result and result.total_cost not in seen_costs:
                    seen_costs.add(result.total_cost)
                    alternatives.append(result)

        # Strategy 2: pg_hint_plan scan type hints
        table_aliases = _extract_table_aliases(sql)
        scan_methods = ["SeqScan", "IndexScan"]
        for table_name, alias in table_aliases[:4]:
            for method in scan_methods:
                if len(alternatives) >= max_alternatives:
                    break
                hint = f"/*+ {method}({alias}) */"
                result = explain_with_hint(conn, sql, hint, plan_type="hint_scan")
                if result and result.total_cost not in seen_costs:
                    seen_costs.add(result.total_cost)
                    alternatives.append(result)

    # Strategy 3: GUC toggles for aggregation and join methods
    guc_configs = [
        ({"enable_hashagg": "off"}, "hint_agg"),
        ({"enable_mergejoin": "off"}, "hint_join"),
        ({"enable_hashjoin": "off"}, "hint_join"),
        ({"enable_nestloop": "off"}, "hint_join"),
        ({"enable_sort": "off"}, "hint_agg"),
        ({"enable_seqscan": "off"}, "hint_scan"),
    ]
    for guc_settings, plan_type in guc_configs:
        if len(alternatives) >= max_alternatives:
            break
        result = explain_with_guc(conn, sql, guc_settings, plan_type=plan_type)
        if result and result.total_cost not in seen_costs:
            seen_costs.add(result.total_cost)
            alternatives.append(result)

    query_plans.alternative_plans = alternatives[:max_alternatives]
    return query_plans


# ---------------------------------------------------------------------------
# Batch collection
# ---------------------------------------------------------------------------

def collect_plans_for_manifest(
    pg_config: PgConnectionConfig,
    manifest: dict[str, Any],
    artifacts: list[Any],
    *,
    use_pg_hint_plan: bool = True,
    max_alternatives: int = 5,
) -> list[dict[str, Any]]:
    """Collect EXPLAIN plans for all queries in a manifest.

    Returns a list of plan dicts suitable for adding to the manifest.
    """
    conn = _get_connection(pg_config)
    plans: list[dict[str, Any]] = []
    try:
        for artifact in artifacts:
            sql = artifact.sql if hasattr(artifact, "sql") else str(artifact)
            query_id = artifact.metadata.get("query_id", "") if hasattr(artifact, "metadata") else ""
            try:
                query_plans = explain_with_alternatives(
                    conn, sql,
                    use_pg_hint_plan=use_pg_hint_plan,
                    max_alternatives=max_alternatives,
                )
                query_plans.query_id = query_id
                plans.append(query_plans.to_dict())
            except Exception:
                # Skip queries that fail EXPLAIN
                plans.append({"query_id": query_id, "sql": sql, "error": "explain_failed"})
    finally:
        conn.close()
    return plans
