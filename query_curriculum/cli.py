from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from query_curriculum.core import (
    GeneratorConfig,
    PgConnectionConfig,
    QueryArtifact,
    close_stats_snapshot,
    default_stage_budgets,
    load_config_file,
    load_schema_catalog,
    load_stats_snapshot,
    repo_root,
    resolve_benchmark_dir,
    resolve_output_dir,
    resolve_schema_path,
    write_output,
    build_join_edges,
)
from query_curriculum.spj import generate_spj_workload
from query_curriculum.agg import generate_agg_workload
from query_curriculum.subquery import generate_subquery_workload
from query_curriculum.setop import generate_setop_workload
from query_curriculum.window import generate_window_workload
from query_curriculum.cte import generate_cte_workload


REPO_ROOT = repo_root()


def parse_stage_budgets(values: list[str] | None, current: dict[str, int]) -> dict[str, int]:
    budgets = dict(current)
    for value in values or []:
        stage, raw_budget = value.split("=", 1)
        normalized = stage.strip().replace("-", "_")
        budgets[normalized] = int(raw_budget)
    return budgets


def build_config(args: argparse.Namespace) -> GeneratorConfig:
    config_data: dict[str, Any] = {}
    if args.config:
        config_data = load_config_file(args.config)

    benchmark = args.benchmark or config_data.get("benchmark")
    suffix = args.suffix or config_data.get("suffix")
    if not benchmark or not suffix:
        raise ValueError("--benchmark and --suffix are required")

    explicit_stage_values = args.stage_budget or []
    if explicit_stage_values:
        base = default_stage_budgets(args.max_join_tables if args.max_join_tables is not None else 4)
        stage_budgets = parse_stage_budgets(explicit_stage_values, base)
    else:
        stage_budgets = default_stage_budgets(config_data.get("max_join_tables", args.max_join_tables or 4))
        stage_budgets.update(config_data.get("stage_budgets", {}))

    pg_config_data = config_data.get("pg", {})
    pg_enabled = bool(args.pg_enabled or pg_config_data.get("enabled"))
    pg = PgConnectionConfig(
        enabled=pg_enabled,
        host=args.pg_host or pg_config_data.get("host", "127.0.0.1"),
        port=args.pg_port or pg_config_data.get("port", 5432),
        user=args.pg_user or pg_config_data.get("user", "postgres"),
        password=args.pg_password or pg_config_data.get("password", ""),
        database=args.pg_database or pg_config_data.get("database", "postgres"),
    )

    benchmarks_root = Path(args.benchmarks_root or config_data.get("benchmarks_root") or REPO_ROOT / "benchmarks")
    schema_path = Path(args.schema_path) if args.schema_path else (Path(config_data["schema_path"]) if config_data.get("schema_path") else None)

    return GeneratorConfig(
        benchmark=benchmark,
        suffix=suffix,
        branch=args.branch or config_data.get("branch", "spj"),
        schema_path=schema_path,
        benchmarks_root=benchmarks_root,
        replace_output=bool(args.replace_output or config_data.get("replace_output", False)),
        seed=args.seed if args.seed is not None else int(config_data.get("seed", 0)),
        max_join_tables=args.max_join_tables if args.max_join_tables is not None else int(config_data.get("max_join_tables", 4)),
        join_types=tuple(args.join_types or config_data.get("join_types", ["inner", "left"])),
        stage_budgets=stage_budgets,
        max_predicates_per_table=args.max_predicates_per_table if args.max_predicates_per_table is not None else int(config_data.get("max_predicates_per_table", 3)),
        probe_workers=args.probe_workers if args.probe_workers is not None else int(config_data.get("probe_workers", 4)),
        stats_mode=args.stats_mode or config_data.get("stats_mode", "auto"),
        llm_seed_model=args.llm_seed_model or config_data.get("llm_seed_model", "gpt-4o-mini"),
        pg=pg if pg.enabled else None,
        template_packs=tuple(args.template_pack or config_data.get("template_packs", [])),
    )


BRANCH_GENERATORS = {
    "spj": generate_spj_workload,
    "agg": generate_agg_workload,
    "subquery": generate_subquery_workload,
    "setop": generate_setop_workload,
    "window": generate_window_workload,
    "cte": generate_cte_workload,
}

FULL_BRANCH_ORDER = ["spj", "agg", "subquery", "setop", "window", "cte"]


def _merge_manifests(
    manifests: list[tuple[str, dict[str, Any], list[QueryArtifact]]],
    benchmark: str,
    schema_path: str,
    suffix: str,
    seed: int,
) -> tuple[dict[str, Any], list[QueryArtifact]]:
    """Merge manifests and artifacts from multiple branches into one."""
    all_artifacts: list[QueryArtifact] = []
    all_queries: list[dict[str, Any]] = []
    branch_diagnostics: dict[str, Any] = {}

    for branch_name, manifest, artifacts in manifests:
        all_artifacts.extend(artifacts)
        all_queries.extend(manifest.get("queries", []))
        if "generation_diagnostics" in manifest:
            branch_diagnostics[branch_name] = manifest["generation_diagnostics"]

    merged_manifest = {
        "branch": "full",
        "benchmark": benchmark,
        "schema_path": schema_path,
        "suffix": suffix,
        "seed": seed,
        "query_count": len(all_artifacts),
        "branches": [name for name, _, _ in manifests],
        "queries": all_queries,
    }
    if branch_diagnostics:
        merged_manifest["generation_diagnostics"] = branch_diagnostics
    return merged_manifest, all_artifacts


def run_generate(args: argparse.Namespace) -> dict[str, Any]:
    config = build_config(args)
    benchmark_dir = resolve_benchmark_dir(config.benchmarks_root, config.benchmark)
    schema_path = resolve_schema_path(benchmark_dir, config.schema_path)
    output_dir = resolve_output_dir(benchmark_dir, config.suffix)
    catalog = load_schema_catalog(schema_path)
    join_edges = build_join_edges(catalog)
    snapshot = load_stats_snapshot(config, catalog)
    try:
        if config.branch == "full":
            # Run all generators and merge
            results: list[tuple[str, dict[str, Any], list[QueryArtifact]]] = []
            for branch_name in FULL_BRANCH_ORDER:
                generator = BRANCH_GENERATORS[branch_name]
                manifest, artifacts = generator(catalog, join_edges, config, str(schema_path), snapshot)
                results.append((branch_name, manifest, artifacts))
            manifest, artifacts = _merge_manifests(
                results, config.benchmark, str(schema_path), config.suffix, config.seed,
            )
        elif config.branch in BRANCH_GENERATORS:
            generator = BRANCH_GENERATORS[config.branch]
            manifest, artifacts = generator(catalog, join_edges, config, str(schema_path), snapshot)
        else:
            raise ValueError(f"Unsupported branch: {config.branch}. "
                             f"Choose from: {', '.join(list(BRANCH_GENERATORS) + ['full'])}")

        # Optional plan collection
        if getattr(args, 'collect_plans', False) and config.pg and config.pg.enabled:
            from query_curriculum.plan_collector import collect_plans_for_manifest
            plans = collect_plans_for_manifest(
                config.pg, manifest, artifacts,
                use_pg_hint_plan=getattr(args, 'use_pg_hint_plan', True),
                max_alternatives=getattr(args, 'max_plan_alternatives', 5),
            )
            manifest["plans"] = plans

        write_output(output_dir, manifest, artifacts, replace_output=config.replace_output)
        return {"output_dir": str(output_dir), "query_count": len(artifacts), "schema_path": str(schema_path)}
    finally:
        close_stats_snapshot(snapshot)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate curriculum SQL workloads")
    subparsers = parser.add_subparsers(dest="command")
    generate = subparsers.add_parser("generate", help="Generate a benchmark workload")
    generate.add_argument("--config", type=Path, default=None)
    generate.add_argument("--benchmarks-root", type=Path, default=None)
    generate.add_argument("--benchmark", type=str, default=None)
    generate.add_argument("--branch", type=str, default="spj")
    generate.add_argument("--suffix", type=str, default=None)
    generate.add_argument("--schema-path", type=Path, default=None)
    generate.add_argument("--replace-output", action="store_true")
    generate.add_argument("--seed", type=int, default=None)
    generate.add_argument("--max-join-tables", type=int, default=None)
    generate.add_argument("--join-types", nargs="+", default=None)
    generate.add_argument("--stage-budget", action="append", default=None, help="Override stage budget, e.g. 2_table=12")
    generate.add_argument("--max-predicates-per-table", type=int, default=None)
    generate.add_argument("--probe-workers", type=int, default=None)
    generate.add_argument("--stats-mode", choices=["auto", "schema_only", "schema_plus_llm", "stats_only", "stats_plus_selective_probes", "stats_plus_estimated_probes"], default=None)
    generate.add_argument("--llm-seed-model", type=str, default=None, help="OpenAI model for LLM seed generation (default: gpt-4o-mini)")
    generate.add_argument("--pg-enabled", action="store_true")
    generate.add_argument("--pg-host", type=str, default=None)
    generate.add_argument("--pg-port", type=int, default=None)
    generate.add_argument("--pg-user", type=str, default=None)
    generate.add_argument("--pg-password", type=str, default=None)
    generate.add_argument("--pg-database", type=str, default=None)
    generate.add_argument("--template-pack", action="append", default=None)
    generate.add_argument("--collect-plans", action="store_true", help="Collect EXPLAIN plans for generated queries")
    generate.add_argument("--use-pg-hint-plan", action="store_true", default=True, help="Use pg_hint_plan for alternative plans")
    generate.add_argument("--no-pg-hint-plan", action="store_false", dest="use_pg_hint_plan", help="Disable pg_hint_plan hints")
    generate.add_argument("--max-plan-alternatives", type=int, default=5, help="Max alternative plans per query")
    return parser


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.command = args.command or "generate"
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.command != "generate":
        raise ValueError(f"Unsupported command: {args.command}")
    result = run_generate(args)
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
