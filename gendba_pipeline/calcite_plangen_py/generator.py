"""
Main PlanGenerator class for generating unoptimized logical plans.
"""

import subprocess
import json
import re
from pathlib import Path
from typing import Optional, Dict, List
import logging

from .models import (
    QueryPlan,
    GlobalColumnMapping,
    GlobalColumnCatalog,
    GenerationStats,
    PlanCost,
    AlternativePlan,
    MultiPlanResult,
)
from .exceptions import (
    PlanGenerationError,
    ToolNotFoundError,
    InvalidSchemaError,
)

logger = logging.getLogger(__name__)


class PlanGenerator:
    """
    Wrapper for the Calcite plangen tool.

    Provides a Pythonic interface to generate unoptimized logical plans
    from SQL queries with global column ID mappings.

    Example:
        >>> gen = PlanGenerator(jar_path="path/to/plangen")
        >>> gen.generate_plans(
        ...     schema_path="job.dbschema.json",
        ...     queries_dir="queries/",
        ...     output_dir="output/plans"
        ... )
        >>> plan = gen.load_plan("output/plans/10a")
        >>> print(plan.global_mappings)
    """

    def __init__(self, jar_path: Optional[str] = None, verbose: bool = False):
        """
        Initialize the plan generator.

        Args:
            jar_path: Path to plangen binary or JAR file. If None, looks for
                     benchmarks/plangen/build/install/plangen/bin/plangen
            verbose: Enable verbose output
        """
        self.verbose = verbose
        self.jar_path = self._find_jar(jar_path)
        logger.info(f"Using plangen tool at: {self.jar_path}")

    def _find_jar(self, jar_path: Optional[str]) -> str:
        """Find the plangen JAR/binary."""
        if jar_path:
            jar = Path(jar_path)
            if jar.exists():
                return str(jar)
            raise ToolNotFoundError(f"Plangen tool not found at: {jar_path}")

        # Try default locations
        default_paths = [
            Path("benchmarks/plangen/build/install/plangen/bin/plangen"),
            Path.home() / ".local/bin/plangen",
            Path("/usr/local/bin/plangen"),
        ]

        for path in default_paths:
            if path.exists():
                return str(path)

        raise ToolNotFoundError(
            f"Plangen tool not found. Specify jar_path or install to one of: {default_paths}"
        )

    def generate_plans(
        self,
        schema_path: str,
        queries_dir: str,
        output_dir: str,
    ) -> GenerationStats:
        """
        Generate unoptimized logical plans for all queries.

        Args:
            schema_path: Path to schema JSON file
            queries_dir: Directory containing .sql query files
            output_dir: Output directory for generated plans

        Returns:
            GenerationStats: Summary of generation results

        Raises:
            PlanGenerationError: If generation fails
        """
        schema_path = Path(schema_path)
        queries_dir = Path(queries_dir)
        output_dir = Path(output_dir)

        # Validate inputs
        if not schema_path.exists():
            raise InvalidSchemaError(f"Schema file not found: {schema_path}")

        if not queries_dir.exists():
            raise PlanGenerationError(f"Queries directory not found: {queries_dir}")

        output_dir.mkdir(parents=True, exist_ok=True)

        # Run plangen
        cmd = [
            self.jar_path,
            str(schema_path),
            str(queries_dir),
            str(output_dir),
        ]

        logger.info(f"Running: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=3600,  # 1 hour timeout
            )
        except subprocess.TimeoutExpired:
            raise PlanGenerationError("Plan generation timed out after 1 hour")
        except Exception as e:
            raise PlanGenerationError(f"Failed to run plangen: {e}")

        if result.returncode != 0:
            raise PlanGenerationError(
                f"Plangen exited with code {result.returncode}",
                stderr=result.stderr,
            )

        # Parse output
        stats = self._parse_stats(result.stdout, result.stderr)

        logger.info(f"Generation complete: {stats}")
        return stats

    def load_plan(self, plan_dir: str) -> QueryPlan:
        """
        Load a complete plan with all associated files.

        Args:
            plan_dir: Directory containing plan files, or path without extension
                     e.g., "output/plans/10a" will load:
                     - output/plans/10a.plan.txt
                     - output/plans/10a.plan.json
                     - output/plans/10a.global-mapping.json
                     - output/plans/10a.annotated.txt

        Returns:
            QueryPlan: Complete plan with text, JSON, and mappings

        Raises:
            FileNotFoundError: If required plan files not found
        """
        plan_path = Path(plan_dir)
        if plan_path.is_dir():
            raise ValueError(f"plan_dir should be a path without extension, not a directory")

        query_name = plan_path.name

        # Load text plan
        text_file = Path(str(plan_path) + ".plan.txt")
        if not text_file.exists():
            raise FileNotFoundError(f"Plan text file not found: {text_file}")
        text_plan = text_file.read_text()

        # Load JSON plan
        json_file = Path(str(plan_path) + ".plan.json")
        if not json_file.exists():
            raise FileNotFoundError(f"Plan JSON file not found: {json_file}")
        json_plan = json.loads(json_file.read_text())

        # Load global mappings
        mapping_file = Path(str(plan_path) + ".global-mapping.json")
        if not mapping_file.exists():
            raise FileNotFoundError(f"Mapping file not found: {mapping_file}")

        mapping_data = json.loads(mapping_file.read_text())
        global_mappings = [
            GlobalColumnMapping.from_dict(field)
            for field in mapping_data.get("fields", [])
        ]

        # Load annotated plan
        annotated_file = Path(str(plan_path) + ".annotated.txt")
        annotated_plan = (
            annotated_file.read_text() if annotated_file.exists() else ""
        )

        return QueryPlan(
            query_name=query_name,
            text_plan=text_plan,
            json_plan=json_plan,
            global_mappings=global_mappings,
            annotated_plan=annotated_plan,
        )

    def load_plan_json(self, plan_json_path: str) -> Dict:
        """
        Load and parse a plan JSON file.

        Args:
            plan_json_path: Path to .plan.json file

        Returns:
            dict: Parsed JSON plan
        """
        path = Path(plan_json_path)
        if not path.exists():
            raise FileNotFoundError(f"Plan JSON file not found: {path}")
        return json.loads(path.read_text())

    def load_plan_text(self, plan_txt_path: str) -> str:
        """
        Load a plan text file.

        Args:
            plan_txt_path: Path to .plan.txt file

        Returns:
            str: Plan text
        """
        path = Path(plan_txt_path)
        if not path.exists():
            raise FileNotFoundError(f"Plan text file not found: {path}")
        return path.read_text()

    def load_global_mapping(self, mapping_json_path: str) -> List[GlobalColumnMapping]:
        """
        Load the global column mapping file.

        Args:
            mapping_json_path: Path to .global-mapping.json file

        Returns:
            List[GlobalColumnMapping]: Field mappings
        """
        path = Path(mapping_json_path)
        if not path.exists():
            raise FileNotFoundError(f"Mapping file not found: {path}")

        data = json.loads(path.read_text())
        return [
            GlobalColumnMapping.from_dict(field) for field in data.get("fields", [])
        ]

    def load_catalog(self, catalog_json_path: str) -> GlobalColumnCatalog:
        """
        Load the global column catalog.

        Args:
            catalog_json_path: Path to global-column-catalog.json file

        Returns:
            GlobalColumnCatalog: Complete schema catalog
        """
        path = Path(catalog_json_path)
        if not path.exists():
            raise FileNotFoundError(f"Catalog file not found: {path}")

        data = json.loads(path.read_text())
        return GlobalColumnCatalog.from_dict(data)

    @staticmethod
    def _parse_stats(stdout: str, stderr: str = "") -> GenerationStats:
        """Parse generation statistics from tool output."""
        total = 0
        successful = 0
        failed = 0
        failed_names = []

        # Parse summary section
        for line in stdout.split("\n"):
            if "Total queries:" in line:
                total = int(line.split(":")[-1].strip())
            elif "Successful:" in line:
                successful = int(line.split(":")[-1].strip())
            elif "Failed:" in line:
                failed = int(line.split(":")[-1].strip())
            elif line.strip().endswith(".sql:"):
                # Extract failed query name
                match = re.search(r"- (\S+\.sql):", line)
                if match:
                    failed_names.append(match.group(1))

        return GenerationStats(
            total_queries=total,
            successful_queries=successful,
            failed_queries=failed,
            failed_query_names=failed_names,
        )


class MultiPlanGenerator:
    """
    Wrapper for the Calcite plangen-multi tool.

    Generates multiple complete physical query execution plans from SQL queries.
    Unlike PlanGenerator (single unoptimized logical plan), this extracts ALL
    physical plan alternatives explored by the Volcano optimizer.

    Example:
        >>> gen = MultiPlanGenerator()
        >>> gen.generate_multiple_plans(
        ...     'schema.json',
        ...     'queries/',
        ...     'output/'
        ... )
        >>> result = gen.load_all_plans('query_1', 'output/')
        >>> print(f"Generated {result.total_plans} alternative plans")
        >>> best = result.get_best_plan()
        >>> print(f"Best plan cost: {best.cost.rows} rows")
    """

    def __init__(self, jar_path: Optional[str] = None, verbose: bool = False):
        """
        Initialize the multi-plan generator.

        Args:
            jar_path: Path to plangen-multi binary. If None, tries to find it
                     relative to calcite root.
            verbose: If True, prints command output
        """
        self.verbose = verbose
        self.jar_path = self._find_jar(jar_path)
        logger.info(f"Using plangen-multi tool at: {self.jar_path}")

    def _find_jar(self, jar_path: Optional[str]) -> str:
        """Find the plangen-multi JAR/binary."""
        if jar_path:
            jar = Path(jar_path)
            if jar.exists():
                return str(jar)
            raise ToolNotFoundError(f"Plangen-multi tool not found at: {jar_path}")

        # Try default locations
        default_paths = [
            Path("/scratch1/yrayhan/calcite/benchmarks/plangen-multi/build/install/plangen-multi/bin/plangen-multi"),
            Path.home() / ".local/bin/plangen-multi",
            Path("/usr/local/bin/plangen-multi"),
        ]

        for path in default_paths:
            if path.exists():
                return str(path)

        raise ToolNotFoundError(
            f"Plangen-multi tool not found. Please build it first:\n"
            f"  cd calcite && ./gradlew :benchmarks:plangen-multi:installDist\n"
            f"Or specify jar_path explicitly."
        )

    def generate_multiple_plans(
        self,
        schema_path: str,
        queries_dir: str,
        output_dir: str,
        timeout: int = 3600,
    ) -> GenerationStats:
        """
        Generate multiple physical plans for all SQL queries.

        Args:
            schema_path: Path to schema JSON file
            queries_dir: Directory containing .sql files
            output_dir: Directory for output files
            timeout: Maximum execution time in seconds (default: 3600 = 1 hour)

        Returns:
            GenerationStats with counts of successful/failed queries

        Raises:
            ToolNotFoundError: If plangen-multi binary not found
            InvalidSchemaError: If schema file is invalid
            PlanGenerationError: If generation fails
        """
        schema_path = Path(schema_path)
        queries_dir = Path(queries_dir)
        output_dir = Path(output_dir)

        # Validate inputs
        if not schema_path.exists():
            raise InvalidSchemaError(f"Schema file not found: {schema_path}")
        if not queries_dir.exists():
            raise PlanGenerationError(f"Queries directory not found: {queries_dir}")

        # Create output directory
        output_dir.mkdir(parents=True, exist_ok=True)

        # Build command
        cmd = [
            self.jar_path,
            str(schema_path),
            str(queries_dir),
            str(output_dir),
        ]

        if self.verbose:
            print(f"Running: {' '.join(cmd)}")

        logger.info(f"Running: {' '.join(cmd)}")

        # Execute
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout
            )

            if self.verbose:
                print(result.stdout)
                if result.stderr:
                    print(f"Errors: {result.stderr}")

            # Parse statistics from output
            stats = self._parse_stats(result.stdout, result.stderr)

            if result.returncode != 0:
                raise PlanGenerationError(
                    f"Multi-plan generation failed with exit code {result.returncode}",
                    stderr=result.stderr
                )

            logger.info(f"Generation complete: {stats}")
            return stats

        except subprocess.TimeoutExpired:
            raise PlanGenerationError(
                f"Multi-plan generation timed out after {timeout} seconds"
            )
        except Exception as e:
            if isinstance(e, (PlanGenerationError, ToolNotFoundError, InvalidSchemaError)):
                raise
            raise PlanGenerationError(f"Multi-plan generation failed: {str(e)}")

    def load_all_plans(
        self,
        query_name: str,
        output_dir: str,
    ) -> MultiPlanResult:
        """
        Load all alternative plans for a specific query.

        Args:
            query_name: Query identifier (without .sql extension)
            output_dir: Directory containing output files

        Returns:
            MultiPlanResult with all alternative plans

        Raises:
            FileNotFoundError: If plans file doesn't exist
        """
        output_dir = Path(output_dir)
        plans_json_path = output_dir / f"{query_name}.plans.json"

        if not plans_json_path.exists():
            raise FileNotFoundError(
                f"Plans file not found: {plans_json_path}"
            )

        with open(plans_json_path) as f:
            data = json.load(f)

        return MultiPlanResult.from_dict(data)

    def load_summary(
        self,
        query_name: str,
        output_dir: str,
    ) -> str:
        """
        Load human-readable summary of all plans.

        Args:
            query_name: Query identifier (without .sql extension)
            output_dir: Directory containing output files

        Returns:
            Summary text

        Raises:
            FileNotFoundError: If summary file doesn't exist
        """
        output_dir = Path(output_dir)
        summary_path = output_dir / f"{query_name}.plans-summary.txt"

        if not summary_path.exists():
            raise FileNotFoundError(
                f"Summary file not found: {summary_path}"
            )

        with open(summary_path) as f:
            return f.read()

    @staticmethod
    def _parse_stats(stdout: str, stderr: str = "") -> GenerationStats:
        """Parse generation statistics from tool output."""
        total = 0
        successful = 0
        failed_names = []

        # Parse summary section
        lines = stdout.split('\n')
        for i, line in enumerate(lines):
            if 'Total queries:' in line:
                total = int(line.split(':')[1].strip())
            elif 'Successful:' in line:
                successful = int(line.split(':')[1].strip())
            elif line.startswith('✗'):
                # Failed query - extract name
                parts = line.split()
                if len(parts) >= 2:
                    query_name = parts[1]
                    failed_names.append(query_name)

        failed = total - successful

        return GenerationStats(
            total_queries=total,
            successful_queries=successful,
            failed_queries=failed,
            failed_query_names=failed_names,
        )
