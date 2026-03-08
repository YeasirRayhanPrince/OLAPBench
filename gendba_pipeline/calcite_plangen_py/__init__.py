"""
Apache Calcite Plan Generator Python Wrapper

A Python package providing easy access to Calcite plan generation tools:
- plangen: Generates unoptimized logical plans from SQL queries
- plangen-multi: Generates multiple complete physical plans from SQL queries

Example (Single Plan):
    >>> from calcite_plangen_py import PlanGenerator
    >>> gen = PlanGenerator(jar_path="/path/to/plangen")
    >>> gen.generate_plans(
    ...     schema_path="job.dbschema.json",
    ...     queries_dir="queries/",
    ...     output_dir="output/plans"
    ... )
    >>> plan = gen.load_plan("output/plans/query")

Example (Multiple Plans):
    >>> from calcite_plangen_py import MultiPlanGenerator
    >>> gen = MultiPlanGenerator()
    >>> gen.generate_multiple_plans(
    ...     schema_path="job.dbschema.json",
    ...     queries_dir="queries/",
    ...     output_dir="output/plans"
    ... )
    >>> result = gen.load_all_plans("query", "output/plans")
    >>> best = result.get_best_plan()
    >>> print(f"Best cost: {best.cost.rows} rows")
"""

__version__ = "0.1.0"
__author__ = "Apache Calcite Contributors"

from .generator import PlanGenerator, MultiPlanGenerator
from .models import (
    QueryPlan,
    GlobalColumnMapping,
    ColumnInfo,
    GlobalColumnCatalog,
    GenerationStats,
    PlanCost,
    AlternativePlan,
    MultiPlanResult,
)
from .exceptions import (
    PlanGenerationError,
    QueryFileNotFoundError,
    InvalidSchemaError,
    ToolNotFoundError,
)

__all__ = [
    # Generators
    "PlanGenerator",
    "MultiPlanGenerator",
    # Models
    "QueryPlan",
    "GlobalColumnMapping",
    "ColumnInfo",
    "GlobalColumnCatalog",
    "GenerationStats",
    "PlanCost",
    "AlternativePlan",
    "MultiPlanResult",
    # Exceptions
    "PlanGenerationError",
    "QueryFileNotFoundError",
    "InvalidSchemaError",
    "ToolNotFoundError",
]
