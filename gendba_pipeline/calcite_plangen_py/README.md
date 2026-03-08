# Apache Calcite Unoptimized Plan Generator - Python Wrapper

A Python package providing a simple, Pythonic interface to Apache Calcite's unoptimized logical plan generator (`plangen`).

## Features

- **Easy-to-use API** - Generate logical plans with a single function call
- **Global Column IDs** - Plans include consistent global IDs for cross-query analysis
- **Comprehensive Metadata** - Access field mappings, column catalogs, and plan annotations
- **Error Handling** - Clear exceptions with helpful error messages
- **Type Hints** - Full type annotations for IDE support
- **Well-Documented** - Extensive docstrings and examples

## Installation

### Option 1: Install from source (within calcite repository)

```bash
cd calcite/calcite_plangen_py
pip install -e .
```

### Option 2: Install as standalone package

```bash
pip install calcite-plangen
```

## Quick Start

```python
from calcite_plangen import PlanGenerator

# Initialize generator
gen = PlanGenerator(jar_path="path/to/plangen")

# Generate plans for all queries
stats = gen.generate_plans(
    schema_path="benchmarks/job/job.dbschema.json",
    queries_dir="benchmarks/job/queries/benchmark",
    output_dir="output/plans"
)

print(f"Generated {stats.successful_queries}/{stats.total_queries} plans")

# Load a specific plan
plan = gen.load_plan("output/plans/10a")

# Access the plan data
print(f"Plan: {plan.query_name}")
print(f"Output fields: {len(plan.global_mappings)}")

for mapping in plan.global_mappings:
    print(mapping)  # Prints: $0 field_name: G1 (table.column)
```

## API Reference

### PlanGenerator

Main class for generating and loading plans.

#### Constructor

```python
PlanGenerator(jar_path: Optional[str] = None, verbose: bool = False)
```

- `jar_path`: Path to plangen binary or JAR. If None, auto-detects standard locations.
- `verbose`: Enable verbose logging

**Example:**
```python
gen = PlanGenerator()
# or
gen = PlanGenerator(jar_path="/usr/local/bin/plangen")
```

#### Methods

##### generate_plans()

```python
def generate_plans(
    schema_path: str,
    queries_dir: str,
    output_dir: str,
) -> GenerationStats
```

Generates unoptimized logical plans for all SQL queries in a directory.

**Args:**
- `schema_path`: Path to schema JSON file (e.g., job.dbschema.json)
- `queries_dir`: Directory containing .sql query files
- `output_dir`: Output directory for generated plans

**Returns:**
- `GenerationStats`: Object with total, successful, and failed counts

**Raises:**
- `PlanGenerationError`: If generation fails
- `InvalidSchemaError`: If schema file is invalid
- `ToolNotFoundError`: If plangen tool cannot be found

**Example:**
```python
stats = gen.generate_plans(
    schema_path="job.dbschema.json",
    queries_dir="queries/",
    output_dir="output/plans"
)
print(f"Success rate: {stats.success_rate:.1f}%")
if stats.failed_queries > 0:
    print(f"Failed: {stats.failed_query_names}")
```

##### load_plan()

```python
def load_plan(plan_dir: str) -> QueryPlan
```

Loads a complete plan with all associated files.

**Args:**
- `plan_dir`: Path to plan files without extension (e.g., "output/plans/10a")
  - Will load: 10a.plan.txt, 10a.plan.json, 10a.global-mapping.json, 10a.annotated.txt

**Returns:**
- `QueryPlan`: Complete plan object

**Example:**
```python
plan = gen.load_plan("output/plans/10a")
print(f"Query: {plan.query_name}")
print(f"Fields: {len(plan.global_mappings)}")
print(f"Text plan:\n{plan.text_plan}")
```

##### load_plan_json()

```python
def load_plan_json(plan_json_path: str) -> Dict
```

Loads just the JSON plan file.

**Returns:**
- `dict`: Parsed JSON plan

##### load_plan_text()

```python
def load_plan_text(plan_txt_path: str) -> str
```

Loads just the text plan file.

**Returns:**
- `str`: Plan text

##### load_global_mapping()

```python
def load_global_mapping(mapping_json_path: str) -> List[GlobalColumnMapping]
```

Loads the global column mapping file.

**Returns:**
- `List[GlobalColumnMapping]`: Field mappings with global IDs

**Example:**
```python
mappings = gen.load_global_mapping("output/plans/10a.global-mapping.json")
for mapping in mappings:
    print(f"${mapping.local_position} = {mapping.field_name}")
    for col in mapping.global_ids:
        print(f"  -> G{col.global_id}: {col.table}.{col.column}")
```

##### load_catalog()

```python
def load_catalog(catalog_json_path: str) -> GlobalColumnCatalog
```

Loads the global column catalog.

**Returns:**
- `GlobalColumnCatalog`: Complete schema catalog with all columns

**Example:**
```python
catalog = gen.load_catalog("output/plans/global-column-catalog.json")
print(f"Schema: {catalog.schema}")
print(f"Total columns: {catalog.total_columns}")

# Get columns from a specific table
title_cols = catalog.get_columns_by_table("title")
for col in title_cols:
    print(f"  G{col.global_id}: {col.column}")
```

### Data Models

#### QueryPlan

```python
@dataclass
class QueryPlan:
    query_name: str
    text_plan: str
    json_plan: Dict
    global_mappings: List[GlobalColumnMapping]
    annotated_plan: str
```

**Methods:**
- `get_input_fields()` - Returns all output fields
- `get_field_mapping(local_position)` - Get mapping for a specific field
- `get_fields_from_table(table_name)` - Get all fields from a table

#### GlobalColumnMapping

```python
@dataclass
class GlobalColumnMapping:
    local_position: int      # Field position ($0, $1, etc.)
    field_name: str          # Field name from query
    global_ids: List[ColumnInfo]  # Source column(s)
```

#### ColumnInfo

```python
@dataclass
class ColumnInfo:
    global_id: int
    table: str
    column: str
    type: str
```

#### GlobalColumnCatalog

```python
@dataclass
class GlobalColumnCatalog:
    schema: str
    total_columns: int
    columns: List[ColumnInfo]
```

**Methods:**
- `get_column(global_id)` - Get column info by ID
- `get_columns_by_table(table_name)` - Get all columns in a table

#### GenerationStats

```python
@dataclass
class GenerationStats:
    total_queries: int
    successful_queries: int
    failed_queries: int
    failed_query_names: List[str]

    @property
    def success_rate: float  # (0-100%)
```

## Examples

### Example 1: Generate plans and analyze output

```python
from calcite_plangen import PlanGenerator

gen = PlanGenerator()

# Generate plans
stats = gen.generate_plans(
    schema_path="benchmarks/job/job.dbschema.json",
    queries_dir="benchmarks/job/queries/benchmark",
    output_dir="output/plans"
)

print(f"Generated {stats.successful_queries}/{stats.total_queries} plans")
print(f"Success rate: {stats.success_rate:.1f}%")

if stats.failed_queries > 0:
    print(f"Failed queries: {stats.failed_query_names}")
```

### Example 2: Analyze a single plan

```python
from calcite_plangen import PlanGenerator

gen = PlanGenerator()
plan = gen.load_plan("output/plans/10a")

print(f"Query: {plan.query_name}")
print(f"\nOutput fields:")
for mapping in plan.global_mappings:
    print(f"  {mapping}")

print(f"\nPlan structure:")
print(plan.text_plan)
```

### Example 3: Cross-query analysis

```python
from calcite_plangen import PlanGenerator
from pathlib import Path

gen = PlanGenerator()
catalog = gen.load_catalog("output/plans/global-column-catalog.json")

# Find all plans that use title.title
plan_dir = Path("output/plans")
for plan_file in plan_dir.glob("*.global-mapping.json"):
    mappings = gen.load_global_mapping(str(plan_file))
    for mapping in mappings:
        for col in mapping.global_ids:
            if col.table == "title" and col.column == "title":
                print(f"{plan_file.stem}: Uses title.title at G{col.global_id}")
```

### Example 4: Integration with gen-dba

```python
from calcite_plangen import PlanGenerator

class QueryOptimizer:
    def __init__(self, schema_path, plangen_path=None):
        self.gen = PlanGenerator(jar_path=plangen_path)
        self.schema_path = schema_path
        self.catalog = None

    def analyze_queries(self, queries_dir, output_dir):
        """Generate and analyze plans for optimization."""
        # Generate all plans
        stats = self.gen.generate_plans(
            self.schema_path, queries_dir, output_dir
        )

        # Load catalog
        self.catalog = self.gen.load_catalog(
            f"{output_dir}/global-column-catalog.json"
        )

        return {
            "stats": stats,
            "catalog": self.catalog
        }

    def get_plan_features(self, plan_path):
        """Extract features from a plan for ML model."""
        plan = self.gen.load_plan(plan_path)

        return {
            "num_joins": plan.json_plan["rels"].count(
                next(r for r in plan.json_plan["rels"] if r.get("relOp") == "LogicalJoin")
            ),
            "num_filters": sum(
                1 for r in plan.json_plan["rels"] if r.get("relOp") == "LogicalFilter"
            ),
            "output_fields": len(plan.global_mappings),
            "text": plan.text_plan
        }
```

## Error Handling

```python
from calcite_plangen import (
    PlanGenerator,
    PlanGenerationError,
    InvalidSchemaError,
    ToolNotFoundError,
)

try:
    gen = PlanGenerator()
except ToolNotFoundError as e:
    print(f"Plangen not found: {e}")
    # Install or specify path

try:
    stats = gen.generate_plans("schema.json", "queries/", "output/")
except InvalidSchemaError as e:
    print(f"Invalid schema: {e}")
except PlanGenerationError as e:
    print(f"Generation failed: {e}")
    if e.query_name:
        print(f"Failed query: {e.query_name}")
    if e.stderr:
        print(f"Error details: {e.stderr}")
```

## Contributing

Contributions are welcome! Please submit issues and pull requests to the [Apache Calcite repository](https://github.com/apache/calcite).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](../LICENSE) for details.
