"""
Data models for representing plans and metadata.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional, Set
import json


@dataclass
class ColumnInfo:
    """Information about a column in the schema."""

    global_id: int
    table: str
    column: str
    type: str

    def __str__(self):
        return f"{self.table}.{self.column} (G{self.global_id}: {self.type})"

    @classmethod
    def from_dict(cls, data: dict) -> "ColumnInfo":
        """Create from dictionary."""
        return cls(
            global_id=data["global_id"],
            table=data["table"],
            column=data["column"],
            type=data.get("type", "unknown"),
        )


@dataclass
class GlobalColumnMapping:
    """Mapping of a field to its source column(s)."""

    local_position: int
    field_name: str
    global_ids: List[ColumnInfo]

    def __str__(self):
        if self.global_ids:
            ids_str = ", ".join(f"G{c.global_id}" for c in self.global_ids)
            cols_str = ", ".join(f"{c.table}.{c.column}" for c in self.global_ids)
            return f"${self.local_position} {self.field_name}: {ids_str} ({cols_str})"
        return f"${self.local_position} {self.field_name}: (derived/computed)"

    @classmethod
    def from_dict(cls, data: dict) -> "GlobalColumnMapping":
        """Create from dictionary."""
        global_ids = [ColumnInfo.from_dict(g) for g in data.get("global_ids", [])]
        return cls(
            local_position=data["local_position"],
            field_name=data["field_name"],
            global_ids=global_ids,
        )


@dataclass
class GlobalColumnCatalog:
    """Complete catalog of all columns in the schema with global IDs."""

    schema: str
    total_columns: int
    columns: List[ColumnInfo]

    def __str__(self):
        return f"{self.schema}: {self.total_columns} columns"

    def get_column(self, global_id: int) -> Optional[ColumnInfo]:
        """Get column info by global ID."""
        for col in self.columns:
            if col.global_id == global_id:
                return col
        return None

    def get_columns_by_table(self, table_name: str) -> List[ColumnInfo]:
        """Get all columns in a table."""
        return [c for c in self.columns if c.table == table_name]

    @classmethod
    def from_dict(cls, data: dict) -> "GlobalColumnCatalog":
        """Create from dictionary."""
        columns = [ColumnInfo.from_dict(c) for c in data["columns"]]
        return cls(
            schema=data["schema"],
            total_columns=data["total_columns"],
            columns=columns,
        )


@dataclass
class QueryPlan:
    """Complete logical plan for a query."""

    query_name: str
    text_plan: str
    json_plan: Dict
    global_mappings: List[GlobalColumnMapping]
    annotated_plan: str

    def __str__(self):
        return f"Plan for {self.query_name}: {len(self.global_mappings)} output fields"

    def get_input_fields(self) -> List[GlobalColumnMapping]:
        """Get all output fields with their global ID mappings."""
        return self.global_mappings

    def get_field_mapping(self, local_position: int) -> Optional[GlobalColumnMapping]:
        """Get mapping for a specific output field position."""
        for mapping in self.global_mappings:
            if mapping.local_position == local_position:
                return mapping
        return None

    def get_fields_from_table(self, table_name: str) -> List[GlobalColumnMapping]:
        """Get all output fields sourced from a specific table."""
        return [
            m
            for m in self.global_mappings
            if any(c.table == table_name for c in m.global_ids)
        ]


@dataclass
class PlanCost:
    """Cost information for a physical plan."""

    rows: float
    cpu: float
    io: float

    def __str__(self):
        return f"rows={self.rows:.2f}, cpu={self.cpu:.2f}, io={self.io:.2f}"

    @classmethod
    def from_dict(cls, data: Dict) -> 'PlanCost':
        """Create from dictionary."""
        return cls(
            rows=float(data.get('rows', 0)),
            cpu=float(data.get('cpu', 0)),
            io=float(data.get('io', 0))
        )


@dataclass
class AlternativePlan:
    """Represents one alternative physical plan."""

    plan_id: int
    is_best: bool
    cost: PlanCost
    text_plan: str
    json_plan: Dict

    def __str__(self):
        best_marker = " [BEST]" if self.is_best else ""
        return f"Plan {self.plan_id}{best_marker}: {self.cost}"

    @classmethod
    def from_dict(cls, data: Dict) -> 'AlternativePlan':
        """Create from dictionary."""
        return cls(
            plan_id=data['plan_id'],
            is_best=data.get('is_best', False),
            cost=PlanCost.from_dict(data['cost']),
            text_plan=data['text'],
            json_plan=data['json']
        )


@dataclass
class MultiPlanResult:
    """Result of multi-plan generation for a single query."""

    query_name: str
    total_plans: int
    alternatives: List[AlternativePlan]

    def __str__(self):
        best_count = sum(1 for p in self.alternatives if p.is_best)
        return f"{self.query_name}: {self.total_plans} plans ({best_count} marked as best)"

    def get_best_plan(self) -> Optional[AlternativePlan]:
        """Returns the plan marked as best by the optimizer."""
        for plan in self.alternatives:
            if plan.is_best:
                return plan
        return None

    def get_all_best_plans(self) -> List[AlternativePlan]:
        """Returns all plans marked as best (may be multiple with same cost)."""
        return [p for p in self.alternatives if p.is_best]

    def get_plans_by_cost_range(self,
                                 min_rows: float = 0,
                                 max_rows: float = float('inf')) -> List[AlternativePlan]:
        """Returns plans within a cost range."""
        return [p for p in self.alternatives
                if min_rows <= p.cost.rows <= max_rows]

    def sort_by_cost(self, key: str = 'rows') -> List[AlternativePlan]:
        """Returns plans sorted by cost metric (rows, cpu, or io)."""
        if key not in ('rows', 'cpu', 'io'):
            raise ValueError(f"Invalid cost key: {key}. Must be 'rows', 'cpu', or 'io'")
        return sorted(self.alternatives,
                     key=lambda p: getattr(p.cost, key))

    @classmethod
    def from_dict(cls, data: Dict) -> 'MultiPlanResult':
        """Create from dictionary."""
        return cls(
            query_name=data['query'],
            total_plans=data['total_plans'],
            alternatives=[AlternativePlan.from_dict(p) for p in data['plans']]
        )


@dataclass
class GenerationStats:
    """Statistics from a plan generation run."""

    total_queries: int
    successful_queries: int
    failed_queries: int
    failed_query_names: List[str]

    @property
    def success_rate(self) -> float:
        """Success rate as percentage."""
        if self.total_queries == 0:
            return 0.0
        return (self.successful_queries / self.total_queries) * 100

    def __str__(self):
        return (
            f"Generated {self.successful_queries}/{self.total_queries} plans "
            f"({self.success_rate:.1f}% success)"
        )
