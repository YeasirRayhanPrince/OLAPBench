"""PostgreSQL physical plan normalizer.

Converts PostgreSQL EXPLAIN ANALYZE plans to canonical Gen-DBA action vocabulary
in IR-PTR-Scheduled format: every IR PTR appears exactly once in topological
(leaves-first) order, annotated with a physical operator.

No [INPUT] or [TABm] tokens are emitted — tree structure and table identity are
implicit in the IR prefix already seen by the model.

Usage::

    from openmu.data.pg_plan_normalizer import PGPlanNormalizer
    from openmu.data.schema_registry import SchemaRegistry
    from openmu.data.ir_tokenizer import build_local_mapping

    registry = SchemaRegistry.from_job_schema()
    normalizer = PGPlanNormalizer(registry)

    raw_plans = normalizer.load_plans('training_dataset/phy_qep/pgsql.job.jsonl')

    record = raw_plans['1a']
    ir_str  = ...  # logical IR string for this query
    mapping = build_local_mapping(ir_str)
    phys_data = normalizer.normalize(record, mapping, ir_str)
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path

from openmu.data.ir_tokenizer import LocalMapping
from openmu.data.schema_registry import SchemaRegistry
from openmu.data.synthetic import IRNode, _LOGICAL_TO_PHYS_OTHER, parse_ir_nodes


@dataclass
class PhysicalPlanData:
    """Normalized PostgreSQL physical plan with cardinalities."""

    query_key: str
    plan_tokens: list[str]           # canonical Gen-DBA tokens (IR-PTR-Scheduled)
    per_node_costs: dict[int, int]   # ir_ptr → actual_rows_total (int)
    total_cost: int                  # sum(per_node_costs.values())
    rtg_value: float                 # log(total_cost)


# ---------------------------------------------------------------------------
# Normalizer
# ---------------------------------------------------------------------------

class PGPlanNormalizer:
    """Normalizes PostgreSQL EXPLAIN ANALYZE plans to canonical Gen-DBA format.

    Produces the same token vocabulary as ``generate_physical_plan()`` in
    ``synthetic.py`` (IR-PTR-Scheduled format), so real and synthetic plans
    are interchangeable as training targets.

    Each PG node is matched to its corresponding IR PTR:
      - TableScan: matched by table name → IR LogicalTableScan PTR.
      - Join: matched by (left_child_ir_ptr, right_child_ir_ptr) pair.
      - Unary (GroupBy, Sort, Filter, Limit): matched by child IR PTR + op type.

    Fused PG nodes (e.g. HashAggregate fusing Filter+Project+LogicalAggregate)
    are handled by auto-inserting intermediate IR PTRs with default physical ops
    immediately before the fusing parent node.
    """

    _SCAN_TYPE_MAP: list[tuple[str, str]] = [
        ("Index Only Scan", "IndexOnlyScan"),
        ("Bitmap Index Scan", "BitmapIndexScan"),
        ("Bitmap Heap Scan", "BitmapHeapScan"),
        ("Index Scan", "IndexScan"),
        ("Seq Scan", "SeqScan"),
    ]

    _JOIN_METHOD_MAP = {
        "hash": "HashJoin",
        "merge": "MergeJoin",
        "nested loop": "NestedLoop",
        "nl": "NestedLoop",   # job.jsonl uses "nl" shorthand
    }

    _AGG_METHOD_MAP = {
        "Hashed": "HashAggregate",
        "Plain": "GroupAggregate",
        "Sorted": "GroupAggregate",
    }

    _JOIN_TYPE_MAP = {
        "Inner": "[INNER]",
        "Left": "[LEFT]",
        "Right": "[RIGHT]",
        "Anti": "[ANTI]",
        "Semi": "[SEMI]",
        "Full": "[FULL]",
    }

    # Maps PG node label → set of compatible IR logical op names
    _PG_LABEL_TO_IR_OPS: dict[str, set[str]] = {
        "GroupBy": {"LogicalAggregate"},
        "Sort":    {"LogicalSort"},
        "Limit":   {"LogicalLimit"},
        "Filter":  {"LogicalFilter"},
    }

    def __init__(self, schema_registry: SchemaRegistry):
        self.schema_registry = schema_registry
        self._table_name_to_id: dict[str, int] = {}
        for tid in schema_registry.table_ids:
            tbl = schema_registry.get_table(tid)
            if tbl:
                self._table_name_to_id[tbl.name] = tid

    def load_plans(self, jsonl_path: str | Path) -> dict[str, dict]:
        """Load plans from JSONL. Returns query_key (without .sql) → raw record."""
        plans: dict[str, dict] = {}
        with open(jsonl_path) as f:
            for line in f:
                record = json.loads(line)
                query_key = record["query_key"].replace(".sql", "")
                plans[query_key] = record
        return plans

    def normalize(
        self,
        record: dict,
        local_mapping: LocalMapping,
        ir_str: str = "",
    ) -> PhysicalPlanData | None:
        """Normalize a single PostgreSQL plan to canonical IR-PTR-Scheduled format.

        Args:
            record:        Raw EXPLAIN ANALYZE record (with ``plan`` and ``operators``).
            local_mapping: Per-query global→local slot mapping (kept for API compat).
            ir_str:        Logical IR string for this query (required for PTR matching).

        Returns:
            ``PhysicalPlanData`` or ``None`` on failure.
        """
        try:
            query_key = record["query_key"].replace(".sql", "")
            tree = record["plan"]["queryPlan"]

            # Parse IR to build matching maps
            ir_nodes = parse_ir_nodes(ir_str) if ir_str else {}

            # Build cardinality map: operator_id → actual_rows_total
            op_card: dict[int | None, float] = {}
            for op in record.get("operators", []):
                op_id = op.get("operator_id")
                if op_id not in op_card:
                    op_card[op_id] = float(op.get("actual_rows_total", 0.0))

            # Build IR matching maps from parsed IR nodes
            scan_map: dict[int, int] = {}       # table_id → ir_ptr
            join_map: dict[tuple[int, int], int] = {}   # (child1, child2) → ir_ptr
            unary_map: dict[tuple[int, str], list[int]] = {}  # (child_ptr, pg_label) → [ir_ptrs]

            for ptr, node in ir_nodes.items():
                if node.op == "LogicalTableScan" and node.tab is not None:
                    scan_map[node.tab] = ptr
                elif node.op == "LogicalJoin" and len(node.inputs) == 2:
                    key = (node.inputs[0], node.inputs[1])
                    join_map[key] = ptr
                    join_map[(key[1], key[0])] = ptr  # commutative match
                elif node.op not in ("LogicalTableScan", "LogicalJoin") and node.inputs:
                    child = node.inputs[0]
                    ir_target_ops = self._PG_LABEL_TO_IR_OPS
                    # Build reverse lookup: child_ptr → [ir_ptrs for this child]
                    for pg_label, ir_ops in ir_target_ops.items():
                        if node.op in ir_ops:
                            key2 = (child, pg_label)
                            unary_map.setdefault(key2, []).append(ptr)
                    # Also add generic lookup
                    unary_map.setdefault((child, "_any"), []).append(ptr)

            ptr_costs: dict[int, int] = {}
            used_ir_ptrs: set[int] = set()

            # Traverse PG tree; collect (ir_ptr, phys_op, join_type) entries
            entries, _root = self._traverse(
                tree, op_card, ptr_costs, used_ir_ptrs,
                ir_nodes, scan_map, join_map, unary_map,
            )

            if not entries and not ir_nodes:
                return None

            # Auto-insert any missing IR PTRs (fused into a PG node) with default ops
            missing_ptrs = set(ir_nodes.keys()) - used_ir_ptrs
            if missing_ptrs:
                entries = self._insert_missing_ptrs(
                    entries, missing_ptrs, ir_nodes, ptr_costs,
                )

            if not entries:
                return None

            # Flatten entries to token list
            tokens: list[str] = []
            for ir_ptr, phys_op, join_type in entries:
                tokens.append(f"[PTR_{ir_ptr}]")
                tokens.append(phys_op)
                if join_type:
                    tokens.append(join_type)

            plan_tokens = ["[PHYSICAL_PLAN]"] + tokens + ["[/PHYSICAL_PLAN]"]
            total_cost = sum(ptr_costs.values())

            return PhysicalPlanData(
                query_key=query_key,
                plan_tokens=plan_tokens,
                per_node_costs=ptr_costs,
                total_cost=total_cost,
                rtg_value=math.log(max(1, total_cost)),
            )
        except Exception as e:
            print(f"Error normalizing {record.get('query_key', 'unknown')}: {e}")
            return None

    # ------------------------------------------------------------------
    # Tree traversal
    # ------------------------------------------------------------------

    def _traverse(
        self,
        node: dict,
        op_card: dict[int | None, float],
        ptr_costs: dict[int, int],
        used_ir_ptrs: set[int],
        ir_nodes: dict[int, IRNode],
        scan_map: dict[int, int],
        join_map: dict[tuple[int, int], int],
        unary_map: dict[tuple[int, str], list[int]],
    ) -> tuple[list[tuple[int, str, str | None]], int | None]:
        """Post-order traversal of the PG plan tree.

        Returns:
            (entries, root_ir_ptr) where entries is a list of
            (ir_ptr, phys_op, join_type_or_None) in topological order,
            and root_ir_ptr is the IR PTR of this subtree's root node.
        """
        label = node.get("_label", "")
        attrs = node.get("_attrs", {})

        # Transparent wrappers: pass through, collect child results
        if label in ("Result", "CustomOperator"):
            all_entries: list[tuple[int, str, str | None]] = []
            last_ptr: int | None = None
            for child in node.get("_children", []):
                child_entries, child_ptr = self._traverse(
                    child, op_card, ptr_costs, used_ir_ptrs,
                    ir_nodes, scan_map, join_map, unary_map,
                )
                all_entries.extend(child_entries)
                if child_ptr is not None:
                    last_ptr = child_ptr
            return all_entries, last_ptr

        # Recurse into children first (post-order)
        child_results: list[tuple[list[tuple[int, str, str | None]], int | None]] = []
        for child in node.get("_children", []):
            child_results.append(self._traverse(
                child, op_card, ptr_costs, used_ir_ptrs,
                ir_nodes, scan_map, join_map, unary_map,
            ))

        combined_entries: list[tuple[int, str, str | None]] = []
        child_ir_ptrs: list[int] = []
        for child_entries, child_ptr in child_results:
            combined_entries.extend(child_entries)
            if child_ptr is not None:
                child_ir_ptrs.append(child_ptr)

        # Unknown label → pass through children
        if label not in ("TableScan", "Join", "GroupBy", "Sort", "Limit", "Filter"):
            return combined_entries, child_ir_ptrs[-1] if child_ir_ptrs else None

        # Extract cardinality
        op_id = attrs.get("operator_id")
        rows = op_card.get(op_id, 0.0)
        if rows == 0.0 and op_id is None:
            rows = float(attrs.get("exact_cardinality", 0.0))
        cardinality = max(1, int(round(rows)))

        sys_dict = self._parse_sys_rep(attrs)

        # Match to IR PTR and emit node tokens
        if label == "TableScan":
            table_name = sys_dict.get("Relation Name", "")
            table_id = self._table_name_to_id.get(table_name)
            ir_ptr = scan_map.get(table_id) if table_id is not None else None
            if ir_ptr is None or ir_ptr in used_ir_ptrs:
                return combined_entries, child_ir_ptrs[-1] if child_ir_ptrs else None

            node_type = sys_dict.get("Node Type", "")
            phys_op = "SeqScan"
            for pattern, op_name in self._SCAN_TYPE_MAP:
                if pattern in node_type:
                    phys_op = op_name
                    break

            used_ir_ptrs.add(ir_ptr)
            ptr_costs[ir_ptr] = cardinality
            return combined_entries + [(ir_ptr, phys_op, None)], ir_ptr

        elif label == "Join":
            if len(child_ir_ptrs) < 2:
                return combined_entries, child_ir_ptrs[-1] if child_ir_ptrs else None

            # Find matching IR join PTR by child IR PTRs
            key = (child_ir_ptrs[0], child_ir_ptrs[1])
            ir_ptr = join_map.get(key)
            if ir_ptr is None or ir_ptr in used_ir_ptrs:
                # Try any unused join node
                ir_ptr = self._find_any_unused_join(ir_nodes, used_ir_ptrs)
            if ir_ptr is None:
                return combined_entries, child_ir_ptrs[-1] if child_ir_ptrs else None

            method = attrs.get("method", "").lower()
            node_type_fallback = sys_dict.get("Node Type", "").lower()
            phys_op = "HashJoin"
            for pat, op_name in self._JOIN_METHOD_MAP.items():
                if pat in method or pat in node_type_fallback:
                    phys_op = op_name
                    break

            pg_join_type = sys_dict.get("Join Type", "Inner")
            join_type = self._JOIN_TYPE_MAP.get(pg_join_type, "[INNER]")

            # Auto-insert fused IR PTRs between children and this join node
            fused = self._collect_fused(child_ir_ptrs, ir_ptr, ir_nodes, used_ir_ptrs, ptr_costs)

            used_ir_ptrs.add(ir_ptr)
            ptr_costs[ir_ptr] = cardinality
            return combined_entries + fused + [(ir_ptr, phys_op, join_type)], ir_ptr

        else:  # GroupBy, Sort, Limit, Filter
            child_ir_ptr = child_ir_ptrs[0] if child_ir_ptrs else None
            if child_ir_ptr is None:
                return combined_entries, None

            # Find matching IR unary PTR
            ir_ptr = None
            key2 = (child_ir_ptr, label)
            candidates = unary_map.get(key2, [])
            for cand in candidates:
                if cand not in used_ir_ptrs:
                    ir_ptr = cand
                    break
            if ir_ptr is None:
                # Try generic any-unary match
                for cand in unary_map.get((child_ir_ptr, "_any"), []):
                    if cand not in used_ir_ptrs:
                        ir_ptr = cand
                        break
            if ir_ptr is None:
                return combined_entries, child_ir_ptr

            phys_op = self._get_unary_op(label, attrs)

            # Auto-insert fused IR PTRs between child and this unary node
            fused = self._collect_fused([child_ir_ptr], ir_ptr, ir_nodes, used_ir_ptrs, ptr_costs)

            used_ir_ptrs.add(ir_ptr)
            ptr_costs[ir_ptr] = cardinality
            return combined_entries + fused + [(ir_ptr, phys_op, None)], ir_ptr

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _get_unary_op(self, pg_label: str, attrs: dict) -> str:
        if pg_label == "GroupBy":
            method = attrs.get("method", "Plain")
            return self._AGG_METHOD_MAP.get(method, "GroupAggregate")
        elif pg_label == "Sort":
            return "Sort"
        elif pg_label == "Limit":
            return "Limit"
        elif pg_label == "Filter":
            return "Filter"
        return "Filter"

    @staticmethod
    def _find_any_unused_join(
        ir_nodes: dict[int, IRNode],
        used_ir_ptrs: set[int],
    ) -> int | None:
        for ptr, node in ir_nodes.items():
            if ptr not in used_ir_ptrs and node.op == "LogicalJoin":
                return ptr
        return None

    @staticmethod
    def _collect_fused(
        child_ir_ptrs: list[int],
        parent_ir_ptr: int,
        ir_nodes: dict[int, IRNode],
        used_ir_ptrs: set[int],
        ptr_costs: dict[int, int],
    ) -> list[tuple[int, str, str | None]]:
        """Find IR PTRs that sit between child_ir_ptrs and parent_ir_ptr in the IR DAG.

        These are IR nodes whose inputs are covered but that haven't been placed yet
        (fused by the PG optimizer into the parent node).  Inserts them with a
        default physical operator and marks them as used.
        """
        fused: list[tuple[int, str, str | None]] = []
        parent_node = ir_nodes.get(parent_ir_ptr)
        if parent_node is None:
            return fused

        child_set = set(child_ir_ptrs)
        # BFS/DFS upward from parent inputs toward child_set
        visited: set[int] = set()
        stack = list(parent_node.inputs)
        while stack:
            inp = stack.pop()
            if inp in visited or inp in child_set or inp in used_ir_ptrs:
                continue
            visited.add(inp)
            node = ir_nodes.get(inp)
            if node is None:
                continue
            # This IR node needs to be inserted
            phys = _LOGICAL_TO_PHYS_OTHER.get(node.op)
            if isinstance(phys, list):
                phys_op = phys[0]
            elif phys is not None:
                phys_op = phys
            else:
                phys_op = "Filter"
            used_ir_ptrs.add(inp)
            ptr_costs.setdefault(inp, 1)
            fused.append((inp, phys_op, None))
            # Recurse into this node's inputs
            stack.extend(node.inputs)

        # Reverse so leaves come first (topological order)
        fused.reverse()
        return fused

    @staticmethod
    def _insert_missing_ptrs(
        entries: list[tuple[int, str, str | None]],
        missing_ptrs: set[int],
        ir_nodes: dict[int, IRNode],
        ptr_costs: dict[int, int],
    ) -> list[tuple[int, str, str | None]]:
        """Insert missing IR PTRs into the entries list in topological order.

        Missing PTRs are those IR nodes that weren't matched to any PG node
        (fused or otherwise absent). We insert them in topological order,
        choosing default physical ops.
        """
        # Topological sort of missing PTRs (leaves first)
        done_ptrs = {e[0] for e in entries}
        ordered_missing: list[int] = []
        remaining = set(missing_ptrs)

        while remaining:
            # Find PTRs in remaining whose inputs are all in done_ptrs
            ready = {
                ptr for ptr in remaining
                if all(c in done_ptrs or c not in ir_nodes for c in ir_nodes[ptr].inputs)
            }
            if not ready:
                # Cycle or unresolvable — just append in order
                ready = remaining
            for ptr in sorted(ready):
                ordered_missing.append(ptr)
                done_ptrs.add(ptr)
                remaining.discard(ptr)

        # Assign default physical ops for missing PTRs
        extra: list[tuple[int, str, str | None]] = []
        for ptr in ordered_missing:
            node = ir_nodes[ptr]
            ptr_costs.setdefault(ptr, 1)
            if node.op == "LogicalTableScan":
                extra.append((ptr, "SeqScan", None))
            elif node.op == "LogicalJoin":
                jt = node.join_type if node.join_type else "[INNER]"
                extra.append((ptr, "HashJoin", jt))
            else:
                phys = _LOGICAL_TO_PHYS_OTHER.get(node.op)
                if isinstance(phys, list):
                    phys_op = phys[0]
                elif phys is not None:
                    phys_op = phys
                else:
                    phys_op = "Filter"
                extra.append((ptr, phys_op, None))

        # Insert extra entries at the correct positions (topological order):
        # for simplicity, prepend leaves and append the rest, then re-sort.
        # Since entries is already topological, appending/interleaving by IR DAG order works.
        all_entries = entries + extra
        # Re-sort by a topological key: the position in the IR PTR definition order
        ir_order = {ptr: i for i, ptr in enumerate(sorted(ir_nodes.keys()))}
        all_entries.sort(key=lambda e: ir_order.get(e[0], 999))
        return all_entries

    @staticmethod
    def _parse_sys_rep(attrs: dict) -> dict:
        """Extract the first dict from system_representation JSON."""
        raw = attrs.get("system_representation", "[]")
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return parsed[0]
            if isinstance(parsed, dict):
                return parsed
        except (json.JSONDecodeError, TypeError):
            pass
        return {}


# ---------------------------------------------------------------------------
# job.jsonl adapter
# ---------------------------------------------------------------------------

def normalize_job_phys_entry(
    phys_entry: dict,
    local_mapping: LocalMapping,
    schema_registry: SchemaRegistry,
    ir_str: str = "",
) -> "PhysicalPlanData | None":
    """Normalize a job.jsonl ``ir_physical_token`` entry into PhysicalPlanData.

    job.jsonl plan trees already have ``_label`` on each node.
    The flat ``operators`` array supplies ``actual_rows_total`` per operator_id.

    Args:
        phys_entry:      Raw physical plan entry from job.jsonl.
        local_mapping:   Per-query global→local slot mapping.
        schema_registry: JOB schema registry.
        ir_str:          Logical IR string (required for IR PTR matching).
    """
    tree_root = phys_entry.get("plan", {}).get("queryPlan")
    if tree_root is None:
        return None

    operators_list = phys_entry.get("operators", [])
    synthetic_record = {
        "query_key": "",
        "plan": {"queryPlan": tree_root},
        "operators": operators_list,
    }
    normalizer = PGPlanNormalizer(schema_registry)
    return normalizer.normalize(synthetic_record, local_mapping, ir_str=ir_str)
