"""Standalone physical plan tokenizer — no openmu dependencies.

Converts PG EXPLAIN ANALYZE plan trees (as stored in training JSONL
``ir_physical_token``) into flat ``[PHYSICAL_PLAN] … [/PHYSICAL_PLAN]``
token strings where each ``[PTR_N]`` matches the corresponding logical IR PTR.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# IR dataclass
# ---------------------------------------------------------------------------

@dataclass
class IRNode:
    ptr: int
    op: str                         # "LogicalTableScan", "LogicalJoin", …
    table_id: int | None = None     # from [TN]
    join_type: str | None = None    # "[INNER]" etc.
    inputs: list[int] = field(default_factory=list)
    subquery_ptrs: set[int] = field(default_factory=set)  # PTRs inside [SUBQUERY] blocks


@dataclass
class PredInfo:
    """Predicate metadata parsed from logical IR ``[P_n]`` tokens."""
    pred_id: int
    global_ids: list[int]
    tables: set[int]


@dataclass
class _TraversalCtx:
    """Mutable context collected during PG plan tree traversal."""
    join_children_tables: dict[int, tuple[set[int], set[int]]] = field(default_factory=dict)
    entry_sys_reps: dict[int, dict] = field(default_factory=dict)
    scan_table_map: dict[int, int] = field(default_factory=dict)
    alias_to_table: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Default logical → physical mapping (for fused / missing PTRs)
# ---------------------------------------------------------------------------

_LOGICAL_TO_PHYS_DEFAULT: dict[str, str] = {
    "LogicalFilter": "Filter",
    "LogicalProject": "Project",
    "LogicalAggregate": "HashAggregate",
    "LogicalSort": "Sort",
    "LogicalLimit": "Limit",
    "LogicalTableScan": "SeqScan",
    # Set operations
    "LogicalMinus":     "SetOp",   # EXCEPT / MINUS
    "LogicalUnion":     "SetOp",   # UNION / UNION ALL
    "LogicalIntersect": "SetOp",   # INTERSECT
}

# Calcite IR op names for set-operation nodes
_LOGICAL_SETOP_OPS: frozenset[str] = frozenset(
    {"LogicalMinus", "LogicalUnion", "LogicalIntersect"}
)

# Maps Calcite set-op IR node to the annotation token used in ir_physical_plan_token
_LOGICAL_SETOP_ANNOTATION: dict[str, str] = {
    "LogicalMinus":     "[EXCEPT]",
    "LogicalUnion":     "[UNION_ALL]",
    "LogicalIntersect": "[INTERSECT]",
}

# ---------------------------------------------------------------------------
# PG label → compatible IR op names (for unary matching)
# ---------------------------------------------------------------------------

_PG_LABEL_TO_IR_OPS: dict[str, set[str]] = {
    "GroupBy": {"LogicalAggregate"},
    "Sort": {"LogicalSort"},
    "Limit": {"LogicalLimit"},
    "Filter": {"LogicalFilter"},
    "Window": {"LogicalWindow", "LogicalAggregate"},   # Calcite may model windows as LogicalWindow or LogicalAggregate
    "Map":    {"LogicalFilter", "LogicalProject"},     # PG Result node (constant projection / one-time filter)
}

# ---------------------------------------------------------------------------
# PG physical operator maps
# ---------------------------------------------------------------------------

_SCAN_TYPE_MAP: list[tuple[str, str]] = [
    ("Index Only Scan", "IndexOnlyScan"),
    ("Bitmap Index Scan", "BitmapIndexScan"),
    ("Bitmap Heap Scan", "BitmapHeapScan"),
    ("Index Scan", "IndexScan"),
    ("Seq Scan", "SeqScan"),
]

_JOIN_METHOD_MAP: dict[str, str] = {
    "hash": "HashJoin",
    "merge": "MergeJoin",
    "nested loop": "NestedLoop",
    "nl": "NestedLoop",
}

_AGG_METHOD_MAP: dict[str, str] = {
    "Hashed": "HashAggregate",
    "Plain": "GroupAggregate",
    "Sorted": "GroupAggregate",
}

_JOIN_TYPE_MAP: dict[str, str] = {
    # PostgreSQL / Hyper style (capitalized)
    "Inner": "[INNER]",
    "Left": "[LEFT]",
    "Right": "[RIGHT]",
    "Anti": "[ANTI]",
    "Semi": "[SEMI]",
    "Full": "[FULL]",
    # DuckDB style (lowercase, compound) produced by Join.fill() in queryoperator.py
    "inner": "[INNER]",
    "single": "[INNER]",        # correlated subquery 1-row lookup
    "leftouter": "[LEFT]",
    "rightouter": "[RIGHT]",
    "fullouter": "[FULL]",
    "leftsemi": "[LEFT] [SEMI]",
    "rightsemi": "[RIGHT] [SEMI]",
    "leftanti": "[LEFT] [ANTI]",
    "rightanti": "[RIGHT] [ANTI]",
    "rightmark": "[RIGHT] [ANTI]",  # mark join has anti semantics
}

# ---------------------------------------------------------------------------
# IR parsing
# ---------------------------------------------------------------------------

_PTR_LINE_RE = re.compile(
    r"\[PTR_(\d+)\]\s+\[(Logical\w+)\]"
    r"(?:\s+\[T(\d+)\])?"           # optional table id
    r"(?:\s+\[(INNER|LEFT|RIGHT|ANTI|SEMI|FULL)\])?"  # optional join type
)

_INPUT_LINE_RE = re.compile(r"\[INPUT\]\s*((?:\[PTR_\d+\]\s*)+)")
_INPUT_PTR_RE = re.compile(r"\[PTR_(\d+)\]")


def parse_ir_nodes(ir_str: str) -> dict[int, IRNode]:
    """Parse ``[LOGICAL_PLAN] … [/LOGICAL_PLAN]`` into {ptr: IRNode}."""
    nodes: dict[int, IRNode] = {}
    current_ptr: int | None = None
    # Stack of filter PTRs owning nested [SUBQUERY] blocks.
    subquery_owners: list[int] = []

    for line in ir_str.splitlines():
        stripped = line.strip()

        # Track [SUBQUERY] / [/SUBQUERY] boundaries.
        if "[SUBQUERY]" in stripped and "[/SUBQUERY]" not in stripped:
            if current_ptr is not None:
                subquery_owners.append(current_ptr)
            continue
        if "[/SUBQUERY]" in stripped:
            if subquery_owners:
                # Restore current_ptr to the filter that owns this subquery
                # so subsequent [INPUT] lines attach to the correct node.
                current_ptr = subquery_owners.pop()
            continue

        # Check INPUT lines first (they contain [PTR_N] tokens too)
        m = _INPUT_LINE_RE.search(stripped)
        if m and current_ptr is not None:
            ptrs = [int(p) for p in _INPUT_PTR_RE.findall(m.group(1))]
            nodes[current_ptr].inputs = ptrs
            continue

        m = _PTR_LINE_RE.search(stripped)
        if m:
            ptr = int(m.group(1))
            op = m.group(2)
            table_id = int(m.group(3)) if m.group(3) is not None else None
            join_type = f"[{m.group(4)}]" if m.group(4) else None
            nodes[ptr] = IRNode(ptr=ptr, op=op, table_id=table_id,
                                join_type=join_type)
            current_ptr = ptr

            # Register PTRs defined inside a [SUBQUERY] block.
            if subquery_owners:
                owner = subquery_owners[-1]
                if owner in nodes:
                    nodes[owner].subquery_ptrs.add(ptr)

    return nodes


# ---------------------------------------------------------------------------
# Predicate parsing from logical IR
# ---------------------------------------------------------------------------

_PRED_RE = re.compile(r'\[P_(\d+)\]')
_GREF_RE = re.compile(r'\[G(\d+)\]')


def parse_pred_map(ir_str: str) -> dict[int, PredInfo]:
    """Extract ``[P_n]`` tokens and their ``[G_n]`` column refs from logical IR."""
    pred_map: dict[int, PredInfo] = {}
    for line in ir_str.splitlines():
        m = _PRED_RE.search(line)
        if m:
            pred_id = int(m.group(1))
            global_ids = [int(g) for g in _GREF_RE.findall(line)]
            pred_map[pred_id] = PredInfo(pred_id=pred_id, global_ids=global_ids, tables=set())
    return pred_map


def build_global_to_table_map(schema_path: str | Path) -> dict[int, int]:
    """Build ``{global_column_id: table_id}`` from cumulative column offsets in schema."""
    doc = json.loads(Path(schema_path).read_text())
    g_to_t: dict[int, int] = {}
    offset = 0
    for table_id, table in enumerate(doc.get("tables", [])):
        n_cols = len(table.get("columns", []))
        for col_idx in range(n_cols):
            g_to_t[offset + col_idx] = table_id
        offset += n_cols
    return g_to_t


# ---------------------------------------------------------------------------
# Table name → ID helper
# ---------------------------------------------------------------------------

def load_table_name_to_id(schema_path: str | Path) -> dict[str, int]:
    """Read schema JSON and return ``{table_name: positional_index}``."""
    doc = json.loads(Path(schema_path).read_text())
    return {
        tbl["name"]: idx
        for idx, tbl in enumerate(doc.get("tables", []))
        if tbl.get("name")
    }


# ---------------------------------------------------------------------------
# PG plan helpers
# ---------------------------------------------------------------------------

def _parse_sys_rep(attrs: dict) -> dict:
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


def _get_unary_op(pg_label: str, attrs: dict) -> str:
    if pg_label == "GroupBy":
        method = attrs.get("method", "Plain")
        return _AGG_METHOD_MAP.get(method, "GroupAggregate")
    if pg_label == "Sort":
        return "Sort"
    if pg_label == "Limit":
        return "Limit"
    return "Filter"


# ---------------------------------------------------------------------------
# Synthetic PTR helpers
# ---------------------------------------------------------------------------

def _get_synth_ptr(ir_nodes: dict[int, "IRNode"], used_ir_ptrs: set[int]) -> int:
    """Return a PTR integer that does not collide with any real IR node or used PTR."""
    base = max(ir_nodes.keys(), default=999) + 1
    while base in used_ir_ptrs:
        base += 1
    return base


def _find_any_unused_scan(ir_nodes: dict[int, "IRNode"], used: set[int]) -> int | None:
    """Return any unused LogicalTableScan PTR (used by EmptyResult matching)."""
    for ptr, node in ir_nodes.items():
        if node.op == "LogicalTableScan" and ptr not in used:
            return ptr
    return None


# ---------------------------------------------------------------------------
# Core traversal
# ---------------------------------------------------------------------------

# Entry = (ir_ptr, phys_op, join_type_or_None, input_tokens_or_None)
# input_tokens are pre-formatted: "[PTR_N]" for operators, "[TN]" for table scans
Entry = tuple[int, str, str | None, list[str] | None]


def _traverse(
    node: dict,
    op_card: dict[int | None, float],
    ptr_costs: dict[int, int],
    used_ir_ptrs: set[int],
    ir_nodes: dict[int, IRNode],
    scan_map: dict[int, list[int]],
    join_map: dict[tuple[int, int], int],
    unary_map: dict[tuple[int, str], list[int]],
    table_name_to_id: dict[str, int],
    subquery_filters: list[int] | None = None,
    ctx: _TraversalCtx | None = None,
    ir_table_coverage: dict[int, frozenset[int]] | None = None,
) -> tuple[list[Entry], int | None, set[int]]:
    """Post-order traversal of PG plan tree.

    Returns (entries, root_ir_ptr, tables_in_subtree).
    """
    label = node.get("_label", "")
    attrs = node.get("_attrs", {})

    # DuckDB Select (FILTER) node: transparent when it sits above a scan-level
    # operator so the predicate fuses into SeqScan [FILTER] annotation (PG-compatible).
    # Emit a real Filter node when it sits above a Join or GroupBy (post-join /
    # HAVING-style filter that cannot be pushed to scan level).
    if label == "Select":
        child_labels = {c.get("_label", "") for c in node.get("_children", [])}
        _SCAN_LEVEL = {"TableScan", "Select", "CustomOperator"}
        if child_labels <= _SCAN_LEVEL:
            label = ""   # force fall-through to transparent pass-through below
        else:
            label = "Filter"   # post-join / post-agg filter — emit as Filter node

    # Transparent wrappers and newly-emitted wrapper operators
    if label in ("Result", "CustomOperator", ""):
        all_entries: list[Entry] = []
        last_ptr: int | None = None
        child_ptrs_all: list[int] = []   # all child root ptrs (needed by Append)
        all_tables: set[int] = set()
        for child in node.get("_children", []):
            ce, cp, ct = _traverse(child, op_card, ptr_costs, used_ir_ptrs,
                               ir_nodes, scan_map, join_map, unary_map,
                               table_name_to_id, subquery_filters, ctx,
                               ir_table_coverage)
            all_entries.extend(ce)
            all_tables |= ct
            if cp is not None:
                last_ptr = cp
                child_ptrs_all.append(cp)

        # ---- Result: handled at the top level in tokenize_physical_plan so
        #      it always appears as the final entry (after _insert_missing_ptrs).
        #      Here we simply pass through.
        if label == "Result":
            return all_entries, last_ptr, all_tables

        # ---- CustomOperator: dispatch by name ----
        name = attrs.get("name", "")

        if name == "EmptyResult":
            # Optimizer replaced a table scan with a zero-row short-circuit.
            # Match to any unused LogicalTableScan PTR so the IR pointer is consumed.
            ir_ptr = _find_any_unused_scan(ir_nodes, used_ir_ptrs)
            if ir_ptr is None:
                ir_ptr = _get_synth_ptr(ir_nodes, used_ir_ptrs)
            used_ir_ptrs.add(ir_ptr)
            ptr_costs[ir_ptr] = 0  # EmptyResult always produces 0 rows
            tbl_id = ir_nodes[ir_ptr].table_id if ir_ptr in ir_nodes else None
            inputs = [f"[T{tbl_id}]"] if tbl_id is not None else None
            # Propagate table_id upward so parent joins can use coverage matching.
            table_set = {tbl_id} if tbl_id is not None else set()
            return all_entries + [(ir_ptr, "EmptyResult", None, inputs)], ir_ptr, all_tables | table_set

        if name == "Limit":
            candidates = (unary_map.get((last_ptr, "Limit"), []) +
                          unary_map.get((last_ptr, "_any"), []))
            ir_ptr = next((c for c in candidates if c not in used_ir_ptrs), None)
            if ir_ptr is None:
                ir_ptr, consumed = _find_chain_unary_match(
                    last_ptr, "Limit", ir_nodes, used_ir_ptrs)
                for p in consumed:
                    used_ir_ptrs.add(p)
                    ptr_costs.setdefault(p, 1)
            if ir_ptr is None:
                ir_ptr = _get_synth_ptr(ir_nodes, used_ir_ptrs)
            used_ir_ptrs.add(ir_ptr)
            ptr_costs[ir_ptr] = int(op_card.get(attrs.get("operator_id"), 0))
            inputs = [f"[PTR_{last_ptr}]"] if last_ptr is not None else None
            return all_entries + [(ir_ptr, "Limit", None, inputs)], ir_ptr, all_tables

        if name == "Materialize":
            synth = _get_synth_ptr(ir_nodes, used_ir_ptrs)
            used_ir_ptrs.add(synth)
            ptr_costs[synth] = int(op_card.get(attrs.get("operator_id"), 0))
            inputs = [f"[PTR_{last_ptr}]"] if last_ptr is not None else None
            return all_entries + [(synth, "Materialize", None, inputs)], synth, all_tables

        if name == "Append":
            synth = _get_synth_ptr(ir_nodes, used_ir_ptrs)
            used_ir_ptrs.add(synth)
            ptr_costs[synth] = int(op_card.get(attrs.get("operator_id"), 0))
            inputs = [f"[PTR_{p}]" for p in child_ptrs_all] if child_ptrs_all else None
            return all_entries + [(synth, "Append", None, inputs)], synth, all_tables

        # All other CustomOperator subtypes remain transparent pass-throughs:
        #   Hash (absorbed into HashJoin), Gather/Gather Merge (parallel artifact),
        #   Projection, DelimScan, CrossProduct, Memoize, BitmapOr, ProjectSet,
        #   Function Scan, Values Scan, INOUT_FUNCTION.
        # TODO: Consider emitting the following in a future iteration:
        #   DelimScan, CrossProduct, Memoize, BitmapOr, ProjectSet,
        #   Function Scan, Values Scan, INOUT_FUNCTION
        return all_entries, last_ptr, all_tables

    # Recurse children (post-order)
    child_results: list[tuple[list[Entry], int | None, set[int]]] = []
    for child in node.get("_children", []):
        child_results.append(_traverse(
            child, op_card, ptr_costs, used_ir_ptrs,
            ir_nodes, scan_map, join_map, unary_map, table_name_to_id,
            subquery_filters, ctx, ir_table_coverage,
        ))

    combined: list[Entry] = []
    child_ir_ptrs: list[int] = []
    child_tables_list: list[set[int]] = []
    all_child_tables: set[int] = set()
    for ce, cp, ct in child_results:
        combined.extend(ce)
        all_child_tables |= ct
        if cp is not None:
            child_ir_ptrs.append(cp)
            child_tables_list.append(ct)

    # Unknown label → pass through
    if label not in (
        "TableScan", "Join", "GroupBy", "Sort", "Limit", "Filter",
        "Window", "SetOperation", "Map", "Subquery",
    ):
        return combined, child_ir_ptrs[-1] if child_ir_ptrs else None, all_child_tables

    # Cardinality
    op_id = attrs.get("operator_id")
    rows = op_card.get(op_id, 0.0)
    if rows == 0.0 and op_id is None:
        rows = float(attrs.get("exact_cardinality", 0.0))
    cardinality = max(1, int(round(rows)))

    sys_dict = _parse_sys_rep(attrs)

    # ---- TableScan ----
    if label == "TableScan":
        table_name = attrs.get("table_name") or sys_dict.get("Relation Name", "")
        table_id = table_name_to_id.get(table_name)
        ir_ptr: int | None = None
        if table_id is not None:
            for cand in scan_map.get(table_id, []):
                if cand not in used_ir_ptrs:
                    ir_ptr = cand
                    break
        if ir_ptr is None:
            return combined, child_ir_ptrs[-1] if child_ir_ptrs else None, all_child_tables

        node_type = sys_dict.get("Node Type", "")
        phys_op = "SeqScan"
        for pattern, op_name in _SCAN_TYPE_MAP:
            if pattern in node_type:
                phys_op = op_name
                break

        used_ir_ptrs.add(ir_ptr)
        ptr_costs[ir_ptr] = cardinality

        table_set = {table_id} if table_id is not None else set()
        if ctx is not None:
            if table_id is not None:
                ctx.scan_table_map[ir_ptr] = table_id
            ctx.entry_sys_reps[ir_ptr] = sys_dict
            alias = sys_dict.get("Alias", "")
            if alias:
                ctx.alias_to_table[alias] = table_name

        table_tok = f"[T{table_id}]" if table_id is not None else ""
        inputs = [table_tok] if table_tok else []
        return combined + [(ir_ptr, phys_op, None, inputs)], ir_ptr, table_set | all_child_tables

    # ---- Join ----
    if label == "Join":
        if len(child_ir_ptrs) < 2:
            return combined, child_ir_ptrs[-1] if child_ir_ptrs else None, all_child_tables

        key = (child_ir_ptrs[0], child_ir_ptrs[1])
        ir_ptr = join_map.get(key)
        if ir_ptr is None or ir_ptr in used_ir_ptrs:
            # Coverage-based fallback: match by which tables the subtrees cover
            if ir_table_coverage is not None and all_child_tables:
                ir_ptr = _find_join_by_coverage(
                    all_child_tables, ir_nodes, ir_table_coverage, used_ir_ptrs)
            if ir_ptr is None or ir_ptr in used_ir_ptrs:
                ir_ptr = _find_any_unused_join(ir_nodes, used_ir_ptrs)
        # Fallback: PG may have decorrelated an EXISTS/IN subquery into a
        # join.  The IR has no LogicalJoin — only a LogicalFilter owning a
        # [SUBQUERY] block.  Match it here.
        if ir_ptr is None and subquery_filters:
            ir_ptr = _find_subquery_filter(
                child_ir_ptrs, ir_nodes, used_ir_ptrs, subquery_filters)
        if ir_ptr is None:
            return combined, child_ir_ptrs[-1] if child_ir_ptrs else None, all_child_tables

        method = attrs.get("method", "").lower()
        node_type_fallback = sys_dict.get("Node Type", "").lower()
        phys_op = "HashJoin"
        for pat, op_name in _JOIN_METHOD_MAP.items():
            if pat in method or pat in node_type_fallback:
                phys_op = op_name
                break

        pg_join_type = sys_dict.get("Join Type", attrs.get("type", "Inner"))
        join_type = _JOIN_TYPE_MAP.get(pg_join_type, "[INNER]")

        # Track left/right table coverage for predicate assignment.
        left_tables = child_tables_list[0] if child_tables_list else set()
        right_tables = child_tables_list[1] if len(child_tables_list) > 1 else set()
        if ctx is not None:
            ctx.join_children_tables[ir_ptr] = (left_tables, right_tables)
            ctx.entry_sys_reps[ir_ptr] = sys_dict

        fused = _collect_fused(child_ir_ptrs, ir_ptr, ir_nodes,
                               used_ir_ptrs, ptr_costs)
        used_ir_ptrs.add(ir_ptr)
        ptr_costs[ir_ptr] = cardinality
        return combined + fused + [(ir_ptr, phys_op, join_type, [f"[PTR_{p}]" for p in child_ir_ptrs])], ir_ptr, all_child_tables

    # ---- Window (both engines — unary, try IR match then synthetic fallback) ----
    if label == "Window":
        child_ir_ptr = child_ir_ptrs[0] if child_ir_ptrs else None
        ir_ptr = None
        for cand in unary_map.get((child_ir_ptr, "Window"), []):
            if cand not in used_ir_ptrs:
                ir_ptr = cand
                break
        if ir_ptr is None:
            ir_ptr, consumed = _find_chain_unary_match(
                child_ir_ptr, "Window", ir_nodes, used_ir_ptrs)
            for p in consumed:
                used_ir_ptrs.add(p)
                ptr_costs.setdefault(p, 1)
        if ir_ptr is None:
            ir_ptr = _get_synth_ptr(ir_nodes, used_ir_ptrs)
        used_ir_ptrs.add(ir_ptr)
        ptr_costs[ir_ptr] = cardinality
        inputs = [f"[PTR_{child_ir_ptr}]"] if child_ir_ptr is not None else None
        return combined + [(ir_ptr, "Window", None, inputs)], ir_ptr, all_child_tables

    # ---- Map (PG Result node — constant projection / one-time filter) ----
    if label == "Map":
        child_ir_ptr = child_ir_ptrs[0] if child_ir_ptrs else None
        ir_ptr = None
        for cand in unary_map.get((child_ir_ptr, "Map"), []):
            if cand not in used_ir_ptrs:
                ir_ptr = cand
                break
        if ir_ptr is None:
            ir_ptr, consumed = _find_chain_unary_match(
                child_ir_ptr, "Map", ir_nodes, used_ir_ptrs)
            for p in consumed:
                used_ir_ptrs.add(p)
                ptr_costs.setdefault(p, 1)
        if ir_ptr is None:
            ir_ptr = _get_synth_ptr(ir_nodes, used_ir_ptrs)
        used_ir_ptrs.add(ir_ptr)
        ptr_costs[ir_ptr] = cardinality
        inputs = [f"[PTR_{child_ir_ptr}]"] if child_ir_ptr is not None else None
        return combined + [(ir_ptr, "Map", None, inputs)], ir_ptr, all_child_tables

    # ---- SetOperation (UNION / INTERSECT / EXCEPT) ----
    if label == "SetOperation":
        # Derive annotation from physical plan metadata
        raw = (attrs.get("type") or sys_dict.get("Command") or "unionall")
        _SET_TOK: dict[str, str] = {
            "unionall": "UNION_ALL", "union": "UNION_ALL",
            "intersect": "INTERSECT", "except": "EXCEPT",
        }
        annotation = f"[{_SET_TOK.get(raw.lower().replace(' ', ''), 'UNION_ALL')}]"

        # Prefer to consume a matching LogicalMinus/LogicalUnion/LogicalIntersect
        # IR node so _insert_missing_ptrs never sees it as dangling.
        ir_ptr = None
        for ptr, node in ir_nodes.items():
            if ptr not in used_ir_ptrs and node.op in _LOGICAL_SETOP_OPS:
                ir_ptr = ptr
                # Refine annotation from the IR node type if available
                annotation = _LOGICAL_SETOP_ANNOTATION.get(node.op, annotation)
                break
        if ir_ptr is None:
            ir_ptr = _get_synth_ptr(ir_nodes, used_ir_ptrs)

        used_ir_ptrs.add(ir_ptr)
        ptr_costs[ir_ptr] = cardinality
        inputs = [f"[PTR_{p}]" for p in child_ir_ptrs] if child_ir_ptrs else None
        return combined + [(ir_ptr, "SetOp", annotation, inputs)], ir_ptr, all_child_tables

    # ---- Subquery (PG Subquery Scan — unary, synthetic PTR) ----
    if label == "Subquery":
        child_ir_ptr = child_ir_ptrs[0] if child_ir_ptrs else None
        synth = _get_synth_ptr(ir_nodes, used_ir_ptrs)
        used_ir_ptrs.add(synth)
        ptr_costs[synth] = cardinality
        inputs = [f"[PTR_{child_ir_ptr}]"] if child_ir_ptr is not None else None
        return combined + [(synth, "SubqueryScan", None, inputs)], synth, all_child_tables

    # ---- Unary: GroupBy, Sort, Limit, Filter ----
    child_ir_ptr = child_ir_ptrs[0] if child_ir_ptrs else None
    if child_ir_ptr is None:
        return combined, None, all_child_tables

    ir_ptr = None
    candidates = unary_map.get((child_ir_ptr, label), [])
    for cand in candidates:
        if cand not in used_ir_ptrs:
            ir_ptr = cand
            break
    # No _any fallback here: grabbing an incompatible IR node (e.g. LogicalProject
    # for a GroupBy) produces a phantom extra operator when the real target sits
    # one hop further up the chain.  Go straight to _find_chain_unary_match.
    if ir_ptr is None:
        ir_ptr, chain_consumed = _find_chain_unary_match(
            child_ir_ptr, label, ir_nodes, used_ir_ptrs)
        for p in chain_consumed:
            used_ir_ptrs.add(p)
            ptr_costs.setdefault(p, 1)
    if ir_ptr is None:
        return combined, child_ir_ptr, all_child_tables

    phys_op = _get_unary_op(label, attrs)
    fused = _collect_fused([child_ir_ptr], ir_ptr, ir_nodes,
                           used_ir_ptrs, ptr_costs)
    used_ir_ptrs.add(ir_ptr)
    ptr_costs[ir_ptr] = cardinality
    return combined + fused + [(ir_ptr, phys_op, None, [f"[PTR_{child_ir_ptr}]"])], ir_ptr, all_child_tables


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _find_any_unused_join(
    ir_nodes: dict[int, IRNode],
    used: set[int],
) -> int | None:
    for ptr, node in ir_nodes.items():
        if ptr not in used and node.op == "LogicalJoin":
            return ptr
    return None


def _find_subquery_filter(
    child_ir_ptrs: list[int],
    ir_nodes: dict[int, IRNode],
    used: set[int],
    subquery_filters: list[int],
) -> int | None:
    """Find an unused LogicalFilter-with-subquery that matches a decorrelated join.

    PG decorrelates EXISTS/IN subqueries into joins.  The IR has no
    LogicalJoin for these — instead a LogicalFilter owns the subquery.
    We match when one child_ir_ptr is reachable from the filter's outer
    input and the other is reachable from its subquery PTRs.
    """
    for fptr in subquery_filters:
        if fptr in used:
            continue
        fnode = ir_nodes[fptr]
        inner = fnode.subquery_ptrs

        # Build transitive closure of outer inputs.
        outer_reachable: set[int] = set()
        stack = [p for p in fnode.inputs if p not in inner]
        while stack:
            p = stack.pop()
            if p in outer_reachable:
                continue
            outer_reachable.add(p)
            if p in ir_nodes:
                stack.extend(ir_nodes[p].inputs)

        # Build transitive closure of inner (subquery) PTRs.
        inner_reachable: set[int] = set()
        stack = list(inner)
        while stack:
            p = stack.pop()
            if p in inner_reachable:
                continue
            inner_reachable.add(p)
            if p in ir_nodes:
                stack.extend(ir_nodes[p].inputs)

        c0, c1 = child_ir_ptrs[0], child_ir_ptrs[1]
        if (c0 in outer_reachable and c1 in inner_reachable) or \
           (c1 in outer_reachable and c0 in inner_reachable):
            return fptr
    return None


def _find_chain_unary_match(
    child_ir_ptr: int,
    pg_label: str,
    ir_nodes: dict[int, IRNode],
    used_ir_ptrs: set[int],
) -> tuple[int | None, list[int]]:
    """Walk up the IR unary chain from child_ir_ptr to find a matching node.

    PG fuses Filter→Project→Aggregate into a single GroupBy node whose physical
    child is the join root, not the logical input of the aggregate.  This helper
    climbs the logical IR parent chain until it finds a node whose op matches
    pg_label, collecting intermediate nodes to be silently consumed.

    Primary attempt: walk from child_ir_ptr.
    Fallback: walk from any used (matched) IR ptr — handles the case where the
    physical child was re-assigned to a different IR join than the one at the
    bottom of the unary chain (join-order reordering by PG).

    Returns (matched_ptr, consumed_intermediates).
    """
    # Build reverse parent map: ptr -> list of unary parents
    parents_of: dict[int, list[int]] = {}
    for ptr, node in ir_nodes.items():
        if ptr in used_ir_ptrs:
            continue
        if node.op in ("LogicalTableScan", "LogicalJoin") or node.subquery_ptrs:
            continue
        for inp in node.inputs:
            parents_of.setdefault(inp, []).append(ptr)

    target_ops = _PG_LABEL_TO_IR_OPS.get(pg_label, set())
    if not target_ops:
        return None, []

    def _walk(start: int) -> tuple[int | None, list[int]]:
        consumed: list[int] = []
        visited: set[int] = set()
        current = start
        while True:
            upward = [p for p in parents_of.get(current, [])
                      if p not in used_ir_ptrs and p not in visited]
            if not upward:
                return None, []
            candidate = upward[0]
            visited.add(candidate)
            node = ir_nodes[candidate]
            if node.op in target_ops:
                return candidate, consumed
            consumed.append(candidate)
            current = candidate

    # Primary: walk directly from the physical child
    result, consumed = _walk(child_ir_ptr)
    if result is not None:
        return result, consumed

    # Fallback: walk from any used ptr that has unary parents in the IR.
    # This handles join-order reordering where the physical child PTR differs
    # from the logical join root that anchors the unary chain.
    for used_ptr in used_ir_ptrs:
        if used_ptr not in parents_of:
            continue
        result, consumed = _walk(used_ptr)
        if result is not None:
            return result, consumed

    return None, []


def _collect_fused(
    child_ir_ptrs: list[int],
    parent_ir_ptr: int,
    ir_nodes: dict[int, IRNode],
    used_ir_ptrs: set[int],
    ptr_costs: dict[int, int],
) -> list[Entry]:
    """Insert IR PTRs that sit between children and parent (fused by PG)."""
    parent_node = ir_nodes.get(parent_ir_ptr)
    if parent_node is None:
        return []

    child_set = set(child_ir_ptrs)
    visited: set[int] = set()
    stack = list(parent_node.inputs)
    fused: list[Entry] = []

    while stack:
        inp = stack.pop()
        if inp in visited or inp in child_set or inp in used_ir_ptrs:
            continue
        visited.add(inp)
        node = ir_nodes.get(inp)
        if node is None:
            continue
        # Table scans and joins must only be matched by their own handlers
        if node.op in ("LogicalTableScan", "LogicalJoin"):
            continue
        # Silently consumed — fused into the parent physical operator; do not emit
        used_ir_ptrs.add(inp)
        ptr_costs.setdefault(inp, 1)
        stack.extend(node.inputs)

    return []


def _insert_missing_ptrs(
    entries: list[Entry],
    missing_ptrs: set[int],
    ir_nodes: dict[int, IRNode],
    ptr_costs: dict[int, int],
    root_ir_ptr: int | None = None,
) -> list[Entry]:
    """Insert IR PTRs not matched to any physical node, in topological order."""
    done = {e[0] for e in entries}
    ordered: list[int] = []
    remaining = set(missing_ptrs)

    while remaining:
        ready = {
            ptr for ptr in remaining
            if all(c in done or c not in ir_nodes for c in ir_nodes[ptr].inputs)
        }
        if not ready:
            ready = remaining
        for ptr in sorted(ready):
            ordered.append(ptr)
            done.add(ptr)
            remaining.discard(ptr)

    # Build a set of PTRs that were skipped (not emitted) so we can resolve
    # inputs through them when generating fallback entries.
    skipped: set[int] = set()

    # Track PTRs already referenced as inputs by traversal-derived entries.
    # A missing join whose logical inputs overlap this set would create a DAG
    # (one PTR with two parent operators), so we skip it instead.
    _PTR_RE = re.compile(r'\[PTR_(\d+)\]')
    referenced_as_input: set[int] = set()
    for _, _, _, inp_toks in entries:
        if inp_toks:
            for tok in inp_toks:
                m = _PTR_RE.match(tok)
                if m:
                    referenced_as_input.add(int(m.group(1)))
    # The traversal root will be consumed by Result (appended after this function).
    # Pre-mark it so missing joins don't create a second parent for it.
    if root_ir_ptr is not None:
        referenced_as_input.add(root_ir_ptr)

    extra: list[Entry] = []
    for ptr in ordered:
        node = ir_nodes[ptr]
        ptr_costs.setdefault(ptr, 1)
        # Filter and Project are always fused into scans/aggregates — skip
        if node.op in ("LogicalFilter", "LogicalProject"):
            skipped.add(ptr)
            continue

        def _resolve_inputs(ptrs: list[int]) -> list[str]:
            """Walk through skipped nodes to find the nearest emitted ancestor."""
            result = []
            for p in ptrs:
                visited: set[int] = set()
                stack = [p]
                while stack:
                    cur = stack.pop()
                    if cur in visited:
                        continue
                    visited.add(cur)
                    if cur not in skipped:
                        result.append(f"[PTR_{cur}]")
                        break
                    # cur was skipped — follow its inputs
                    parent = ir_nodes.get(cur)
                    if parent:
                        stack.extend(parent.inputs)
            return result

        if node.op == "LogicalTableScan":
            t_tok = f"[T{node.table_id}]" if node.table_id is not None else ""
            extra.append((ptr, "SeqScan", None, [t_tok] if t_tok else []))
        elif node.op == "LogicalJoin":
            resolved = _resolve_inputs(node.inputs)
            resolved_ptr_ints = {
                int(m.group(1))
                for tok in resolved
                for m in [_PTR_RE.match(tok)]
                if m
            }
            # If any input PTR is already used by another operator, emitting
            # this join would give that PTR two parents — a DAG.  Skip it.
            if resolved_ptr_ints & referenced_as_input:
                skipped.add(ptr)
                continue
            jt = node.join_type or "[INNER]"
            extra.append((ptr, "HashJoin", jt, resolved))
            referenced_as_input.update(resolved_ptr_ints)
        else:
            phys_op = _LOGICAL_TO_PHYS_DEFAULT.get(node.op, "Filter")
            resolved = _resolve_inputs(node.inputs)
            resolved_ptr_ints = {
                int(m.group(1))
                for tok in resolved
                for m in [_PTR_RE.match(tok)]
                if m
            }
            # Same DAG guard as LogicalJoin: skip if inputs already referenced.
            # E.g. PG folds ORDER BY ... LIMIT into a single Limit node, leaving
            # LogicalSort "missing" — it would create a DAG pointing to the Limit.
            if resolved_ptr_ints & referenced_as_input:
                skipped.add(ptr)
                continue
            extra.append((ptr, phys_op, None, resolved))
            referenced_as_input.update(resolved_ptr_ints)

    # Preserve physical post-order for matched entries; missing PTRs appended after.
    return entries + extra


# ---------------------------------------------------------------------------
# Build matching maps from parsed IR
# ---------------------------------------------------------------------------

def _build_matching_maps(
    ir_nodes: dict[int, IRNode],
) -> tuple[
    dict[int, list[int]],
    dict[tuple[int, int], int],
    dict[tuple[int, str], list[int]],
    list[int],
]:
    """Build scan_map, join_map, unary_map, subquery_filters from parsed IR nodes."""
    scan_map: dict[int, list[int]] = {}
    join_map: dict[tuple[int, int], int] = {}
    unary_map: dict[tuple[int, str], list[int]] = {}
    subquery_filters: list[int] = []

    for ptr, node in ir_nodes.items():
        if node.op == "LogicalTableScan" and node.table_id is not None:
            scan_map.setdefault(node.table_id, []).append(ptr)
        elif node.op == "LogicalJoin" and len(node.inputs) == 2:
            k = (node.inputs[0], node.inputs[1])
            join_map[k] = ptr
            join_map[(k[1], k[0])] = ptr
        elif node.op not in ("LogicalTableScan", "LogicalJoin") and node.inputs:
            # LogicalFilter with subquery → potential decorrelated join target.
            # Do NOT add to unary_map (prevents PG-inserted operators from
            # incorrectly consuming the filter PTR).
            if node.subquery_ptrs:
                subquery_filters.append(ptr)
                continue
            child = node.inputs[0]
            for pg_label, ir_ops in _PG_LABEL_TO_IR_OPS.items():
                if node.op in ir_ops:
                    unary_map.setdefault((child, pg_label), []).append(ptr)
            unary_map.setdefault((child, "_any"), []).append(ptr)

    return scan_map, join_map, unary_map, subquery_filters


# ---------------------------------------------------------------------------
# IR table coverage (for join matching by subtree tables)
# ---------------------------------------------------------------------------

def _build_ir_table_coverage(
    ir_nodes: dict[int, IRNode],
) -> dict[int, frozenset[int]]:
    """Compute ``{ptr: frozenset[table_ids]}`` for every IR node.

    Walks inputs down to ``LogicalTableScan`` leaves via memoized DFS.
    """
    cache: dict[int, frozenset[int]] = {}

    def _cover(ptr: int) -> frozenset[int]:
        if ptr in cache:
            return cache[ptr]
        node = ir_nodes.get(ptr)
        if node is None:
            cache[ptr] = frozenset()
            return cache[ptr]
        if node.op == "LogicalTableScan" and node.table_id is not None:
            cache[ptr] = frozenset({node.table_id})
            return cache[ptr]
        tables: set[int] = set()
        for inp in node.inputs:
            tables |= _cover(inp)
        cache[ptr] = frozenset(tables)
        return cache[ptr]

    for ptr in ir_nodes:
        _cover(ptr)
    return cache


def _find_join_by_coverage(
    all_child_tables: set[int],
    ir_nodes: dict[int, IRNode],
    ir_table_coverage: dict[int, frozenset[int]],
    used: set[int],
) -> int | None:
    """Find an unused LogicalJoin whose IR table coverage matches the PG join's tables.

    Prefers exact match; falls back to smallest superset.
    """
    target = frozenset(all_child_tables)
    best_ptr: int | None = None
    best_extra = float('inf')

    for ptr, node in ir_nodes.items():
        if ptr in used or node.op != "LogicalJoin":
            continue
        coverage = ir_table_coverage.get(ptr, frozenset())
        if not coverage:
            continue
        # Must cover all child tables
        if not target <= coverage:
            continue
        extra = len(coverage) - len(target)
        if extra == 0:
            return ptr  # exact match — use immediately
        if extra < best_extra:
            best_extra = extra
            best_ptr = ptr

    return best_ptr


# ---------------------------------------------------------------------------
# Predicate → physical operator assignment
# ---------------------------------------------------------------------------

_INDEX_COND_REF_RE = re.compile(r'(\w+)\.(\w+)')


def _assign_predicates(
    entries: list[Entry],
    pred_registry: list[dict],
    ctx: _TraversalCtx,
    table_name_to_id: dict[str, int],
) -> dict[int, dict[str, list[int]]]:
    """Assign ``[P_n]`` predicates to physical operators with role tags.

    Returns ``{ir_ptr: {role_tag: [pred_id, …]}}``
    """
    entry_map = {e[0]: e for e in entries}

    # Invert scan_table_map: table_id → ir_ptr (first scan wins).
    table_to_scan: dict[int, int] = {}
    for ptr, tid in ctx.scan_table_map.items():
        if ptr in entry_map:
            table_to_scan.setdefault(tid, ptr)

    annotations: dict[int, dict[str, list[int]]] = {}
    assigned: set[int] = set()

    # --- Pass 1: INDEX_COND on IndexScan operators --------------------------
    for ptr, (_, phys_op, _, _) in entry_map.items():
        if phys_op not in ("IndexScan", "IndexOnlyScan"):
            continue
        sys_rep = ctx.entry_sys_reps.get(ptr, {})
        index_cond = sys_rep.get("Index Cond")
        if not index_cond:
            continue

        scan_tid = ctx.scan_table_map.get(ptr)
        if scan_tid is None:
            continue

        # Identify the "other" table referenced in the Index Cond text.
        scan_alias = sys_rep.get("Alias", "")
        other_tids: set[int] = set()
        for alias, _col in _INDEX_COND_REF_RE.findall(index_cond):
            if alias == scan_alias:
                continue
            tbl_name = ctx.alias_to_table.get(alias)
            if tbl_name:
                tid = table_name_to_id.get(tbl_name)
                if tid is not None:
                    other_tids.add(tid)

        # Match a join predicate involving the scan table and the other table.
        for pred in pred_registry:
            if pred["pred_id"] in assigned:
                continue
            pred_tables = set(pred["tables"])
            if len(pred_tables) < 2:
                continue
            if scan_tid in pred_tables and other_tids & pred_tables:
                annotations.setdefault(ptr, {}).setdefault("INDEX_COND", []).append(pred["pred_id"])
                assigned.add(pred["pred_id"])
                # Also assign to the parent join if this predicate straddles
                # both sides (e.g. EXISTS decorrelated into NestedLoop).
                for j_ptr, (lt, rt) in ctx.join_children_tables.items():
                    if j_ptr not in entry_map:
                        continue
                    if (pred_tables & lt) and (pred_tables & rt):
                        _, j_phys_op, _, _ = entry_map[j_ptr]
                        if j_phys_op == "HashJoin":
                            role = "HASH_COND"
                        elif j_phys_op == "MergeJoin":
                            role = "MERGE_COND"
                        elif j_phys_op == "NestedLoop":
                            role = "JOIN_FILTER"
                        else:
                            role = "HASH_COND"
                        annotations.setdefault(j_ptr, {}).setdefault(role, []).append(pred["pred_id"])
                        break
                break  # one index cond per scan

    # --- Pass 2: Selection predicates → FILTER on scan ----------------------
    for pred in pred_registry:
        pid = pred["pred_id"]
        if pid in assigned:
            continue
        tables = set(pred["tables"])
        if len(tables) != 1:
            continue
        tid = next(iter(tables))
        scan_ptr = table_to_scan.get(tid)
        if scan_ptr is not None and scan_ptr in entry_map:
            annotations.setdefault(scan_ptr, {}).setdefault("FILTER", []).append(pid)
            assigned.add(pid)

    # --- Pass 3: Join predicates → lowest join with table split -------------
    for pred in pred_registry:
        pid = pred["pred_id"]
        if pid in assigned:
            continue
        tables = set(pred["tables"])
        if len(tables) < 2:
            continue

        best_ptr: int | None = None
        best_size = float('inf')
        for j_ptr, (lt, rt) in ctx.join_children_tables.items():
            if j_ptr not in entry_map:
                continue
            if (tables & lt) and (tables & rt):
                size = len(lt) + len(rt)
                if size < best_size:
                    best_size = size
                    best_ptr = j_ptr

        if best_ptr is not None:
            _, phys_op, _, _ = entry_map[best_ptr]
            if phys_op == "HashJoin":
                role = "HASH_COND"
            elif phys_op == "MergeJoin":
                role = "MERGE_COND"
            elif phys_op == "NestedLoop":
                role = "JOIN_FILTER"
            else:
                role = "HASH_COND"
            annotations.setdefault(best_ptr, {}).setdefault(role, []).append(pid)
            assigned.add(pid)

    return annotations


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def tokenize_physical_plan(
    ir_logical_token: str,
    pg_physical: dict,
    table_name_to_id: dict[str, int],
    pred_registry: list[dict] | None = None,
) -> tuple[str, list[int]] | None:
    """Convert a PG EXPLAIN plan into a ``[PHYSICAL_PLAN] … [/PHYSICAL_PLAN]`` string.

    Args:
        ir_logical_token: Logical IR token string (``[LOGICAL_PLAN] … [/LOGICAL_PLAN]``).
        pg_physical:      Raw physical plan dict with ``plan.queryPlan`` and ``operators``.
        table_name_to_id: ``{table_name: table_id}`` from schema.
        pred_registry:    Optional predicate metadata from logical tokenization.

    Returns:
        ``(token_string, cardinalities)`` or ``None`` on failure.
        ``cardinalities`` is a per-PTR list of actual rows, ordered to
        match the PTR entries in the token string.
    """
    try:
        tree = pg_physical.get("plan", {}).get("queryPlan")
        if tree is None:
            return None

        ir_nodes = parse_ir_nodes(ir_logical_token)
        if not ir_nodes:
            return None

        # Build cardinality map
        op_card: dict[int | None, float] = {}
        for op in pg_physical.get("operators", []):
            op_id = op.get("operator_id")
            if op_id not in op_card:
                op_card[op_id] = float(op.get("actual_rows_total", 0.0))

        scan_map, join_map, unary_map, subquery_filters = _build_matching_maps(ir_nodes)
        ir_table_coverage = _build_ir_table_coverage(ir_nodes)

        ptr_costs: dict[int, int] = {}
        used_ir_ptrs: set[int] = set()
        ctx = _TraversalCtx()

        # Strip the top-level Result wrapper so it can be appended last,
        # after _insert_missing_ptrs has added all missing IR nodes.
        result_card: int | None = None
        if tree.get("_label") == "Result":
            result_attrs = tree.get("_attrs", {})
            result_card = int(op_card.get(result_attrs.get("operator_id"), 0))
            children = tree.get("_children", [])
            if children:
                tree = children[0]  # traverse the real plan root directly

        entries, root_ir_ptr, _ = _traverse(
            tree, op_card, ptr_costs, used_ir_ptrs,
            ir_nodes, scan_map, join_map, unary_map,
            table_name_to_id, subquery_filters, ctx,
            ir_table_coverage,
        )

        # Insert any missing IR PTRs
        missing = set(ir_nodes.keys()) - used_ir_ptrs
        if missing:
            entries = _insert_missing_ptrs(entries, missing, ir_nodes, ptr_costs, root_ir_ptr)

        # Append Result as the final (outermost) entry.
        # Use the traversal root (not entries[-1]) so that missing-PTR fallbacks
        # appended by _insert_missing_ptrs don't hijack the Result pointer.
        if result_card is not None and entries:
            result_input = root_ir_ptr if root_ir_ptr is not None else entries[-1][0]
            synth = _get_synth_ptr(ir_nodes, used_ir_ptrs)
            used_ir_ptrs.add(synth)
            ptr_costs[synth] = result_card
            entries.append((synth, "Result", None, [f"[PTR_{result_input}]"]))

        if not entries:
            return None

        # Assign predicates to operators
        pred_annotations: dict[int, dict[str, list[int]]] = {}
        if pred_registry:
            pred_annotations = _assign_predicates(entries, pred_registry, ctx, table_name_to_id)

        # Build per-PTR cardinality array (actual rows, default 1)
        cardinalities = [ptr_costs.get(e[0], 1) for e in entries]

        # Flatten to token string
        lines = ["[PHYSICAL_PLAN]"]
        for ir_ptr, phys_op, join_type, child_ptrs in entries:
            tok = f"  [PTR_{ir_ptr}] {phys_op}"
            if join_type:
                tok += f" {join_type}"
            for role, preds in sorted(pred_annotations.get(ir_ptr, {}).items()):
                tok += f" [{role} {' '.join(f'P_{p}' for p in preds)}]"
            lines.append(tok)
            if child_ptrs is not None:
                lines.append(f"    [INPUT] {' '.join(child_ptrs)}")
        lines.append("[/PHYSICAL_PLAN]")
        return ("\n".join(lines), cardinalities)

    except Exception as e:
        print(f"Error tokenizing physical plan: {e}")
        return None


def tokenize_training_jsonl(
    input_jsonl: Path,
    output_jsonl: Path,
    schema_path: Path,
    engine_key: str = "postgres@12.5",
) -> int:
    """Batch re-tokenize existing training JSONL.

    Reads each row, tokenizes the physical plan for ``engine_key``,
    adds ``ir_physical_plan_token`` field, and writes to ``output_jsonl``.

    Returns count of successfully tokenized rows.
    """
    table_name_to_id = load_table_name_to_id(schema_path)
    count = 0

    output_jsonl.parent.mkdir(parents=True, exist_ok=True)
    with open(input_jsonl) as fin, open(output_jsonl, "w") as fout:
        for line in fin:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            ir_logical = row.get("ir_logical_token", "")
            phys_map = row.get("ir_physical_token", {})
            raw_phys = phys_map.get(engine_key)

            pred_reg = row.get("pred_registry")
            phys_token_strs: dict[str, str] = {}
            phys_cardinalities: dict[str, list[int]] = {}
            if raw_phys and ir_logical:
                result = tokenize_physical_plan(ir_logical, raw_phys, table_name_to_id,
                                                pred_registry=pred_reg)
                if result:
                    tok, cards = result
                    phys_token_strs[engine_key] = tok
                    phys_cardinalities[engine_key] = cards
                    count += 1

            row["ir_physical_plan_token"] = phys_token_strs
            row["ir_physical_plan_cardinalities"] = phys_cardinalities
            fout.write(json.dumps(row))
            fout.write("\n")

    return count
