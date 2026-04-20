# Physical Plan Token Vocabulary

This document defines every token that can appear in `ir_physical_plan_token`,
how each maps from `ir_physical_token` operator types, and which tokens are
shared vs. engine-specific.

Statistics are derived from the full PG `full_vn.training.jsonl` dataset
(81,532 records), re-tokenized live with the current tokenizer.
DuckDB counts are from the JOB validation slice (113 records); full-corpus
DuckDB counts will differ proportionally.

---

## Complete Token Vocabulary

### Tokens shared by both engines

| Token | Description | DuckDB occurrences | PG occurrences |
|---|---|---|---|
| `SeqScan` | Sequential table scan | — | 312,411 |
| `HashJoin` | Hash join | — | 200,931 |
| `Result` | Top-level pipeline result wrapper (always last) | — | 81,532 |
| `MergeJoin` | Sort-merge join | — | 17,139 |
| `NestedLoop` | Nested-loop join | — | 105,362 |
| `Sort` | Sort / ORDER BY (physical Sort present; two-phase agg Sort is transparent) | — | 14 |
| `HashAggregate` | Hash-based aggregate | — | 54 |
| `Filter` | Post-join or post-aggregate predicate filter | — (DuckDB JOB slice: 3) | — |
| `SetOp` | Set operation: `[UNION_ALL]`, `[INTERSECT]`, `[EXCEPT]` | — | 12 |
| `Window` | Window function | — | 19 |
| `Limit` | `LIMIT` clause | — | 385 |

### DuckDB-only tokens

| Token | Description | DuckDB occurrences |
|---|---|---|
| `EmptyResult` | Optimizer short-circuit: query provably returns zero rows | — (full-corpus count TBD) |

### PostgreSQL-only tokens

| Token | Description | PG occurrences |
|---|---|---|
| `IndexScan` | B-tree / GiST index scan with heap fetch | 51,834 |
| `IndexOnlyScan` | Index-only scan (no heap fetch) | 42,006 |
| `BitmapHeapScan` | Bitmap heap scan — heap fetch driven by a bitmap built by child `Bitmap Index Scan` | — |
| `Materialize` | Explicit result materialization (CTE / subquery boundary) | 8,927 |
| `Map` | PG `Result` node — constant projection or one-time filter | 357 |
| `SubqueryScan` | FROM-clause inline view / each branch of UNION-INTERSECT-EXCEPT | 24 |
| `Append` | `UNION ALL` append of N subplan branches | 24 |

---

## Token Grammar

Every token entry in `[PHYSICAL_PLAN]` follows this format:

```
  [PTR_N] TokenName [optional_annotation]
    [INPUT] [PTR_x] ...          (omitted for leaf nodes)
```

### Annotations by token

| Token | Possible annotations | Example |
|---|---|---|
| `SeqScan` | `[FILTER P_n ...]` | `[PTR_0] SeqScan [FILTER P_0 P_1]` |
| `IndexScan` | `[INDEX_COND P_n ...]` | `[PTR_0] IndexScan [INDEX_COND P_0]` |
| `IndexOnlyScan` | `[INDEX_COND P_n ...]` | `[PTR_0] IndexOnlyScan [INDEX_COND P_0]` |
| `BitmapHeapScan` | `[FILTER P_n ...]` | `[PTR_0] BitmapHeapScan [FILTER P_0]` |
| `HashJoin` | `[INNER\|LEFT\|RIGHT\|FULL\|SEMI\|ANTI]`, `[HASH_COND P_n ...]` | `[PTR_2] HashJoin [INNER] [HASH_COND P_0]` |
| `MergeJoin` | join type, `[MERGE_COND P_n ...]` | `[PTR_2] MergeJoin [LEFT] [MERGE_COND P_0]` |
| `NestedLoop` | join type, `[JOIN_FILTER P_n ...]` | `[PTR_2] NestedLoop [INNER]` |
| `Filter` | `[FILTER P_n ...]` | `[PTR_3] Filter [FILTER P_0]` |
| `SetOp` | `[UNION_ALL]`, `[INTERSECT]`, `[EXCEPT]` | `[PTR_6] SetOp [EXCEPT]` |
| All others | none | `[PTR_1] Result` |

### Input conventions

| Token | INPUT format | Notes |
|---|---|---|
| `SeqScan`, `IndexScan`, `IndexOnlyScan` | `[T{id}]` | Table ID from schema |
| `EmptyResult` | `[T{id}]` if table known, else omitted | Zero-row scan |
| `HashJoin`, `MergeJoin`, `NestedLoop` | `[PTR_left] [PTR_right]` | Two operator inputs |
| `Append` | `[PTR_0] [PTR_1] ... [PTR_n]` | N branch inputs |
| All unary operators | `[PTR_x]` | Single input |
| `Result` | `[PTR_x]` | Always wraps the final operator |

---

## Mapping from `ir_physical_token` → `ir_physical_plan_token`

### DuckDB

| `ir_physical_token` operator_type | Underlying DuckDB node | → token(s) emitted |
|---|---|---|
| `TableScan` | `SEQ_SCAN` | `SeqScan` |
| `Select` (above scan) | `FILTER` — predicate on table scan | *(transparent)* — predicate fuses into `SeqScan [FILTER P_n]` |
| `Select` (above join/agg) | `FILTER` — post-join or post-agg predicate | `Filter` |
| `Join` | `HASH_JOIN`, `MERGE_JOIN`, `PIECEWISE_MERGE_JOIN`, `NESTED_LOOP_JOIN` | `HashJoin`, `MergeJoin`, `NestedLoop` |
| `GroupBy` | `HASH_GROUP_BY`, `STREAMING_GROUP_BY`, `SIMPLE_AGGREGATE` | `GroupAggregate`, `HashAggregate` |
| `Sort` | `ORDER_BY`, `TOP_N` | `Sort` |
| `Window` | `WINDOW`, `STREAMING_WINDOW` | `Window` |
| `SetOperation` | `UNION` | `SetOp [UNION_ALL]` |
| `CustomOperator` / `EmptyResult` | `EMPTY_RESULT` | `EmptyResult` |
| `CustomOperator` / `Limit` | `LIMIT`, `STREAMING_LIMIT` | `Limit` |
| `Result` | *(pipeline top-level wrapper)* | `Result` (always last) |
| `CustomOperator` / `Hash` | *(Hash build phase, absorbed into HashJoin)* | *(transparent)* |
| `CustomOperator` / `Gather` | *(parallel gather, execution artifact)* | *(transparent)* |
| `CustomOperator` / `Projection` | *(projection, fused)* | *(transparent)* |
| `PipelineBreakerScan` | `CTE_SCAN` | *(transparent — TODO)* |
| `Temp` | `CTE` | *(transparent — TODO)* |
| `CustomOperator` / `CrossProduct` | `CROSS_PRODUCT` | *(transparent — TODO)* |

### PostgreSQL

| `ir_physical_token` operator_type | Underlying PG node | → token(s) emitted |
|---|---|---|
| `TableScan` (seq) | `Seq Scan` | `SeqScan` |
| `TableScan` (index) | `Index Scan` | `IndexScan` |
| `TableScan` (index-only) | `Index Only Scan` | `IndexOnlyScan` |
| `TableScan` (bitmap) | `Bitmap Heap Scan` | `BitmapHeapScan` |
| `CustomOperator` / `Bitmap Index Scan` | `Bitmap Index Scan` *(index phase of bitmap scan, no heap access)* | *(transparent — child of `BitmapHeapScan`)* |
| `Join` | `Hash Join` | `HashJoin` |
| `Join` | `Merge Join` | `MergeJoin` |
| `Join` | `Nested Loop` | `NestedLoop` |
| `Sort` | `Sort`, `Incremental Sort` | `Sort` |
| `GroupBy` | `Aggregate` (hashed) | `HashAggregate` |
| `GroupBy` | `Aggregate` (sorted/plain) | `GroupAggregate` |
| `Window` | `WindowAgg` | `Window` |
| `SetOperation` | `SetOp` | `SetOp [INTERSECT\|EXCEPT]` |
| `CustomOperator` / `Limit` | `Limit` | `Limit` |
| `CustomOperator` / `Materialize` | `Materialize` | `Materialize` |
| `CustomOperator` / `Append` | `Append`, `Merge Append` | `Append` |
| `Map` | PG `Result` (one-time filter / const projection) | `Map` |
| `Subquery` | `Subquery Scan` | `SubqueryScan` |
| `Result` | *(pipeline top-level wrapper)* | `Result` (always last) |
| `CustomOperator` / `Hash` | `Hash` *(build side of HashJoin, absorbed)* | *(transparent)* |
| `CustomOperator` / `Gather` | `Gather`, `Gather Merge` *(parallel artifact)* | *(transparent)* |
| `CustomOperator` / `Memoize` | `Memoize` | *(transparent — TODO)* |
| `CustomOperator` / `BitmapOr` | `BitmapOr` | *(transparent — TODO)* |
| `CustomOperator` / `ProjectSet` | `ProjectSet` | *(transparent — TODO)* |
| `PipelineBreakerScan` | `CTE Scan` | *(transparent — TODO)* |
| `Temp` | *(CTE materialization)* | *(transparent — TODO)* |

---

## Notes

**`Filter` (DuckDB only, post-join context)**
DuckDB's `FILTER` is a distinct pipeline stage. When it sits above a `TableScan`
the predicate is fused into `SeqScan [FILTER P_n]` to match PG's format.
When it sits above a `Join` or aggregate it cannot be pushed down — a standalone
`Filter` node is emitted instead.

**`Result` placement**
`Result` is always the final entry in every plan. It is stripped from the plan
tree root before traversal and re-appended after `_insert_missing_ptrs`, so no
other operator can appear after it.

**`EmptyResult` (DuckDB only)**
Emitted when the DuckDB optimizer determines at planning time that the result
set is empty (e.g. a WHERE clause that provably matches no rows). The node is a
leaf — it has no operator children, only a table input.

**`Map` (PG only)**
PG's `Result` executor node used for constant projections and one-time filters
(e.g. `WHERE false`). Named `Map` in the token vocabulary to avoid collision with
the top-level `Result` wrapper.

**`SubqueryScan` (PG only)**
Marks the boundary of a FROM-clause inline view or each branch of a
`UNION`/`INTERSECT`/`EXCEPT` that PG wraps individually before appending.
Distinct from the `[SUBQUERY]` block in the logical IR, which represents a
correlated WHERE/EXISTS subquery that PG decorrelates into a `Join`.

**`Sort` — two-phase aggregate transparency**
PG sometimes executes an aggregate as two GroupBy passes separated by a Sort
(Sort-based aggregate). Both the Sort and the outer GroupBy are implementation
artifacts: the logical plan has a single `LogicalAggregate`. The tokenizer
collapses the pair into one `HashAggregate` token matching `LogicalAggregate`.
`Sort` tokens only appear for user-visible ORDER BY (when `LogicalSort` is
present in the IR and the physical Sort is not folded into a Limit).

**`BitmapHeapScan` (PG only)**
PG's two-phase bitmap scan: a `Bitmap Index Scan` child scans the index and builds an in-memory bitmap of matching TIDs; the `Bitmap Heap Scan` parent then fetches the corresponding heap pages. Only `Bitmap Heap Scan` is a `TableScan` in the parsed plan (it carries `Relation Name`). `Bitmap Index Scan` is parsed as a `CustomOperator` and is transparent to the tokenizer — it is silently consumed as a child, and `BitmapHeapScan` is the emitted token. Predicates are not currently annotated on `BitmapHeapScan` (the index condition is on the transparent child).

**Operators deferred for future tokenization**
The following appear in `ir_physical_token` but are currently transparent
(not emitted). They are candidates for a future iteration:
`DelimScan`, `CrossProduct`, `Memoize`, `BitmapOr`, `ProjectSet`,
`Function Scan`, `Values Scan`, `INOUT_FUNCTION`,
`PipelineBreakerScan` / `CTE Scan`, `Temp` / CTE materialization,
`Iteration` / `IterationScan` (recursive CTEs), `ArrayUnnest`.
