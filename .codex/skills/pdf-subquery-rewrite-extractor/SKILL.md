---
name: pdf-subquery-rewrite-extractor
description: Extract subquery rewrite examples from optimizer papers and PDF docs, including the original SQL, rewritten SQL, unnesting logic, semantic preconditions, and query_curriculum mapping. Use when the user asks to read papers like subquery_papers/oracle.pdf or similar PDFs and turn the examples into structured records for downstream query template generation.
---

# PDF Subquery Rewrite Extractor

Extract SQL rewrite examples from optimizer PDFs and convert them into structured records that can seed `query_curriculum` subquery/template work.

## When to use

Use this skill when the user wants:
- SQL examples pulled from papers or optimizer docs in PDF form
- before/after SQL rewrite pairs
- the logic behind unnesting, decorrelation, coalescing, or related rewrites
- records that can be consumed downstream for `query_curriculum`

Do not use it for:
- generic paper summaries with no SQL extraction goal
- benchmarking or executing the extracted SQL
- rewriting `query_curriculum` code unless the user asks for that separately

## Workflow

1. Identify the target PDF and a short paper id.
2. Extract text from the PDF with the best available local tool:
   - Prefer `pdftotext -layout <pdf> -`
   - Fallback: `gs -q -dNOPAUSE -dBATCH -sDEVICE=txtwrite -sOutputFile=- <pdf>`
   - Last resort: `strings -n 8 <pdf>`
3. Search the extracted text for rewrite-heavy terms:
   - `unnest`
   - `decorrel`
   - `coalesc`
   - `semijoin`
   - `antijoin`
   - `derived`
   - `scalar`
   - `exists`
   - `not exists`
   - `in (`
   - `group by`
   - `intersect`
   - `minus`
4. Locate example queries and pair the original SQL with the rewritten SQL.
5. For each pair, write a structured record using the contract in [output_contract.json](references/output_contract.json).
6. Map each rewrite to the nearest `query_curriculum` family:
   - `exists` -> `subquery_type=exists`, `stage_id=2_table_semi`, `rule_family=correlated_exists`
   - `not_exists` -> `subquery_type=not_exists`, `stage_id=2_table_anti`, `rule_family=correlated_not_exists`
   - `in_subquery` -> `subquery_type=in_subquery`, `stage_id=1_table_sub`, `rule_family=in_subquery`
   - `derived` -> `subquery_type=derived`, `stage_id=2_table_derived`, `rule_family=derived_table`
   - `scalar` -> `subquery_type=scalar`, `stage_id=future`, `rule_family=scalar_subquery`
   - `coalescing`, `setop rewrite`, `window-function rewrite`, `view merge`, `subquery removal` -> `stage_id=future`
7. Mark whether the example is usable now or needs generator support later:
   - `generator_fit=exact` for current `query_curriculum/subquery.py` coverage
   - `generator_fit=approximate` for similar but not identical support
   - `generator_fit=future` for unsupported rewrite families
8. Return the records as JSON only unless the user explicitly asks for prose too.

## Extraction rules

- Preserve SQL meaning over formatting. Normalize whitespace lightly, but do not silently change predicates or query shape.
- Keep both `before_sql` and `after_sql` when both are visible.
- If the paper shows only one side of the rewrite, do not invent the missing SQL.
- If you infer a normalized or canonical form, store it separately in `normalized_before_sql` or `normalized_after_sql` and mark `sql_source=inferred_normalization`.
- Keep page numbers or page hints whenever available.
- Capture the reason the rewrite is valid in one tight paragraph in `unnesting_logic`.
- Record semantic requirements in `semantic_preconditions`, not buried in prose.
- Prefer paper terminology in `transformation_name`, but map it to repo terminology in `query_curriculum_mapping`.

## What counts as "unnesting logic"

Capture the optimizer reason the rewrite is valid and useful, such as:
- correlated subquery becomes semijoin because the correlation predicate matches FK/PK-style join semantics
- `NOT EXISTS` becomes antijoin because absence can be tested set-at-a-time
- scalar aggregate subquery becomes join plus aggregate because aggregation can be computed per correlation key
- derived table or inline view can be merged when the view does not block equivalent join semantics
- multiple subqueries can be coalesced when equivalence or containment conditions hold
- a rewrite is blocked because null semantics, duplicate sensitivity, or cardinality checks would change results

## Validation checklist

- The before/after pair expresses the same result set semantics, or the record explicitly says why equivalence is only partial.
- Correlation columns and join keys are called out explicitly.
- The mapped `subquery_type`, `stage_id`, and `rule_family` are consistent with `query_curriculum/subquery.py`.
- Unsupported but useful examples are retained with `generator_fit=future`.
- No missing SQL is hallucinated.

## Output expectations

Default output is a JSON array of records following [output_contract.json](references/output_contract.json).

If the user asks for a compact summary too, append a short flat list:
- rewrite families found
- exact-vs-future generator fit counts
- the highest-value examples to operationalize first
