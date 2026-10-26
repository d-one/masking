---
ticket: "#24"
links:
  - decisions/0003-null-operation-in-pipelines.md
owner: petmarco75
status: in progress
affected_components:
  - src/masking/base_concordance_table/concordance_table.py
  - src/masking/base_pipeline/pipeline.py
  - src/masking/mask/pipeline.py
  - src/masking/mask_polars/pipeline.py
  - src/masking/mask_spark/pipeline.py
  - docs/operations/operation_null.md
assumptions:
  - Decision A from #24, recorded in ADR 0003.
out_of_scope:
  - Null masked values in `pipeline.concordance_tables`.
  - Direct `NullOperation` calls, which keep returning kept values as strings.
---

# Feature: Support NullOperation in Pipelines

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#24](https://github.com/d-one/masking/issues/24)
- ADR: [0003](../../decisions/0003-null-operation-in-pipelines.md)

## Problem

Pipelines reject `NullOperation` with a `TypeError`, because a concordance table cannot express "mask to null".

## Proposed Behavior

Pipelines null the values a `NullOperation` selects directly and add no table entries for them; see ADR 0003.

## Acceptance Criteria

1. **AC1** — A pipeline with a `NullOperation` nulls the whole column on pandas, polars, and Spark, and `pipeline.concordance_tables[col]` is empty.
1. **AC2** — With a `condition`, only matching values are nulled; other values are unchanged; the backends agree.
1. **AC3** — In a chain, a `NullOperation` before or after another operation nulls the selected values, the other values get the other operation's output, and nulled clear values are absent from the column's table; the backends agree.
1. **AC4** — A second call returns the same output, and a `NullOperation` writing to a new output column keeps the input column unchanged.
1. **AC5** — `ConcordanceTable(NullOperation)` returns an empty table; the tests asserting the old `TypeError` now assert the supported behavior.

## Tests

| AC      | Test type   | Location                                        |
| ------- | ----------- | ----------------------------------------------- |
| AC1-AC4 | equivalence | `tests/mask_spark/test_pipeline_equivalence.py` |
| AC4-AC5 | unit        | `tests/mask*/test_pipeline.py`                  |
