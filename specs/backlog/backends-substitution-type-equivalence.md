---
ticket: TBD
links:
  - src/masking/mask/pipeline.py
  - src/masking/mask_polars/pipeline.py
  - src/masking/mask_spark/pipeline.py
owner: petmarco7
status: draft
affected_components:
  - src/masking/mask/pipeline.py
  - src/masking/mask_polars/pipeline.py
  - src/masking/mask_spark/pipeline.py
  - tests/mask_spark/test_pipeline_equivalence.py
assumptions:
  - AGENTS.md requires the three backends to be behaviorally equivalent.
out_of_scope:
  - Changing which values a concordance table stores.
  - Output dtypes of operations themselves (all return strings).
---

# Bugfix: Backends Disagree on Values Missing from the Concordance Table

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none

## Behavior

A concordance table stores only values whose masked value differs from the clear value, so substitution meets values that are not in the table.

|           | Description                                                                                                                                                                                                                                                                    |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Current   | pandas keeps the original object (`table.get(x, x)`), giving mixed-type `object` columns; polars and Spark return `str(x)`. In pandas, an integer column with nulls is a float column, so unmapped values come out as floats (`2.0` instead of `"2"`).                         |
| Expected  | For the same input and configuration, the three backends return identical masked columns, including the type of unmapped values and nulls. [NEEDS CLARIFICATION: Should unmapped values become strings in every backend (polars/Spark behavior) or keep their original value?] |
| Unchanged | Values in the table are replaced by their masked value; nulls stay null; column order and names.                                                                                                                                                                               |

## Root Cause

Confirmed: `mask/pipeline.py` substitutes with `concordance_tables[col_name].get(x, x)`; `mask_polars/pipeline.py` uses `t.get(x, str(x))` with `return_dtype=pl.Utf8`; `mask_spark/pipeline.py` uses a join with `coalesce(masked, cast(string))` or a UDF with `table.get(x, str(x))`.

## Acceptance Criteria

1. **AC1** — An equivalence test runs one configuration on pandas, polars, and Spark over integer (with and without nulls), date, and string columns whose concordance table misses some values, and asserts identical output values and types.
1. **AC2** — The pandas output of an integer column with nulls contains no float renderings such as `2.0`.
1. **AC3** — `docs/available_modules.md` states the type of unmapped values.
1. **AC4** — The PR states which backend's output changes for existing inputs (AGENTS.md: changing masking output needs approval).

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
