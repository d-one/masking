---
ticket: TBD
links:
  - AGENTS.md
owner: petmarco7
status: draft
affected_components:
  - tests/base_operations/test_operation_unique_values.py
  - tests/generate_hashed_tests.py
  - tests/mask_spark/test_pipeline_equivalence.py
assumptions: []
out_of_scope:
  - Changes to `src/`.
  - New tests for behavior covered by other specs.
---

# Feature: Test-Suite Hygiene

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: [backends-substitution-type-equivalence.md](backends-substitution-type-equivalence.md)

## Problem

| Issue                                                                                                                 | Impact                            |
| --------------------------------------------------------------------------------------------------------------------- | --------------------------------- |
| `test_unique_masking_is_linear` asserts 50,000 calls finish in under 2 s of wall-clock time.                          | Flaky on slow or busy CI runners. |
| `tests/generate_hashed_tests.py` is a generator script, not a test; AGENTS.md places one-off tools in `scripts/`.     | Collected by pytest; misplaced.   |
| `tests/mask_spark/test_pipeline_equivalence.py` compares pandas with Spark only; no test compares polars with either. | polars divergence goes unnoticed. |

## Proposed Behavior

- The linearity test compares the time for n and 4n calls (ratio well below quadratic) instead of an absolute limit.
- The generator script lives in `scripts/` and the expected hashes in `test_operation_hash.py` stay unchanged.
- The equivalence test runs the same configurations on pandas, polars, and Spark.

## Acceptance Criteria

1. **AC1** — The linearity test has no absolute time limit and fails when `_check_mask_line` is made quadratic.
1. **AC2** — `tests/` contains no non-test scripts; `uv run python scripts/generate_hashed_tests.py` still prints the expected hashes.
1. **AC3** — Every deterministic operation in the equivalence test is also checked on polars.

## Tests

| AC  | Test type   | Location                                                |
| --- | ----------- | ------------------------------------------------------- |
| AC1 | unit        | `tests/base_operations/test_operation_unique_values.py` |
| AC2 | manual      | `scripts/generate_hashed_tests.py`                      |
| AC3 | equivalence | `tests/mask_spark/test_pipeline_equivalence.py`         |
