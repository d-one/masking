---
ticket: "#14"
links:
  - docs/operations/operation_null.md
  - src/masking/base_operations/operation_null.py
owner: petmarco7
status: draft
affected_components:
  - src/masking/mask/operations/
  - src/masking/mask_polars/operations/
  - src/masking/mask_spark/operations/
  - docs/operations/operation_null.md
assumptions:
  - The documented behavior in `docs/operations/operation_null.md` is the intended behavior.
out_of_scope:
  - Changes to `NullOperationBase` beyond what the backends need.
  - Changes to other operations or to the pipeline.
---

# Bugfix: `null` Operation Has No Backend Implementation

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#14](https://github.com/d-one/masking/issues/14)
- Related specs: [docs-user-documentation-refresh.md](docs-user-documentation-refresh.md)

## Behavior

|           | Description                                                                                                                                                                                                                                                                                                                                                 |
| --------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | `docs/operations/operation_null.md` shows `from masking.mask.operations.operation_null import NullOperation`, which raises `ModuleNotFoundError`. `NullOperationBase` exists in `src/masking/base_operations/operation_null.py`, but no `NullOperation` exists in `mask/`, `mask_polars/`, or `mask_spark/`, so the operation cannot be used in a pipeline. |
| Expected  | `NullOperation` exists in `mask/`, `mask_polars/`, and `mask_spark/` and behaves as documented in `docs/operations/operation_null.md`.                                                                                                                                                                                                                      |
| Unchanged | All other operations, pipelines, and concordance tables behave as today.                                                                                                                                                                                                                                                                                    |

## Root Cause

Confirmed: `NullOperationBase` was added in `a50cc24` without the thin backend classes that every other operation has (for example `HashOperation(HashOperationBase, PandasOperation)`).

## Acceptance Criteria

1. **AC1** — A regression test per backend imports `NullOperation` and masks a column so that every value becomes null when no `condition` is given.
1. **AC2** — With a `condition`, only values for which `condition(str(value))` is `True` become null; the others are unchanged in every backend. \[NEEDS CLARIFICATION: `_mask_line` returns `str(line)` for kept values, turning numbers into strings; is that intended, or should kept values keep their type?\]
1. **AC3** — Masking a column with `NullOperation` in a pipeline produces a concordance table whose masked values for nulled entries are null. \[NEEDS CLARIFICATION: should nulled entries appear in `pipeline.concordance_tables` at all?\]
1. **AC4** — Existing tests still pass.
1. **AC5** — `docs/operations/operation_null.md` shows working imports for every backend.

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
