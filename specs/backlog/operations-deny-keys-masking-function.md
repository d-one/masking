---
ticket: "#29"
links: []
owner: petmarco75
status: in progress
affected_components:
  - src/masking/base_operations/operation_dict.py
assumptions:
  - Denied paths have no default masking; `docs/operations/operation_dict.md` already requires a `masking_function` for them.
out_of_scope:
  - A default masking for denied paths.
  - Masking output of valid configurations.
---

# Bugfix: Deny Keys Fail on Dict Operations without a Masking Function

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#29](https://github.com/d-one/masking/issues/29)
- Related specs: none

## Behavior

|           | Description                                                                                                                                                         |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | `StringMatchDictOperation(col_name="j", deny_keys=["a"])` is created, then masking a line with a denied path raises `TypeError: 'NoneType' object is not callable`. |
| Expected  | Creating a dict operation with `deny_keys` (strings or callables) and no `masking_function` raises `ValueError` naming the missing `masking_function`.              |
| Unchanged | Dict operations without `deny_keys` need no `masking_function`; configurations with both keep their masking output.                                                 |

## Root Cause

Confirmed with a synthetic reproduction. `DictOperationBase.masking_function` defaults to `None`, and `PresidioHandler.__init__` sets it only when one is passed. `_handle_denied_paths()` calls it for every denied path without checking.

## Acceptance Criteria

1. **AC1** — Creating a dict operation with string or callable `deny_keys` and no `masking_function` raises `ValueError`, on the base class and on the pandas, polars, and Spark `StringMatchDictOperation`.
1. **AC2** — A dict operation without `deny_keys` and without `masking_function` still masks lines.
1. **AC3** — Existing tests pass; tests that set `deny_keys` pass a `masking_function`.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
