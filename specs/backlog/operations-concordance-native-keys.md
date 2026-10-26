---
ticket: "#27"
links: []
owner: petmarco75
status: in progress
affected_components:
  - src/masking/base_operations/operation.py
assumptions: []
out_of_scope:
  - Masking output and the key types of pipeline concordance tables.
  - Normalizing concordance table keys to strings.
---

# Bugfix: Pipelines Fail on a Second Call for Columns with Non-String Values

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#27](https://github.com/d-one/masking/issues/27)
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                                                      |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | Calling a pandas, polars, or Spark pipeline twice on an integer column (e.g. `MedStatsOperation` on `[1000, 2000]`) raises `TypeError: Invalid concordance table, expected a dictionary, got <class 'dict'>` on the second call. |
| Expected  | The second call returns the same masked values as the first.                                                                                                                                                                     |
| Unchanged | Masking output; the native key types of `pipeline.concordance_tables`; rejection of non-dict tables and non-string masked values.                                                                                                |

## Root Cause

Confirmed with a synthetic reproduction on all three backends. Pipeline concordance tables keep clear values in the column's native type, and `ConcordanceTableBase.__call__` hands them to `Operation.update_concordance_table`. That method requires string keys, unlike the operation constructor, which only requires string values. Its error message also claims a non-dict was given.

## Acceptance Criteria

1. **AC1** — Calling a pipeline twice on an integer column returns identical output on pandas, polars, and Spark, for `MedStatsOperation` and `HashOperation`.
1. **AC2** — `update_concordance_table` accepts non-string keys and still rejects non-dict tables and non-string values, with an accurate message.
1. **AC3** — Tests that required non-string keys to be rejected (`test_operation_core.py`, `test_concordance_table.py`) now cover non-string masked values instead, plus a test that non-string keys are accepted; all other tests pass unchanged.

## Decision

Option A from #27, chosen by the maintainer: accept native keys, matching the operation constructor. Normalizing keys to strings was rejected because it changes the `pipeline.concordance_tables` format for downstream users.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
