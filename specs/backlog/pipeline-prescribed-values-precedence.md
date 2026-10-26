---
ticket: "#10"
links:
  - src/masking/base_concordance_table/concordance_table.py
  - docs/available_modules.md
owner: petmarco7
status: draft
affected_components:
  - src/masking/base_concordance_table/concordance_table.py
  - tests/base_concordance_table/test_concordance_table.py
assumptions:
  - Prescribed concordance tables are meant to fix the masked value of the listed clear values.
out_of_scope:
  - The format of prescribed concordance tables.
---

# Bugfix: Operation Output Overrides Prescribed Masked Values

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#10](https://github.com/d-one/masking/issues/10)
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                    |
| --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | `ConcordanceTableBase` hands prescribed values to the operation and trusts it to keep them. When the operation returns a different masked value for a prescribed clear value, that value wins. |
| Expected  | For every prescribed clear value, the concordance table and the masked column contain the prescribed masked value, whatever the operation returns.                                             |
| Unchanged | Prescribed tables passed to the backends' pipelines today, where all operations already honour them (`TestPrescribedValuesOverrideMasking`).                                                   |

## Root Cause

Confirmed by the strict xfail test `test_prescription_wins_over_operation_output` in `tests/base_concordance_table/test_concordance_table.py`. \[NEEDS CLARIFICATION: Can any shipped operation return a different value for a prescribed key, or is this only reachable with custom operations? If only custom operations, is a guard in `ConcordanceTableBase` still wanted?\]

## Acceptance Criteria

1. **AC1** — The xfail test passes with the `xfail` marker removed.
1. **AC2** — `TestPrescribedValuesOverrideMasking` still passes for every backend.

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
