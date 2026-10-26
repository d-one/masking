---
ticket: TBD
links:
  - src/masking/base_operations/operation_fake_name.py
  - docs/operations/operation_fake_name.md
owner: petmarco7
status: draft
affected_components:
  - src/masking/base_operations/operation_fake_name.py
  - tests/base_operations/test_operation_fake_name.py
assumptions:
  - The documented behavior ("reuses previously generated names") is the intended behavior.
out_of_scope:
  - Changes to `reuse_existing=False`.
  - Changes to the name providers in `src/masking/faker/name.py`.
---

# Bugfix: `FakeNameOperation` with `reuse_existing=True` Outputs Clear Input Names

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                                                                                                                       |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | With `reuse_existing=True` (the default), the operation adds every **clear input** to `_SEEN_INPUTS` and, on about half the rows, returns a random entry from that pool. Masking 200 synthetic names `Clear0`…`Clear199` with the pandas pipeline outputs 91 of the clear names as masked values. |
| Expected  | Masked values never contain a clear input value of the column. Reused names come from names the operation generated earlier, as `docs/operations/operation_fake_name.md` states.                                                                                                                  |
| Unchanged | `reuse_existing=False`; gender and `name_type` handling; uniqueness of masked values per concordance table; thread safety of the shared pool.                                                                                                                                                     |

## Root Cause

Confirmed: `FakeNameBase._mask_like_faker` calls `self._SEEN_INPUTS.add(line)` with the clear `line` and later returns `self._SEEN_INPUTS.choice()`, so the pool holds clear inputs instead of generated names.

## Acceptance Criteria

1. **AC1** — A regression test masks a column of synthetic names with `reuse_existing=True` and asserts that no masked value equals any clear value of the column, in every backend.
1. **AC2** — With `reuse_existing=True`, the same fake name can appear as the masked value of more than one clear input over a run. [NEEDS CLARIFICATION: Is reuse across different clear inputs the intended "realistic distribution", or should each fake name map to at most one clear value per column?]
1. **AC3** — The pool is shared across columns and operations as today. [NEEDS CLARIFICATION: Should the pool stay shared across columns, or become per operation instance?]
1. **AC4** — Existing tests for gender, `name_type`, and thread safety still pass; `test_seen_input_is_reused_and_removed` is rewritten to assert the documented behavior.
1. **AC5** — The PR states that masked output for `reuse_existing=True` changes for existing inputs (AGENTS.md: changing masking output needs approval).

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
