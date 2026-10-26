---
ticket: "#11"
links: []
owner: petmarco7
status: draft
affected_components:
  - src/masking/utils/hash.py
  - src/masking/utils/string_handler.py
  - src/masking/utils/presidio_handler.py
  - src/masking/faker/date.py
  - src/masking/faker/plz.py
assumptions: []
out_of_scope:
  - Hash values and other masking output.
---

# Bugfix: Input Validation Relies on `assert` and Small Contract Bugs in Utils

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#11](https://github.com/d-one/masking/issues/11)
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                                                                                                                                                                                                                                              |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Current   | (1) `hash.py:29`, `faker/date.py:35`, and `faker/plz.py:60,114` validate arguments with `assert`, which `python -O` removes. (2) `generate_unique_name("a", ["a"], max_iter=1)` raises although `"a_"` is free (off by one). (3) `hash_string` is annotated `-> str` but returns `None` for `None`. (4) `PresidioHandler.allow_list` and `deny_list` default to the tuple `(None,)` while annotated `list[str] \| None`. |
| Expected  | (1) Invalid arguments raise `TypeError` or `ValueError` with and without `-O`. (2) The last allowed iteration can return a unique name. (3) and (4) Annotations and defaults match the values.                                                                                                                                                                                                                           |
| Unchanged | Hash values, generated names for every case that works today, and fake dates and PLZs.                                                                                                                                                                                                                                                                                                                                   |

## Root Cause

Confirmed by reading the code; (2) is the strict xfail test `test_generate_unique_name_succeeds_on_last_iteration` in `tests/utils/test_string_handler.py`.

## Acceptance Criteria

1. **AC1** — For each `assert` above, a test passes invalid input and expects the documented exception type; `noqa: S101` comments are gone.
1. **AC2** — The `generate_unique_name` xfail test passes with the marker removed.
1. **AC3** — `hash_string(None, secret)` is annotated as returning `str | None`, and `PresidioHandler` class defaults match their annotations.
1. **AC4** — Existing hash and faker tests pass unchanged.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
