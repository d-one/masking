---
ticket: TBD
links:
  - src/masking/utils/string_match_handler.py
  - tests/utils/test_string_match_handler.py
owner: petmarco7
status: draft
affected_components:
  - src/masking/utils/string_match_handler.py
  - tests/utils/test_string_match_handler.py
assumptions: []
out_of_scope:
  - New date formats.
---

# Bugfix: String Matching Masks `"None"` and Ignores `datetime` Values

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                                                                                                                   |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | (1) `_get_pii_values` turns a missing or `None` PII column into the literal `"None"`, so the word "None" in the text is masked. (2) `_get_pattern_date` is annotated to accept `datetime`, but passes it to `dateparser.parse`, which accepts strings only, so no date patterns are produced. |
| Expected  | Missing or `None` PII columns are skipped. A `datetime` or `date` PII value produces the same date patterns as its ISO string.                                                                                                                                                                |
| Unchanged | Patterns and scores for string values; `re.escape` on every value.                                                                                                                                                                                                                            |

## Root Cause

Confirmed by the two strict xfail tests in `tests/utils/test_string_match_handler.py`: `test_missing_and_none_columns_are_skipped` and the `datetime` input test at line 131.

## Acceptance Criteria

1. **AC1** — Both xfail tests pass with the `xfail` marker removed.
1. **AC2** — A `StringMatchOperation` test in each backend shows that the word "None" in the text is not masked when a PII column is null.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
