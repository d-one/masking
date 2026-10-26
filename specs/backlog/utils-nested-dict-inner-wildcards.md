---
ticket: "#9"
links:
  - src/masking/utils/multi_nested_dict.py
  - docs/operations/operation_dict.md
owner: petmarco7
status: draft
affected_components:
  - src/masking/utils/multi_nested_dict.py
  - tests/utils/test_multi_nested_dict.py
assumptions: []
out_of_scope:
  - New path syntax.
---

# Bugfix: `[*]` Wildcards Inside Allow/Deny Keys Never Match

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#9](https://github.com/d-one/masking/issues/9)
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                 |
| --------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | `allow_keys=["items.[*].name"]` does not match `items.[0].name`; only a leading `[*]` works. The dict operations then mask (or keep) fields the user excluded (or allowed). |
| Expected  | A `[*]` at any position in an allow or deny key matches every list index at that position.                                                                                  |
| Unchanged | Leading `[*]` wildcards, exact keys, and callable matchers.                                                                                                                 |

## Root Cause

Confirmed by the strict xfail test `test_inner_bracket_wildcard_skips_key` in `tests/utils/test_multi_nested_dict.py`: a key is only considered when it starts with the current key path, which is never true for `items.[*]` against `items.[0]`.

## Acceptance Criteria

1. **AC1** — The xfail test passes with the `xfail` marker removed.
1. **AC2** — A `MaskDictOperation` and a `StringMatchDictOperation` test show an inner-wildcard allow key in effect in every backend.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
