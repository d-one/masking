---
ticket: TBD
links:
  - docs/security/README.md
owner: petmarco7
status: draft
affected_components:
  - src/masking/base_operations/operation_fake.py
  - src/masking/base_operations/operation_yyyy_hash.py
  - src/masking/utils/multi_nested_dict.py
  - src/masking/faker/date.py
assumptions:
  - Exception messages end up in logs, notebooks, and job UIs that are less protected than the source data.
out_of_scope:
  - Exceptions raised by third-party libraries (Presidio, dateparser, Spark).
  - Logging; `src/masking` has no logging calls today.
---

# Bugfix: Exception Messages Contain Clear Values

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                          |
| --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Current   | Four exceptions interpolate clear data into their message (see Root Cause). A failing row prints the PII it was masking, for example `Could not parse date from line: <birth date>`. |
| Expected  | Exception messages name the column, operation, and failure, but contain no clear input value, no masked value, and no part of the input document.                                    |
| Unchanged | Exception types, when they are raised, and chaining with `from e`.                                                                                                                   |

## Root Cause

Confirmed by reading the code:

| Location                                    | Message                                                                   |
| ------------------------------------------- | ------------------------------------------------------------------------- |
| `base_operations/operation_fake.py:70`      | `Unable to mask the line {line} after … attempts for column …`            |
| `base_operations/operation_yyyy_hash.py:35` | `Could not parse date from line: {line}. Error: {e}`                      |
| `utils/multi_nested_dict.py:403`            | `Key {key} not found in {d} with data {data}` (the whole JSON record)     |
| `faker/date.py:170`                         | `… within the ranges {dates_ranges}` (bounds derived from the clear date) |

`{e}` from dateparser can also echo the input.

## Acceptance Criteria

1. **AC1** — For each location above, a regression test triggers the exception with a synthetic sentinel value and asserts the sentinel does not appear in `str(exc)` or in the chained exception's message.
1. **AC2** — Each message still names the column (where known) and the reason for the failure.
1. **AC3** — `docs/security/README.md` states that exception messages contain no clear values.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
