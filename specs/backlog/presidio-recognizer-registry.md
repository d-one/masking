---
ticket: TBD
links:
  - src/masking/utils/entity_detection.py
  - tests/utils/test_entity_detection.py
owner: petmarco7
status: draft
affected_components:
  - src/masking/utils/entity_detection.py
  - tests/utils/test_entity_detection.py
assumptions: []
out_of_scope:
  - Adding or changing recognizers.
---

# Bugfix: Custom and Filtered Presidio Registries Are Ignored

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: TBD
- Related specs: none

## Behavior

|           | Description                                                                                                                                                                                                                                                                                              |
| --------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Current   | (1) `PresidioRegistry` calls `remove_recognizer()` with a recognizer object, but Presidio expects a name, so region-specific recognizers are never removed. (2) `PresidioMultilingualAnalyzer` builds a registry but never passes it to `AnalyzerEngine`, so a custom `registry` argument has no effect. |
| Expected  | Region-specific recognizers listed for exclusion are absent from the registry, and the analyzer uses the registry it was given.                                                                                                                                                                          |
| Unchanged | Default models, `ner_model_mapping`, supported languages, and entity detection results for the default registry.                                                                                                                                                                                         |

## Root Cause

Confirmed by the two strict xfail tests in `tests/utils/test_entity_detection.py`: `test_region_specific_recognizers_are_removed` and `test_custom_registry_is_used`.

## Acceptance Criteria

1. **AC1** — Both xfail tests pass with the `xfail` marker removed.
1. **AC2** — Removing region-specific recognizers changes which entities the default analyzer detects. [NEEDS CLARIFICATION: Is that change in masking output for free text acceptable, and should the PR list the removed recognizers?]
1. **AC3** — All other tests in `tests/utils/` still pass.

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers remain
- [x] Root cause is confirmed, not guessed
- [x] Fix is limited to the root cause; no unrelated refactoring
