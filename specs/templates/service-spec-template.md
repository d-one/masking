---
ticket: PROJ-0000
links: []
owner: TODO
status: draft
affected_components: []
assumptions: []
out_of_scope: []
---

# Service: TODO

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket:
- Related specs:
- Related ADRs:

## Purpose

Service responsibility and explicit boundaries: what it owns and what it does not.

## Interfaces

| Kind                               | Name | Contract |
| ---------------------------------- | ---- | -------- |
| API / event / storage / dependency | …    | …        |

## Behavior

Expected behavior, failure handling, idempotency, and limits.

## Acceptance Criteria

1. **AC1** — …

## Operations

Configuration, secrets handling (names only, never values), observability, and deployment notes.

## Tests

Unit, integration, contract, and deployment-validation coverage per acceptance criterion.

## Open Questions

- …

## Spec Quality

- [ ] No `[NEEDS CLARIFICATION]` markers or open questions remain
- [ ] Every acceptance criterion is testable and observable from outside the code
- [ ] Boundaries (what the service owns and does not own) are explicit
- [ ] No speculative or "might need" features; nothing future-proofed
- [ ] Uses existing framework primitives; any new abstraction is justified
