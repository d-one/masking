---
description: Draft a new repo-local spec in specs/backlog/ from a ticket or idea
agent: agent
argument-hint: Ticket ID/link and a short description of the feature
---

Draft a new spec for: ${input:request:ticket and short description}

1. Read [AGENTS.md](../../AGENTS.md), [specs/README.md](../../specs/README.md), related specs in `specs/`, and ADRs in `decisions/`.
1. Right-size the work per **Right-Sizing** in `specs/README.md`. If no spec is needed, say so and stop. If the request bundles several independently testable capabilities, propose one spec per capability and stop for confirmation.
1. Pick the closest template in `specs/templates/` (`bugfix-spec-template.md` for bugs, otherwise `feature-spec-template.md` unless another fits better).
1. Create `specs/backlog/<area>-<short-name>.md` (kebab-case) with `status: draft`.
1. Fill in what and why first (problem, behavior, acceptance criteria). Use only the request and existing repo sources; mark every unknown inline as `[NEEDS CLARIFICATION: question]` and list unverified facts under `assumptions`. Fill Design Notes only for decisions already made.
1. Write acceptance criteria as numbered, testable statements (`AC1`, `AC2`, ...). Make `out_of_scope` explicit.
1. Apply a simplicity check: no speculative features, no future-proofing, existing framework primitives first.
1. If the change needs an architecture decision, say so and propose an ADR title; do not write the ADR unless asked.
1. Fill the **Spec Quality** checklist honestly; leave unmet items unchecked.
1. Do not write implementation code. Stop and list every `[NEEDS CLARIFICATION]` marker for human review; the spec cannot be approved while any remain.
