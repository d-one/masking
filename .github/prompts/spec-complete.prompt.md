---
description: Close out an implemented spec - status, archive or delete, docs
agent: agent
argument-hint: Path to the spec, e.g. specs/backlog/<name>.md
---

Complete the spec: ${input:spec:path to spec in specs/backlog/}

1. Verify every acceptance criterion is covered by a passing test. If not, stop and list the gaps.
1. Set `status: implemented`.
1. Decide the spec's future:
   - It defines an enduring contract (interfaces, invariants, data shapes) → rewrite it to describe the resulting behavior only, remove plans/notes/open questions, and move it to `specs/completed/`.
   - Otherwise → delete it; the PR and ticket keep the history.
1. Update links to the spec across the repository.
1. Ensure module READMEs and `docs/` reflect the current behavior and contain no superseded statements.
1. Summarize what moved, what was deleted, and which docs changed.
