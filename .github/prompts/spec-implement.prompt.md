---
description: Implement an approved spec from specs/backlog/ with tests
agent: agent
argument-hint: Path to the spec, e.g. specs/backlog/<name>.md
---

Implement the spec: ${input:spec:path to spec in specs/backlog/}

1. Start from a fresh session for this spec. Read [AGENTS.md](../../AGENTS.md), the spec, every ADR and spec it references, and the local `README.md` of each directory you will touch. Load nothing else unless a task needs it.
1. Check the spec `status`:
   - `ready for development` → set it to `in progress` and continue.
   - `in progress` → continue.
   - anything else, or any `[NEEDS CLARIFICATION]` marker remaining → stop and report; do not implement unapproved specs.
1. Map each acceptance criterion (`AC1`, `AC2`, ...) to the test(s) that will prove it. Work one criterion or slice at a time.
1. Write the tests first, then the smallest implementation that passes them. Stay inside `affected_components` and the spec's scope.
1. Run the relevant tests and linters; fix failures rather than weakening tests.
1. Update the READMEs/docs of touched modules to describe the resulting behavior.
1. If reality diverges from the spec, update the spec in the same change and flag the divergence in your summary. Record architecture-relevant decisions as ADRs.
1. Do not deploy or mutate remote systems. Do not commit secrets.
1. Self-check: re-read the spec and compare it with the final code and tests. Report each acceptance criterion as met or unmet, and list anything implemented that the spec does not ask for.
1. Finish with: AC → test mapping with met/unmet, files changed, out-of-scope follow-ups, and a filled-in PR description following [the PR template](../pull_request_template.md).
