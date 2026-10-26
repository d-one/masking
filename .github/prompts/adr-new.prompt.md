---
description: Record an Architecture Decision Record in decisions/
agent: agent
argument-hint: The decision to record and its context
---

Record an ADR for: ${input:decision:decision and context}

1. Read [decisions/README.md](../../decisions/README.md) and existing ADRs; check whether this decision extends or supersedes one.
1. Copy [decisions/adr-template.md](../../decisions/adr-template.md) to `decisions/NNNN-<kebab-title>.md` using the next free number.
1. Set `Status: Proposed`. Fill Context (constraints, references), Decision (one clear statement), and Consequences (tradeoffs, code placement, follow-ups).
1. If it supersedes an ADR, set the old one to `Superseded by NNNN` and link both ways.
1. Update `AGENTS.md` or docs if the decision changes agent rules or code placement.
1. Keep it short. Stop for human acceptance.
