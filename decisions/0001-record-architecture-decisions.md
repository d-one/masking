# ADR 0001: Record Architecture Decisions

## Status

Accepted

## Context

Humans and coding agents need to know why the codebase is shaped the way it is. Decisions buried in chat, tickets, or PR threads are invisible to agents working from the repository.

## Decision

Record architecture-relevant decisions as short ADRs in `decisions/`, using [adr-template.md](adr-template.md).

## Consequences

- Agents read relevant ADRs before editing and propose a new ADR when a change needs an architecture decision.
- Accepted ADRs are superseded, never rewritten.
