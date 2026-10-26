# ADR 0002: Spec-Driven Agentic Development

## Status

Accepted

## Context

Coding agents produce code quickly but drift from intent when requirements are implicit. Reviewers need a stable reference to judge whether a change is correct and in scope.

## Decision

Non-trivial changes follow a spec cycle: draft spec → human approval → implementation with tests → completion. Specs live in `specs/`, carry a `status` lifecycle, and only a human moves a spec to `ready for development`. `AGENTS.md` is the single source of agent rules; tool-specific files (`CLAUDE.md`, `.github/copilot-instructions.md`) only point to it.

## Consequences

- Each PR links one spec (or a slice of one) and maps acceptance criteria to tests.
- Implemented specs are moved to `specs/completed/` when they remain enduring contracts, otherwise deleted.
- Agent rules stay consistent across tools because they exist in one file.
