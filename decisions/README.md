# Architecture Decision Records

ADRs record repository-relevant technical decisions and the reasoning behind them. They drive code placement and conventions in [AGENTS.md](../AGENTS.md).

## Conventions

- File name: `NNNN-<kebab-title>.md`, numbered sequentially.
- Start from [adr-template.md](adr-template.md), or run the `/adr-new` prompt.
- Status: `Proposed` → `Accepted` (human) → optionally `Deprecated` or `Superseded by NNNN`.
- Never rewrite an accepted ADR's decision; supersede it with a new one.

## Index

| ADR                                             | Title                                                        | Status   |
| ----------------------------------------------- | ------------------------------------------------------------ | -------- |
| [0001](0001-record-architecture-decisions.md)   | Record architecture decisions                                | Accepted |
| [0002](0002-spec-driven-agentic-development.md) | Spec-driven agentic development                              | Accepted |
| [0003](0003-distribute-template-with-copier.md) | Distribute the template with Copier and a uv-installable CLI | Accepted |
