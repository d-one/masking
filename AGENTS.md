# Agent Instructions

These instructions apply to every coding agent (GitHub Copilot, Claude Code, Codex, Cursor, ...) and human working in this repository.

## Before Editing

- Read the directory's local `README.md` plus the relevant specs in `specs/` and ADRs in `decisions/`.
- Read [docs/development/coding-agent-workflow.md](docs/development/coding-agent-workflow.md) once per session.
- Write or update a repo-local spec before implementing non-trivial code. See [specs/README.md](specs/README.md).
- Only implement specs whose `status` is `ready for development` or `in progress`. If no approved spec exists for a non-trivial change, draft one and stop for review.

## Commands

| Task          | Command                                                                                  |
| ------------- | ---------------------------------------------------------------------------------------- |
| Install       | `make install` (installs uv if missing, `uv sync`, pre-commit hooks), then `apm install` |
| Test          | `uv run pytest tests`                                                                    |
| Lint / format | `uv run pre-commit run --all-files`                                                      |
| Build         | `uv build`                                                                               |

Spark tests need a local Java runtime. Presidio tests need the spaCy models from the `dev` dependency group.

## Boundaries

### Always

- Stay within the approved spec's scope; list anything else as a follow-up.
- Keep generated code small, obvious, and easy to replace; prefer established framework primitives over hand-rolled equivalents.
- Keep the pandas, polars, and Spark backends behaviorally equivalent: a change to an operation's behavior lands in `base_operations/` or in all three backends.
- Run local builds, tests, linters, read-only remote inspection, and validation-only / dry-run commands freely.
- Use repo specs, ADRs, and linked tickets as the source of truth for architecture.

### Ask First

- Starting a non-trivial change without an approved spec: draft the spec and stop.
- Architecture-relevant choices: propose an ADR in `decisions/` before or alongside the implementation.
- Adding or upgrading dependencies, including spaCy models and bundled data sources.
- Changing CI/CD, build configuration, or public interfaces not covered by the spec.
- Changing masking output for existing inputs (hash values, fake-value generation, concordance table format): downstream users may rely on reproducible results.
- Deploying or mutating any remote environment: only when the user explicitly commands that specific action.

### Never

- Infer authorization for remote mutation from a general implementation, debugging, or validation request.
- Add secrets, credentials, tenant or subscription IDs, real hostnames, tokens, or production URLs; print or commit `.env` values.
- Invent production architecture.
- Weaken, skip, or delete tests to make them pass.
- Commit real personal data. This library processes PII: tests, docs, and `data/` use synthetic values only.

## Code Placement

| Concern                                                           | Location                                                                   |
| ----------------------------------------------------------------- | -------------------------------------------------------------------------- |
| Backend-agnostic operations (`*Base` classes)                     | `src/masking/base_operations/`                                             |
| Backend-agnostic pipeline and concordance table                   | `src/masking/base_pipeline/`, `src/masking/base_concordance_table/`        |
| pandas / polars / Spark backends                                  | `src/masking/mask/`, `src/masking/mask_polars/`, `src/masking/mask_spark/` |
| Fake-data providers and their bundled sources                     | `src/masking/faker/`                                                       |
| Shared helpers (hashing, Presidio, string matching, nested dicts) | `src/masking/utils/`                                                       |
| Tests, mirroring `src/masking/` (synthetic fixtures only)         | `tests/`                                                                   |
| Example runs, benchmarks, and one-off tools                       | `scripts/`                                                                 |
| User and developer documentation                                  | `docs/`                                                                    |

## Code Style

Behavior lives in a `*Base` class in `base_operations/`; each backend adds a thin class that mixes in its backend operation:

```python
from masking.base_operations.operation_hash import HashOperationBase
from masking.mask.operations.operation import PandasOperation


class HashOperation(HashOperationBase, PandasOperation):
    """Hashes a column using SHA256 algorithm."""
```

- Ruff with `select = ["ALL"]` formats and lints; fix findings instead of adding `noqa` unless the rule is a false positive.
- Type-hint every signature with Python 3.11+ syntax (`str | None`).
- Docstrings use the `Args:` / `Returns:` sections with `----` underlines, as in existing modules.
- Backend classes keep the same name across `mask/`, `mask_polars/`, and `mask_spark/`.

## Testing

- Every behavior in a spec's acceptance criteria maps to at least one test.
- Use synthetic fixtures only.
- Cover a behavior change in every backend it affects.
- Run the relevant test suite and linters locally before declaring work done.

## Context

- Work on one spec (or one slice of it) per session; start a fresh session for the next one.
- Load only the spec sections, ADRs, and files relevant to the current task.
- Before finishing, re-read the spec and report each acceptance criterion as met or unmet.

## Docs and Specs

- Update docs when adding or changing modules.
- READMEs and reference docs describe the current codebase: purpose, structure, interfaces, invariants, configuration, and repeatable procedures. Document the resulting behavior, not the process used to reach it.
- Remove superseded statements. Do not keep coding-session plans, investigation logs, failed approaches, temporary workarounds, or change-by-change narration; git and PR history record that.
- Put proposed behavior, scope, acceptance criteria, and rationale in `specs/backlog/`. Move implemented specs to `specs/completed/` only when they remain useful as an enduring contract; otherwise delete them.
- Transient agent notes with no post-merge value do not belong in repository files.
- Skills defer to these conventions: specs go in `specs/` and ADRs in `decisions/`, never `SPEC.md`, `tasks/`, or `docs/decisions/`.
- Keep docs and specs concise: headings, short lists, tables, and diagrams over long prose.

## Git Workflow and Pull Requests

- Branches: `<type>/<short-name>`, e.g. `perf/spark-arrow`, `fix/base-operation-tests`. Never commit to `main`; a pre-commit hook blocks it.
- Commit messages: one imperative sentence, e.g. `Add unit tests and fix composed pipelines`.
- Prefer small pull requests with explicit boundaries; one spec (or one slice of a spec) per PR.
- Link the ticket and spec in the PR template's References section.
- Tick every PR checklist item that is satisfied or not relevant; an unchecked item looks like an open problem.
- The issue tracker is the authoritative backlog. `specs/` holds implementation-adjacent detail only; do not mirror the backlog there.
