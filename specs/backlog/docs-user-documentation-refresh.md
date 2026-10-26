---
ticket: "#3"
links:
  - README.md
  - docs/
owner: petmarco7
status: in progress
affected_components:
  - README.md
  - docs/
assumptions:
  - Documentation targets `main` at 865d3b2; open PR #2 (perf/spark-arrow) is reconciled with these changes when either merges.
  - Existing doc paths (`docs/installation.md`, `docs/available_modules.md`, `docs/operations/*.md`) stay in place so external links keep working.
  - "`docs/operations/` stays the operation reference; the template's runbook folder is not used because the library has no deployed environment."
out_of_scope:
  - Source code changes, including adding `NullOperation` to the backends (see `operations-null-backends.md`).
  - Documenting changes that only exist on PR #2 (Spark performance work, `batch_size`, `docs/spark.md`).
  - A generated docs site, API reference generated from docstrings, or translations.
  - Automated doc tests in CI.
---

# Feature: Refresh User and Developer Documentation

<!-- Mark every unknown inline as [NEEDS CLARIFICATION: question]. Do not guess. -->

## References

- Ticket: [#3](https://github.com/d-one/masking/issues/3)
- Related specs: [operations-null-backends.md](operations-null-backends.md)
- Related ADRs: [0002](../../decisions/0002-spec-driven-agentic-development.md)

## Problem

Users and coding agents learn the library from `README.md` and `docs/`, but parts of them no longer match the code:

| Doc                                 | Drift                                                                                                                                                                                                                                      |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `README.md` Presidio example        | Imports `PresidioMultilingualAnalyzer` from `masking.utils.presidio_handler` (it lives in `masking.utils.entity_detection`), configures column `name` on a DataFrame without it, passes an unused `secret`, and never passes the analyzer. |
| `docs/available_modules.md`         | Lists the pandas and Spark backends only; `masking.mask_polars` is missing.                                                                                                                                                                |
| `docs/operations/operation_null.md` | Imports `masking.mask.operations.operation_null.NullOperation`, which exists in no backend; only `NullOperationBase` exists.                                                                                                               |
| Pipeline usage                      | The configuration format (output-column keys, single operation vs. chain, `workers`) and how to read or clear `pipeline.concordance_tables` are not documented.                                                                            |
| Structure                           | No architecture overview, no index of `docs/`, no security notes for a library that processes PII, no contributor entry point to the agentic workflow.                                                                                     |

Wrong examples fail on first use, and agents that read the docs as context reproduce the errors.

## Proposed Behavior

After this change the documentation describes the code on `main` and nothing else:

| Page                          | Content                                                                                                                                                                |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `README.md`                   | What the library does, install, a runnable quick start per backend, documentation index, contributing.                                                                 |
| `docs/README.md`              | Index of every page under `docs/`.                                                                                                                                     |
| `docs/installation.md`        | Local setup: `make install`, spaCy models, Java for Spark, tests, pre-commit, optional `apm install` for agent tooling.                                                |
| `docs/available_modules.md`   | The three backends, the pipeline configuration format, chaining, and concordance tables.                                                                               |
| `docs/architecture/README.md` | Layers (`base_*`, backends, `faker`, `utils`), how backend operations combine a `*Base` class with a backend mixin, and the pipeline data flow, with Mermaid diagrams. |
| `docs/security/README.md`     | Handling of PII: concordance tables hold clear values, hash secrets, synthetic test data.                                                                              |
| `docs/operations/*.md`        | Correct import paths for every backend and an accurate status for the `null` operation.                                                                                |

## Acceptance Criteria

1. **AC1** — Every `from masking… import …` statement in `README.md` and `docs/**/*.md` imports successfully with `uv run python`.
1. **AC2** — The README quick-start examples for pandas and polars run on their synthetic data and print a masked DataFrame; the Spark example runs when Java is available.
1. **AC3** — `docs/available_modules.md` lists the pandas, polars, and Spark backends with their pipeline import path, and states that all three expose the same operation classes.
1. **AC4** — `docs/available_modules.md` documents the configuration format: keys are output columns, values are one operation config or a list of configs on the same input column, plus `workers`, prescribed concordance tables, `pipeline.concordance_tables`, and `clear_concordance_tables()`. Each documented behavior matches `src/masking/base_pipeline/pipeline.py`.
1. **AC5** — `docs/architecture/README.md` contains the layer overview and at least one Mermaid diagram of class relationships and one of the pipeline data flow.
1. **AC6** — `docs/security/README.md` states that concordance tables map clear to masked values and must be protected like the source data, how hash secrets are supplied, and that tests and docs use synthetic data only.
1. **AC7** — `docs/installation.md` gives the commands for install, spaCy models, tests, lint, and build that match `Makefile`, `pyproject.toml`, and `AGENTS.md`, and states the Java requirement for Spark.
1. **AC8** — `docs/README.md` links every Markdown page under `docs/`, and every relative link in `README.md` and `docs/` resolves to an existing file.
1. **AC9** — `README.md` has a Contributing section linking `AGENTS.md`, the spec-driven workflow, and the PR template.
1. **AC10** — The `null` operation docs state that no backend provides `NullOperation` yet and do not show an import that fails; they link the bugfix spec.

## Design Notes

- Keep existing file paths; add only `docs/README.md`, `docs/architecture/README.md`, and `docs/security/README.md`.
- Follow the repo's Markdown style enforced by mdformat and markdownlint.
- Use the `docs-sync-audit` skill to check README and docs against the code before review.

## Tests

| AC                  | Test type                                                              | Location                                            |
| ------------------- | ---------------------------------------------------------------------- | --------------------------------------------------- |
| AC1                 | Manual check: import every `from masking` line with `uv run python -c` | `README.md`, `docs/`                                |
| AC2                 | Manual check: run each quick-start example                             | `README.md`                                         |
| AC3, AC4, AC7, AC10 | Review against code; `docs-sync-audit` reports no P0–P2 drift          | `docs/`                                             |
| AC5, AC6, AC9       | Review                                                                 | `docs/architecture/`, `docs/security/`, `README.md` |
| AC8                 | `docs-sync-audit` link check (`scripts/docs_drift.py`)                 | `README.md`, `docs/`                                |

## Open Questions

- None.

## Spec Quality

- [x] No `[NEEDS CLARIFICATION]` markers or open questions remain
- [x] Every acceptance criterion is testable and observable from outside the code
- [x] `out_of_scope` and `assumptions` are explicit
- [x] No speculative or "might need" features; nothing future-proofed
- [x] Uses existing framework primitives; any new abstraction is justified in Design Notes
