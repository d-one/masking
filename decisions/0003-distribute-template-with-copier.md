# ADR 0003: Distribute the Template with Copier and a uv-Installable CLI

## Status

Accepted

## Context

Child projects start by cloning this repository, deleting `.git`, and replacing `TODO` placeholders by hand. Template improvements (skills, prompts, agent rules) never reach existing child projects.

Constraints:

- One copy of every file; no second set of template files to keep in sync.
- Child projects fill in `TODO`s themselves; no up-front questionnaire.
- Installable with uv from GitHub.

| Option                                                                            | One copy of each file     | Updates child projects                   |
| --------------------------------------------------------------------------------- | ------------------------- | ---------------------------------------- |
| GitHub template repository                                                        | Yes                       | No                                       |
| CLI that copies files bundled in its package                                      | No (package holds a copy) | No                                       |
| [Copier](https://copier.readthedocs.io/) with the repository root as the template | Yes                       | Yes, three-way merge via `copier update` |

## Decision

- The repository root is a Copier template. Shared files are copied verbatim, without Jinja; `copier.yml` excludes files that belong only to the template repository.
- A thin `agentic-engineering` Python package in `cli/` provides an `agentic-engineering` CLI (`new`, `update`) that wraps Copier's Python API. It bundles no template files.
- The CLI renders this GitHub repository at the git tag matching its own version (`v<version>`), because `copier update` needs a versioned git source.
- Install from GitHub: `uvx --from "git+https://github.com/petmarco7/agentic_engineering#subdirectory=cli" agentic-engineering new <dir>`. No PyPI publishing.

## Consequences

- Child projects receive this ADR: it explains where the project came from and how to pull template updates.
- Template-only content (`cli/`, `copier.yml`, the template repository's own specs) never reaches child projects.
- Releases are git tags matching the CLI version; a human creates and pushes them.
- Copier is a runtime dependency of the CLI; post-copy tasks (`git init`, `uv sync`, `pre-commit install`) run on the user's machine.
- `copier update` requires the child project to be a clean git repository.
