# Specs

Repo-local specs are implementation-adjacent engineering documents for pull requests and coding agents. They make intent precise enough to implement and test. They complement the issue tracker; they do not replace it.

## Folders

| Folder       | Contains                                                                         |
| ------------ | -------------------------------------------------------------------------------- |
| `backlog/`   | Active specs (`outline` → `in progress`) tied to a ticket or upcoming PR         |
| `completed/` | Implemented specs that remain useful as an enduring contract or design reference |
| `templates/` | Reusable spec templates                                                          |

Top-level Markdown files in `specs/` are index or guidance documents only.

## What Does Not Belong Here

- The authoritative backlog or a mirror of it.
- Broad business-facing documentation.
- Session notes, investigation logs, or other transient agent output.

## Naming

`<area>-<short-name>.md`, kebab-case, e.g. `auth-token-refresh.md`.

## Right-Sizing

Match the spec to the change; a heavyweight spec for a small change is waste.

| Change                                                  | Spec                                                          |
| ------------------------------------------------------- | ------------------------------------------------------------- |
| Typo, refactor without behavior change, dependency bump | None                                                          |
| Bug fix                                                 | [Bugfix spec](templates/bugfix-spec-template.md), a few lines |
| Small, well-understood feature                          | Feature spec with Problem, Acceptance Criteria, Tests only    |
| New feature, service, agent, or interface               | Full template                                                 |
| Several independently testable capabilities             | One spec per capability                                       |

## Clarification Markers

Write every unknown inline as `[NEEDS CLARIFICATION: question]` instead of guessing. A spec cannot move to `ready for development` while any marker remains or its **Spec Quality** checklist is incomplete.

## Frontmatter

```yaml
ticket: PROJ-0000
links: []
owner: TODO
status: draft
affected_components: []
assumptions: []
out_of_scope: []
```

## Lifecycle

```text
outline → draft → ready for development → in progress → implemented
```

| Status                  | Meaning                                           | Set by                    |
| ----------------------- | ------------------------------------------------- | ------------------------- |
| `outline`               | Placeholder; not detailed enough for development  | Human or agent            |
| `draft`                 | Written; needs review and approval                | Agent (`/spec-new`)       |
| `ready for development` | Approved; implementation can begin                | **Human only**            |
| `in progress`           | Implementation underway                           | Agent (`/spec-implement`) |
| `implemented`           | Code matches spec; move to `completed/` or delete | Agent (`/spec-complete`)  |

Do not invent intermediate states such as `partially implemented`.

## Templates

| Template                                                       | Use for                                            |
| -------------------------------------------------------------- | -------------------------------------------------- |
| [feature-spec-template.md](templates/feature-spec-template.md) | Default: any user-visible or internal feature      |
| [bugfix-spec-template.md](templates/bugfix-spec-template.md)   | A bug: current, expected, and unchanged behavior   |
| [service-spec-template.md](templates/service-spec-template.md) | A new service or component with its own interfaces |
| [agent-spec-template.md](templates/agent-spec-template.md)     | An AI agent, its tools, and its prompt contract    |
