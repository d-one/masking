# Coding-Agent Workflow

## Roles

| Human                                     | Agent                                             |
| ----------------------------------------- | ------------------------------------------------- |
| Owns intent, priorities, and approval     | Drafts specs, ADRs, tests, code, and docs         |
| Moves specs to `ready for development`    | Implements only approved specs                    |
| Accepts ADRs and merges PRs               | Proposes ADRs and fills in PR descriptions        |
| Authorizes any remote mutation explicitly | Runs local and read-only/validation commands only |

## Agent Checklist

1. Start a fresh session per spec; read [AGENTS.md](../../AGENTS.md), the spec, referenced ADRs, and local READMEs only.
1. Confirm the spec status allows implementation and no `[NEEDS CLARIFICATION]` markers remain.
1. Keep the change small with explicit service boundaries.
1. Map acceptance criteria to tests; write tests first.
1. Run tests and linters locally.
1. Update docs to describe the resulting behavior.
1. Self-check against the spec: each acceptance criterion met or unmet, nothing extra built.
1. Report: AC → test mapping, files changed, follow-ups.

## Instruction Files

| File                              | Read by                                                | Content                      |
| --------------------------------- | ------------------------------------------------------ | ---------------------------- |
| `AGENTS.md`                       | All agents                                             | Single source of agent rules |
| `CLAUDE.md`                       | Claude Code                                            | Imports `AGENTS.md`          |
| `.github/copilot-instructions.md` | GitHub Copilot                                         | Points to `AGENTS.md`        |
| `.github/prompts/*.prompt.md`     | Copilot (slash commands); others as plain instructions | Spec-cycle steps             |

Add path-specific rules as `.github/instructions/<topic>.instructions.md` with an `applyTo` glob, or as a nested `AGENTS.md` in the relevant directory.

## Anti-Patterns

- Implementing from a chat message instead of a spec.
- Guessing at unknowns instead of marking `[NEEDS CLARIFICATION]`.
- One long session across several specs, or loading the whole repo into context.
- Expanding scope "while we're here".
- Committing session notes, plans, or investigation logs.
- Weakening or deleting tests to make them pass.
- Running deploy/apply/delete against remote systems without an explicit command.
