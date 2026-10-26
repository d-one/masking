# Development

| Doc                                                      | Purpose                                  |
| -------------------------------------------------------- | ---------------------------------------- |
| [../installation.md](../installation.md)                 | Local setup, tests, lint, and build      |
| [spec-driven-development.md](spec-driven-development.md) | Spec lifecycle and the development cycle |
| [coding-agent-workflow.md](coding-agent-workflow.md)     | How humans and coding agents collaborate |

Agent rules live in [AGENTS.md](../../AGENTS.md); `CLAUDE.md` and `.github/copilot-instructions.md` point to it.

## Agent Skills

Skills for coding agents are declared in [apm.yml](../../apm.yml), pinned in `apm.lock.yaml`, and installed with [APM](https://github.com/microsoft/apm) into `.agents/` and `.claude/` (targets: GitHub Copilot, Claude Code). Both folders are generated and ignored by git.

```sh
apm install   # install the pinned skills
apm audit     # scan installed skills for hidden Unicode and drift
```

| Skill                           | Use                                                                               |
| ------------------------------- | --------------------------------------------------------------------------------- |
| `grill-me` / `grilling`         | Stress-test an idea or plan before `/spec-new`                                    |
| `spec-driven-development`       | Specify → plan → tasks → implement discipline; specs stored per `specs/README.md` |
| `documentation-and-adrs`        | When and how to write ADRs and docs; ADRs stored per `decisions/README.md`        |
| `crafting-effective-readmes`    | Audience-matched README structure and section checklists                          |
| `documentation-writer`          | Diátaxis docs: tutorials, how-to guides, reference, explanation                   |
| `c4-architecture`               | C4 model architecture diagrams in Mermaid                                         |
| `writing-clearly-and-concisely` | Clear prose for docs, commit messages, and PR descriptions                        |
| `docs-sync-audit`               | Read-only audit of docs drift against code, config, and commands                  |
| `ponytail`                      | Smallest change that fully solves the task                                        |
