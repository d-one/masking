# Docs

Reference documentation for users and contributors. Start with the [README](../README.md) for installation and a quick start.

| Page                                                                             | Content                                                            |
| -------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| [installation.md](installation.md)                                               | Development setup, commands, troubleshooting                       |
| [available_modules.md](available_modules.md)                                     | Backends, pipeline configuration, chaining, concordance tables     |
| [spark.md](spark.md)                                                             | How the Spark backend runs, configuration, limits, local setup     |
| [operations/operations.md](operations/operations.md)                             | Overview of every masking operation and the `Operation` base class |
| [architecture/README.md](architecture/README.md)                                 | Package layers, class structure, pipeline data flow, bundled data  |
| [security/README.md](security/README.md)                                         | Handling PII, hash secrets, concordance tables, known issues       |
| [development/README.md](development/README.md)                                   | Spec-driven development and the coding-agent workflow              |
| [development/spec-driven-development.md](development/spec-driven-development.md) | How a spec moves from draft to implemented                         |
| [development/coding-agent-workflow.md](development/coding-agent-workflow.md)     | Roles and steps when working with coding agents                    |

## Operation Reference

| Page                                                                        | Operation                              |
| --------------------------------------------------------------------------- | -------------------------------------- |
| [operation_hash.md](operations/operation_hash.md)                           | `HashOperation`                        |
| [operation_yyyy_hash.md](operations/operation_yyyy_hash.md)                 | `YYYYHashOperation`                    |
| [operation_null.md](operations/operation_null.md)                           | `NullOperation` (not in pipelines yet) |
| [operation_fake_name.md](operations/operation_fake_name.md)                 | `FakeNameOperation`                    |
| [operation_fake_date.md](operations/operation_fake_date.md)                 | `FakeDate`                             |
| [operation_fake_plz.md](operations/operation_fake_plz.md)                   | `FakePLZ`                              |
| [operation_med_stats.md](operations/operation_med_stats.md)                 | `MedStatsOperation`                    |
| [operation_presidio.md](operations/operation_presidio.md)                   | `MaskPresidio`                         |
| [operation_presidio_dict.md](operations/operation_presidio_dict.md)         | `MaskDictOperation`                    |
| [operation_string_match.md](operations/operation_string_match.md)           | `StringMatchOperation`                 |
| [operation_string_match_dict.md](operations/operation_string_match_dict.md) | `StringMatchDictOperation`             |
| [operation_fake.md](operations/operation_fake.md)                           | `FakerOperation` (base class)          |
| [operation_dict.md](operations/operation_dict.md)                           | `DictOperationBase` (base class)       |

## Conventions

- Pages describe the current code on `main`. Remove statements the code no longer supports.
- Code examples are runnable and use synthetic data only.
- Headings, short lists, tables, and diagrams over long prose.
