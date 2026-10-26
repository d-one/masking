# Installation

Development setup for working on the package. To use the package in another project, see [Installation in the README](../README.md#installation).

## Prerequisites

| Tool                                               | Needed for                                                                                                 |
| -------------------------------------------------- | ---------------------------------------------------------------------------------------------------------- |
| Python 3.11 or later                               | Everything                                                                                                 |
| `make` and `curl`                                  | `make install`, which installs [uv](https://docs.astral.sh/uv/) if it is missing                           |
| Java 17 or later                                   | The Spark backend and its tests (PySpark 4.0); see [Running Spark locally](spark.md#running-spark-locally) |
| [APM](https://github.com/microsoft/apm) (optional) | Agent skills for coding agents; see [Development](development/README.md)                                   |

## Set Up

```sh
git clone https://github.com/d-one/masking.git
cd masking
make install   # installs uv if missing, runs `uv sync`, installs the pre-commit hooks
apm install    # optional: agent skills pinned in apm.lock.yaml
```

`uv sync` installs the runtime and `dev` dependencies into `.venv`, including the spaCy models `en_core_web_trf` and `de_core_news_lg` used by the Presidio operations.

## Commands

| Task                                           | Command                             |
| ---------------------------------------------- | ----------------------------------- |
| Run tests                                      | `uv run pytest tests`               |
| Lint and format (ruff, mdformat, markdownlint) | `uv run pre-commit run --all-files` |
| Build the wheel and sdist into `dist/`         | `uv build`                          |
| Check that `uv.lock` matches `pyproject.toml`  | `uv lock --check`                   |

The pre-commit hooks run on every commit, block commits to `main`, and run `uv build` when `src/` or `pyproject.toml` change.

## Troubleshooting

| Symptom                                                        | Fix                                                                                           |
| -------------------------------------------------------------- | --------------------------------------------------------------------------------------------- |
| `OSError: [E050] Can't find model ...`                         | Run `uv sync`, or pass `models` to `PresidioMultilingualAnalyzer` with installed models only. |
| Spark fails with `JAVA_GATEWAY_EXITED` or a Java version error | Install Java 17 or later and set `JAVA_HOME`.                                                 |
| `uv: command not found` after `make install`                   | Open a new shell so the `uv` install directory is on `PATH`.                                  |
