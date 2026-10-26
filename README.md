# Masking

Python library that masks personally identifiable information (PII) in tables. You assign a masking operation to each column, such as hashing, realistic fake values, region mapping, or entity detection in free text, and run the same pipeline on pandas, polars, or Spark DataFrames.

## Installation

```bash
pip install "masking @ git+https://github.com/d-one/masking"
```

Requires Python 3.11 or later. The Spark backend needs Java 17 or later. Operations based on Presidio need spaCy models:

```bash
python -m spacy download en_core_web_trf
python -m spacy download de_core_news_lg
```

For a development setup, see [Installation](docs/installation.md).

## Quick Start

Map each output column to a masking operation, build a pipeline, and call it on a DataFrame:

```python
import pandas as pd

from masking.mask.operations.operation_hash import HashOperation
from masking.mask.pipeline import MaskDataFramePipeline

df = pd.DataFrame({
    "name": ["Alice", "Bob", "Alice"],
    "ssn": ["123-45-6789", "987-65-4321", "123-45-6789"],
})

pipeline = MaskDataFramePipeline({
    "name": {"masking_operation": HashOperation(col_name="name", secret="my_secret")},
    "ssn_masked": {"masking_operation": HashOperation(col_name="ssn", secret="my_secret")},
})
masked_df = pipeline(df.copy())

print(masked_df)  # name is hashed in place; ssn_masked is a new hashed column next to ssn
print(pipeline.concordance_tables)  # {"name": {"Alice": ..., "Bob": ...}, "ssn": {...}}
```

- A key equal to the operation's `col_name` masks that column in place; any other key adds a new masked column and keeps the original.
- `pipeline.concordance_tables` maps each input column's clear values to their masked values. Protect it like the source data; see [Security](docs/security/README.md).
- The pandas pipeline modifies the DataFrame it receives; pass a copy to keep the original.

### polars and Spark

The polars and Spark backends expose the same classes under `masking.mask_polars` and `masking.mask_spark`:

```python
import polars as pl

from masking.mask_polars.operations.operation_hash import HashOperation
from masking.mask_polars.pipeline import MaskDataFramePipeline

pipeline = MaskDataFramePipeline({
    "name": {"masking_operation": HashOperation(col_name="name", secret="my_secret")},
})
print(pipeline(pl.DataFrame({"name": ["Alice", "Bob"]})))
```

```python
from pyspark.sql import SparkSession

from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline

spark = SparkSession.builder.getOrCreate()
pipeline = MaskDataFramePipeline({
    "name": {"masking_operation": HashOperation(col_name="name", secret="my_secret")},
})
pipeline(spark.createDataFrame([("Alice",), ("Bob",)], ["name"])).show(truncate=False)
```

### Free Text with Presidio

`MaskPresidio` detects entities such as names, email addresses, and phone numbers with [Microsoft Presidio](https://microsoft.github.io/presidio/) and [spaCy](https://spacy.io/) models, then replaces them:

```python
import pandas as pd

from masking.mask.operations.operation_presidio import MaskPresidio
from masking.mask.pipeline import MaskDataFramePipeline
from masking.utils.entity_detection import PresidioMultilingualAnalyzer

analyzer = PresidioMultilingualAnalyzer(models={"en": "en_core_web_trf"}).analyzer

pipeline = MaskDataFramePipeline({
    "description": {
        "masking_operation": MaskPresidio(col_name="description", analyzer=analyzer)
    },
})
df = pd.DataFrame({"description": ["James Bond was a very clever inspector."]})
print(pipeline(df)["description"][0])  # <PERSON> was a very clever inspector.
```

Always pass `models`: without it, the analyzer loads `en_core_web_lg`, which neither the commands above nor the development setup install.

## Documentation

| Page                                                | Content                                                        |
| --------------------------------------------------- | -------------------------------------------------------------- |
| [Installation](docs/installation.md)                | Development setup, tests, and build                            |
| [Backends and pipelines](docs/available_modules.md) | Backends, pipeline configuration, chaining, concordance tables |
| [Operations](docs/operations/operations.md)         | Every masking operation and its parameters                     |
| [Spark](docs/spark.md)                              | How the Spark backend runs, configuration, local setup         |
| [Architecture](docs/architecture/README.md)         | Package layers and data flow                                   |
| [Security](docs/security/README.md)                 | Handling PII, secrets, and concordance tables                  |
| [Development](docs/development/README.md)           | Spec-driven workflow with coding agents                        |

## Contributing

Changes follow a spec-driven workflow shared by humans and coding agents:

- [AGENTS.md](AGENTS.md): rules, commands, code placement, and conventions.
- [Spec-driven development](docs/development/spec-driven-development.md): how a spec moves from draft to implemented. Specs live in [specs/](specs/), decisions in [decisions/](decisions/).
- [Pull request template](.github/pull_request_template.md): link the ticket and spec in every PR.

```bash
make install                       # uv, dependencies, pre-commit hooks
uv run pytest tests                # tests
uv run pre-commit run --all-files  # lint and format
```
