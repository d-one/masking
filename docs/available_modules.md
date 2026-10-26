# Backends and Pipelines

The package masks DataFrames through a pipeline that applies one masking operation, or a chain of operations, per column.

## Backends

| Backend | Pipeline                                             | Operations                                | Input                   |
| ------- | ---------------------------------------------------- | ----------------------------------------- | ----------------------- |
| pandas  | `masking.mask.pipeline.MaskDataFramePipeline`        | `masking.mask.operations.<module>`        | `pandas.DataFrame`      |
| polars  | `masking.mask_polars.pipeline.MaskDataFramePipeline` | `masking.mask_polars.operations.<module>` | `polars.DataFrame`      |
| Spark   | `masking.mask_spark.pipeline.MaskDataFramePipeline`  | `masking.mask_spark.operations.<module>`  | `pyspark.sql.DataFrame` |

All three backends provide the same operation classes with the same names and parameters, for example `masking.mask_polars.operations.operation_hash.HashOperation`. Use operations from the same backend as the pipeline. See [Operations](operations/operations.md) for the list.

## Approach

Each backend masks a column in three steps:

1. **Extract distinct values**: collect the distinct non-null values of the column.
1. **Generate the concordance table**: mask each distinct value once and store the mapping `clear value → masked value`.
1. **Substitute values**: replace every value in the column with its masked value from the concordance table.

Masking each distinct value once makes the result consistent: equal clear values always get the same masked value within a pipeline. Null values stay null.

## Configuration

`MaskDataFramePipeline(configuration, workers=1)` takes a dictionary that maps output column names to a column configuration:

```python
from masking.mask.operations.operation_hash import HashOperation
from masking.mask.pipeline import MaskDataFramePipeline

pipeline = MaskDataFramePipeline({
    # Mask the "name" column in place
    "name": {"masking_operation": HashOperation(col_name="name", secret="my_secret")},
    # Add a masked copy of "ssn" as "ssn_masked"; "ssn" keeps its clear values
    "ssn_masked": {
        "masking_operation": HashOperation(col_name="ssn"),
        "concordance_table": {"123-45-6789": "known-masked-value"},
    },
})
masked_df = pipeline(df)
```

| Key                 | Required | Meaning                                                                                                                                                                                                                                                                              |
| ------------------- | -------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `masking_operation` | Yes      | The operation to apply. Its `col_name` is the input column.                                                                                                                                                                                                                          |
| `concordance_table` | No       | Prescribed `clear → masked` mappings, as a `dict` or a pandas or Spark DataFrame with the columns `clear_values` and `masked_values`. Prescribed values are used instead of masking those clear values. Operations also accept a `concordance_table` argument with the same meaning. |

- **Output column**: if the key equals the operation's `col_name`, the column is masked in place. Otherwise the pipeline adds the key as a new column with the masked values and keeps the input column unchanged. A key that is also an input or serving column of another operation raises `ValueError`.
- **Column order**: the output keeps the input's column order; new columns are appended.
- **`workers`**: with `workers > 1`, the columns are masked in parallel threads.
- **Input mutation**: the pandas pipeline modifies the DataFrame it receives. Pass `df.copy()` to keep the original.

### Chaining Operations

A column can be configured with a list of operations, which are applied in order:

```python
config = {"name": [{"masking_operation": op1}, {"masking_operation": op2}]}
```

- The masked values of each operation are the input of the next one.
- The concordance table of each operation (including a prescribed `concordance_table`) is keyed by that operation's own input values. For `op2`, these are the masked values produced by `op1`; entries for values `op2` never receives are ignored.
- The resulting concordance table of the column is the composition of all the mappings: each clear value `x` maps to `op2(op1(x))`.
- Every operation in the list must use the same `col_name`; otherwise the pipeline raises `ValueError`.

## Concordance Tables

After a call, `pipeline.concordance_tables` holds one `dict` per input column, keyed by the operation's `col_name`:

```python
masked_df = pipeline(df)
pipeline.concordance_tables
# {"name": {"Alice": "2377ea...", "Bob": "f79c2f..."}, "ssn": {"123-45-6789": "known-masked-value", ...}}
```

- Only values whose masked value differs from the clear value are stored.
- Tables accumulate across calls on the same pipeline, so a value seen in an earlier DataFrame keeps its masked value in later ones.
- `pipeline.clear_concordance_tables()` empties the tables. Operations keep a `concordance_table` passed to their constructor; a `concordance_table` passed in the pipeline configuration is dropped.

A concordance table reverses the masking. Store it like the clear data; see [Security](security/README.md).
