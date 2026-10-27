# ADR 0003: NullOperation in Pipelines

## Status

Accepted

## Context

Pipelines (`MaskDataFramePipeline`) mask a column by building a concordance table (`clear -> masked`) and substituting each value through it; a value missing from the table keeps its clear value. `NullOperation` maps values to null, which this mechanism cannot express:

- polars and Spark drop `clear -> null` entries while building the table (`clear != masked` is null), so the values would stay unmasked.
- pandas keeps them as `None`, and `Operation.update_concordance_table` rejects them on the next call.
- A `clear -> null` mapping cannot be reversed, so it has no use in `pipeline.concordance_tables`.

Pipelines therefore rejected `NullOperation` with a `TypeError` ([#24](https://github.com/d-one/masking/issues/24)).

## Decision

Pipelines null values directly instead of storing null mappings:

- A `NullOperation` stage adds no entries to its concordance table or to `pipeline.concordance_tables`.
- The base pipeline records, per column and call, the clear values that end up null; each backend nulls them with `_null_values` before substitution, while the column still holds the clear values in their own type.
- Without a `condition`, the whole column is nulled without collecting its distinct values, also when the `NullOperation` is part of a list.
- With a `condition`, the stage nulls the distinct values for which `condition(str(value))` is true.
- In a chain, the condition sees the previous stage's output, values nulled by a stage stay null in later stages, and the column's table excludes nulled clear values.
- Values a `NullOperation` keeps are left unchanged, like any value without a mapping.

## Consequences

- `NullOperation` works in pandas, polars, and Spark pipelines, including chains and output columns.
- `pipeline.concordance_tables` never contains null masked values; nulled values cannot be recovered from it, by design.
- Each backend implements `_null_values(data, col_name, values)`; a conditional `NullOperation` collects the column's distinct values, and Spark nulls them with `isin`.
- Kept values differ from a direct `NullOperation` call, which returns them as strings; the pipeline keeps their type, as it does for unmapped values.
