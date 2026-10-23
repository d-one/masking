# Available modules

This page is dedicated to the documentation of the available modules in the package.

The current modules are available:

- mask (support for Pandas)
- mask_spark (support for Spark)

## Approach

Existing modules follow a structured approach to extract, mask, and substitute sensitive data while maintaining a concordance table for clear-to-masked value mapping.

1. **Extract Distinct Values**:
   Extract unique values from a specified column in the dataset that need masking.

1. **Generate Concordance Table**:
   Create a concordance table containing:

   - `clear_value`: Original value from the dataset.
   - `masked_value`: Masked representation of the `clear_value`.

1. **Substitute Values**:
   Replace the original values in the dataset with their corresponding masked values using the concordance table.

The aforementioned apporach is applied column-wise in a pipeline.

### Chaining operations

A column can be configured with a list of operations, which are applied in order:

```python
config = {"name": [{"masking_operation": op1}, {"masking_operation": op2}]}
```

- The masked values of each operation are the input of the next one.
- The concordance table of each operation (including a prescribed `concordance_table`) is keyed by that operation's own input values. For `op2`, these are the masked values produced by `op1`; entries for values `op2` never receives are ignored.
- The resulting concordance table of the column is the composition of all the mappings: each clear value `x` maps to `op2(op1(x))`.
