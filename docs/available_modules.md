# Available modules

This page is dedicated to the documentation of the available modules in the package.

The current modules are available:
- mask (support for Pandas)
- mask_spark (support for Spark)

## Approach

Existing modules follow a structured approach to extract, mask, and substitute sensitive data while maintaining a concordance table for clear-to-masked value mapping.

1. **Extract Distinct Values**:
   Extract unique values from a specified column in the dataset that need masking.

2. **Generate Concordance Table**:
   Create a concordance table containing:
   - `clear_value`: Original value from the dataset.
   - `masked_value`: Masked representation of the `clear_value`.

3. **Substitute Values**:
   Replace the original values in the dataset with their corresponding masked values using the concordance table.

The aforementioned apporach is applied column-wise in a pipeline.
