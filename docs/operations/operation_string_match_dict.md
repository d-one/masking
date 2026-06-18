# Operation `string_match_dict`

Detects and masks sensitive values in columns containing JSON or dictionary-structured data by matching known PII values from other columns in the same row. Extends the base [`dict`](./operation_dict.md) operation with pattern-based matching using Presidio's pattern recognizer.

______________________________________________________________________

## Class: `StringMatchDictOperationBase`

### Inherits from

- `masking.base_operations.operation_dict.DictOperationBase`
- `masking.utils.string_match_handler.StringMatchHandler`
- `masking.utils.presidio_handler.PresidioHandler`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):
  The name of the column containing JSON/dictionary data to be masked.

- **`pii_cols`** (`list[str]`):
  List of column names whose values should be used as patterns to match against. For example, if `pii_cols=["Vorname", "Name"]`, the values in those columns for each row are used to build regex patterns.

- **`masking_function`** (`Callable[[str], str]`, optional):
  A custom function to replace matched values and denied paths.

- **`anonymizer`** (`AnonymizerEngine`, optional):
  A Presidio anonymizer engine. Defaults to a standard `AnonymizerEngine`.

- **`allow_list`** (`list[str]`, optional):
  Values to exclude from pattern matching.

- **`allow_keys`** (`list[str]`, optional):
  Paths within the dictionary to consider for masking.

- **`deny_keys`** (`list[str]`, optional):
  Paths within the dictionary to mask directly without pattern analysis.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base classes.

______________________________________________________________________

## Behavior

1. Parses each cell value as a JSON dictionary.
1. Collects PII values from the specified `pii_cols` for the current row.
1. Builds a `PatternRecognizer` with regex patterns for each PII value (including date format variants for date-like values).
1. For **mask paths**: applies the pattern recognizer to each leaf value, anonymizing any matches.
1. For **deny paths**: applies the `masking_function` directly.
1. Serializes the result back to a JSON string.

### Serving Columns

This operation requires access to multiple columns during masking. The `serving_columns` property returns `[col_name, *pii_cols]`, which instructs the pipeline to pass these columns for each row.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_string_match_dict import StringMatchDictOperation

op = StringMatchDictOperation(
    col_name="Report",
    masking_function=lambda x: "<MASKED>",
    pii_cols=["Vorname", "Name", "Strasse", "Geburtsdatum"],
)
```

### Pipeline Usage

```python
config = {
    "Report": [
        {
            "masking_operation": StringMatchDictOperation(
                col_name="Report",
                masking_function=lambda x: "<MASKED>",
                pii_cols=["Vorname", "Name", "Strasse", "Geburtsdatum"],
            )
        },
        {
            "masking_operation": MaskDictOperation(
                col_name="Report",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                pii_entities=["PERSON"],
            )
        },
    ],
}
```

> **Tip:** Combine `string_match_dict` with `presidio_dict` in a chain: first use `string_match_dict` to mask known values from structured columns, then use `presidio_dict` to catch any remaining entities via NLP.

______________________________________________________________________

## Dependencies

- `DictOperationBase` (from `masking.base_operations.operation_dict`)
- `StringMatchHandler` (from `masking.utils.string_match_handler`)
- `PresidioHandler` (from `masking.utils.presidio_handler`)
- Microsoft Presidio (`presidio_analyzer`, `presidio_anonymizer`)
