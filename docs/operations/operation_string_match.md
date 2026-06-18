# Operation `string_match`

Detects and masks known PII values in free-text by matching them against values from other columns in the same row. Unlike [`presidio`](./operation_presidio.md) which uses NLP, this operation uses exact/fuzzy pattern matching — making it faster and more predictable for known values.

______________________________________________________________________

## Class: `StringMatchOperationBase`

### Inherits from

- `masking.base_operations.operation.Operation`
- `masking.utils.string_match_handler.StringMatchHandler`
- `masking.utils.presidio_handler.PresidioHandler`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing free-text to be masked.

- **`pii_cols`** (`list[str]`):\
  List of column names whose values should be used as match patterns. For each row, the values from these columns are compiled into regex patterns used to find and replace occurrences in the target column.

- **`masking_function`** (`Callable[[str], str]`, optional):\
  A custom function to replace matched values.

- **`anonymizer`** (`AnonymizerEngine`, optional):\
  A Presidio anonymizer engine. Defaults to a standard `AnonymizerEngine`.

- **`allow_list`** (`list[str]`, optional):\
  Values to exclude from matching.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base classes.

______________________________________________________________________

## Behavior

- For each row, collects PII values from the specified `pii_cols`.
- Builds a `PatternRecognizer` with regex patterns for each PII value, including multiple date format variants for date-like values.
- Applies the recognizer to the target column text.
- If matches are found, anonymizes them using the configured operator.
- If no matches are found, returns the original text unchanged.
- Skips `NaN` and `NaT` values in PII columns.

### Serving Columns

This operation requires multiple columns. The `serving_columns` property returns `[col_name, *pii_cols]`, instructing the pipeline to pass these columns for each row.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_string_match import StringMatchOperation

op = StringMatchOperation(
    col_name="Beschrieb",
    masking_function=lambda x: "<MASKED>",
    pii_cols=["Vorname", "Name"],
)
```

### Pipeline Usage

Often combined with `presidio` in a chain: first mask known values via string matching, then use NLP to catch remaining entities:

```python
config = {
    "Beschrieb": [
        {
            "masking_operation": StringMatchOperation(
                col_name="Beschrieb",
                masking_function=lambda x: "<MASKED>",
                pii_cols=["Vorname", "Name"],
            )
        },
        {
            "masking_operation": MaskPresidio(
                col_name="Beschrieb",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                pii_entities=["PERSON"],
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Properties and Methods

### `serving_columns` (property)

Returns `[col_name, *pii_cols]` — the list of columns the pipeline must provide for each row.

### `_mask_line(line: str, additional_values: dict | None = None, **kwargs) -> str`

Detects PII strings in a line based on pattern matching with values from other columns.

**Parameters:**

- `line` (`str`): The input text line.
- `additional_values` (`dict`, optional): Dictionary of `{column_name: value}` pairs used for pattern building.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `str`: The masked text line.

______________________________________________________________________

## Dependencies

- `PresidioHandler` (from `masking.utils.presidio_handler`)
- `StringMatchHandler` (from `masking.utils.string_match_handler`)
- Microsoft Presidio (`presidio_analyzer`, `presidio_anonymizer`)
