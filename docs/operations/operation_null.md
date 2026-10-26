# Operation `null`

Replaces entries with `None` values. This is the simplest masking operation — it removes data entirely. An optional condition function allows selective nullification based on the cell value.

> **Status:** `NullOperation` works when called on a DataFrame directly. Pipelines (`MaskDataFramePipeline`) reject it with a `TypeError` until pipeline support lands ([#24](https://github.com/d-one/masking/issues/24)).

______________________________________________________________________

## Class: `NullOperation`

| Backend | Import                                                                    |
| ------- | ------------------------------------------------------------------------- |
| pandas  | `from masking.mask.operations.operation_null import NullOperation`        |
| polars  | `from masking.mask_polars.operations.operation_null import NullOperation` |
| Spark   | `from masking.mask_spark.operations.operation_null import NullOperation`  |

### Inherits from

`masking.base_operations.operation_null.NullOperationBase`, which inherits from `masking.base_operations.operation.Operation`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column to be masked.

- **`condition`** (`Callable[[str], bool]`, optional):\
  A function that receives the cell value (as a string) and returns `True` if the value should be replaced with `None`. When not provided, all values are replaced.
  **Default:** `lambda x: True`

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Converts the input value to a string.
- Evaluates the `condition` function on the string value.
- If the condition returns `True`, replaces the value with `None`.
- If the condition returns `False`, the value is kept, converted to a string.

______________________________________________________________________

## Internal Methods

### `_mask_line(line: str, **kwargs) -> str | None`

Returns `None` if `condition(line)` is `True`, otherwise returns the original value as a string.

**Parameters:**

- `line` (`str`): The input value to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `None` if the condition is met, otherwise `str`.

______________________________________________________________________

## Dependencies

- `Operation` (from `masking.base_operations.operation`)

______________________________________________________________________

## Example

```python
import pandas as pd
from masking.mask.operations.operation_null import NullOperation

df = pd.DataFrame({"ssn": ["123-45-6789", "n/a"]})

# Null every value
df["ssn_all_null"] = NullOperation(col_name="ssn")(df)

# Null only values matching a condition; other values are kept as strings
df["ssn_known_null"] = NullOperation(col_name="ssn", condition=lambda v: v != "n/a")(df)
# ssn_known_null: [None, "n/a"]
```
