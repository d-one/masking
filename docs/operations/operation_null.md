# Operation `null`

Replaces entries with `None` values. This is the simplest masking operation — it removes data entirely. An optional condition function allows selective nullification based on the cell value.

______________________________________________________________________

## Class: `NullOperationBase`

### Inherits from

`masking.base_operations.operation.Operation`

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
- If the condition returns `False`, the original value is kept unchanged.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_null import NullOperation

# Replace all values with None
op = NullOperation(col_name="SensitiveColumn")

# Replace only values that start with "PRIVATE"
op = NullOperation(
    col_name="Notes",
    condition=lambda x: x.startswith("PRIVATE"),
)
```

### Pipeline Usage

```python
config = {
    "SensitiveColumn": [
        {"masking_operation": NullOperation(col_name="SensitiveColumn")},
    ],
}
```

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
