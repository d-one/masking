# Operation `null`

Replaces entries with `None` values.

## Purpose

This operation replaces entries with `None` values.

---

## Class: `NullOperationBase`

### Inherits from
`masking.base_operations.operation.Operation`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`condition`** (`Callable[[str],bool]`, optional):
  Specifies the condition to replace entries with nulls.
  **Default:** `"lambda x: True"`

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Evaluates the condition on every row.
- If condition results in `True`, then replace values with `None`.

---

## Internal Methods

### `_mask_linee(line: str, **kwargs)`

Generates a `None` if the `condition` applied to `line` is `True`.

Parameters:
- **`line`** (`str`): The input value to be masked.

Returns:
- **`str`**: The masked (fake) name.

---

## Dependencies

- `Operation` (from `masking.base_operations.operation`)
