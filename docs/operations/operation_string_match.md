# Operation `string_match`

Uses pattern-based identification of strings to mask entities.

---

## Class: `StringMatchOperationBase`

### Inherits from
- `masking.base_operations.operation.Operation`
- `masking.utils.string_match_handler.StringMatchHandler`
- `masking.utils.presidio_handler.PresidioHandler`

---

## Behavior

- Matches string patterns in a row by comparing against values from other rows or reference columns.
- If a match is found with any configured PII entity, applies the defined anonymization operator (e.g., `replace`, `redact`, `hash`).
- Skips values identified as `NaN` or `NaT`.
- Falls back to returning the original value if no match is detected.

---

## Internal Properties and Methods

### `serving_columns` (property)

Returns a list of columns needed for matching:
- Includes the column being masked and any additional columns used for matching (PII columns).

**Returns**:
- `list[str]`: Column names used in the operation.

---

### `_mask_line(line: str, additional_values: dict | None = None, **kwargs) -> str`

Detects PII strings in a line based on matching with external values.

**Parameters**:
- `line` (`str`): The input text line.
- `additional_values` (`dict`, optional): Dictionary of values used for comparison and matching.
- `**kwargs` (`dict`, optional): Additional arguments (ignored).

**Returns**:
- `str`: The masked text line.

---

## Dependencies

- `PresidioHandler` (for entity detection and anonymization)
- `StringMatchHandler` (for pattern recognizer setup and value matching)
