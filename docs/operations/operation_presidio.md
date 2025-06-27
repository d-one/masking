# Operation `presidio`

Uses Presidio to detect sensitive entities in text (e.g., names, phone numbers, emails) and anonymizes them using a predefined strategy.

---

## Class: `MaskPresidioBase`

### Inherits from
- `masking.base_operations.operation.Operation`
- `masking.utils.presidio_handler.PresidioHandler`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base classes.

---

## Behavior

- Uses Presidio to detect Personally Identifiable Information (PII) entities in a text string.
- Applies a configured anonymization operator (e.g., `replace`, `hash`, `redact`) to mask detected entities.
- If no specific list of entities is provided, it detects all configured entities by default.
- If a list of entities is provided (e.g., `["PHONE_NUMBER", "EMAIL_ADDRESS"]`), it targets only those types for masking.

---

## Internal Method

### `_mask_line(line: str, entities: list[str] | None = None, **kwargs) -> str`

Detects and anonymizes entities in a single text line.

**Parameters**:
- `line` (`str`): The input text to be masked.
- `entities` (`list[str]`, optional): List of entity types to mask. If `None`, all configured entities will be used.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns**:
- `str`: The masked version of the input line.

---

## Dependencies

- `PresidioHandler` (provides entity detection and anonymization logic)
- Microsoft Presidio library (installed externally)
