# Operation `dict` (Base)

Abstract base class for masking operations that work on columns containing JSON or dictionary-structured data. Extends the standard `Operation` with the ability to traverse nested dictionaries, selectively masking or denying specific paths within the structure.

> **Note:** This class cannot be used directly. Use one of its concrete subclasses: [`presidio_dict`](./operation_presidio_dict.md) or [`string_match_dict`](./operation_string_match_dict.md).

______________________________________________________________________

## Class: `DictOperationBase`

### Inherits from

- `masking.base_operations.operation.Operation`
- `masking.utils.multi_nested_dict.MultiNestedDictHandler`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):
  The name of the column containing JSON/dictionary data to be masked.

- **`allow_keys`** (`list[str]`, optional):
  List of dot-separated paths to keys that should be **masked** (i.e., processed by the NLP or pattern-matching engine). Only leaf values matching these paths are considered for masking.

- **`deny_keys`** (`list[str]`, optional):
  List of dot-separated paths to keys that should be **denied** — their values are replaced using the `masking_function` directly, without NLP analysis.

- **`path_separator`** (`str`, default `".[$]."`):
  The separator used to represent nested paths in key specifications.

- **`list_index`** (`dict[str, str]`, default `{"start": "[", "end": "]"}`):
  Characters used to denote list indices within paths.

- **`deny_tag`** (`dict[str, str]`, default `{"start": "deny<", "end": ">"}`):
  Tags used internally to mark denied paths.

- **`case_sensitive`** (`bool`, default `False`):
  Whether key matching is case-sensitive.

- **`masking_function`** (`Callable[[str], str]`, optional):
  A function applied to denied paths. Must be set by subclasses or passed via `PresidioHandler`.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base classes.

______________________________________________________________________

## Behavior

1. **Parses** the column value from a JSON string into a Python dictionary (if needed).
1. **Traverses** the nested structure to identify leaf nodes.
1. **Classifies** each leaf as either a "mask" path (to be analyzed by NLP/pattern matching) or a "deny" path (to be directly replaced).
1. **Denied paths** are replaced using the configured `masking_function`.
1. **Mask paths** are handled by the subclass-specific `_handle_masking_paths()` method.
1. **Serializes** the result back to a JSON string.

______________________________________________________________________

## Internal Methods

### `_handle_denied_paths(line: dict, leaf_to_deny: tuple) -> dict`

Applies `masking_function` to all denied leaf paths.

### `_handle_masking_paths(line: dict, leaf_to_mask: tuple, **kwargs) -> dict` *(abstract)*

Must be implemented by subclasses. Applies NLP-based or pattern-based masking to the specified paths.

### `_mask_line(line: str | dict, leaf_to_mask=None, leaf_to_deny=None, **kwargs) -> str`

Orchestrates the full masking pipeline for a single dictionary value.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_presidio_dict import MaskDictOperation

op = MaskDictOperation(
    col_name="Report",
    masking_function=lambda x: "<MASKED>",
    analyzer=analyzer,
    pii_entities=["PERSON"],
    # Optionally restrict to specific paths:
    # allow_keys=["patient.name", "patient.address"],
    # deny_keys=["metadata.*"],
)
```

______________________________________________________________________

## Subclasses

| Operation                                               | Description                                                         |
| ------------------------------------------------------- | ------------------------------------------------------------------- |
| [`presidio_dict`](./operation_presidio_dict.md)         | NLP-based entity detection on dictionary values                     |
| [`string_match_dict`](./operation_string_match_dict.md) | Pattern-based matching on dictionary values using known PII columns |

______________________________________________________________________

## Dependencies

- `Operation` (from `masking.base_operations.operation`)
- `MultiNestedDictHandler` (from `masking.utils.multi_nested_dict`)
