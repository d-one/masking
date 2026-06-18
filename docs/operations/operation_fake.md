# Operation `fake` (Base)

Abstract base class for all faker-based masking operations. Provides the common logic for generating fake replacement values using the Faker library, with built-in retry logic to ensure the masked value differs from the original.

> **Note:** This class cannot be used directly. Use one of its concrete subclasses: [`fake_name`](./operation_fake_name.md), [`fake_date`](./operation_fake_date.md), or [`fake_plz`](./operation_fake_plz.md).

______________________________________________________________________

## Class: `FakerOperation`

### Inherits from

`masking.base_operations.operation.Operation`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`provider`** (`FakeProvider`):
  A faker provider instance that generates the fake values. Each subclass passes its own provider (e.g., `FakeNameProvider`, `FakeDateProvider`, `FakePLZProvider`).

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base `Operation` class (e.g., `concordance_table`).

______________________________________________________________________

## Behavior

- Calls the subclass-defined `_mask_like_faker()` method to generate a fake value.
- Retries up to `MAX_RETRY_MASK_LINE` (1000) times if the generated value is identical to the original, to ensure actual masking.
- Raises `ValueError` if no different value can be produced after all retries.
- Sets `_needs_unique_values = True`, meaning the pipeline will verify that masked values do not collide with existing clear values.

______________________________________________________________________

## Internal Methods

### `_mask_like_faker(line: str, **kwargs) -> str` *(abstract)*

Must be implemented by subclasses. Generates a single fake replacement value.

### `_mask_line(line: str, **kwargs) -> str`

Wraps `_mask_like_faker` with retry logic to guarantee the output differs from the input.

**Parameters:**

- `line` (`str`): The original value.

**Returns:**

- `str`: A fake replacement value different from `line`.

______________________________________________________________________

## Subclasses

| Operation                               | Provider           | Description                           |
| --------------------------------------- | ------------------ | ------------------------------------- |
| [`fake_name`](./operation_fake_name.md) | `FakeNameProvider` | Synthetic names (first, last, full)   |
| [`fake_date`](./operation_fake_date.md) | `FakeDateProvider` | Fake dates with optional preservation |
| [`fake_plz`](./operation_fake_plz.md)   | `FakePLZProvider`  | Fake Swiss postal codes               |

______________________________________________________________________

## Dependencies

- `FakeProvider` (from `masking.faker.faker`)
- `Operation` (from `masking.base_operations.operation`)
