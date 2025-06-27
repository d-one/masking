# Operation `fake_date`

### Purpose
The `operation_fake_date.py` script defines a masking operation that replaces date values in a column with realistic but fake dates using a customizable faker-based approach. This is particularly useful for preserving the format and plausibility of dates while ensuring privacy.

---

### Class: `FakeDateBase`

#### Description
Masks a column containing date values by replacing each date with a fake one, generated using the `FakeDateProvider`. Optionally, specific parts of the date (e.g. year, month) can be preserved to maintain analytical relevance.

#### Inherits from
`masking.base_operations.operation_fake.FakerOperation`

---

#### Constructor

```python
def __init__(
    col_name: str,
    preserve: str | tuple[str] | None = None,
    **kwargs: dict
) -> None
```

##### Parameters
- **`col_name`** (`str`):
  The name of the column to be masked.

- **`preserve`** (`str` or `tuple[str]`, optional):
  Specifies which part(s) of the date should be preserved during masking.
  Valid options are defined in `masking.faker.date.FakeDateProvider`.
  Examples might include `"year"` or `("year", "month")`.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base `FakerOperation`.

---

#### Method: `_mask_like_faker`

```python
def _mask_like_faker(self, line: str) -> str
```

##### Description
Applies the faker-based masking operation to a single string representing a date.

##### Parameters
- **`line`** (`str`):
  The input string to be masked.

##### Returns
- **`str`**:
  The masked (fake) date string.

---

### Dependencies
- `FakerOperation` (from `masking.base_operations.operation_fake`)
- `FakeDateProvider` (from `masking.faker.date`)
