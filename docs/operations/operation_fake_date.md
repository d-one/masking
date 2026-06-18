# Operation `fake_date`

Replaces date values with realistic but fake dates using a faker-based approach. Optionally preserves specific parts of the date (year, month, day) to maintain analytical relevance.

______________________________________________________________________

## Class: `FakeDateBase`

### Inherits from

[`masking.base_operations.operation_fake.FakerOperation`](./operation_fake.md)

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing date values to be masked.

- **`preserve`** (`str | tuple[str] | None`, optional):\
  Specifies which part(s) of the date should be preserved during masking.\
  **Admissible values:**

  - `"year"` — preserve the year
  - `"month"` — preserve the month
  - `"day"` — preserve the day
  - A tuple combining any of the above, e.g., `("year", "month")`

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear date values to masked values.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Parses the input date string using `dateparser`.
- Generates a fake date within a plausible range.
- Preserves the specified date components (if any), replacing only the remaining parts.
- Retries generation (up to 1000 times) to ensure the fake date differs from the original.
- Input and output dates follow ISO format (`YYYY-MM-DD`).

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_fake_date import FakeDate

# Replace the full date
op = FakeDate(col_name="Geburtsdatum")

# Preserve the year only
op = FakeDate(col_name="Geburtsdatum", preserve="year")

# Preserve year and month
op = FakeDate(col_name="Geburtsdatum", preserve=("year", "month"))
```

### Pipeline Usage

```python
from pandas import DataFrame

config = {
    "Geburtsdatum": [
        {
            "masking_operation": FakeDate(
                col_name="Geburtsdatum",
                preserve=("year", "month"),
                concordance_table=DataFrame({
                    "clear_values": ["1979-04-24"],
                    "masked_values": ["2050-01-01"],
                }),
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Methods

### `_mask_like_faker(line: str) -> str`

Applies the faker-based masking operation to a single date string.

**Parameters:**

- `line` (`str`): The input date string.

**Returns:**

- `str`: The masked (fake) date string.

______________________________________________________________________

## Dependencies

- `FakerOperation` (from `masking.base_operations.operation_fake`)
- `FakeDateProvider` (from `masking.faker.date`)
- `dateparser` (for date parsing)
