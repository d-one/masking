# Operation `fake_plz`

Replaces Swiss postal code (PLZ) values with realistic synthetic alternatives. Optionally preserves parts of the original PLZ to maintain regional consistency.

Generated PLZ values are drawn from official Swiss Post data:

- **Source:** [swisstopo PLZ directory](https://data.geo.admin.ch/ch.swisstopo-vd.ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz_2056.csv.zip)

______________________________________________________________________

## Class: `FakePLZBase`

### Inherits from

[`masking.base_operations.operation_fake.FakerOperation`](./operation_fake.md)

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing PLZ values to be masked.

- **`preserve`** (`str | tuple[str] | None`, optional):\
  Specifies which digit positions of the original PLZ should be preserved.\
  Swiss PLZ codes are 4 digits: each digit has a geographic meaning:

  | Value        | Digit position | Meaning         | Example (`3436`)         |
  | ------------ | -------------- | --------------- | ------------------------ |
  | `"district"` | 1st            | Region/district | `3` (Bern)               |
  | `"area"`     | 2nd            | Area            | `34` (Burgdorf)          |
  | `"route"`    | 3rd            | Postal route    | `343` (Burgdorf–Langnau) |
  | `"postcode"` | 4th            | Post office     | `3436` (Zollbrück)       |

  Preservation is **cumulative from left to right**: e.g., preserving `"area"` also preserves `"district"`.

- **`locale`** (`str`, default `"de_CH"`):\
  Locale code used for generating PLZ codes.

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear PLZ values to masked values.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Generates a fake PLZ from the official Swiss PLZ list.
- If `preserve` is set, constrains the generated PLZ to share the specified leading digits with the original.
- Retries up to 1000 times to ensure the result differs from the original.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_fake_plz import FakePLZ

# Full replacement
op = FakePLZ(col_name="PLZ")

# Preserve district (1st digit)
op = FakePLZ(col_name="PLZ", preserve="district")

# Preserve district, area, and route (first 3 digits)
op = FakePLZ(col_name="PLZ", preserve=("district", "area", "route"))
```

### Pipeline Usage

```python
config = {
    "PLZ": [
        {
            "masking_operation": FakePLZ(
                col_name="PLZ",
                preserve=("district", "area", "route"),
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Method

### `_mask_like_faker(line: str) -> str`

Masks a single PLZ value using the `FakePLZProvider`.

**Parameters:**

- `line` (`str`): The original PLZ value.

**Returns:**

- `str`: The masked PLZ.

______________________________________________________________________

## Dependencies

- `FakePLZProvider` (from `masking.faker.plz`)
- `FakerOperation` (from `masking.base_operations.operation_fake`)
