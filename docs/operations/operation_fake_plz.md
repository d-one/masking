# Operation `fake_plz`

Replaces postal code (`PLZ`) values in a column with realistic, locale-specific synthetic data. Optionally, parts of the original PLZ can be preserved.

Newly generated PLZ are based on the data provided at:
```python
    _DOWNLOAD_URL: str = "https://data.geo.admin.ch/ch.swisstopo-vd.ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz/ortschaftenverzeichnis_plz_2056.csv.zip"
```

---

## Class: `FakePLZBase`

### Inherits from
`masking.base_operations.operation_fake.FakerOperation`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`preserve`** (`str | tuple[str] | None`, optional):
  Specifies which parts of the original PLZ should be preserved.
  **Admissible values:**
    - `district` – first digit from the left
    - `area` – second digit from the left
    - `route` – third digit from the left
    - `postcode` – fourth digit from the left

- **`locale`** (`str`, default=`"de_CH"`):
  Locale code used for generating PLZ codes.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Uses locale-aware Faker logic to generate fake PLZ values.
- Supports partial preservation of the original PLZ format via `preserve`.
  (Preservation is left-to-right only: e.g. if `area` is preserved, `district` must be preserved as well.)
- The result is consistent with regional expectations based on the `locale`.

---

## Internal Method

### `_mask_like_faker(line: str) -> str`

Masks a single input line using the `FakePLZProvider`.

**Parameters**:
- `line` (`str`): The original PLZ value.

**Returns**:
- `str`: The masked PLZ.

---

## Dependencies

- `FakePLZProvider` (from `masking.faker.plz`)
- `FakerOperation` (from `masking.base_operations.operation_fake`)
