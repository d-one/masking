# Operation `fake_name`

Replaces entries with realistic, locale-aware synthetic names.

## Purpose

This operation replaces name values in a column with plausible, locale-aware synthetic names using Faker-based logic. It supports customization of locale, gender, name type, and reuse behavior to meet different anonymization requirements.
By default names are taken from the two datasets:

Lastnames:
`https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208773.html
`

Firstnames:
`https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208757.html
`

---

## Class: `FakeNameBase`

### Inherits from
`masking.base_operations.operation_fake.FakerOperation`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`locale`** (`str`, optional):
  Specifies the locale to use when generating names.
  **Default:** `"de_CH"`

- **`gender`** (`str | None`, optional):
  Specifies the gender for name generation.
  **Valid values:**
    - `"male"`, `"m"`
    - `"female"`, `"f"`
    - `"nonbinary"`, `"nb"`
    - `None`

- **`name_type`** (`str`, optional):
  Specifies which type of name to generate.
  **Default:** `"full"`
  **Valid values:**
    - `"full"`
    - `"first"`
    - `"last"`

- **`reuse_existing`** (`bool`, optional):
  Determines whether to reuse previously encountered names instead of always generating new ones.
  **Default:** `True`

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Generates new names based on the provided configuration.
- Substitutes the entire cell content with newly generated names.
- Optionally reuses previously generated names randomly when `reuse_existing=True`.

---

## Internal Methods

### `_mask_line_generate()`

Generates a name according to the `name_type` and `gender`.

Returns:
- `str`: A newly generated name.

### `_mask_like_faker(line: str) -> str`

Masks a single input line, either generating a new name or reusing one depending on configuration.

Parameters:
- **`line`** (`str`): The input value to be masked.

Returns:
- **`str`**: The masked (fake) name.

---

## Dependencies

- `FakeNameProvider` (from `masking.faker.name`)
- `FakerOperation` (from `masking.base_operations.operation_fake`)
