# Operation `fake_name`

Replaces name values with realistic, locale-aware synthetic names. Supports customization of locale, gender, name type, and reuse behavior.

By default, names are sampled from official Swiss Federal Statistical Office datasets:

- **Last names:** [BFS Lastnames 2024](https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208773.html)
- **First names:** [BFS Firstnames 2024](https://www.bfs.admin.ch/bfs/en/home/statistics/population/births-deaths/names-switzerland.assetdetail.32208757.html)

______________________________________________________________________

## Class: `FakeNameBase`

### Inherits from

[`masking.base_operations.operation_fake.FakerOperation`](./operation_fake.md)

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column to be masked.

- **`locale`** (`str`, default `"de_CH"`):\
  Specifies the locale to use when generating names.

- **`gender`** (`str | None`, optional):\
  Specifies the gender for name generation.\
  **Valid values:** `"male"`, `"m"`, `"female"`, `"f"`, `"nonbinary"`, `"nb"`, `None`\
  **Default:** `None` (random gender)

- **`name_type`** (`str`, default `"full"`):\
  Specifies which type of name to generate.\
  **Valid values:** `"full"`, `"first"`, `"last"`

- **`reuse_existing`** (`bool`, default `True`):\
  When `True`, randomly reuses previously generated names alongside new ones, creating more realistic distributions. When `False`, always generates fresh names.

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear name values to masked values.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Generates new names based on the provided configuration.
- Substitutes the entire cell content with a newly generated name.
- When `reuse_existing=True`, randomly decides whether to generate a new name or reuse a previously seen one, producing a more natural distribution of names.
- Retries up to 1000 times to ensure the generated name differs from the original.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_fake_name import FakeNameOperation

# Full names, default locale (de_CH)
op = FakeNameOperation(col_name="Name", name_type="full")

# Female first names only
op = FakeNameOperation(col_name="Vorname", name_type="first", gender="f")

# Last names without reuse
op = FakeNameOperation(col_name="Name", name_type="last", reuse_existing=False)
```

### Pipeline Usage

```python
from pandas import DataFrame

config = {
    "Name": [
        {
            "masking_operation": FakeNameOperation(
                col_name="Name",
                concordance_table=DataFrame({
                    "clear_values": ["Spiess"],
                    "masked_values": ["SP"],
                }),
                name_type="last",
                reuse_existing=True,
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Methods

### `_mask_line_generate() -> str`

Generates a name according to the `name_type` and `gender`.

**Returns:**

- `str`: A newly generated name.

### `_mask_like_faker(line: str) -> str`

Masks a single input line, either generating a new name or reusing one depending on configuration.

**Parameters:**

- `line` (`str`): The input value to be masked.

**Returns:**

- `str`: The masked (fake) name.

______________________________________________________________________

## Dependencies

- `FakeNameProvider` (from `masking.faker.name`)
- `FakerOperation` (from `masking.base_operations.operation_fake`)
