# Operation `yyyy_hash`

Hashes datetime values while preserving the year in clear text. The output format is `<year>_<hash>`, allowing year-level analysis while keeping the full date irreversible.

**Example:** `1990-01-15` → `1990_a3f2b7c8d9e1...`

______________________________________________________________________

## Class: `YYYYHashOperationBase`

### Inherits from

[`masking.base_operations.operation_hash.HashOperationBase`](./operation_hash.md)

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing datetime values to be masked.

- **`secret`** (`str`, optional):\
  The optional secret used for keyed hashing. See [`hash`](./operation_hash.md) for details.

- **`hash_function`** (`Callable`, default=`hashlib.sha256`):\
  The hashing function to use. Must follow the `hashlib` interface.

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear values to masked values.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Converts the input to a string if it's not already.
- Parses the input using `dateparser.parse()` to extract the year.
- Hashes the original full date using the specified hashing function.
- Returns a combined string in the format `<year>_<hash>` where:
  - `year` is the 4-digit extracted year (zero-padded),
  - `hash` is the hash of the full original date string.
- Raises `ValueError` if the date cannot be parsed.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation

op = YYYYHashOperation(col_name="Geburtsdatum", secret="my_secret")
```

### Pipeline Usage

Often used as a second step after `fake_date` to further anonymize dates:

```python
from pandas import DataFrame

config = {
    "Geburtsdatum": [
        {
            "masking_operation": FakeDate(
                col_name="Geburtsdatum",
                preserve=("year", "month"),
            )
        },
        {
            "masking_operation": YYYYHashOperation(
                col_name="Geburtsdatum",
                secret="my_secret",
                concordance_table=DataFrame({
                    "clear_values": ["2050-01-01"],
                    "masked_values": ["<MASKED>"],
                }),
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Method

### `_mask_line(line: str | datetime, **kwargs) -> str`

Extracts the year, hashes the full date, and returns the combined result.

**Parameters:**

- `line` (`str` or `datetime`): The input datetime to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `str`: A string in the form `<year>_<hashed value>`.

______________________________________________________________________

## Dependencies

- `dateparser.parse` (for parsing and extracting the year)
- `hashlib` (default hash function library)
- `HashOperationBase` (from `masking.base_operations.operation_hash`)
