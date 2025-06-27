# Operation `yyyy_hash`

Hashes datetime values by appending to the front the clear value of the year.

---

## Class: `YYYYHashOperationBase`

### Inherits from
`masking.base_operations.operation_hash.HashOperationBase`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`secret`** (`str`, optional):
  The optional secret used for keyed hashing.

- **`hash_function`** (`Callable`, default=`hashlib.sha256`):
  The hashing function to use. Must follow the `hashlib` interface.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Converts the input to a string if it’s not already.
- Parses the input using `dateparser.parse()` to extract the year.
- Hashes the original full date using the specified hashing function.
- Returns a combined string in the format `<year>_<hash>` where:
  - `year` is the 4-digit extracted year,
  - `hash` is the hash of the full original date.

---

## Internal Method

### `_mask_line(line: str | datetime, **kwargs) -> str`

Processes and masks a datetime value by extracting the year and hashing the full value.

**Parameters**:
- `line` (`str` or `datetime`): The input datetime to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns**:
- `str`: A string in the form `<year>_<hashed value>`.

---

## Dependencies

- `dateparser.parse` (for parsing and extracting the year)
- `hashlib` (default hash function library)
