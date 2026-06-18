# Operation `hash`

Hashes values using a cryptographic hash function (default: SHA256). Optionally, a secret key can be provided to introduce keyed hashing (HMAC-style), enhancing the security of the hash.

The transformation is **deterministic**: the same input always produces the same hash. It is also **irreversible**: the original value cannot be recovered from the hash.

______________________________________________________________________

## Class: `HashOperationBase`

### Inherits from

`masking.base_operations.operation.Operation`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column to be hashed.

- **`secret`** (`str`, optional):\
  An optional secret string used for keyed hashing. When provided, the hash is computed as an HMAC using `hash_string`, making the output unpredictable without knowledge of the secret.

- **`hash_function`** (`Callable`, default=`hashlib.sha256`):\
  The hashing function to apply. Must follow the `hashlib` interface (e.g., `hashlib.sha512`, `hashlib.md5`).

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear values to masked values. Values found in the concordance table are not re-hashed.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- Converts the input to a string if necessary.
- Applies a cryptographic hash function to the input (default is SHA256).
- If a `secret` is provided, uses it to compute a deterministic keyed hash via `hash_string`.
- If no `secret` is provided, computes a plain hash of the input.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_hash import HashOperation

# Simple hashing (no secret)
op = HashOperation(col_name="Name")

# Keyed hashing with a secret
op = HashOperation(col_name="Name", secret="my_secret")

# Using SHA-512 instead of SHA-256
import hashlib
op = HashOperation(col_name="Name", hash_function=hashlib.sha512)
```

### Pipeline Usage

```python
from pandas import DataFrame

config = {
    "Name": [
        {
            "masking_operation": HashOperation(
                col_name="Name",
                secret="my_secret",
                concordance_table=DataFrame({
                    "clear_values": ["known_value"],
                    "masked_values": ["pre_mapped_hash"],
                }),
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Methods

### `_hashing_function(line: str) -> str`

Hashes a single string input.

**Parameters:**

- `line` (`str`): The input value.

**Returns:**

- `str`: The hashed value.

### `_mask_line(line: str | int, **kwargs) -> str`

Prepares the input (e.g., converting it to a string), then applies the hashing function.

**Parameters:**

- `line` (`str` or `int`): The value to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `str`: The masked (hashed) value.

______________________________________________________________________

## Dependencies

- `hashlib` (for default hashing algorithms)
- `hash_string` (from `masking.utils.hash`)
