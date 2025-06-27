# Operation `hash`

Hashes values using a cryptographic hash function (default: SHA256). Optionally, a secret key can be provided to introduce keyed hashing (HMAC-style), enhancing the security of the hash.

---

## Class: `HashOperationBase`

### Inherits from
`masking.base_operations.operation.Operation`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be hashed.

- **`secret`** (`str`, optional):
  An optional secret string used for keyed hashing.

- **`hash_function`** (`Callable`, default=`hashlib.sha256`):
  The hashing function to apply. Must follow the `hashlib` interface.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Converts the input to a string if necessary.
- Applies a cryptographic hash function to the input (default is SHA256).
- If a `secret` is provided, uses it to compute a deterministic keyed hash via `hash_string`.

---

## Internal Methods

### `_hashing_function(line: str) -> str`

Hashes a single string input.

**Parameters**:
- `line` (`str`): The input value.

**Returns**:
- `str`: The hashed value.

### `_mask_line(line: str | int, **kwargs) -> str`

Prepares the input (e.g. converting it to a string), then applies the hashing function.

**Parameters**:
- `line` (`str` or `int`): The value to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns**:
- `str`: The masked (hashed) value.

---

## Dependencies

- `hashlib` (for default hashing algorithms)
- `hash_string` (from `masking.utils.hash`)
