# Masking Operations Documentation

## hash.py

### `hash_string` Function

```python
def hash_string(
    data: str | None,
    secret: str,
    method: Callable = hashlib.sha256,
    text_encoding: str = "utf-8"
) -> str:
```

#### Description

The `hash_string` function hashes a given string using a secret key with a default hashing algorithm of SHA256.

#### Arguments

- `data` (str | None): The input string to be hashed. If `None` is passed, the function returns `None`.
- `secret` (str): The secret key used for hashing the input string.
- `method` (Callable, optional): The hashing method to use. Defaults to `hashlib.sha256`.
- `text_encoding` (str, optional): The text encoding to use. Defaults to `"utf-8"`.

#### Returns

- `str`: The hashed string.

#### Example Usage

```python
hashed_value = hash_string("example_data", "my_secret_key")
print(hashed_value)
```

## operation_hash.py

### `HashOperation` Class

```python
class HashOperation(Operation):
    def __init__(
        self, col_name: str, secret: str, hash_function: Callable = hashlib.sha256
    ) -> None:
```

#### Description

The `HashOperation` class is designed to apply a hashing operation to a specific column in a Pandas DataFrame or Series using a specified hashing algorithm.

#### Key Attributes

- `col_name` (str): The name of the column that will be hashed.
- `secret` (str): The secret key used for hashing the column values.
- `hash_function` (Callable): The hash function to use, with `hashlib.sha256` as the default.

#### Methods

##### `__init__(self, col_name: str, secret: str, hash_function: Callable = hashlib.sha256) -> None`

Initializes the `HashOperation` class.

**Arguments:**
- `col_name` (str): The name of the column to be hashed.
- `secret` (str): The secret key used to hash the column values.
- `hash_function` (Callable, optional): The hash function to use. Defaults to `hashlib.sha256`.

##### `_mask_line(self, line: str) -> str`

Masks a single line using the specified hashing function.

**Arguments:**
- `line` (str): The input string to be masked.

**Returns:**
- `str`: The masked (hashed) string.

##### `_mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series`

Applies the hashing operation to the specified column in the DataFrame or Series.

**Arguments:**
- `data` (pd.DataFrame or pd.Series): The input DataFrame or Series containing the column to be masked.

**Returns:**
- `pd.DataFrame or pd.Series`: The DataFrame or Series with the masked column.

#### Example Usage

```python
import hashlib
import pandas as pd
from operation_hash import HashOperation

# Create a sample DataFrame
df = pd.DataFrame({'sensitive_column': ['value1', 'value2', 'value3']})

# Initialize the HashOperation
hash_operation = HashOperation(col_name='sensitive_column', secret='my_secret_key', hash_function=hashlib.sha256)

# Apply the hash operation
masked_df = hash_operation._mask_data(df)

print(masked_df)
```
