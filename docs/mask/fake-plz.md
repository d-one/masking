# FakePLZ Operation

## `FakePLZ` Class

### Description

The `FakePLZ` class is designed to obfuscate postal code (PLZ) data in a specific column of a DataFrame or Series using a fake PLZ provider. It replaces real postal codes with fake ones while optionally preserving certain parts of the PLZ. The class ensures that each unique PLZ in the column is consistently replaced with the same fake PLZ.

### Key Attributes

- `col_name` (str): The name of the column to be processed.
- `faker` (FakePLZProvider): An instance of `FakePLZProvider` used to generate fake postal codes.

### Methods

#### `__init__(self, col_name: str, preserve: str | tuple[str] | None = None, locale: str = "de_CH") -> None`

Initializes the `FakePLZ` class.

**Arguments:**
- `col_name` (str): The name of the column to be obfuscated.
- `preserve` (str or tuple[str], optional): The part of the PLZ to be preserved during obfuscation. See `masking.fake.plz.FakePLZProvider` for more details. Defaults to `None`.
- `locale` (str, optional): The locale for generating the fake PLZ data. Defaults to `"de_CH"`.

#### `_mask_line(self, line: str) -> str`

Obfuscates a single PLZ string.

**Arguments:**
- `line` (str): The input PLZ string.

**Returns:**
- `str`: The obfuscated (fake) PLZ string.

#### `_mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series`

Applies the PLZ obfuscation operation to the specified column in the DataFrame or Series.

**Arguments:**
- `data` (pd.DataFrame or pd.Series): The input DataFrame or Series containing the column to be obfuscated.

**Returns:**
- `pd.DataFrame or pd.Series`: The DataFrame or Series with the obfuscated PLZ column.

### Example Usage

```python
import pandas as pd
from masking.mask.operations.operation_plz import FakePLZ

# Initialize the FakePLZ operation
fake_plz = FakePLZ(col_name="postal_code", locale="de_CH", preserve=("district", "area"))

# Sample DataFrame
df = pd.DataFrame({'postal_code': ['8000', '9000', '1000']})

# Apply the FakePLZ operation
masked_df = fake_plz(df)

print(masked_df)
```
