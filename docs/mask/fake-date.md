# FakeDate Operation

## `FakeDate` Class

### Description

The `FakeDate` class is designed to obfuscate dates in a specific column of a DataFrame or Series using a date faker. It replaces dates with fake values while optionally preserving certain parts of the date (e.g., the year). The class ensures that each unique date in the column is consistently replaced with the same fake date.

### Key Attributes

- `col_name` (str): The name of the column to be processed.
- `faker` (FakeDateProvider): An instance of `FakeDateProvider` used to generate fake dates.

### Methods

#### `__init__(self, col_name: str, preserve: str | tuple[str] = "year") -> None`

Initializes the `FakeDate` class.

**Arguments:**
- `col_name` (str): The name of the column to be obfuscated.
- `preserve` (str or tuple[str], optional): The part of the date to be preserved during obfuscation. Defaults to `"year"`. See `masking.fake.date.FakeDateProvider` for more details.

#### `_mask_line(self, line: str) -> str`

Obfuscates a single date string.

**Arguments:**
- `line` (str): The input date string.

**Returns:**
- `str`: The obfuscated (fake) date string.

#### `_mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series`

Applies the date obfuscation operation to the specified column in the DataFrame or Series.

**Arguments:**
- `data` (pd.DataFrame or pd.Series): The input DataFrame or Series containing the column to be obfuscated.

**Returns:**
- `pd.DataFrame or pd.Series`: The DataFrame or Series with the obfuscated date column.

### Example Usage

```python
import pandas as pd
from masking.mask.operations.operation_date import FakeDate

# Initialize the FakeDate operation
fake_date = FakeDate(col_name="birthdate", preserve="year")

# Sample DataFrame
df = pd.DataFrame({'birthdate': ['1990-01-01', '1985-05-23', '1990-01-01']})

# Apply the FakeDate operation
masked_df = fake_date(df)

print(masked_df)
```
