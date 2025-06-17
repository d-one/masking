# Operations

This page is dedicated to provide information about available operations in the masking module.

Available operations:
- Masking:
    - fake_name
    - fake_plz
    - fake_date
    - hash
    - yyyy_hash
    - med_stats

- Masking on Dictionaries:
    - presidio_dictionary
    - string_match_dictionary

- Masking on Free-text:
    - presidio
    - string_match

## Operation Class

The `Operation` class defines the interface and core functionality for data masking transformations that operate on a single column in a DataFrame.
It allows for configurable masking behavior using a concordance table to ensure repeatable transformations.

Each subclass must implement the `_mask_line` and `_mask_data` methods, which define how individual values and entire columns are masked, respectively.

### 🔧 Constructor

```python
Operation(
    col_name: str,
    concordance_table: dict | pd.DataFrame | pyspark.sql.DataFrame | None = None,
    **kwargs
)
```

- **`col_name`** (`str`): Name of the column to apply the masking operation to.
- **`concordance_table`** (`dict | pd.DataFrame | pyspark.sql.DataFrame | None`, optional): A mapping between original and masked values. Can be a dictionary or DataFrame with columns `['clear_values', 'masked_values']`.
- **`**kwargs`**: Additional keyword arguments for customization in subclasses.

---

### 📌 Properties

- **`col_name`** → `str`: Returns the name of the column to be masked.
- **`serving_columns`** → `list[str]`: Returns a list of column names used by the operation to produce masked values. Can be overridden in subclasses to include more required columns.
- **`_needs_unique_values`** → `bool`: Indicates whether masked values must be unique. Defaults to `False`. Can be overridden.

---

## 🔄 Methods

### `__call__(self, data: AnyDataFrame) -> AnyDataFrame`

Applies the masking operation to the input DataFrame and returns a DataFrame with the masked column.

---

### `cast_concordance_table(concordance_table) -> dict`

Static method to convert different formats of concordance tables into a standard `dict[str, str]` of the form
```
concordance_table = {
    <clear_value> : <masked_value>
}
```
---

### `update_col_name(col_name: str) -> None`

Updates the target column name.

---

### `update_concordance_table(concordance_table: dict) -> None`

Merges new entries into the existing concordance table after validating input.

---

### `_check_mask_line(line: str | None, additional_values: dict | None = None, **kwargs) -> str | None`

Wrapper around `_mask_line` that handles retries and updates the concordance table if a unique masked value is required.

---

## 🔒 Abstract Methods (must be implemented in subclass)

### `_mask_line(self, line: str) -> str`

Defines how to mask a single input string value.

---

### `_mask_data(self, data: AnyDataFrame, **kwargs) -> AnyDataFrame`

Defines how to apply the masking transformation across an entire DataFrame column.

---

## ⚠️ Constants

- **`MAX_RETRY = 1000`**: Maximum number of retry attempts if a unique masked value is required and a collision occurs.

---
