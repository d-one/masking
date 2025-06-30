# Operations

This page provides an overview of the available operations in the masking module. Each operation is designed to anonymize or pseudonymize specific data types while preserving utility. Use this guide to find the operation that best suits your masking needs.

---

## 🔐 Minimization

Use when deterministic, irreversible masking is required.

- [`null`](./operation_null.md):
  Replace any entry with a null value

---

## 🔐 Cryptographic Hashing

Use when deterministic, irreversible masking is required.

- [`hash`](./operation_hash.md):
  Applies a cryptographic hash (default: SHA256) to input values. Optionally accepts a secret for HMAC-style hashing.

- [`yyyy_hash`](./operation_yyyy_hash.md):
  Hashes full datetime values but prepends the clear-text year (e.g., `1990_<hash>`). Useful when year-level granularity should be preserved.

---

## 🧠 Semantic Replacement

Use when fake but realistic data is needed, especially for test environments or stakeholder demos.

- [`fake_name`](./operation_fake_name.md):
  Replaces names with synthetic, locale-aware full/first/last names. Supports gender specification and reuse for consistent pseudonyms.

- [`fake_plz`](./operation_fake_plz.md):
  Replaces postal codes (PLZ) with realistic, region-consistent synthetic alternatives. Allows preservation of digits (e.g., first two).

- [`fake_date`](./operation_fake_date.md):
  Replaces dates with fake but plausible alternatives. Can preserve parts like the year or month.

---

## 🗺️ Region Mapping

Use when transforming location codes to standardized geographical regions.

- [`med_stats`](./operation_med_stats.md):
  Maps Swiss postal codes (PLZ) to official MedStat region names using data from the Swiss Federal Statistical Office.

---

## 📚 Masking on Free-text

Use for unstructured fields like comments, messages, or any human-written text.

- [`presidio`](./operation_presidio.md):
  Detects and anonymizes sensitive entities (e.g., names, phone numbers, emails) in free-text using Microsoft Presidio.

- [`string_match`](./operation_string_match.md):
  Detects and masks values in free-text by matching known values (e.g., names from other columns or lookup dictionaries).

---

## 🧩 Masking on Dictionaries *(WIP)*

Use for advanced string substitution using structured dictionaries (not yet linked).

- `presidio_dictionary`: Applies entity detection and dictionary-based replacement.
- `string_match_dictionary`: Uses dictionary values for exact or fuzzy matching.

---

## Operation Class

The `Operation` class defines the interface and core functionality for data masking transformations that operate on a single column in a DataFrame. It allows for configurable masking behavior using a concordance table to ensure repeatable transformations.

Each subclass must implement the `_mask_line` and `_mask_data` methods, which define how individual values and entire columns are masked, respectively.

---

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

Static method to convert different formats of concordance tables into a standard `dict[str, str]` of the form:

```python
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
