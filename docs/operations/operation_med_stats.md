# Operation `med_stats`

Maps PLZ by replacing with MedStat region names, based on an official lookup table provided by the Swiss Federal Statistical Office (BFS).

For more information please have a look at:
https://dam-api.bfs.admin.ch/hub/api/dam/assets/33347910/master

---

## Class: `MedStatsOperationBase`

### Inherits from
`masking.base_operations.operation.Operation`

---

## Parameters

- **`col_name`** (`str`):
  The name of the column to be masked.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base class.

---

## Behavior

- Loads a lookup table from `sources/med_stats_2024.xlsx`, mapping Swiss postal codes (`PLZ`) to MedStat region names.
- Replaces each `PLZ` with its corresponding MedStat region name (e.g., `"8001"` → `"Zürich Zentrum"`).
- If a postal code is not found in the lookup table (e.g., a foreign PLZ), the original value is returned.

---

## Internal Methods

### `_get_medstats_lookup_table() -> dict[str, str]`

Parses the Excel lookup file to construct a mapping from `PLZ` to MedStat region names.

**Returns**:
- `dict[str, str]`: Dictionary where keys are PLZ codes and values are MedStat region names.

### `_mask_line(line: str, **kwargs) -> str`

Converts the input to a string and replaces it using the MedStat lookup table.

**Parameters**:
- `line` (`str`): The postal code (PLZ) to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns**:
- `str`: The MedStat region name, or the original value if not found.

---

## Dependencies

- `pandas` (for reading the Excel file)
- `openpyxl` (Excel engine)
- `med_stats_2024.xlsx` located under `sources/`
