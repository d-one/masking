# Operation `med_stats`

Maps Swiss postal codes (PLZ) to official MedStat region names using a lookup table from the Swiss Federal Statistical Office (BFS). This is a **generalization** operation — it reduces geographic precision from postal-code level to region level.

**Example:** `8001` → `Zürich Zentrum`

- **Source:** [BFS MedStat Regions](https://dam-api.bfs.admin.ch/hub/api/dam/assets/33347910/master)

______________________________________________________________________

## Class: `MedStatsOperationBase`

### Inherits from

`masking.base_operations.operation.Operation`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing PLZ values to be mapped.

- **`concordance_table`** (`dict | pd.DataFrame | None`, optional):\
  A pre-existing mapping of clear values to masked values.

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base class.

______________________________________________________________________

## Behavior

- On first instantiation, loads a lookup table from `sources/med_stats_2024.xlsx` (sheet `REGION=CH`), mapping the `NPA/PLZ` column to the `MedStat` column.
- The lookup table is cached as a class-level attribute for efficiency.
- Replaces each PLZ with its corresponding MedStat region name.
- If a postal code is **not found** in the lookup table (e.g., a foreign PLZ), the original value is returned unchanged.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_med_stats import MedStatsOperation

op = MedStatsOperation(col_name="PLZ")
```

### Pipeline Usage

Often used as a second step after `fake_plz` to further generalize postal codes:

```python
config = {
    "PLZ": [
        {
            "masking_operation": FakePLZ(
                col_name="PLZ",
                preserve=("district", "area", "route"),
            )
        },
        {
            "masking_operation": MedStatsOperation(col_name="PLZ")
        },
    ],
}
```

______________________________________________________________________

## Internal Methods

### `_get_medstats_lookup_table() -> dict[str, str]`

Parses the Excel lookup file to construct a mapping from PLZ to MedStat region names.

**Returns:**

- `dict[str, str]`: Dictionary where keys are PLZ codes and values are MedStat region names.

### `_mask_line(line: str, **kwargs) -> str`

Converts the input to a string and replaces it using the MedStat lookup table.

**Parameters:**

- `line` (`str`): The postal code (PLZ) to be masked.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `str`: The MedStat region name, or the original value if not found.

______________________________________________________________________

## Dependencies

- `pandas` (for reading the Excel file)
- `openpyxl` (Excel engine)
- `med_stats_2024.xlsx` located under `sources/`
