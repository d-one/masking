
# Masking Pipelines Documentation

## Introduction

This document provides an overview and usage guide for the pipeline classes, which are part of the Masking Module. These pipelines allow users to apply a series of masking operations to specific columns or entire DataFrames in a flexible and efficient manner.

## MaskColumnPipeline

### Overview

The `MaskColumnPipeline` class is designed to apply a series of operations to mask a specific column within a DataFrame. Each operation is defined in the configuration and is applied sequentially to the column's unique values. The pipeline is responsible for managing the transformation and ensuring that the masked data is correctly populated back into the DataFrame.

### Key Attributes

- `config`: A dictionary defining the masking operation and its configuration.
- `pipeline`: A list of `Operation` objects that will be applied to the column.
- `column_name`: The name of the column that will be masked.
- `concordance_table`: A DataFrame or dictionary storing the unique values in the column and their masked counterparts.

### Methods

#### `__init__(self, column: str, config: dict[str, dict[Any]], concordance_table: pd.DataFrame | None = None) -> None`

Initializes the `MaskColumnPipeline`.

**Arguments:**
- `column` (str or int): The column name or index to be masked.
- `config` (dict): The configuration for the masking operation.
- `concordance_table` (pd.DataFrame or None): A DataFrame storing the unique values in the column and their masked counterparts.

#### `from_dict(cls, configuration: dict[str, dict[dict[str, Any]]]) -> None`

A class method to create a `MaskColumnPipeline` from a dictionary configuration.

#### `__call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series`

Applies the pipeline to the given data, masking the specified column.

**Arguments:**
- `data` (pd.DataFrame or pd.Series): The input DataFrame or Series.

**Returns:**
- `pd.DataFrame or pd.Series`: The DataFrame or Series with the masked column.

### Example Usage

```python
from masking.mask.pipeline import MaskColumnPipeline
import hashlib
import pandas as pd

# Create the pipeline
pipeline = MaskColumnPipeline(column='sensitive_column',
                              config={'masking': 'hash',
                                      'config': {'secret': "my_secret_key",
                                                 'hash_function': hashlib.sha256}}
                              )

# Sample DataFrame
df = pd.DataFrame({'sensitive_column': ['value1', 'value2', 'value3']})

# Print the DataFrame
print("Original DataFrame:")
print(df)

# Apply the pipeline
masked_df = pipeline(df)

# Print the masked DataFrame
print("Masked DataFrame:")
print(masked_df)
```

## MaskDataFramePipeline

### Overview

The `MaskDataFramePipeline` class is designed to apply a series of operations across multiple columns within a DataFrame. It manages multiple `MaskColumnPipeline` objects and allows for parallel processing to efficiently mask large datasets.

### Key Attributes

- `config`: A dictionary defining the configuration for each column to be masked.
- `col_pipelines`: A list of `MaskColumnPipeline` objects for each column.
- `concordance_tables`: A dictionary storing the concordance tables for each column.
- `masked_data`: A dictionary storing the masked data for each column.
- `workers`: The number of worker threads to use for parallel processing.

### Methods

#### `__init__(self, configuration: dict[str, dict[str, tuple[dict[str, Any]]]], workers: int = 1) -> None`

Initializes the `MaskDataFramePipeline`.

**Arguments:**
- `configuration` (dict): The configuration for each column to be masked.
- `workers` (int, optional): The number of worker threads for parallel processing. Defaults to 1.

#### `__call__(self, data: pd.DataFrame) -> pd.DataFrame`

Applies the pipeline to the entire DataFrame, masking the specified columns.

**Arguments:**
- `data` (pd.DataFrame): The input DataFrame.

**Returns:**
- `pd.DataFrame`: The DataFrame with the masked columns.

### Example Usage

```python
from masking.mask.pipeline import MaskDataFramePipeline
import hashlib
import pandas as pd

# Configuration for multiple columns
config = {
    'sensitive_column1': {'masking': 'hash', 'config': {'secret': "secret1", 'hash_function': hashlib.sha256}},
    'sensitive_column2': {'masking': 'hash', 'config': {'secret': "secret2", 'hash_function': hashlib.sha256}},
}

# Create the DataFrame pipeline
pipeline = MaskDataFramePipeline(configuration=config, workers=2)

# Sample DataFrame
df = pd.DataFrame({
    'sensitive_column1': ['value1', 'value2', 'value3'],
    'sensitive_column2': ['info1', 'info2', 'info3'],
})

# Apply the pipeline
masked_df = pipeline(df)

# Print the masked DataFrame
print(masked_df)
```
