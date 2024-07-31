# Masking Package

## Introduction

The Masking Package is a Python library designed to provide various masking operations on data columns. It supports hashing, fake data generation, and other masking techniques to ensure data privacy and security.

## Table of Content

- Development:
    - [Installation](docs/installation.md)
    - [Available Modules](docs/available_modules.md)
        - [mask](docs/mask/mask.md)
        - [demask](docs/demask/demask.md)

## Installation

To install the Masking Package, use the following command:

```bash
pip install masking@git+"https://github.com/d-one/masking.git"
```

## Usage

Here is a basic example of how to use the package

```python
import pandas as pd
from masking.mask.pipeline import MaskColumnPipeline

# Sample DataFrame
data = {'name': ['Alice', 'Bob', 'Charlie'], 'ssn': ['123-45-6789', '987-65-4321', '555-55-5555']}
df = pd.DataFrame(data)

# Configuration for masking
config = {
        'masking': 'hash',
        'config': { "col_name": "ssn", "secret": "my_secret" }
    }

# Create and apply the masking pipeline
pipeline = MaskColumnPipeline(column='ssn', config=config)
masked_df = pipeline(df)

print(masked_df)
```
