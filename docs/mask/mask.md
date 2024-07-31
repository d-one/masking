# The mask module

This page is dedicated to the documentation of the mask module.




## Introduction

The Masking Module provides a flexible and efficient way to anonymize or obfuscate sensitive data in a Pandas DataFrame. This is achieved through a pipeline approach where a series of operations are applied to specific columns within the DataFrame.

### Approach Overview

The core concept of this module revolves around defining a pipeline consisting of a list of operations. Each operation is designed to act on a single column of the DataFrame, allowing for targeted and customizable data transformations. These transformations can include hashing, pseudonymization, or other custom masking techniques as required.

#### Key Concepts

##### Operation

An `Operation` is an abstract class that represents a data transformation step within the pipeline. Each operation is responsible for applying a specific masking technique to a designated column in the DataFrame. The operations are designed to be modular, allowing you to mix and match different masking strategies depending on the requirements of your dataset.

##### Pipeline

A `pipeline` is essentially a sequence of operations that will be applied to a DataFrame. The operations are defined in a specific order, and each operation processes a designated column within the DataFrame.
The pipeline can be of two types:
- `MaskColumnPipeline` : is a pipeline acting on a single column of the Pandas Dataframe
- `MaskDataFramePipeline` : is a pipeline acting on an entire Pandas Dataframe.

#### Workflow

- **Define Operations**: Begin by defining the operations you want to apply to your DataFrame. Each operation should specify the column it will act on and the type of transformation it will perform.
- **Create a Pipeline**: Once the operations are defined, they are assembled into a pipeline—a list where each entry is an operation that will be applied sequentially.
- **Apply the Pipeline**: The pipeline is then executed on the DataFrame, applying each operation in order. The result is a transformed DataFrame where the specified columns have been masked according to the defined operations.

#### Getting Started

Here we provide a simple script that showcases the usage of the module

```python
from masking.mask.pipeline import MaskColumnPipeline
import hashlib
import pandas as pd

# Create the pipeline
pipeline = MaskColumnPipeline(column='sensitive_column',
                              config={'masking': 'hash',
                                      'config': {'secret' : "my_secret_key",
                                                 'hash_function': hashlib.sha256}}
                              )

# Sample DataFrame
df = pd.DataFrame({'sensitive_column': ['value1', 'value2', 'value3']})

# Print the DataFrame
print(df)

# Apply the pipeline
masked_df = pipeline(df)

print(masked_df)
```
