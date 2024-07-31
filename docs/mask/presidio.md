# Operation Presidio

## `HashPresidio` Class

### Description

The `HashPresidio` class is designed to hash text data in a specific column of a DataFrame or Series using Presidio to detect and mask entities such as Personally Identifiable Information (PII). The class leverages the spaCy NLP engine for entity recognition and applies a hashing function to anonymize detected entities.

### Key Attributes

- `col_name` (str): The name of the column to be processed.
- `masking_function` (Callable[[str], str]): The function used to hash the detected entities.
- `nlp_engine` (LoadedSpacyNlpEngine): The spaCy NLP engine used to detect entities.
- `analyzer` (AnalyzerEngine): The Presidio `AnalyzerEngine` used to analyze and detect entities in the text.
- `_PII_ENTITIES` (ClassVar[set[str]]): A set of entity types considered PII, such as `EMAIL_ADDRESS`, `PERSON`, and `PHONE_NUMBER`.
- `delimiter` (dict): Delimiters used to mark the start and end of masked entities.
- `allow_list` (list[str]): A list of entities that should be ignored during masking.
- `executor` (ThreadPoolExecutor): A thread pool executor to parallelize the entity recognition process across multiple languages.

### Methods

#### `__init__(self, col_name: str, masking_function: Callable[[str], str], model: dict[str, str] | None = None, delimiter: str = "<<>>", allow_list: list[str] | None = None) -> None`

Initializes the `HashPresidio` class.

**Arguments:**
- `col_name` (str): The name of the column to be hashed.
- `masking_function` (Callable[[str], str]): The function used to hash detected entities.
- `model` (dict[str, str], optional): A dictionary specifying the spaCy models to be used for different languages. Defaults to `{"de": "de_core_news_lg", "en": "en_core_web_trf"}`.
- `delimiter` (str, optional): The delimiter to mark the start and end of masked entities. Defaults to `"<<" ">>"`.
- `allow_list` (list[str], optional): A list of entities to be ignored during the masking process. Defaults to `None`.

#### `_get_language_entities(self, line: str, language: str) -> set[str]`

Detects entities in the given text for a specific language.

**Arguments:**
- `line` (str): The input text.
- `language` (str): The language of the text.

**Returns:**
- `set[str]`: A set of detected entities in the text.

#### `_get_entities(self, line: str) -> list[str]`

Detects entities in the given text across all configured languages.

**Arguments:**
- `line` (str): The input text.

**Returns:**
- `list[str]`: A sorted list of detected entities, sorted by length in descending order.

#### `_mask_entities_line(self, line: str) -> str`

Masks entities in a single line of text.

**Arguments:**
- `line` (str): The input text.

**Returns:**
- `str`: The text with entities masked.

#### `_mask_entities(self, line: str) -> str`

Masks entities in a text.

**Arguments:**
- `line` (str): The input text.

**Returns:**
- `str`: The text with entities masked.

#### `_mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series`

Applies the hashing operation to the specified column in the DataFrame or Series.

**Arguments:**
- `data` (pd.DataFrame or pd.Series): The input DataFrame or Series containing the column to be masked.

**Returns:**
- `pd.DataFrame or pd.Series`: The DataFrame or Series with the masked column.

### Example Usage

```python
import hashlib
import pandas as pd
from presidio_analyzer import AnalyzerEngine
from operation_presidio import HashPresidio, LoadedSpacyNlpEngine

# Define the hashing function
def simple_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

# Initialize the Presidio-based hash operation
hash_presidio = HashPresidio(
    col_name="sensitive_column",
    masking_function=simple_hash,
    delimiter="<<>>",
    allow_list=["allowed@example.com"]
)

# Sample DataFrame
df = pd.DataFrame({'sensitive_column': ['John Doe works at Acme Corp', 'Jane Smith, 555-1234']})

# Apply the Presidio-based hash operation
masked_df = hash_presidio._mask_data(df)

print(masked_df)
