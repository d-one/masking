# Operation `presidio_dict`

Detects and anonymizes sensitive entities in columns containing JSON or dictionary-structured data using Microsoft Presidio's NLP engine. Extends the base [`dict`](./operation_dict.md) operation with Presidio-powered entity detection applied to each leaf value.

______________________________________________________________________

## Class: `MaskDictOperationBase`

### Inherits from

- `masking.base_operations.operation_dict.DictOperationBase`
- `masking.utils.presidio_handler.PresidioHandler`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):
  The name of the column containing JSON/dictionary data to be masked.

- **`analyzer`** (`AnalyzerEngine`):
  A Presidio multilingual analyzer engine used for NLP-based entity detection.

- **`anonymizer`** (`AnonymizerEngine`, optional):
  A Presidio anonymizer engine. Defaults to a standard `AnonymizerEngine`.

- **`masking_function`** (`Callable[[str], str]`, optional):
  A custom function to mask detected entities and denied paths. When provided, overrides the default Presidio operators.

- **`pii_entities`** (`list[str]`, optional):
  List of entity types to detect. Defaults to `["EMAIL_ADDRESS", "PERSON", "PHONE_NUMBER", "DATE_TIME", "LOCATION"]`.

- **`allow_list`** (`list[str]`, optional):
  Values to exclude from masking (e.g., known safe names).

- **`allow_keys`** (`list[str]`, optional):
  Paths within the dictionary to consider for masking.

- **`deny_keys`** (`list[str]`, optional):
  Paths within the dictionary to mask directly without NLP analysis.

- **`**kwargs`** (`dict`):
  Additional keyword arguments passed to the base classes.

______________________________________________________________________

## Behavior

1. Parses each cell value as a JSON dictionary.
1. Identifies leaf nodes and classifies them as "mask" or "deny" paths.
1. For **mask paths**: runs all leaf values through Presidio's NLP batch processing across all supported languages, collecting detected entities.
1. For **deny paths**: applies the `masking_function` directly.
1. Anonymizes detected entities using Presidio's anonymizer with the configured operators.
1. Serializes the result back to a JSON string.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_presidio_dict import MaskDictOperation
from masking.utils.entity_detection import PresidioMultilingualAnalyzer

analyzer = PresidioMultilingualAnalyzer(
    models={"de": "de_core_news_lg", "en": "en_core_web_trf"}
).analyzer

op = MaskDictOperation(
    col_name="Report",
    masking_function=lambda x: "<MASKED>",
    analyzer=analyzer,
    pii_entities=["PERSON"],
)
```

### Pipeline Usage

```python
config = {
    "Report": [
        {
            "masking_operation": MaskDictOperation(
                col_name="Report",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                pii_entities=["PERSON"],
            )
        },
    ],
}
```

______________________________________________________________________

## Dependencies

- `DictOperationBase` (from `masking.base_operations.operation_dict`)
- `PresidioHandler` (from `masking.utils.presidio_handler`)
- Microsoft Presidio (`presidio_analyzer`, `presidio_anonymizer`)
- spaCy NLP models (e.g., `de_core_news_lg`, `en_core_web_trf`)
