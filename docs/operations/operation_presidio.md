# Operation `presidio`

Uses Microsoft Presidio to detect sensitive entities in free-text (e.g., names, phone numbers, emails, dates, locations) and anonymizes them using a configurable strategy.

______________________________________________________________________

## Class: `MaskPresidioBase`

### Inherits from

- `masking.base_operations.operation.Operation`
- `masking.utils.presidio_handler.PresidioHandler`

______________________________________________________________________

## Parameters

- **`col_name`** (`str`):\
  The name of the column containing free-text to be masked.

- **`analyzer`** (`AnalyzerEngine`):\
  A Presidio multilingual analyzer engine. Required for NLP-based entity detection.

- **`anonymizer`** (`AnonymizerEngine`, optional):\
  A Presidio anonymizer engine. Defaults to a standard `AnonymizerEngine`.

- **`masking_function`** (`Callable[[str], str]`, optional):\
  A custom function to replace detected entities. When provided, overrides the default Presidio operators for all entity types.

- **`pii_entities`** (`list[str]`, optional):\
  List of entity types to detect. If not provided, defaults to:\
  `["EMAIL_ADDRESS", "PERSON", "PHONE_NUMBER", "DATE_TIME", "LOCATION"]`

- **`allow_list`** (`list[str]`, optional):\
  Values that should **not** be masked, even if detected as PII (e.g., known public names).

- **`operators`** (`dict[str, OperatorConfig]`, optional):\
  Fine-grained control over how each entity type is anonymized. Defaults to replacing each entity with `<ENTITY_TYPE>` (e.g., `<PERSON>`).

- **`**kwargs`** (`dict`):\
  Additional keyword arguments passed to the base classes.

______________________________________________________________________

## Behavior

- Runs the Presidio analyzer on each cell value to detect PII entities.
- Applies the configured anonymization operator to each detected entity.
- If `masking_function` is provided, all entities are masked using that function (via a `custom` operator).
- If no entities are detected, the original text is returned unchanged.

______________________________________________________________________

## Configuration Example

```python
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.utils.entity_detection import PresidioMultilingualAnalyzer

analyzer = PresidioMultilingualAnalyzer(
    models={"de": "de_core_news_lg", "en": "en_core_web_trf"}
).analyzer

# Default replacement (entities become <PERSON>, <EMAIL_ADDRESS>, etc.)
op = MaskPresidio(col_name="Beschrieb", analyzer=analyzer)

# Custom masking function
op = MaskPresidio(
    col_name="Beschrieb",
    analyzer=analyzer,
    masking_function=lambda x: "<MASKED>",
    pii_entities=["PERSON"],
    allow_list=["Darius"],
)
```

### Pipeline Usage

```python
config = {
    "Beschrieb": [
        {
            "masking_operation": MaskPresidio(
                col_name="Beschrieb",
                masking_function=lambda x: "<MASKED>",
                analyzer=analyzer,
                allow_list=["Darius"],
                pii_entities=["PERSON"],
            )
        },
    ],
}
```

______________________________________________________________________

## Internal Method

### `_mask_line(line: str, entities: list[str] | None = None, **kwargs) -> str`

Detects and anonymizes entities in a single text line.

**Parameters:**

- `line` (`str`): The input text to be masked.
- `entities` (`list[str]`, optional): Pre-computed entity results. If `None`, detection runs automatically.
- `**kwargs` (`dict`): Additional arguments (ignored).

**Returns:**

- `str`: The masked version of the input line.

______________________________________________________________________

## Dependencies

- `PresidioHandler` (from `masking.utils.presidio_handler`)
- Microsoft Presidio (`presidio_analyzer`, `presidio_anonymizer`)
- spaCy NLP models (e.g., `de_core_news_lg`, `en_core_web_trf`)
