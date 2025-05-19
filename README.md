# Masking Package

## Introduction

The Masking Package is a Python library designed to provide various masking operations on data columns. It supports hashing, fake data generation, and other masking techniques to ensure data privacy and security.

## Table of Content

- Development:
    - [Local Development](docs/installation.md)
    - [Available Modules](docs/available_modules.md)
        - [mask](docs/mask/mask.md)

## Installation

To install the Masking Package, use the following command:

```bash
pip install masking@git+"https://github.com/d-one/masking"
```

## Usage

### Hashing operation

Here is a basic example of how to use the package

```python
import pandas as pd
from masking.mask.pipeline import MaskDataFramePipeline
from masking.mask.operations.operation_hash import HashOperation

# Sample DataFrame
data = {'name': ['Alice', 'Bob', 'Charlie'], 'ssn': ['123-45-6789', '987-65-4321', '555-55-5555']}
df = pd.DataFrame(data)

# Configuration for masking
config = {
        "name": {'masking_operation': HashOperation(col_name="name", secret="my_secret")},
        }

# Create and apply the masking pipeline
pipeline = MaskDataFramePipeline(config)
masked_df = pipeline(df)

print(masked_df)
```

### Using Spacy models

The module makes use of Presidio, a [framework introduced by Microsoft](https://microsoft.github.io/presidio/), to enable entity recognition. This latter leverages [Spacy](https://spacy.io/) as a NLP framework to do entity recognition.

Before using the masking module, make sure to have downloaded the necessary spacy model: open up a terminal in your project and execute the following line of code
```bash
python -m spacy download en_core_web_trf
```

You can make sure to have installed the spacy model correctly by running the following code:
```python
import spacy

# Load English tokenizer, tagger, parser and NER
nlp = spacy.load("en_core_web_trf")

# Process whole documents
text = ("When Sebastian Thrun started working on self-driving cars at "
        "Google in 2007, few people outside of the company took him "
        "seriously. “I can tell you very senior CEOs of major American "
        "car companies would shake my hand and turn away because I wasn’t "
        "worth talking to,” said Thrun, in an interview with Recode earlier "
        "this week.")
doc = nlp(text)

# Analyze syntax
print("Noun phrases:", [chunk.text for chunk in doc.noun_chunks])
print("Verbs:", [token.lemma_ for token in doc if token.pos_ == "VERB"])

# Find named entities, phrases and concepts
for entity in doc.ents:
    print(entity.text, entity.label_)
```

Once the model is correctly installed, one can make use of it for masking in the following manner:

```python
import pandas as pd
from masking.mask.pipeline import MaskDataFramePipeline
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.utils.presidio_handler import PresidioMultilingualAnalyzer

analyzer = PresidioMultilingualAnalyzer(
    models={"en": "en_core_web_trf"}
).analyzer

# Sample DataFrame
data = {'description': ['James Bond was a very clever inspector.']}
df = pd.DataFrame(data)

# Configuration for masking
config = {
        "name": {'masking_operation': MaskPresidio(col_name="name", secret="my_secret")},
        }

# Create and apply the masking pipeline
pipeline = MaskDataFramePipeline(config)
masked_df = pipeline(df)

print(masked_df)
```
