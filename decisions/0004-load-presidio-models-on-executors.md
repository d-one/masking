# ADR 0004: Load Presidio Models on Spark Executors

## Status

Accepted

## Context

Spark pickles each masking operation into every task. Operations that hold a Presidio analyzer carried the loaded spaCy models in that pickle: about 1.15 GB with `en_core_web_trf` and `de_core_news_lg`, and about 3.9 s of unpickling per task before the first row was masked ([#12](https://github.com/d-one/masking/issues/12)).

The target clusters can install the spaCy models on every executor, as a cluster library or with an init script.

## Decision

- `MultilingualSpacyNlpEngine` pickles models it was given **by name** as their name, not as loaded pipelines.
- Unpickling loads each named model through a per-process cache (`entity_detection._load_model_once`), so each executor process loads a model once and reuses it for later tasks.
- Models passed as loaded `spacy.Language` objects are pickled as before, since they have no name to reload them by.
- The driver keeps loading models with `spacy.load` when an analyzer is created.

## Consequences

- A pickled Presidio operation with named models is about 37 KB instead of about 1.15 GB.
- Executors must have the same spaCy models installed as the driver; `docs/spark.md` states this requirement.
- The cache lives for the executor process; models are not unloaded while the process runs.
- Masked output does not change: executors run the same models.
