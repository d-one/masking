import pytest
import spacy
from masking.utils.entity_detection import PresidioMultilingualAnalyzer


# Lightweight blank spaCy pipelines: no model download or NER component required.
@pytest.fixture(scope="session")
def blank_models() -> dict[str, spacy.language.Language]:
    return {"en": spacy.blank("en"), "de": spacy.blank("de")}


# Real presidio AnalyzerEngine backed by blank spaCy pipelines.
@pytest.fixture(scope="session")
def blank_analyzer(blank_models):
    return PresidioMultilingualAnalyzer(models=blank_models).analyzer
