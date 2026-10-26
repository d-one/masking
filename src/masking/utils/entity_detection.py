import warnings

import spacy
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NerModelConfiguration, SpacyNlpEngine
from presidio_analyzer.recognizer_registry import RecognizerRegistryProvider

# Class name prefixes of the region-specific recognizers left out of the default registry
EXCLUDED_RECOGNIZER_PREFIXES = ("Us", "In", "Sg", "Au")

# Ignore the specific FutureWarnings from torch.load
warnings.filterwarnings(
    "ignore", message=".*weights_only=False.*", category=FutureWarning
)
# Ignore the specific FutureWarnings from torch.cuda.amp.autocast
warnings.filterwarnings(
    "ignore", message=".*torch.cuda.amp.autocast.*", category=FutureWarning
)


class MultilingualSpacyNlpEngine(SpacyNlpEngine):
    def __init__(
        self, models: dict[str, str], ner_model_mapping: dict | None = None
    ) -> None:
        """Initialize the MultilingualSpacyNlpEngine class.

        Args:
        ----
            models (dict[str, str]): dictionary with language and model name.
                    Model should be a list of spaCy models, with corresponding language abbreviation:
                    for example, {"de": "de_core_news_lg", "en": "en_core_web_trf"}.
            ner_model_mapping (dict): mapping of model names to Presidio entity types.

        """
        super().__init__()
        self.nlp = self._get_models(models)

        if ner_model_mapping:
            self.ner_model_configuration = NerModelConfiguration(
                model_to_presidio_entity_mapping=ner_model_mapping
            )

    @staticmethod
    def _get_models(models: dict) -> dict[str, spacy.language.Language]:
        """Return the loaded spaCy models.

        Args:
        ----
            models (dict): dictionary with language and <model>, where <model> can be a string or spaCy model.
                          If <model> is a string, we load the spaCy model using spacy.load().

        Returns:
        -------
            dict[str, spacy.language.Language]: dictionary with language and loaded spaCy model.

        """
        nlp_models = {}
        for lang, model in models.items():
            if isinstance(model, str):
                nlp_models[lang] = spacy.load(model, disable=[])
                continue

            if isinstance(model, spacy.language.Language):
                nlp_models[lang] = model
                continue

            msg = f"Model for language '{lang}' should be a string or a spaCy model, but got {type(model)}."
            raise ValueError(msg)

        return nlp_models


class PresidioRegistry:
    registry: RecognizerRegistry | None = None

    def __init__(
        self,
        registry: RecognizerRegistry | None = None,
        supported_languages: list[str] | None = None,
    ) -> None:
        """Initialize the PresidioRegistr class.

        Args:
        ----
            registry (RecognizerRegistry): registry with recognizers to use
            supported_languages (list[str]): list of supported languages

        """
        if not registry:
            configuration = (
                None
                if supported_languages is None
                else {"supported_languages": supported_languages}
            )
            registry = RecognizerRegistryProvider(
                registry_configuration=configuration
            ).create_recognizer_registry()

            # Skip the US-, India-, Singapore-, and Australia-specific recognizers
            registry.recognizers = [
                recognizer
                for recognizer in registry.recognizers
                if not recognizer.__class__.__name__.startswith(
                    EXCLUDED_RECOGNIZER_PREFIXES
                )
            ]

        if supported_languages is not None:
            registry.supported_languages = supported_languages

        self.registry = registry


class PresidioMultilingualAnalyzer:
    analyzer: AnalyzerEngine | None = None

    def __init__(
        self,
        registry: RecognizerRegistry | None = None,
        models: dict[str, str] | None = None,
        ner_model_mapping: dict | None = None,
    ) -> None:
        """Initialize the PresidioMultilingualAnalyzer class.

        Args:
        ----
            registry (RecognizerRegistry): registry with recognizers to use
            models (dict[str, str]): dictionary with language and model name.
                    Model should be a list of spaCy models, with corresponding language abbreviation:
                    for example, {"de": "de_core_news_lg", "en": "en_core_web_trf"}.
            ner_model_mapping (dict): mapping of model names to Presidio entity types.

        """
        if not models:
            models = {"de": "de_core_news_lg", "en": "en_core_web_lg"}

        nlp_engine = MultilingualSpacyNlpEngine(
            models=models, ner_model_mapping=ner_model_mapping
        )

        if not registry:
            registry = PresidioRegistry(
                supported_languages=list(models.keys())
            ).registry
            registry.add_nlp_recognizer(nlp_engine=nlp_engine)

        self.analyzer = AnalyzerEngine(
            registry=registry,
            nlp_engine=nlp_engine,
            supported_languages=list(models.keys()),
        )
