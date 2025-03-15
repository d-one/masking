import warnings

import spacy
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import SpacyNlpEngine

# Ignore the specific FutureWarnings from torch.load
warnings.filterwarnings(
    "ignore", message=".*weights_only=False.*", category=FutureWarning
)
# Ignore the specific FutureWarnings from torch.cuda.amp.autocast
warnings.filterwarnings(
    "ignore", message=".*torch.cuda.amp.autocast.*", category=FutureWarning
)


class MultilingualSpacyNlpEngine(SpacyNlpEngine):
    def __init__(self, models: dict[str, str]) -> None:
        """Initialize the MultilingualSpacyNlpEngine class.

        Args:
        ----
            models (dict[str, str]): dictionary with language and model name.
                    Model should be a list of spaCy models, with corresponding language abbreviation:
                    for example, {"de": "de_core_news_lg", "en": "en_core_web_trf"}.

        """
        super().__init__()
        self.nlp = {
            lang: spacy.load(model, disable=[]) for lang, model in models.items()
        }


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
            registry = RecognizerRegistry()
            registry.load_predefined_recognizers()

            # Remove all recognizers except for the ones that are needed: skip US-, India-, Singapore-, Australia-, and Crypto-recognizers
            for recognizer in registry.recognizers:
                if any(
                    recognizer.__class__.__name__.split(".")[-1].startswith(x)
                    for x in ("Us", "In", "Sg", "Au")
                ):
                    registry.remove_recognizer(recognizer)

        if supported_languages is not None:
            registry.supported_languages = supported_languages

        self.registry = registry


class PresidioMultilingualAnalyzer:
    analyzer: AnalyzerEngine | None = None

    def __init__(
        self,
        registry: RecognizerRegistry | None = None,
        models: dict[str, str] | None = None,
    ) -> None:
        """Initialize the PresidioMultilingualAnalyzer class.

        Args:
        ----
            registry (RecognizerRegistry): registry with recognizers to use
            models (dict[str, str]): dictionary with language and model name.
                    Model should be a list of spaCy models, with corresponding language abbreviation:
                    for example, {"de": "de_core_news_lg", "en": "en_core_web_trf"}.

        """
        if not models:
            models = {"de": "de_core_news_lg", "en": "en_core_web_lg"}

        if not registry:
            registry = PresidioRegistry(
                supported_languages=list(models.keys())
            ).registry

        self.analyzer = AnalyzerEngine(
            nlp_engine=MultilingualSpacyNlpEngine(models=models),
            supported_languages=list(models.keys()),
        )
