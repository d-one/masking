import pytest
import spacy
from masking.utils import entity_detection
from masking.utils.entity_detection import (
    MultilingualSpacyNlpEngine,
    PresidioMultilingualAnalyzer,
    PresidioRegistry,
)
from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_analyzer.nlp_engine import NerModelConfiguration

EXCLUDED_PREFIXES = ("Us", "In", "Sg", "Au")


# Replace spacy.load with a blank-pipeline factory and record its calls.
@pytest.fixture()
def fake_spacy_load(monkeypatch):
    calls = []

    def _load(name, **kwargs):
        calls.append((name, kwargs))
        return spacy.blank(name[:2])

    monkeypatch.setattr(entity_detection.spacy, "load", _load)
    return calls


# ---------------------------------------------------------------------------
# MultilingualSpacyNlpEngine
# ---------------------------------------------------------------------------


class TestMultilingualSpacyNlpEngine:
    def test_get_models_keeps_language_instances(self, blank_models):
        result = MultilingualSpacyNlpEngine._get_models(blank_models)
        assert result == blank_models
        assert result["en"] is blank_models["en"]

    def test_get_models_loads_strings_with_spacy(self, fake_spacy_load):
        result = MultilingualSpacyNlpEngine._get_models({
            "en": "en_core_web_lg",
            "de": "de_core_news_lg",
        })
        assert set(result) == {"en", "de"}
        assert all(isinstance(m, spacy.language.Language) for m in result.values())
        assert fake_spacy_load == [
            ("en_core_web_lg", {"disable": []}),
            ("de_core_news_lg", {"disable": []}),
        ]

    def test_get_models_mixed_inputs(self, fake_spacy_load, blank_models):
        result = MultilingualSpacyNlpEngine._get_models({
            "en": blank_models["en"],
            "de": "de_core_news_lg",
        })
        assert result["en"] is blank_models["en"]
        assert [name for name, _ in fake_spacy_load] == ["de_core_news_lg"]

    def test_get_models_empty(self):
        assert MultilingualSpacyNlpEngine._get_models({}) == {}

    @pytest.mark.parametrize("bad_model", [None, 42, ["en_core_web_lg"], object()])
    def test_get_models_invalid_type_raises(self, bad_model):
        with pytest.raises(ValueError, match="Model for language 'en'"):
            MultilingualSpacyNlpEngine._get_models({"en": bad_model})

    def test_init_sets_loaded_models(self, blank_models):
        engine = MultilingualSpacyNlpEngine(models=blank_models)
        assert engine.nlp == blank_models
        assert engine.is_loaded() is True
        assert sorted(engine.get_supported_languages()) == ["de", "en"]

    def test_init_without_mapping_uses_default_configuration(self, blank_models):
        engine = MultilingualSpacyNlpEngine(models=blank_models)
        default = NerModelConfiguration()
        assert (
            engine.ner_model_configuration.model_to_presidio_entity_mapping
            == default.model_to_presidio_entity_mapping
        )

    def test_init_with_mapping(self, blank_models):
        mapping = {"PER": "PERSON", "LOC": "LOCATION"}
        engine = MultilingualSpacyNlpEngine(
            models=blank_models, ner_model_mapping=mapping
        )
        assert isinstance(engine.ner_model_configuration, NerModelConfiguration)
        assert (
            engine.ner_model_configuration.model_to_presidio_entity_mapping == mapping
        )

    def test_process_text_per_language(self, blank_models):
        engine = MultilingualSpacyNlpEngine(models=blank_models)
        for lang in ("en", "de"):
            artifacts = engine.process_text("Hello world", lang)
            assert [t.text for t in artifacts.tokens] == ["Hello", "world"]

    def test_invalid_model_raises_on_init(self):
        with pytest.raises(ValueError, match="should be a string or a spaCy model"):
            MultilingualSpacyNlpEngine(models={"en": 1})


# ---------------------------------------------------------------------------
# PresidioRegistry
# ---------------------------------------------------------------------------


class TestPresidioRegistry:
    def test_default_registry_is_created(self):
        registry = PresidioRegistry().registry
        assert isinstance(registry, RecognizerRegistry)
        names = {r.__class__.__name__ for r in registry.recognizers}
        assert {"EmailRecognizer", "PhoneRecognizer", "IbanRecognizer"} <= names

    def test_default_supported_languages_untouched(self):
        registry = PresidioRegistry().registry
        assert registry.supported_languages == RecognizerRegistry().supported_languages

    def test_supported_languages_are_set(self):
        registry = PresidioRegistry(supported_languages=["de", "en"]).registry
        assert registry.supported_languages == ["de", "en"]

    def test_custom_registry_is_kept(self):
        custom = RecognizerRegistry()
        result = PresidioRegistry(registry=custom).registry
        assert result is custom
        assert result.recognizers == []

    def test_custom_registry_with_supported_languages(self):
        custom = RecognizerRegistry()
        result = PresidioRegistry(registry=custom, supported_languages=["fr"]).registry
        assert result is custom
        assert result.supported_languages == ["fr"]

    def test_region_specific_recognizers_are_removed(self):
        registry = PresidioRegistry().registry
        names = [r.__class__.__name__ for r in registry.recognizers]
        assert not [n for n in names if n.startswith(EXCLUDED_PREFIXES)]


# ---------------------------------------------------------------------------
# PresidioMultilingualAnalyzer
# ---------------------------------------------------------------------------


class TestPresidioMultilingualAnalyzer:
    def test_analyzer_built_from_models(self, blank_analyzer):
        assert isinstance(blank_analyzer, AnalyzerEngine)
        assert isinstance(blank_analyzer.nlp_engine, MultilingualSpacyNlpEngine)
        assert sorted(blank_analyzer.supported_languages) == ["de", "en"]

    def test_default_models(self, fake_spacy_load):
        analyzer = PresidioMultilingualAnalyzer().analyzer
        assert [name for name, _ in fake_spacy_load] == [
            "de_core_news_lg",
            "en_core_web_lg",
        ]
        assert analyzer.supported_languages == ["de", "en"]

    def test_ner_model_mapping_is_forwarded(self, blank_models):
        mapping = {"PER": "PERSON"}
        analyzer = PresidioMultilingualAnalyzer(
            models=blank_models, ner_model_mapping=mapping
        ).analyzer
        config = analyzer.nlp_engine.ner_model_configuration
        assert config.model_to_presidio_entity_mapping == mapping

    @pytest.mark.parametrize("language", ["en", "de"])
    def test_detects_email_in_each_language(self, blank_analyzer, language):
        text = "Contact john.doe@example.com today"
        results = blank_analyzer.analyze(
            text=text, language=language, entities=["EMAIL_ADDRESS"]
        )
        assert len(results) == 1
        assert text[results[0].start : results[0].end] == "john.doe@example.com"

    def test_unsupported_language_raises(self, blank_analyzer):
        with pytest.raises(ValueError, match="No matching recognizers"):
            blank_analyzer.analyze(text="bonjour", language="fr")

    def test_default_registry_is_presidio_default_without_region_recognizers(
        self, blank_models
    ):
        analyzer = PresidioMultilingualAnalyzer(models=blank_models).analyzer
        presidio = AnalyzerEngine(
            nlp_engine=analyzer.nlp_engine, supported_languages=list(blank_models)
        )

        def names(engine: AnalyzerEngine) -> list[tuple[str, str]]:
            return sorted(
                (r.__class__.__name__, r.supported_language)
                for r in engine.registry.recognizers
            )

        removed = [n for n in names(presidio) if n[0].startswith(EXCLUDED_PREFIXES)]
        assert removed
        assert names(analyzer) == [n for n in names(presidio) if n not in removed]

    def test_custom_registry_is_used(self, blank_models):
        registry = RecognizerRegistry(supported_languages=list(blank_models))
        analyzer = PresidioMultilingualAnalyzer(
            registry=registry, models=blank_models
        ).analyzer
        assert analyzer.registry is registry
