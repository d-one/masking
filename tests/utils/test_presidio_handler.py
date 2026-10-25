import pytest
from masking.utils.presidio_handler import PresidioHandler
from presidio_anonymizer import AnonymizerEngine, OperatorConfig

DEFAULT_ENTITIES = {"EMAIL_ADDRESS", "PERSON", "PHONE_NUMBER", "DATE_TIME", "LOCATION"}


class FakeAnalyzer:
    """Minimal analyzer stub that records calls and returns one result per language."""

    def __init__(self, supported_languages=("en", "de")):
        self.supported_languages = list(supported_languages)
        self.calls = []

    def analyze(self, **kwargs):
        self.calls.append(kwargs)
        return [f"result-{kwargs['language']}"]


def mask(text: str) -> str:
    return "*" * len(text)


# ---------------------------------------------------------------------------
# __init__
# ---------------------------------------------------------------------------


class TestInit:
    def test_defaults(self):
        h = PresidioHandler()
        assert h.analyzer is None
        assert isinstance(h.anonymizer, AnonymizerEngine)
        assert h.allow_list == []
        assert h._PII_ENTITIES == DEFAULT_ENTITIES

    def test_default_operators_replace_with_entity_tag(self):
        h = PresidioHandler()
        assert set(h.operators) == DEFAULT_ENTITIES
        for entity, op in h.operators.items():
            assert op.operator_name == "replace"
            assert op.params == {"new_value": f"<{entity}>"}

    def test_allow_list_is_stripped_and_blanks_removed(self):
        h = PresidioHandler(allow_list=["  alice ", "", "   ", "bob"])
        assert h.allow_list == ["alice", "bob"]

    def test_custom_analyzer_and_anonymizer(self):
        analyzer, anonymizer = FakeAnalyzer(), AnonymizerEngine()
        h = PresidioHandler(analyzer=analyzer, anonymizer=anonymizer)
        assert h.analyzer is analyzer
        assert h.anonymizer is anonymizer

    def test_custom_pii_entities(self):
        h = PresidioHandler(pii_entities=["PERSON"])
        assert h._PII_ENTITIES == ["PERSON"]
        assert set(h.operators) == {"PERSON"}
        assert PresidioHandler._PII_ENTITIES == DEFAULT_ENTITIES

    def test_custom_operators(self):
        operators = {"PERSON": OperatorConfig("redact")}
        h = PresidioHandler(operators=operators)
        assert h.operators is operators

    def test_masking_function_builds_custom_operators(self):
        h = PresidioHandler(masking_function=mask, pii_entities=["PERSON", "LOCATION"])
        assert h.masking_function is mask
        assert set(h.operators) == {"PERSON", "LOCATION"}
        for op in h.operators.values():
            assert op.operator_name == "custom"
            assert op.params == {"lambda": mask}

    def test_masking_function_overrides_operators(self):
        h = PresidioHandler(
            operators={"PERSON": OperatorConfig("redact")}, masking_function=mask
        )
        assert set(h.operators) == DEFAULT_ENTITIES
        assert all(op.operator_name == "custom" for op in h.operators.values())

    def test_no_masking_function_attribute_by_default(self):
        assert not hasattr(PresidioHandler(), "masking_function")

    def test_kwargs_forwarded_to_next_class_in_mro(self):
        class Base:
            def __init__(self, foo=None):
                self.foo = foo

        class Combined(PresidioHandler, Base):
            pass

        assert Combined(foo="bar").foo == "bar"

    def test_unexpected_kwargs_raise(self):
        with pytest.raises(TypeError):
            PresidioHandler(unknown="value")


# ---------------------------------------------------------------------------
# update_analyzer / update_anonymizer
# ---------------------------------------------------------------------------


class TestUpdate:
    def test_update_analyzer(self):
        h = PresidioHandler()
        analyzer = FakeAnalyzer()
        h.update_analyzer(analyzer)
        assert h.analyzer is analyzer

    def test_update_anonymizer(self):
        h = PresidioHandler()
        anonymizer = AnonymizerEngine()
        h.update_anonymizer(anonymizer)
        assert h.anonymizer is anonymizer


# ---------------------------------------------------------------------------
# _get_language_entities / _get_entities (stubbed analyzer)
# ---------------------------------------------------------------------------


class TestGetEntitiesStub:
    def test_get_language_entities_params(self):
        analyzer = FakeAnalyzer()
        h = PresidioHandler(analyzer=analyzer, allow_list=["ok"])

        result = h._get_language_entities("some text", "en")

        assert result == ["result-en"]
        assert analyzer.calls == [
            {
                "text": "some text",
                "language": "en",
                "entities": h._PII_ENTITIES,
                "allow_list": ["ok"],
            }
        ]

    def test_get_language_entities_forwards_nlp_artifacts(self):
        analyzer = FakeAnalyzer()
        h = PresidioHandler(analyzer=analyzer)
        artifacts = ["artifact"]

        h._get_language_entities("text", "de", nlp_artifacts=artifacts)

        assert analyzer.calls[0]["nlp_artifacts"] is artifacts

    @pytest.mark.parametrize("artifacts", [None, []])
    def test_get_language_entities_omits_empty_nlp_artifacts(self, artifacts):
        analyzer = FakeAnalyzer()
        h = PresidioHandler(analyzer=analyzer)

        h._get_language_entities("text", "en", nlp_artifacts=artifacts)

        assert "nlp_artifacts" not in analyzer.calls[0]

    def test_get_entities_concatenates_all_languages(self):
        analyzer = FakeAnalyzer(supported_languages=["en", "de", "fr"])
        h = PresidioHandler(analyzer=analyzer)

        assert h._get_entities("text") == ["result-en", "result-de", "result-fr"]
        assert [c["language"] for c in analyzer.calls] == ["en", "de", "fr"]

    def test_get_entities_single_language(self):
        h = PresidioHandler(analyzer=FakeAnalyzer(supported_languages=["en"]))
        assert h._get_entities("text") == ["result-en"]


class FakeNlpEngine:
    """NLP engine stub recording the texts and batch size of each batch call."""

    def __init__(self):
        self.batches = []

    def process_batch(self, texts, language, batch_size=1):
        texts = list(texts)
        self.batches.append((language, texts, batch_size))
        return ((t, f"artifacts-{language}-{t}") for t in texts)


class TestGetEntitiesBatch:
    @pytest.fixture()
    def analyzer(self):
        analyzer = FakeAnalyzer(supported_languages=["en", "de"])
        analyzer.nlp_engine = FakeNlpEngine()
        return analyzer

    def test_every_language_processes_each_distinct_text_once(self, analyzer):
        h = PresidioHandler(analyzer=analyzer)
        h._get_entities_batch(["a", "b", "a", "b", "c"])
        assert [(lang, texts) for lang, texts, _ in analyzer.nlp_engine.batches] == [
            ("en", ["a", "b", "c"]),
            ("de", ["a", "b", "c"]),
        ]

    def test_entities_of_all_languages_are_merged(self, analyzer):
        h = PresidioHandler(analyzer=analyzer)
        entities = h._get_entities_batch(["a"])
        assert entities == {"a": {"result-en", "result-de"}}
        assert [c["nlp_artifacts"] for c in analyzer.calls] == [
            "artifacts-en-a",
            "artifacts-de-a",
        ]

    @pytest.mark.parametrize("ignored", [None, 1, float("nan"), "", "   "])
    def test_non_strings_and_blanks_are_not_processed(self, analyzer, ignored):
        h = PresidioHandler(analyzer=analyzer)
        entities = h._get_entities_batch(["a", ignored])
        assert [texts for _, texts, _ in analyzer.nlp_engine.batches] == [["a"], ["a"]]
        assert entities["missing"] == set()

    @pytest.mark.parametrize("batch_size", [None, 8])
    def test_batch_size(self, analyzer, batch_size):
        kwargs = {} if batch_size is None else {"batch_size": batch_size}
        PresidioHandler(analyzer=analyzer, **kwargs)._get_entities_batch(["a"])
        assert {size for *_, size in analyzer.nlp_engine.batches} == {batch_size or 32}

    def test_real_analyzer(self, blank_analyzer):
        h = PresidioHandler(analyzer=blank_analyzer)
        text = "Write to john.doe@example.com"
        assert h._get_entities_batch([text, text])[text] == set(h._get_entities(text))

    def test_entities_found_only_in_the_last_language(self, last_language_analyzer):
        h = PresidioHandler(analyzer=last_language_analyzer, pii_entities=["CODE_WORD"])
        entities = h._get_entities_batch(["a zebra", "a cat"])
        assert {r.entity_type for r in entities["a zebra"]} == {"CODE_WORD"}
        assert entities["a cat"] == set()


# ---------------------------------------------------------------------------
# Integration with a real (blank-model) analyzer and anonymizer
# ---------------------------------------------------------------------------


class TestIntegration:
    TEXT = "Write to john.doe@example.com or visit https://example.com"

    def test_detects_only_configured_entities(self, blank_analyzer):
        h = PresidioHandler(analyzer=blank_analyzer)
        results = h._get_entities(self.TEXT)

        # one email hit per supported language, URL is not a configured entity
        assert len(results) == len(blank_analyzer.supported_languages)
        assert {r.entity_type for r in results} == {"EMAIL_ADDRESS"}
        assert all(
            self.TEXT[r.start : r.end] == "john.doe@example.com" for r in results
        )

    def test_custom_pii_entities_restrict_detection(self, blank_analyzer):
        h = PresidioHandler(analyzer=blank_analyzer, pii_entities=["URL"])
        results = h._get_entities(self.TEXT)
        assert {r.entity_type for r in results} == {"URL"}

    def test_allow_list_suppresses_detection(self, blank_analyzer):
        h = PresidioHandler(
            analyzer=blank_analyzer, allow_list=[" john.doe@example.com "]
        )
        assert h._get_entities(self.TEXT) == []

    def test_anonymize_with_default_operators(self, blank_analyzer):
        h = PresidioHandler(analyzer=blank_analyzer)
        result = h.anonymizer.anonymize(
            text=self.TEXT,
            analyzer_results=h._get_entities(self.TEXT),
            operators=h.operators,
        )
        assert result.text == "Write to <EMAIL_ADDRESS> or visit https://example.com"

    def test_anonymize_with_masking_function(self, blank_analyzer):
        h = PresidioHandler(analyzer=blank_analyzer, masking_function=mask)
        result = h.anonymizer.anonymize(
            text=self.TEXT,
            analyzer_results=h._get_entities(self.TEXT),
            operators=h.operators,
        )
        assert result.text == f"Write to {'*' * 20} or visit https://example.com"
