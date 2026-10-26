import pytest
import spacy
from masking.utils.entity_detection import PresidioMultilingualAnalyzer
from presidio_analyzer import Pattern, PatternRecognizer


# Lightweight blank spaCy pipelines: no model download or NER component required.
@pytest.fixture(scope="session")
def blank_models() -> dict[str, spacy.language.Language]:
    return {"en": spacy.blank("en"), "de": spacy.blank("de")}


# Real presidio AnalyzerEngine backed by blank spaCy pipelines.
@pytest.fixture(scope="session")
def blank_analyzer(blank_models):
    return PresidioMultilingualAnalyzer(models=blank_models).analyzer


# Analyzer detecting CODE_WORD ("zebra") only in its last supported language.
@pytest.fixture(scope="session")
def last_language_analyzer(blank_models):
    analyzer = PresidioMultilingualAnalyzer(models=blank_models).analyzer
    *others, last = analyzer.supported_languages
    for language in others:
        # Presidio requires a recognizer for each requested entity and language
        analyzer.registry.add_recognizer(
            PatternRecognizer(
                supported_entity="CODE_WORD",
                supported_language=language,
                patterns=[Pattern("never", r"(?!)", 1.0)],
            )
        )
    analyzer.registry.add_recognizer(
        PatternRecognizer(
            supported_entity="CODE_WORD",
            supported_language=last,
            patterns=[Pattern("code", r"\bzebra\b", 1.0)],
        )
    )
    return analyzer


@pytest.fixture(
    scope="module",
    params=[
        (
            {
                "key1": "value1",
                "key2": "",
                "key3": {
                    "Key4": "value4",
                    "key5": "",
                    "Key6": {"Key7": "value7", "key8": ""},
                },
            },
            [
                "key1",
                "key2",
                ["key3", "Key4"],
                ["key3", "key5"],
                ["key3", "Key6", "Key7"],
                ["key3", "Key6", "key8"],
            ],
            ["value1", "", "value4", "", "value7", ""],
        ),
        (None, [""], [""]),
        ("string", [""], ["string"]),
        ([{"key1": "value1"}], [[0, "key1"]], ["value1"]),
        (
            {
                "Key1": "value1",
                "key2": "",
                "key3": {
                    "Key4": "value4",
                    "key5": "",
                    "key6": {"Key7": "value7", "key8": ""},
                },
                "key9": [
                    {
                        "key10": "value10",
                        "key11": "",
                        "Key12": {
                            "key13": "value13",
                            "key14": "",
                            "key15": {"Key16": "value16", "key17": ""},
                        },
                    },
                    "value17",
                    ["value18", {"Key19": None}],
                ],
            },
            [
                "Key1",
                "key2",
                ["key3", "Key4"],
                ["key3", "key5"],
                ["key3", "key6", "Key7"],
                ["key3", "key6", "key8"],
                ["key9", 0, "key10"],
                ["key9", 0, "key11"],
                ["key9", 0, "Key12", "key13"],
                ["key9", 0, "Key12", "key14"],
                ["key9", 0, "Key12", "key15", "Key16"],
                ["key9", 0, "Key12", "key15", "key17"],
                ["key9", 1],
                ["key9", 2, 0],
                ["key9", 2, 1, "Key19"],
            ],
            [
                "value1",
                "",
                "value4",
                "",
                "value7",
                "",
                "value10",
                "",
                "value13",
                "",
                "value16",
                "",
                "value17",
                "value18",
                "",
            ],
        ),
    ],
)
def multi_nested_dict_and_path_and_value(
    request: pytest.FixtureRequest,
) -> dict | str | list | None:
    return request.param


@pytest.fixture(scope="module", params=[True, False])
def case_sensitivity(request: pytest.FixtureRequest) -> bool:
    return request.param


@pytest.fixture(scope="module", params=[".", ".[$].", ",", "~"])
def path_separator(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(
    scope="module",
    params=[
        {"start": "[", "end": "]"},
        {"start": "{", "end": "}"},
        {"start": "(", "end": ")"},
        {"start": "<", "end": ">"},
    ],
)
def list_index(request: pytest.FixtureRequest) -> dict:
    return request.param
