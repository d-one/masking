import datetime
import re

import pytest
from masking.utils.presidio_handler import PresidioHandler
from masking.utils.string_match_handler import StringMatchHandler
from presidio_analyzer import Pattern, PatternRecognizer

ENTITY = "PATIENT_DATA"


class Handler(StringMatchHandler, PresidioHandler):
    """StringMatchHandler is a mixin that relies on PresidioHandler.allow_list."""


@pytest.fixture()
def handler():
    return Handler(pii_cols=["name", "surname", "birthdate"])


def matches(recognizer: PatternRecognizer, text: str) -> list[str]:
    return [text[r.start : r.end] for r in recognizer.analyze(text, entities=[ENTITY])]


# Extract the escaped value from the first alternative of each pattern regex.
def escaped_values(patterns: list[Pattern]) -> list[str]:
    return [re.search(r"\\b(.*?)\\b", p.regex).group(1) for p in patterns]


# ---------------------------------------------------------------------------
# __init__ / class attributes
# ---------------------------------------------------------------------------


class TestInit:
    def test_default_pii_cols(self):
        assert Handler().pii_cols == []

    def test_custom_pii_cols(self, handler):
        assert handler.pii_cols == ["name", "surname", "birthdate"]

    def test_pii_entities(self):
        assert {ENTITY} == StringMatchHandler._PII_ENTITIES

    def test_kwargs_forwarded_to_presidio_handler(self):
        h = Handler(pii_cols=["a"], allow_list=[" x "])
        assert h.allow_list == ["x"]
        assert h.pii_cols == ["a"]

    def test_pattern_template_formats_value(self):
        regex = StringMatchHandler._PATTERN_TEMPLATE.format(value="abc")
        assert regex.count("abc") == 3
        assert regex.startswith("(?ix)")


# ---------------------------------------------------------------------------
# _get_pii_values
# ---------------------------------------------------------------------------


class TestGetPiiValues:
    def test_extracts_and_strips_values_in_column_order(self, handler):
        line = {"surname": " Doe ", "name": "John", "birthdate": "2000-01-31"}
        assert handler._get_pii_values(line) == ["John", "Doe", "2000-01-31"]

    def test_ignores_non_pii_columns(self):
        h = Handler(pii_cols=["name"])
        assert h._get_pii_values({"name": "John", "city": "Zurich"}) == ["John"]

    def test_skips_blank_values(self):
        h = Handler(pii_cols=["a", "b"])
        assert h._get_pii_values({"a": "", "b": "   "}) == []

    def test_non_string_values_are_stringified(self):
        h = Handler(pii_cols=["a", "b"])
        assert h._get_pii_values({"a": 123, "b": 4.5}) == ["123", "4.5"]

    def test_respects_allow_list(self):
        h = Handler(pii_cols=["a", "b"], allow_list=["John"])
        assert h._get_pii_values({"a": "John", "b": "Doe"}) == ["Doe"]

    def test_no_pii_cols(self):
        assert Handler()._get_pii_values({"a": "John"}) == []

    def test_missing_and_none_columns_are_skipped(self):
        h = Handler(pii_cols=["a", "b", "c"])
        assert h._get_pii_values({"a": "John", "b": None}) == ["John"]


# ---------------------------------------------------------------------------
# _get_pattern_date
# ---------------------------------------------------------------------------


class TestGetPatternDate:
    @pytest.mark.parametrize(
        "value", ["2023-01-05", "2023/01/05", "5 January 2023", "January 5, 2023"]
    )
    def test_returns_all_date_formats(self, handler, value):
        patterns = handler._get_pattern_date(value)

        assert len(patterns) == 6
        assert all(isinstance(p, Pattern) for p in patterns)
        assert all(p.score == pytest.approx(0.8) for p in patterns)
        assert all(p.name == {ENTITY} for p in patterns)
        assert escaped_values(patterns) == [
            re.escape(v)
            for v in (
                "05-01-2023",
                "2023-01-05",
                "05.01.2023",
                "2023/01/05",
                "05/01/2023",
                "01/05/2023",
            )
        ]

    @pytest.mark.parametrize("value", [123, None, ["2023-01-05"]])
    def test_unparsable_input_returns_empty_list(self, handler, value):
        assert handler._get_pattern_date(value) == []

    def test_non_date_string_raises(self, handler):
        # dateparser returns None for non-dates; callers must handle the error.
        with pytest.raises(AttributeError):
            handler._get_pattern_date("definitely not a date")

    @pytest.mark.parametrize(
        "value",
        [datetime.datetime(2023, 1, 5, tzinfo=datetime.UTC), datetime.date(2023, 1, 5)],
    )
    def test_date_input_matches_iso_string(self, handler, value):
        assert escaped_values(handler._get_pattern_date(value)) == escaped_values(
            handler._get_pattern_date("2023-01-05")
        )


# ---------------------------------------------------------------------------
# _get_pattern_recognizer
# ---------------------------------------------------------------------------


class TestGetPatternRecognizer:
    @pytest.mark.parametrize("values", [[], [""], [None, ""]])
    def test_no_values_returns_none(self, handler, values):
        assert handler._get_pattern_recognizer(values) is None

    def test_returns_pattern_recognizer(self, handler):
        recognizer = handler._get_pattern_recognizer(["John"])
        assert isinstance(recognizer, PatternRecognizer)
        assert recognizer.supported_entities == [ENTITY]
        assert len(recognizer.patterns) == 1

    def test_date_value_adds_date_patterns(self, handler):
        recognizer = handler._get_pattern_recognizer(["John", "2023-01-05"])
        # 1 string pattern for "John" + 1 string pattern and 6 date patterns
        assert len(recognizer.patterns) == 8

    def test_skips_empty_values(self, handler):
        recognizer = handler._get_pattern_recognizer(["", "John", None])
        assert len(recognizer.patterns) == 1

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("Patient John was admitted.", ["John"]),
            ("Patient JOHN was admitted.", ["JOHN"]),
            ("patient john", ["john"]),
            ("John,John", ["John", "John"]),
            ("x\tJohn\ty", ["John"]),
            ("line\nJohn\nline", ["John"]),
            ("Johnson was admitted.", []),
            ("Littlejohn was admitted.", []),
            ("No match here.", []),
        ],
    )
    def test_string_matching(self, handler, text, expected):
        recognizer = handler._get_pattern_recognizer(["John"])
        assert matches(recognizer, text) == expected

    def test_special_characters_are_escaped(self, handler):
        recognizer = handler._get_pattern_recognizer(["a.b"])
        assert matches(recognizer, "axb a.b") == ["a.b"]

    def test_multiple_values(self, handler):
        recognizer = handler._get_pattern_recognizer(["John", "Doe"])
        assert matches(recognizer, "John Doe") == ["John", "Doe"]

    @pytest.mark.parametrize(
        "text",
        [
            "born 2023-01-05",
            "born 05-01-2023",
            "born 05.01.2023",
            "born 2023/01/05",
            "born 05/01/2023",
            "born 01/05/2023",
        ],
    )
    def test_date_matching_in_other_formats(self, handler, text):
        recognizer = handler._get_pattern_recognizer(["2023-01-05"])
        assert matches(recognizer, text) == [text.removeprefix("born ")]

    def test_scores(self, handler):
        recognizer = handler._get_pattern_recognizer(["John"])
        results = recognizer.analyze("John", entities=[ENTITY])
        assert [r.score for r in results] == [0.8]
        assert results[0].entity_type == ENTITY
