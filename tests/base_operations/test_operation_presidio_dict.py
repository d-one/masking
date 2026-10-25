import json

import pytest
from masking.base_operations.operation_presidio_dict import MaskDictOperationBase
from presidio_analyzer import RecognizerResult

EMAIL = "john.doe@example.com"
LINE = {"contact": f"mail {EMAIL}", "note": "hello", "list": ["x@example.org"]}


class MaskDictOperation(MaskDictOperationBase):
    def _mask_data(self, data):
        return data


@pytest.fixture()
def make(blank_analyzer):
    def _make(**kwargs) -> MaskDictOperation:
        return MaskDictOperation(col_name="j", analyzer=blank_analyzer, **kwargs)

    return _make


def mask(op: MaskDictOperation, line, **kwargs) -> object:
    return json.loads(op._mask_line(json.dumps(line), **kwargs))


def test_entities_in_leaves_are_masked(make):
    assert mask(make(), LINE) == {
        "contact": "mail <EMAIL_ADDRESS>",
        "note": "hello",
        "list": ["<EMAIL_ADDRESS>"],
    }


def test_denied_keys_use_masking_function(make):
    op = make(deny_keys=["note"], masking_function=lambda _: "#")
    assert mask(op, LINE) == {"contact": "mail #", "note": "#", "list": ["#"]}


def test_allowed_keys_are_kept(make):
    assert mask(make(allow_keys=["contact"]), LINE)["contact"] == LINE["contact"]


def test_given_entities_skip_detection(make):
    op = make()
    op.analyzer = None
    entities = {
        "hello": [RecognizerResult("PERSON", start=0, end=5, score=1.0)],
        "hello world": [],
    }
    out = mask(
        op,
        {"note": "hello", "other": "hello world"},
        leaf_to_mask=iter(["note", "other"]),
        leaf_to_deny=iter([]),
        entities=entities,
    )
    assert out == {"note": "<PERSON>", "other": "hello world"}


def test_given_entities_may_omit_values():
    op = MaskDictOperation(col_name="j", analyzer=None)
    entities = {"hello": [RecognizerResult("PERSON", start=0, end=5, score=1.0)]}
    out = mask(op, {"note": "hello", "other": "bye"}, entities=entities)
    assert out == {"note": "<PERSON>", "other": "bye"}


def test_entities_of_every_language_are_masked(last_language_analyzer):
    op = MaskDictOperation(
        col_name="j", analyzer=last_language_analyzer, pii_entities=["CODE_WORD"]
    )
    assert mask(op, {"a": "a zebra", "b": ["zebra", "cat"]}) == {
        "a": "a <CODE_WORD>",
        "b": ["<CODE_WORD>", "cat"],
    }


def test_non_string_leaves_are_kept(make):
    line = {"n": 1, "f": 1.5, "b": True, "none": None, "e": EMAIL}
    assert mask(make(), line) == {**line, "e": "<EMAIL_ADDRESS>"}


class TestEntitiesOfLines:
    def test_values_of_all_lines_are_collected(self, make):
        lines = [json.dumps({"a": "x", "b": ["y"]}), {"c": {"d": "z"}}, ["w", 1]]
        assert list(make()._iter_values_to_mask(lines)) == ["x", "y", "z", "w", 1]

    def test_denied_and_missing_lines_are_skipped(self, make):
        op = make(deny_keys=["secret"])
        lines = [json.dumps({"a": "x", "secret": "s"}), None, float("nan")]
        assert list(op._iter_values_to_mask(lines)) == ["x"]

    def test_one_batch_per_language(self, make, monkeypatch):
        op = make()
        batches = []
        process_batch = op.analyzer.nlp_engine.process_batch

        def record(texts, language, **kwargs):
            texts = list(texts)
            batches.append(texts)
            return process_batch(texts, language, **kwargs)

        monkeypatch.setattr(op.analyzer.nlp_engine, "process_batch", record)
        lines = [json.dumps({"a": EMAIL, "b": "ok"}), json.dumps({"c": EMAIL})]
        entities = op._get_entities_of_lines(lines)

        assert batches == [[EMAIL, "ok"]] * len(op.analyzer.supported_languages)
        assert {r.entity_type for r in entities[EMAIL]} == {"EMAIL_ADDRESS"}
