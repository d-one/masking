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
