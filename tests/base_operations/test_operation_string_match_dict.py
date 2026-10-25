import json

from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from presidio_analyzer import RecognizerResult

LINE = {"note": "John was here", "other": "ok", "nested": {"n": "Dear john"}}
VALUES = {"first": "John"}


class StringMatchDict(StringMatchDictOperationBase):
    def _mask_data(self, data):
        return data


def mask(op, line, **kwargs) -> object:
    return json.loads(op._mask_line(json.dumps(line), **kwargs))


def test_serving_columns_include_pii_columns():
    op = StringMatchDict(col_name="j", pii_cols=["first"])
    assert op.serving_columns == ["j", "first"]


def test_pii_values_are_masked_in_every_leaf():
    op = StringMatchDict(col_name="j", pii_cols=["first"])
    assert mask(op, LINE, additional_values=VALUES) == {
        "note": "<PATIENT_DATA> was here",
        "other": "ok",
        "nested": {"n": "Dear <PATIENT_DATA>"},
    }


def test_without_pii_values_is_unchanged():
    op = StringMatchDict(col_name="j", pii_cols=["first"])
    assert mask(op, LINE, additional_values={"first": ""}) == LINE


def test_denied_keys_use_masking_function():
    op = StringMatchDict(
        col_name="j",
        pii_cols=["first"],
        deny_keys=["other"],
        masking_function=lambda _: "#",
    )
    assert mask(op, LINE, additional_values=VALUES) == {
        "note": "# was here",
        "other": "#",
        "nested": {"n": "Dear #"},
    }


def test_given_analyzer_results_skip_detection():
    op = StringMatchDict(col_name="j", pii_cols=["first"])
    results = {"ok": [RecognizerResult("PATIENT_DATA", start=0, end=2, score=1.0)]}
    out = mask(
        op,
        {"note": "John", "other": "ok"},
        leaf_to_mask=iter(["note", "other"]),
        leaf_to_deny=iter([]),
        analyzer_results=results,
        additional_values=VALUES,
    )
    assert out == {"note": "John", "other": "<PATIENT_DATA>"}
