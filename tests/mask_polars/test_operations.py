import hashlib
import json

import polars as pl
import pytest
from masking.base_operations.operation_fake_date import FakeDateBase
from masking.base_operations.operation_fake_name import FakeNameBase
from masking.base_operations.operation_fake_plz import FakePLZBase
from masking.base_operations.operation_hash import HashOperationBase
from masking.base_operations.operation_med_stats import MedStatsOperationBase
from masking.base_operations.operation_null import NullOperationBase
from masking.base_operations.operation_presidio import MaskPresidioBase
from masking.base_operations.operation_presidio_dict import MaskDictOperationBase
from masking.base_operations.operation_string_match import StringMatchOperationBase
from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from masking.base_operations.operation_yyyy_hash import YYYYHashOperationBase
from masking.mask_polars.operations.operation import PolarsOperation
from masking.mask_polars.operations.operation_fake_date import FakeDate
from masking.mask_polars.operations.operation_fake_name import FakeNameOperation
from masking.mask_polars.operations.operation_fake_plz import FakePLZ
from masking.mask_polars.operations.operation_hash import HashOperation
from masking.mask_polars.operations.operation_med_stats import MedStatsOperation
from masking.mask_polars.operations.operation_null import NullOperation
from masking.mask_polars.operations.operation_presidio import MaskPresidio
from masking.mask_polars.operations.operation_presidio_dict import MaskDictOperation
from masking.mask_polars.operations.operation_string_match import StringMatchOperation
from masking.mask_polars.operations.operation_string_match_dict import (
    StringMatchDictOperation,
)
from masking.mask_polars.operations.operation_yyyy_hash import YYYYHashOperation
from masking.utils.hash import hash_string

EMAIL = "john.doe@example.com"
SECRET = "s"  # noqa: S105


class Recorder(PolarsOperation):
    def __init__(self, serving: list[str] | None = None, **kwargs) -> None:
        super().__init__(**kwargs)
        self.serving = serving or []
        self.calls = []

    @property
    def serving_columns(self) -> list[str]:
        return [self.col_name, *self.serving]

    def _mask_line(self, line, additional_values=None, **kwargs):  # noqa: ARG002
        self.calls.append((line, additional_values))
        return f"m_{line}"


@pytest.mark.parametrize(
    ("operation", "base"),
    [
        (FakeDate, FakeDateBase),
        (FakeNameOperation, FakeNameBase),
        (FakePLZ, FakePLZBase),
        (HashOperation, HashOperationBase),
        (MedStatsOperation, MedStatsOperationBase),
        (NullOperation, NullOperationBase),
        (MaskPresidio, MaskPresidioBase),
        (MaskDictOperation, MaskDictOperationBase),
        (StringMatchOperation, StringMatchOperationBase),
        (StringMatchDictOperation, StringMatchDictOperationBase),
        (YYYYHashOperation, YYYYHashOperationBase),
    ],
)
def test_operations_combine_base_and_backend(operation, base):
    assert issubclass(operation, base)
    assert issubclass(operation, PolarsOperation)


class TestPolarsOperation:
    def test_series_is_mapped_and_nulls_skipped(self):
        op = Recorder(col_name="c")
        out = op(pl.Series(["a", "b", None]))
        assert out.dtype == pl.Utf8
        assert out.to_list() == ["m_a", "m_b", None]
        assert op.calls == [("a", None), ("b", None)]

    def test_dataframe_without_serving_columns(self):
        op = Recorder(col_name="c")
        out = op(pl.DataFrame({"c": ["a", None], "x": [1, 2]}))
        assert out.to_list() == ["m_a", None]

    def test_serving_columns_are_passed_as_additional_values(self):
        data = pl.DataFrame({
            "c": ["a", "b"],
            "x": [1, 2],
            "y": ["p", "q"],
            "z": [0, 0],
        })
        op = Recorder(col_name="c", serving=["x", "y"])
        assert op(data).to_list() == ["m_a", "m_b"]
        assert op.calls == [("a", {"x": 1, "y": "p"}), ("b", {"x": 2, "y": "q"})]

    def test_concordance_table_is_used(self):
        op = Recorder(col_name="c", concordance_table={"a": "A"})
        assert op(pl.DataFrame({"c": ["a", "b"]})).to_list() == ["A", "m_b"]


class TestHashOperations:
    def test_hash(self):
        out = HashOperation(col_name="c")(pl.DataFrame({"c": ["a", "b"]}))
        assert out.to_list() == [hashlib.sha256(v.encode()).hexdigest() for v in "ab"]

    def test_hash_int_column(self):
        out = HashOperation(col_name="c")(pl.DataFrame({"c": [1, None]}))
        assert out.to_list() == [hashlib.sha256(b"1").hexdigest(), None]

    def test_hash_with_secret(self):
        out = HashOperation(col_name="c", secret=SECRET)(pl.Series(["a"]))
        assert out.to_list() == [hash_string("a", SECRET)]

    def test_yyyy_hash(self):
        out = YYYYHashOperation(col_name="d", secret=SECRET)(
            pl.DataFrame({"d": ["2000-01-31"]})
        )
        assert out.to_list() == ["2000_" + hash_string("2000-01-31", SECRET)]


class TestLookupOperations:
    def test_med_stats(self):
        out = MedStatsOperation(col_name="p")(pl.DataFrame({"p": ["1000", "0000"]}))
        assert out.to_list() == ["VD02", "0000"]

    def test_null_every_value(self):
        out = NullOperation(col_name="c")(pl.DataFrame({"c": ["a", None]}))
        assert out.to_list() == [None, None]

    def test_null_with_condition_keeps_others_as_strings(self):
        op = NullOperation(col_name="c", condition=lambda v: v == "10")
        assert op(pl.DataFrame({"c": [10, 20]})).to_list() == [None, "20"]


class TestFakeOperations:
    @pytest.mark.parametrize(
        ("operation", "values"),
        [
            (
                FakeDate(col_name="c", preserve="year"),
                [f"2000-01-{d:02d}" for d in range(1, 11)],
            ),
            (FakePLZ(col_name="c"), ["8001", "3436", "1000"]),
            (
                FakeNameOperation(col_name="c"),
                ["Anna Muster", "Hans Meier", "Eva Graf"],
            ),
        ],
        ids=["date", "plz", "name"],
    )
    def test_values_are_changed_and_unique(self, operation, values):
        out = operation(pl.DataFrame({"c": values})).to_list()
        assert all(o != v for o, v in zip(out, values, strict=True))
        assert len(set(out)) == len(values)

    def test_fake_name_reuse_never_outputs_clear_values(self):
        clear = [f"Clear{i}" for i in range(200)]
        out = FakeNameOperation(col_name="c", reuse_existing=True)(
            pl.DataFrame({"c": clear})
        )
        assert not set(out.to_list()) & set(clear)


class TestPresidioOperations:
    @pytest.mark.parametrize("as_series", [False, True])
    def test_mask_presidio(self, blank_analyzer, as_series):
        values = [f"Mail {EMAIL}", "hi"]
        data = pl.Series(values) if as_series else pl.DataFrame({"t": values})
        out = MaskPresidio(col_name="t", analyzer=blank_analyzer)(data)
        assert out.to_list() == ["Mail <EMAIL_ADDRESS>", "hi"]

    @pytest.mark.parametrize("as_series", [False, True])
    def test_mask_dict(self, blank_analyzer, as_series):
        values = [json.dumps({"c": f"mail {EMAIL}", "n": "hi"})]
        data = pl.Series(values) if as_series else pl.DataFrame({"j": values})
        out = MaskDictOperation(col_name="j", analyzer=blank_analyzer)(data)
        assert [json.loads(v) for v in out] == [
            {"c": "mail <EMAIL_ADDRESS>", "n": "hi"}
        ]

    def test_mask_dict_inner_wildcard_allow_key(self, blank_analyzer):
        line = json.dumps({"items": [{"a": EMAIL, "b": EMAIL}]})
        op = MaskDictOperation(
            col_name="j", analyzer=blank_analyzer, allow_keys=["items.[$].[*].[$].a"]
        )
        out = op(pl.DataFrame({"j": [line]}))
        assert [json.loads(v) for v in out] == [
            {"items": [{"a": EMAIL, "b": "<EMAIL_ADDRESS>"}]}
        ]

    def test_mask_presidio_every_language(self, last_language_analyzer):
        op = MaskPresidio(
            col_name="t", analyzer=last_language_analyzer, pii_entities=["CODE_WORD"]
        )
        out = op(pl.DataFrame({"t": ["a zebra", "a cat"]}))
        assert out.to_list() == ["a <CODE_WORD>", "a cat"]

    def test_mask_dict_every_language(self, last_language_analyzer):
        op = MaskDictOperation(
            col_name="j", analyzer=last_language_analyzer, pii_entities=["CODE_WORD"]
        )
        lines = [json.dumps({"a": "zebra", "n": 1}), json.dumps(["cat", "zebra"])]
        out = op(pl.DataFrame({"j": lines}))
        assert [json.loads(v) for v in out] == [
            {"a": "<CODE_WORD>", "n": 1},
            ["cat", "<CODE_WORD>"],
        ]


class TestStringMatchOperations:
    def test_string_match_uses_serving_columns(self):
        data = pl.DataFrame({"t": ["John here", "Ann here"], "first": ["John", "Ann"]})
        out = StringMatchOperation(col_name="t", pii_cols=["first"])(data)
        assert out.to_list() == ["<PATIENT_DATA> here", "<PATIENT_DATA> here"]

    def test_string_match_skips_null_pii_value(self):
        data = pl.DataFrame({
            "t": ["None of John", "None of Ann"],
            "first": ["John", None],
        })
        out = StringMatchOperation(col_name="t", pii_cols=["first"])(data)
        assert out.to_list() == ["None of <PATIENT_DATA>", "None of Ann"]

    def test_string_match_dict(self):
        data = pl.DataFrame({
            "j": [json.dumps({"n": "John here"}), json.dumps({"n": "Ann"})],
            "first": ["John", "Ann"],
        })
        out = StringMatchDictOperation(col_name="j", pii_cols=["first"])(data)
        assert [json.loads(v) for v in out] == [
            {"n": "<PATIENT_DATA> here"},
            {"n": "<PATIENT_DATA>"},
        ]

    def test_string_match_dict_inner_wildcard_allow_key(self):
        data = pl.DataFrame({
            "j": [json.dumps({"items": [{"a": "John", "b": "John"}]})],
            "first": ["John"],
        })
        op = StringMatchDictOperation(
            col_name="j", pii_cols=["first"], allow_keys=["items.[$].[*].[$].a"]
        )
        assert [json.loads(v) for v in op(data)] == [
            {"items": [{"a": "John", "b": "<PATIENT_DATA>"}]}
        ]

    def test_string_match_dict_denies_scalar_list_entries(self):
        data = pl.DataFrame({
            "j": [json.dumps({"items": [123, "John", None, True]})],
            "first": ["John"],
        })
        op = StringMatchDictOperation(
            col_name="j",
            pii_cols=["first"],
            deny_keys=["items.[$].[*]"],
            masking_function=lambda s: f"<{s}>",
        )
        assert [json.loads(v) for v in op(data)] == [
            {"items": ["<123>", "<John>", "<null>", "<true>"]}
        ]

    def test_string_match_dict_series(self):
        line = json.dumps({"n": "John"})
        out = StringMatchDictOperation(col_name="j")(pl.Series([line]))
        assert out.to_list() == [line]
