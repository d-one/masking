import hashlib
import json

import pandas as pd
import pytest
from masking.base_operations.operation_fake_date import FakeDateBase
from masking.base_operations.operation_fake_name import FakeNameBase
from masking.base_operations.operation_fake_plz import FakePLZBase
from masking.base_operations.operation_hash import HashOperationBase
from masking.base_operations.operation_med_stats import MedStatsOperationBase
from masking.base_operations.operation_presidio import MaskPresidioBase
from masking.base_operations.operation_presidio_dict import MaskDictOperationBase
from masking.base_operations.operation_string_match import StringMatchOperationBase
from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from masking.base_operations.operation_yyyy_hash import YYYYHashOperationBase
from masking.mask.operations.operation import PandasOperation
from masking.mask.operations.operation_fake_date import FakeDate
from masking.mask.operations.operation_fake_name import FakeNameOperation
from masking.mask.operations.operation_fake_plz import FakePLZ
from masking.mask.operations.operation_hash import HashOperation
from masking.mask.operations.operation_med_stats import MedStatsOperation
from masking.mask.operations.operation_presidio import MaskPresidio
from masking.mask.operations.operation_presidio_dict import MaskDictOperation
from masking.mask.operations.operation_string_match import StringMatchOperation
from masking.mask.operations.operation_string_match_dict import StringMatchDictOperation
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation
from masking.utils.hash import hash_string

EMAIL = "john.doe@example.com"
SECRET = "s"  # noqa: S105


class Recorder(PandasOperation):
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
        (MaskPresidio, MaskPresidioBase),
        (MaskDictOperation, MaskDictOperationBase),
        (StringMatchOperation, StringMatchOperationBase),
        (StringMatchDictOperation, StringMatchDictOperationBase),
        (YYYYHashOperation, YYYYHashOperationBase),
    ],
)
def test_operations_combine_base_and_backend(operation, base):
    assert issubclass(operation, base)
    assert issubclass(operation, PandasOperation)


class TestPandasOperation:
    def test_series_is_mapped(self):
        op = Recorder(col_name="c")
        out = op(pd.Series(["a", "b"], index=[10, 11]))
        assert out.tolist() == ["m_a", "m_b"]
        assert out.index.tolist() == [10, 11]
        assert op.calls == [("a", None), ("b", None)]

    @pytest.mark.parametrize(
        "missing", [None, float("nan"), pd.NA, pd.NaT], ids=["none", "nan", "na", "nat"]
    )
    def test_missing_values_are_not_masked(self, missing):
        op = Recorder(col_name="c")
        out = op(pd.Series(["a", missing], dtype=object))
        assert out.isna().tolist() == [False, True]
        assert op.calls == [("a", None)]

    def test_missing_values_of_string_series_are_not_masked(self):
        out = Recorder(col_name="c")(pd.Series(["a", None]))
        assert out.isna().tolist() == [False, True]

    def test_dataframe_index_is_kept(self):
        data = pd.DataFrame({"c": ["a", "b"], "x": [1, 2]}, index=[5, 7])
        out = Recorder(col_name="c")(data)
        assert out.tolist() == ["m_a", "m_b"]
        assert out.index.tolist() == [5, 7]

    def test_serving_columns_are_passed_as_additional_values(self):
        data = pd.DataFrame({
            "c": ["a", "b"],
            "x": [1, 2],
            "y": ["p", "q"],
            "z": [0, 0],
        })
        op = Recorder(col_name="c", serving=["x", "y"])
        op(data)
        assert op.calls == [("a", {"x": 1, "y": "p"}), ("b", {"x": 2, "y": "q"})]

    def test_concordance_table_is_used(self):
        op = Recorder(col_name="c", concordance_table={"a": "A"})
        assert op(pd.DataFrame({"c": ["a", "b"]})).tolist() == ["A", "m_b"]


class TestHashOperations:
    def test_hash(self):
        data = pd.DataFrame({"c": ["a", "b"]})
        expected = [hashlib.sha256(v.encode()).hexdigest() for v in ["a", "b"]]
        assert HashOperation(col_name="c")(data).tolist() == expected

    def test_hash_with_secret(self):
        out = HashOperation(col_name="c", secret=SECRET)(pd.Series(["a"]))
        assert out.tolist() == [hash_string("a", SECRET)]

    def test_yyyy_hash(self):
        out = YYYYHashOperation(col_name="d", secret=SECRET)(
            pd.DataFrame({"d": ["2000-01-31", "1999-12-01"]})
        )
        assert out.tolist() == [
            "2000_" + hash_string("2000-01-31", SECRET),
            "1999_" + hash_string("1999-12-01", SECRET),
        ]


class TestLookupOperations:
    def test_med_stats(self):
        out = MedStatsOperation(col_name="p")(pd.DataFrame({"p": ["1000", "0000"]}))
        assert out.tolist() == ["VD02", "0000"]


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
        out = operation(pd.DataFrame({"c": values})).tolist()
        assert all(o != v for o, v in zip(out, values, strict=True))
        assert len(set(out)) == len(values)


class TestPresidioOperations:
    @pytest.mark.parametrize("as_series", [False, True])
    def test_mask_presidio(self, blank_analyzer, as_series):
        values = [f"Mail {EMAIL}", "hi"]
        data = pd.Series(values) if as_series else pd.DataFrame({"t": values})
        out = MaskPresidio(col_name="t", analyzer=blank_analyzer)(data)
        assert out.tolist() == ["Mail <EMAIL_ADDRESS>", "hi"]

    def test_mask_dict(self, blank_analyzer):
        line = json.dumps({"c": f"mail {EMAIL}", "n": "hi"})
        out = MaskDictOperation(col_name="j", analyzer=blank_analyzer)(
            pd.DataFrame({"j": [line]})
        )
        assert [json.loads(v) for v in out] == [
            {"c": "mail <EMAIL_ADDRESS>", "n": "hi"}
        ]

    def test_mask_dict_series(self, blank_analyzer):
        line = json.dumps({"c": f"mail {EMAIL}"})
        out = MaskDictOperation(col_name="j", analyzer=blank_analyzer)(
            pd.Series([line], index=[3])
        )
        assert [json.loads(v) for v in out] == [{"c": "mail <EMAIL_ADDRESS>"}]
        assert out.index.tolist() == [3]


class TestStringMatchOperations:
    def test_string_match_uses_serving_columns(self):
        data = pd.DataFrame({"t": ["John here", "Ann here"], "first": ["John", "Ann"]})
        out = StringMatchOperation(col_name="t", pii_cols=["first"])(data)
        assert out.tolist() == ["<PATIENT_DATA> here", "<PATIENT_DATA> here"]

    def test_string_match_dict(self):
        data = pd.DataFrame({
            "j": [json.dumps({"n": "John here"}), json.dumps({"n": "Ann"})],
            "first": ["John", "Ann"],
        })
        out = StringMatchDictOperation(col_name="j", pii_cols=["first"])(data)
        assert [json.loads(v) for v in out] == [
            {"n": "<PATIENT_DATA> here"},
            {"n": "<PATIENT_DATA>"},
        ]
        assert out.index.tolist() == data.index.tolist()

    def test_string_match_dict_series(self):
        line = json.dumps({"n": "John"})
        out = StringMatchDictOperation(col_name="j")(pd.Series([line]))
        assert out.tolist() == [line]
