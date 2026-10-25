import json

import pytest
from masking.mask.operations.operation import PandasOperation
from masking.mask.operations.operation_fake_date import FakeDate as FakeDatePd
from masking.mask.operations.operation_fake_name import FakeNameOperation as FakeNamePd
from masking.mask.operations.operation_fake_plz import FakePLZ as FakePLZPd
from masking.mask.operations.operation_hash import HashOperation as HashPd
from masking.mask.operations.operation_med_stats import MedStatsOperation as MedStatsPd
from masking.mask.operations.operation_presidio import MaskPresidio as MaskPresidioPd
from masking.mask.operations.operation_presidio_dict import (
    MaskDictOperation as MaskDictPd,
)
from masking.mask.operations.operation_string_match import (
    StringMatchOperation as StringMatchPd,
)
from masking.mask.operations.operation_string_match_dict import (
    StringMatchDictOperation as StringMatchDictPd,
)
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation as YYYYPd
from masking.mask_spark.operations.operation import SparkOperation
from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_fake_name import FakeNameOperation
from masking.mask_spark.operations.operation_fake_plz import FakePLZ
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_med_stats import MedStatsOperation
from masking.mask_spark.operations.operation_presidio import MaskPresidio
from masking.mask_spark.operations.operation_presidio_dict import MaskDictOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.operations.operation_string_match_dict import (
    StringMatchDictOperation,
)
from masking.mask_spark.operations.operation_yyyy_hash import YYYYHashOperation
from masking.utils.hash import hash_string
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, LongType, StringType

EMAIL = "john.doe@example.com"
SECRET = "s"  # noqa: S105


@pytest.mark.parametrize(
    ("operation", "pandas_operation"),
    [
        (FakeDate, FakeDatePd),
        (FakeNameOperation, FakeNamePd),
        (FakePLZ, FakePLZPd),
        (HashOperation, HashPd),
        (MedStatsOperation, MedStatsPd),
        (MaskPresidio, MaskPresidioPd),
        (MaskDictOperation, MaskDictPd),
        (StringMatchOperation, StringMatchPd),
        (StringMatchDictOperation, StringMatchDictPd),
        (YYYYHashOperation, YYYYPd),
    ],
)
def test_operations_wrap_pandas_operations(operation, pandas_operation):
    assert issubclass(operation, SparkOperation)
    assert issubclass(operation, pandas_operation)
    assert issubclass(operation, PandasOperation)


def column(data, name: str) -> list:
    return [row[name] for row in data.orderBy("id").collect()]


class TestSparkOperation:
    def test_python_masking_keeps_other_columns(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, "a", "x"), (2, "b", None)], "id int, c string, other string"
        ).repartition(2)
        out = HashOperation(col_name="c", secret=SECRET)(data)

        assert out.schema == data.schema
        assert column(out, "c") == [hash_string("a", SECRET), hash_string("b", SECRET)]
        assert column(out, "other") == ["x", None]

    @pytest.mark.xfail(
        strict=True,
        reason="Bug: nulls reach the pandas masking as NaN and are masked as 'nan'.",
    )
    def test_python_masking_keeps_nulls(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "1000"), (2, None)], "id int, p string")
        assert column(MedStatsOperation(col_name="p")(data), "p") == ["VD02", None]

    def test_masked_column_becomes_string(self, spark: SparkSession):
        data = spark.createDataFrame([(1, 10), (2, 20)], "id int, c bigint")
        out = HashOperation(col_name="c", secret=SECRET)(data)

        assert isinstance(out.schema["c"].dataType, StringType)
        assert isinstance(out.schema["id"].dataType, IntegerType)
        assert column(out, "c") == [
            hash_string("10", SECRET),
            hash_string("20", SECRET),
        ]

    def test_native_expression_skips_python(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "a")], "id int, c string")
        op = HashOperation(col_name="c")
        assert op._native_mask_expression(data) is not None
        assert "MapInPandas" not in op(data)._jdf.queryExecution().toString()

    def test_python_masking_uses_map_in_pandas(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "a")], "id int, c string")
        op = HashOperation(col_name="c", secret=SECRET)
        assert op._native_mask_expression(data) is None
        assert "MapInPandas" in op(data)._jdf.queryExecution().toString()

    def test_yyyy_hash(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "2000-01-31")], "id int, d string")
        out = YYYYHashOperation(col_name="d", secret=SECRET)(data)
        assert column(out, "d") == ["2000_" + hash_string("2000-01-31", SECRET)]

    def test_med_stats(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "1000"), (2, "0000")], "id int, p string")
        assert column(MedStatsOperation(col_name="p")(data), "p") == ["VD02", "0000"]

    def test_fake_plz(self, spark: SparkSession):
        values = ["8001", "3436", "1000"]
        data = spark.createDataFrame(list(enumerate(values)), "id int, p string")
        out = column(FakePLZ(col_name="p")(data), "p")
        assert all(len(o) == 4 and o != v for o, v in zip(out, values, strict=True))


class TestServingColumns:
    def test_string_match(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, "John here", "John"), (2, "Ann here", "Ann")],
            "id int, t string, first string",
        )
        out = StringMatchOperation(col_name="t", pii_cols=["first"])(data)
        assert out.columns == data.columns
        assert column(out, "t") == ["<PATIENT_DATA> here", "<PATIENT_DATA> here"]
        assert column(out, "first") == ["John", "Ann"]

    def test_string_match_dict(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, json.dumps({"n": "John here"}), "John")],
            "id int, j string, first string",
        )
        out = StringMatchDictOperation(col_name="j", pii_cols=["first"])(data)
        assert [json.loads(v) for v in column(out, "j")] == [
            {"n": "<PATIENT_DATA> here"}
        ]


class TestPresidio:
    def test_mask_presidio(self, spark: SparkSession, blank_analyzer):
        data = spark.createDataFrame(
            [(1, f"Mail {EMAIL}"), (2, "hi")], "id int, t string"
        )
        out = MaskPresidio(col_name="t", analyzer=blank_analyzer)(data)
        assert column(out, "t") == ["Mail <EMAIL_ADDRESS>", "hi"]

    def test_mask_dict(self, spark: SparkSession, blank_analyzer):
        data = spark.createDataFrame(
            [(1, json.dumps({"c": f"mail {EMAIL}", "n": "hi"}))], "id long, j string"
        )
        out = MaskDictOperation(col_name="j", analyzer=blank_analyzer)(data)
        assert isinstance(out.schema["id"].dataType, LongType)
        assert [json.loads(v) for v in column(out, "j")] == [
            {"c": "mail <EMAIL_ADDRESS>", "n": "hi"}
        ]
