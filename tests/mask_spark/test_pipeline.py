import datetime as dt
import hashlib

import pytest
from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_med_stats import MedStatsOperation
from masking.mask_spark.operations.operation_null import NullOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.pipeline import ConcordanceTable, MaskDataFramePipeline
from pyspark.sql import SparkSession


def sha256(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def rows(data) -> list[tuple]:
    return sorted(tuple(r) for r in data.collect())


class TestConcordanceTable:
    def test_build_keeps_only_changed_values(self, spark: SparkSession):
        concordance = ConcordanceTable(
            masking_operation=MedStatsOperation(col_name="p")
        )
        data = spark.createDataFrame([("1000",), ("0000",)], "p string")
        assert concordance._build_concordance_table(data) == {"1000": "VD02"}

    def test_build_passes_serving_columns(self, spark: SparkSession):
        op = StringMatchOperation(col_name="t", pii_cols=["first"])
        data = spark.createDataFrame(
            [("John here", "John"), ("nothing", "Ann")], "t string, first string"
        )
        assert ConcordanceTable(masking_operation=op)._build_concordance_table(
            data
        ) == {"John here": "<PATIENT_DATA> here"}

    def test_build_errors_are_wrapped(self, spark: SparkSession):
        concordance = ConcordanceTable(
            masking_operation=HashOperation(col_name="missing")
        )
        data = spark.createDataFrame([("a",)], "c string")
        with pytest.raises(ValueError, match="Invalid concordance table"):
            concordance._build_concordance_table(data)


class TestResolveCollisions:
    @staticmethod
    def make(table: dict | None = None) -> ConcordanceTable:
        op = FakeDate(col_name="d", preserve="year", concordance_table=table)
        return ConcordanceTable(masking_operation=op)

    def test_without_collisions_values_are_kept(self):
        concordance = self.make()
        new_values = {"2000-01-01": "2000-02-02", "2000-01-02": "2000-03-03"}
        assert concordance._resolve_collisions(new_values) == new_values
        assert concordance.masking_operation.concordance_table == {}

    def test_colliding_values_are_masked_again(self):
        concordance = self.make({"1999-01-01": "2000-09-09"})
        new_values = {
            "2000-01-01": "2000-02-02",
            "2000-01-02": "2000-02-02",
            "2000-01-03": "2000-09-09",
        }
        resolved = concordance._resolve_collisions(new_values)

        assert resolved.keys() == new_values.keys()
        assert resolved["2000-01-01"] == "2000-02-02"
        assert len(set(resolved.values())) == 3
        assert "2000-09-09" not in resolved.values()
        assert all(v.startswith("2000-") for v in resolved.values())
        assert (
            concordance.masking_operation.concordance_table.items() >= resolved.items()
        )


class TestStaticMethods:
    def test_get_unique_values(self, spark: SparkSession):
        data = spark.createDataFrame([("a",), (None,), ("b",), ("a",)], "c string")
        assert sorted(MaskDataFramePipeline._get_unique_values(data, "c")) == ["a", "b"]

    def test_filter_data(self, spark: SparkSession):
        data = spark.createDataFrame(
            [("a", 1, "x"), ("a", 1, "y"), (None, 2, "z"), ("b", 1, "w")],
            "c string, s int, other string",
        )
        out = MaskDataFramePipeline._filter_data(data, "c", ["c", "s"])
        assert out.columns == ["c", "s"]
        assert rows(out) == [("a", 1), ("b", 1)]

    def test_substitute_masked_values(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, "x", "x"), (2, "y", "x"), (3, None, "x")], "id int, a string, b string"
        )
        pipelines = [
            ConcordanceTable(masking_operation=HashOperation(col_name="a")),
            [ConcordanceTable(masking_operation=HashOperation(col_name="b"))] * 2,
        ]
        out = MaskDataFramePipeline._substitute_masked_values(
            data, pipelines, {"a": {"x": "X"}, "b": {}}
        )
        assert out.columns == data.columns
        assert rows(out) == [(1, "X", "x"), (2, "y", "x"), (3, None, "x")]

    def test_copy_column(self, spark: SparkSession):
        data = spark.createDataFrame([(1,)], "a int")
        out = MaskDataFramePipeline._copy_column(data, "a", "b")
        assert out.columns == ["a", "b"]
        assert rows(out) == [(1, 1)]

    @pytest.mark.parametrize(
        ("mapping", "expected"),
        [
            ({"a": "b", "b": "a"}, ["b", "a", "c"]),
            ({"a": "x"}, ["x", "b", "c"]),
            ({"a": "b", "b": "c", "c": "a"}, ["b", "c", "a"]),
        ],
    )
    def test_rename_columns(self, spark: SparkSession, mapping, expected):
        data = spark.createDataFrame([(1, 2, 3)], "a int, b int, c int")
        out = MaskDataFramePipeline._rename_columns(data, mapping)
        assert out.columns == expected
        assert rows(out) == [(1, 2, 3)]

    def test_rename_columns_ignores_case_of_temporary_names(self, spark: SparkSession):
        data = spark.createDataFrame([(1, 2)], "a int, ___TEMP_0___ int")
        out = MaskDataFramePipeline._rename_columns(data, {"a": "b"})
        assert out.columns == ["b", "___TEMP_0___"]
        assert rows(out) == [(1, 2)]


class TestPipeline:
    def test_null_operation_nulls_the_column(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, "a"), (2, None), (3, "b")], "o long, c string"
        )
        pipeline = MaskDataFramePipeline({
            "c": {"masking_operation": NullOperation(col_name="c")}
        })
        rows = pipeline(data).orderBy("o").collect()
        assert [r["c"] for r in rows] == [None, None, None]
        assert [r["o"] for r in rows] == [1, 2, 3]
        assert pipeline.concordance_tables == {"c": {}}

    def test_null_operation_concordance_table_is_empty(self, spark: SparkSession):
        table = ConcordanceTable(masking_operation=NullOperation(col_name="c"))
        assert table(spark.createDataFrame([("a",)], "c string")) == {}

    def test_null_operation_in_new_column_keeps_input(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "a"), (2, "b")], "o long, c string")
        pipeline = MaskDataFramePipeline({
            "out": {
                "masking_operation": NullOperation(
                    col_name="c", condition=lambda v: v == "a"
                )
            }
        })
        for _ in range(2):
            rows = pipeline(data).orderBy("o").collect()
            assert [r["c"] for r in rows] == ["a", "b"]
            assert [r["out"] for r in rows] == [None, "b"]

    def test_conditional_null_on_timestamp_ntz(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, dt.datetime(2024, 1, 1, 12)), (2, dt.datetime(2000, 1, 1))],  # noqa: DTZ001
            "o long, c timestamp_ntz",
        )
        pipeline = MaskDataFramePipeline({
            "c": {
                "masking_operation": NullOperation(
                    col_name="c", condition=lambda v: v == "2024-01-01 12:00:00"
                )
            }
        })
        # A session time zone other than the driver's local one
        local_offset = dt.datetime.now().astimezone().utcoffset()
        session_tz = (
            "Etc/GMT+12" if local_offset != dt.timedelta(hours=-12) else "Etc/GMT-14"
        )
        previous_tz = spark.conf.get("spark.sql.session.timeZone")
        spark.conf.set("spark.sql.session.timeZone", session_tz)
        try:
            out = pipeline(data)
            assert [r["c"] for r in out.orderBy("o").collect()] == [
                None,
                dt.datetime(2000, 1, 1),  # noqa: DTZ001
            ]
            assert out.schema["c"].dataType == data.schema["c"].dataType
        finally:
            spark.conf.set("spark.sql.session.timeZone", previous_tz)

    def test_end_to_end(self, spark: SparkSession):
        data = spark.createDataFrame(
            [(1, "1000"), (2, "0000"), (3, None), (4, "1000")], "id int, p string"
        )
        pipeline = MaskDataFramePipeline({
            "p": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert rows(out.select("id", "p")) == [
            (1, "VD02"),
            (2, "0000"),
            (3, None),
            (4, "VD02"),
        ]
        assert pipeline.concordance_tables == {"p": {"1000": "VD02"}}

    def test_output_in_new_column(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "1000"), (2, "0000")], "id int, p string")
        pipeline = MaskDataFramePipeline({
            "region": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        out = pipeline(data)
        assert rows(out.select("id", "p", "region")) == [
            (1, "1000", "VD02"),
            (2, "0000", "0000"),
        ]

    def test_integer_column(self, spark: SparkSession):
        data = spark.createDataFrame([(1, 1000), (2, 0), (3, None)], "id int, p int")
        pipeline = MaskDataFramePipeline({
            "p": {"masking_operation": MedStatsOperation(col_name="p")}
        })
        assert rows(pipeline(data).select("id", "p")) == [
            (1, "VD02"),
            (2, "0"),
            (3, None),
        ]

    def test_prescribed_concordance_table(self, spark: SparkSession):
        data = spark.createDataFrame([(1, "x"), (2, "y")], "id int, a string")
        pipeline = MaskDataFramePipeline({
            "a": {
                "masking_operation": HashOperation(col_name="a"),
                "concordance_table": {"x": "X"},
            }
        })
        assert rows(pipeline(data).select("id", "a")) == [(1, "X"), (2, sha256("y"))]
