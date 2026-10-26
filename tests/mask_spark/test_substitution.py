import datetime as dt

import pytest
from masking.mask_spark.pipeline import build_lookup_table, substitute_column
from pyspark.sql import SparkSession

CASES = {
    # name: (values, schema type, table, join expected)
    "string": (["a", "b", None, "c"], "string", {"a": "A", "c": "C"}, True),
    "int": ([1, 2, None, 3], "int", {1: "one", 3: "three"}, True),
    "long": ([1, 2, None, 3], "bigint", {2: "two"}, True),
    "date": (
        [dt.date(2020, 1, 1), dt.date(2021, 1, 1), None],
        "date",
        {dt.date(2020, 1, 1): "D"},
        True,
    ),
    "mixed_keys": ([1, 2, None, 3], "int", {1: "one", "3": "three"}, False),
    "bool": ([True, False, None], "boolean", {True: "T"}, False),
    "double": ([1.5, 2.0, None, 1e20], "double", {1.5: "x"}, False),
    "timestamp": (
        [dt.datetime(2020, 1, 1, 10), None],  # noqa: DTZ001
        "timestamp",
        {dt.datetime(2020, 1, 1, 10): "T"},  # noqa: DTZ001
        False,
    ),
}


@pytest.mark.parametrize("partitions", [1, 3])
@pytest.mark.parametrize("case", list(CASES))
def test_substitute_column(spark: SparkSession, case: str, partitions: int) -> None:
    values, spark_type, table, join_expected = CASES[case]
    data = spark.createDataFrame(
        list(enumerate(values)), schema=f"row_id int, c {spark_type}"
    ).repartition(partitions)

    assert (build_lookup_table(data, "c", table, "k", "v") is not None) is join_expected

    out = substitute_column(data, "c", table)
    result = dict(out.select("row_id", "c").collect())

    expected = {
        i: None if v is None else table.get(v, str(v)) for i, v in enumerate(values)
    }
    assert out.columns == data.columns
    assert result == expected


@pytest.mark.parametrize("other", ["C_CLEAR", "c_Masked", "c_clear"])
def test_temporary_names_ignore_case(spark: SparkSession, other: str) -> None:
    data = spark.createDataFrame([("a", "x"), ("b", "y")], f"c string, {other} string")
    out = substitute_column(data, "c", {"a": "A"})
    assert out.columns == data.columns
    assert sorted(out.collect()) == [("A", "x"), ("b", "y")]
