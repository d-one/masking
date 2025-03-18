import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

CT_TYPE = dict | pd.DataFrame | None | list[dict] | DataFrame

CT_ALLOWED_TYPES = {dict, pd.DataFrame, DataFrame}


@pytest.fixture(
    params=[
        {},
        {"a": "b", "line": "L", "e": "f"},
        pd.DataFrame(
            [("a", "b"), ("line", "L"), ("e", "f")],
            columns=["clear_values", "masked_values"],
        ),
        None,
        [{"a": "b"}, {"c": "d"}, {"e": "f"}],
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("line", "L")], ["clear_values", "masked_values"]
        ),
    ]
)  # Valid & invalid concordance tables input
def vi_input_concordance_table(request: pytest.FixtureRequest) -> CT_TYPE:
    return request.param


@pytest.fixture(
    params=["col_name", "col_name2", "col_name3", None, []]
)  # Valid & invalid column names
def vi_name(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(
    params=[
        pd.DataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")],
            columns=["clear_values_", "masked_values"],
        ),
        pd.DataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")],
            columns=["clear_values", "_masked_values"],
        ),
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")], ["_clear_values", "masked_values"]
        ),
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")], ["clear_values", "masked_values_"]
        ),
    ]
)  # Invalid concordance tables input
def invalid_table(request: pytest.FixtureRequest) -> DataFrame:
    return request.param


def get_v_input_name_and_line_str() -> tuple[str, str]:
    return [
        ("col_name_None", None),
        ("col_name", "line"),
        ("col_name_", pd.Series(["line"], index=["col_name_"])),
        ("col_name2_", pd.Series(["line2", "line"], index=["col_name_", "col_name2_"])),
    ]


def get_v_input_name_and_line_date() -> tuple[str, str]:
    return [
        ("col_name", "2021-01-01"),
        ("col_name_", "2021-01-02"),
        ("col_name2_", "2021-01-03T00:00:00"),
        ("col_name3_", "2021-01-04T00:00:00.000000"),
        ("col_name4_", "2021-01-05T00:00:00.000000+00:00"),
        ("col_name5_", pd.Series(["2021-01-06"], index=["col_name5_"])),
        ("col_name6_", pd.Series(["2021-01-07T00:00:00"], index=["col_name6_"])),
        ("col_name7_", pd.Series(["2021-01-08T00:00:00.000000"], index=["col_name7_"])),
        (
            "col_name8_",
            pd.Series(["2021-01-09T00:00:00.000000+00:00"], index=["col_name8_"]),
        ),
        (
            "col_name9_",
            pd.Series(["2021-01-10", "2021-01-11"], index=["col_name9", "col_name9_"]),
        ),
        (
            "col_name10_",
            pd.Series(
                ["2021-01-12T00:00:00", "2021-01-13T00:00:00"],
                index=["col_name10", "col_name10_"],
            ),
        ),
        (
            "col_name11_",
            pd.Series(
                ["2021-01-14T00:00:00.000000", "2021-01-15T00:00:00.000000"],
                index=["col_name11", "col_name11_"],
            ),
        ),
        (
            "col_name12_",
            pd.Series(
                [
                    "2021-01-16T00:00:00.000000+00:00",
                    "2021-01-17T00:00:00.000000+00:00",
                ],
                index=["col_name12_", "col_name12"],
            ),
        ),
    ]
