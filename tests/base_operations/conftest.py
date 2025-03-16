import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

CT_TYPE = dict | pd.DataFrame | None | list[dict] | DataFrame

CT_ALLOWED_TYPES = {dict, pd.DataFrame, DataFrame}


@pytest.fixture(
    scope="module",
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
    ],
)  # Valid & invalid concordance tables input
def vi_input_concordance_table(request: pytest.FixtureRequest) -> CT_TYPE:
    return request.param


@pytest.fixture(
    scope="module",
    params=[
        ("col_name_None", None),
        ("col_name", "line"),
        ("col_name_", pd.Series(["line"], index=["col_name_"])),
        ("col_name2_", pd.Series(["line2", "line"], index=["col_name_", "col_name2_"])),
    ],
)  # Valid input name and line
def v_input_name_and_line(
    request: pytest.FixtureRequest,
) -> tuple[str, str | pd.Series]:
    return request.param


@pytest.fixture(
    scope="module", params=["col_name", "col_name2", "col_name3", None, []]
)  # Valid & invalid column names
def vi_name(request: pytest.FixtureRequest) -> str:
    return request.param


@pytest.fixture(
    scope="module",
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
    ],
)  # Invalid concordance tables input
def invalid_table(request: pytest.FixtureRequest) -> DataFrame:
    return request.param
