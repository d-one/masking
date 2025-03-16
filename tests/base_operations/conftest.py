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
)
def input_concordance_table(request: pytest.FixtureRequest) -> CT_TYPE:
    return request.param


@pytest.fixture(
    scope="module",
    params=[
        ("col_name_None", None),
        ("col_name", "line"),
        ("col_name_", pd.Series(["line"], index=["col_name_"])),
        ("col_name2_", pd.Series(["line2", "line"], index=["col_name_", "col_name2_"])),
    ],
)
def input_name_and_line(request: pytest.FixtureRequest) -> tuple[str, str | pd.Series]:
    return request.param
