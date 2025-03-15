import pandas as pd
import pytest
from pyspark.sql import DataFrame, SparkSession

CT_TYPE = dict | pd.DataFrame | None | list[dict] | DataFrame


@pytest.fixture(
    scope="module",
    params=[
        {},
        {"a": "b", "c": "d", "e": "f"},
        pd.DataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")],
            columns=["clear_values", "masked_values"],
        ),
        None,
        [{"a": "b"}, {"c": "d"}, {"e": "f"}],
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")], ["clear_values", "masked_values"]
        ),
    ],
)
def input_concordance_table(request: pytest.FixtureRequest) -> CT_TYPE:
    return request.param
