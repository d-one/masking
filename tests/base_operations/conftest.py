from typing import Any

import pandas as pd
import pytest
from pyspark.sql import SparkSession


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
def input_concordance_table(request) -> Any:
    return request.param
