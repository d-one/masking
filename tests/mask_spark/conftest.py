import datetime as dt
import math

import pandas as pd
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[4]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


@pytest.fixture(scope="session")
def clear_data() -> pd.DataFrame:
    """Small dataset with duplicates, nulls and several column types.

    Returns
    -------
        pd.DataFrame: input data

    """
    n, distinct = 3000, 600
    names = [None if i % 97 == 0 else f"name_{i % distinct}" for i in range(n)]
    start = dt.date(1950, 1, 1)
    return pd.DataFrame({
        "row_id": list(range(n)),
        "name": names,
        "date": [
            None
            if i % 101 == 0
            else (start + dt.timedelta(days=7 * (i % distinct))).isoformat()
            for i in range(n)
        ],
        "text": [None if nm is None else f"Patient {nm} was admitted." for nm in names],
        "age": [i % 90 for i in range(n)],
    })


@pytest.fixture(scope="session")
def dense_dates() -> pd.DataFrame:
    """Dates covering ~half of the days of each month.

    With preserve=('year', 'month') the fake values of different partitions
    are very likely to collide, which exercises cross-partition uniqueness.

    Returns
    -------
        pd.DataFrame: input data

    """
    dates = [
        dt.date(y, m, d).isoformat()
        for y in range(1990, 2010)
        for m in range(1, 13)
        for d in range(1, 29, 2)
    ]
    return pd.DataFrame({"row_id": range(len(dates)), "date": dates})


def normalize(value: object) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    return str(value)
