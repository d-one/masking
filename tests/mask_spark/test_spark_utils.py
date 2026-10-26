import datetime as dt
from decimal import Decimal

import pytest
from masking.base_operations.operation import Operation
from masking.utils.spark_utils import spark_df_to_dict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F  # noqa: N812


def test_multi_partition_pairs_are_consistent(spark: SparkSession) -> None:
    sdf = (
        spark.range(5000)
        .select(
            F.col("id").cast("string").alias("clear_values"),
            F.concat(F.lit("m"), F.col("id").cast("string")).alias("masked_values"),
        )
        .repartition(16)
    )
    expected = {str(i): f"m{i}" for i in range(5000)}

    assert spark_df_to_dict(sdf) == expected
    assert Operation.cast_concordance_table(sdf) == expected


@pytest.mark.parametrize(
    "values",
    [
        ["a", "b"],
        [1, 2],
        [1.5, 2.5],
        [True, False],
        [dt.date(2020, 1, 1), dt.date(2021, 2, 3)],
        [Decimal("1.10"), Decimal("2.20")],
        [dt.datetime(2020, 1, 1, 10, 30), dt.datetime(2021, 2, 3, 4, 5)],  # noqa: DTZ001
    ],
    ids=["string", "int", "float", "bool", "date", "decimal", "timestamp"],
)
def test_same_values_as_collect(spark: SparkSession, values: list) -> None:
    sdf = spark.createDataFrame(
        [(v, f"m{i}") for i, v in enumerate(values)],
        schema=["clear_values", "masked_values"],
    )
    expected = {row[0]: row[1] for row in sdf.collect()}
    result = spark_df_to_dict(sdf)

    assert result == expected
    assert [type(k) for k in result] == [type(k) for k in expected]
