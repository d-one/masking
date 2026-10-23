"""Equivalence tests between the pandas and the Spark pipelines."""

from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pandas as pd
import pytest
from masking.mask.operations.operation_fake_date import FakeDate as FakeDatePd
from masking.mask.operations.operation_fake_name import FakeNameOperation as FakeNamePd
from masking.mask.operations.operation_hash import HashOperation as HashPd
from masking.mask.operations.operation_string_match import (
    StringMatchOperation as StringMatchPd,
)
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation as YYYYHashPd
from masking.mask.pipeline import MaskDataFramePipeline as PandasPipeline
from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_fake_name import FakeNameOperation
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.operations.operation_yyyy_hash import YYYYHashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline as SparkPipeline
from pyspark.sql import DataFrame, SparkSession

from .conftest import normalize


def _mask(_: str) -> str:
    return "<MASKED>"


SECRET = "secret"  # noqa: S105

SPARK_OPS = SimpleNamespace(
    hash=HashOperation, yyyy=YYYYHashOperation, match=StringMatchOperation
)
PANDAS_OPS = SimpleNamespace(hash=HashPd, yyyy=YYYYHashPd, match=StringMatchPd)

# Each entry builds a configuration from a namespace of operation classes
DETERMINISTIC_CONFIGS: dict[str, Callable] = {
    "hash_secret": lambda o: {
        "name": {"masking_operation": o.hash(col_name="name", secret=SECRET)}
    },
    "hash_nosecret": lambda o: {"name": {"masking_operation": o.hash(col_name="name")}},
    "hash_int_column": lambda o: {
        "age": {"masking_operation": o.hash(col_name="age", secret=SECRET)}
    },
    "hash_with_table": lambda o: {
        "name": {
            "masking_operation": o.hash(
                col_name="name",
                secret=SECRET,
                concordance_table=pd.DataFrame({
                    "clear_values": ["name_1", "name_2"],
                    "masked_values": ["N1", "N2"],
                }),
            )
        }
    },
    "yyyy_hash": lambda o: {
        "date": {"masking_operation": o.yyyy(col_name="date", secret=SECRET)}
    },
    "string_match_serving_col": lambda o: {
        "text": {
            "masking_operation": o.match(
                col_name="text", masking_function=_mask, pii_cols=["name"]
            )
        }
    },
    "chained_operations": lambda o: {
        "name": [
            {"masking_operation": o.hash(col_name="name", secret=SECRET)},
            {"masking_operation": o.hash(col_name="name")},
        ]
    },
    "multiple_columns": lambda o: {
        "name": {"masking_operation": o.hash(col_name="name", secret=SECRET)},
        "date": {"masking_operation": o.yyyy(col_name="date", secret=SECRET)},
    },
}


def _to_spark(spark: SparkSession, data: pd.DataFrame, partitions: int) -> DataFrame:
    # Build from python rows: pandas' string dtype turns None into NaN, which
    # spark.createDataFrame(pandas_df) would convert into the string "NaN".
    rows = [
        tuple(None if pd.isna(v) else v for v in row)
        for row in data.itertuples(index=False)
    ]
    return spark.createDataFrame(rows, schema=list(data.columns)).repartition(
        partitions
    )


def _run_spark(
    spark: SparkSession, data: pd.DataFrame, config: dict, partitions: int = 4
) -> tuple[pd.DataFrame, dict]:
    pipeline = SparkPipeline(config)
    sdf = _to_spark(spark, data, partitions)
    out = pipeline(sdf).toPandas().sort_values("row_id").reset_index(drop=True)
    return out, {k: dict(v) for k, v in pipeline.concordance_tables.items()}


def _run_pandas(data: pd.DataFrame, config: dict) -> tuple[pd.DataFrame, dict]:
    pipeline = PandasPipeline(config)
    out = pipeline(data.copy()).sort_values("row_id").reset_index(drop=True)
    return out, {k: dict(v) for k, v in pipeline.concordance_tables.items()}


@pytest.mark.parametrize("config_name", list(DETERMINISTIC_CONFIGS))
def test_spark_matches_pandas(
    spark: SparkSession, clear_data: pd.DataFrame, config_name: str
) -> None:
    build = DETERMINISTIC_CONFIGS[config_name]
    spark_out, spark_tables = _run_spark(spark, clear_data, build(SPARK_OPS))
    pandas_out, pandas_tables = _run_pandas(clear_data, build(PANDAS_OPS))

    assert list(spark_out.columns) == list(pandas_out.columns)
    for c in pandas_out.columns:
        assert (
            spark_out[c].map(normalize).tolist()
            == pandas_out[c].map(normalize).tolist()
        ), f"column {c} differs"

    assert spark_tables.keys() == pandas_tables.keys()
    for c, table in pandas_tables.items():
        assert {str(k): v for k, v in spark_tables[c].items()} == {
            str(k): v for k, v in table.items()
        }


def _assert_fake_invariants(
    clear: pd.Series, masked: pd.Series, *, unique: bool = True
) -> None:
    clear, masked = clear.map(normalize), masked.map(normalize)

    # nulls are preserved
    assert (clear.isna() == masked.isna()).all()

    pairs = pd.DataFrame({"clear": clear, "masked": masked}).dropna()
    # every value is changed
    assert (pairs["clear"] != pairs["masked"]).all()
    # consistent: same clear value -> same masked value
    assert (pairs.groupby("clear")["masked"].nunique() == 1).all()  # noqa: PD101
    if unique:
        # unique: different clear values -> different masked values
        assert pairs["masked"].nunique() == pairs["clear"].nunique()


FAKE_CONFIGS = {
    "fake_name": (
        lambda: {"name": {"masking_operation": FakeNameOperation(col_name="name")}},
        "name",
    ),
    "fake_date": (
        lambda: {
            "date": {"masking_operation": FakeDate(col_name="date", preserve=("year",))}
        },
        "date",
    ),
}


@pytest.mark.parametrize("config_name", list(FAKE_CONFIGS))
def test_spark_fake_invariants(
    spark: SparkSession, clear_data: pd.DataFrame, config_name: str
) -> None:
    config, column = FAKE_CONFIGS[config_name]
    out, _ = _run_spark(spark, clear_data, config())
    _assert_fake_invariants(clear_data[column], out[column], unique=False)


@pytest.fixture()
def _no_partition_coalescing(spark: SparkSession) -> Iterator[None]:
    """Keep all shuffle partitions, so that masking runs on many partitions."""
    key = "spark.sql.adaptive.coalescePartitions.enabled"
    previous = spark.conf.get(key)
    spark.conf.set(key, "false")
    yield
    spark.conf.set(key, previous)


@pytest.mark.parametrize("config_name", list(FAKE_CONFIGS))
@pytest.mark.usefixtures("_no_partition_coalescing")
def test_spark_fake_unique(
    spark: SparkSession, clear_data: pd.DataFrame, config_name: str
) -> None:
    config, column = FAKE_CONFIGS[config_name]
    out, _ = _run_spark(spark, clear_data, config())
    _assert_fake_invariants(clear_data[column], out[column])


@pytest.mark.usefixtures("_no_partition_coalescing")
def test_spark_fake_unique_across_partitions(
    spark: SparkSession, dense_dates: pd.DataFrame
) -> None:
    config = {
        "date": {
            "masking_operation": FakeDate(col_name="date", preserve=("year", "month"))
        }
    }
    out, _ = _run_spark(spark, dense_dates, config, partitions=8)
    _assert_fake_invariants(dense_dates["date"], out["date"])


def test_pandas_fake_unique(dense_dates: pd.DataFrame) -> None:
    config = {
        "date": {
            "masking_operation": FakeDatePd(col_name="date", preserve=("year", "month"))
        }
    }
    out, _ = _run_pandas(dense_dates, config)
    _assert_fake_invariants(dense_dates["date"], out["date"])


def test_pandas_fake_name_unique(clear_data: pd.DataFrame) -> None:
    config = {"name": {"masking_operation": FakeNamePd(col_name="name")}}
    out, _ = _run_pandas(clear_data, config)
    _assert_fake_invariants(clear_data["name"], out["name"])


@pytest.mark.usefixtures("_no_partition_coalescing")
def test_spark_fake_unique_with_existing_table(
    spark: SparkSession, dense_dates: pd.DataFrame
) -> None:
    first, second = dense_dates["date"].iloc[0], dense_dates["date"].iloc[1]
    config = {
        "date": {
            "masking_operation": FakeDate(
                col_name="date",
                preserve=("year", "month"),
                concordance_table={first: second},
            )
        }
    }
    out, tables = _run_spark(spark, dense_dates, config, partitions=8)

    assert tables["date"][first] == second
    assert (out["date"] == second).sum() == 1
    _assert_fake_invariants(dense_dates["date"], out["date"])
