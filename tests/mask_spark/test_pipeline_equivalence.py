"""Equivalence tests between the pandas, polars, and Spark pipelines."""

import datetime as dt
from collections.abc import Callable, Iterator
from types import SimpleNamespace

import pandas as pd
import polars as pl
import pytest
from masking.mask.operations.operation_fake_date import FakeDate as FakeDatePd
from masking.mask.operations.operation_fake_name import FakeNameOperation as FakeNamePd
from masking.mask.operations.operation_hash import HashOperation as HashPd
from masking.mask.operations.operation_med_stats import MedStatsOperation as MedStatsPd
from masking.mask.operations.operation_string_match import (
    StringMatchOperation as StringMatchPd,
)
from masking.mask.operations.operation_yyyy_hash import YYYYHashOperation as YYYYHashPd
from masking.mask.pipeline import MaskDataFramePipeline as PandasPipeline
from masking.mask_polars.operations.operation_hash import HashOperation as HashPl
from masking.mask_polars.operations.operation_med_stats import (
    MedStatsOperation as MedStatsPl,
)
from masking.mask_polars.pipeline import MaskDataFramePipeline as PolarsPipeline
from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_fake_name import FakeNameOperation
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_med_stats import MedStatsOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.operations.operation_yyyy_hash import YYYYHashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline as SparkPipeline
from masking.utils.hash import hash_string
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


# MedStats maps known PLZs (1000 -> VD02) and keeps all other values, so the
# concordance tables miss some values of every column.
UNMAPPED_COLUMNS = ["row_id", "plz", "plz_null", "text", "day"]
UNMAPPED_ROWS = [
    (0, 1000, 1000, "1000", dt.date(2020, 1, 1)),
    (1, 99999, None, "abc", dt.date(2021, 2, 3)),
    (2, 1000, 99999, None, None),
]
UNMAPPED_EXPECTED = {
    "plz": ["VD02", "99999", "VD02"],
    "plz_null": ["VD02", None, "99999"],
    "text": ["VD02", "abc", None],
    "day": ["D1", "2021-02-03", None],
}


def _unmapped_config(med_stats: type) -> dict:
    return {
        **{
            c: {"masking_operation": med_stats(col_name=c)}
            for c in ("plz", "plz_null", "text")
        },
        "day": {
            "masking_operation": med_stats(
                col_name="day", concordance_table={dt.date(2020, 1, 1): "D1"}
            )
        },
    }


def test_backends_agree_on_values_missing_from_the_table(spark: SparkSession) -> None:
    pandas_out = PandasPipeline(_unmapped_config(MedStatsPd))(
        pd.DataFrame(UNMAPPED_ROWS, columns=UNMAPPED_COLUMNS)
    )
    polars_out = PolarsPipeline(_unmapped_config(MedStatsPl))(
        pl.DataFrame(UNMAPPED_ROWS, schema=UNMAPPED_COLUMNS, orient="row")
    ).sort("row_id")
    # collect() keeps Spark's nulls; toPandas() turns null strings into NaN
    spark_rows = (
        SparkPipeline(_unmapped_config(MedStatsOperation))(
            spark.createDataFrame(
                UNMAPPED_ROWS,
                "row_id long, plz long, plz_null long, text string, day date",
            )
        )
        .orderBy("row_id")
        .collect()
    )

    for c, expected in UNMAPPED_EXPECTED.items():
        assert pandas_out[c].tolist() == expected, f"pandas column {c}"
        assert polars_out[c].to_list() == expected, f"polars column {c}"
        assert [row[c] for row in spark_rows] == expected, f"spark column {c}"


def test_backends_agree_on_chained_operations(spark: SparkSession) -> None:
    # MedStats keeps 99999, so the hash must apply to its string "99999"
    rows = [(0, 1000), (1, 99999), (2, None)]
    expected = [hash_string("VD02", SECRET), hash_string("99999", SECRET), None]

    def config(med_stats: type, hash_op: type) -> dict:
        return {
            "c": [
                {"masking_operation": med_stats(col_name="c")},
                {"masking_operation": hash_op(col_name="c", secret=SECRET)},
            ]
        }

    pandas_out = PandasPipeline(config(MedStatsPd, HashPd))(
        pd.DataFrame(rows, columns=["row_id", "c"])
    )
    polars_out = PolarsPipeline(config(MedStatsPl, HashPl))(
        pl.DataFrame(rows, schema=["row_id", "c"], orient="row")
    ).sort("row_id")
    spark_rows = (
        SparkPipeline(config(MedStatsOperation, HashOperation))(
            spark.createDataFrame(rows, "row_id long, c long")
        )
        .orderBy("row_id")
        .collect()
    )

    assert pandas_out["c"].tolist() == expected
    assert polars_out["c"].to_list() == expected
    assert [row["c"] for row in spark_rows] == expected


@pytest.mark.parametrize("operation", ["med_stats", "hash"])
def test_backends_repeat_calls_on_integer_columns(
    spark: SparkSession, operation: str
) -> None:
    # The first call stores integer keys; the second call must reuse them
    ops = {
        "med_stats": (MedStatsPd, MedStatsPl, MedStatsOperation, {}),
        "hash": (HashPd, HashPl, HashOperation, {"secret": SECRET}),
    }
    pd_op, pl_op, spark_op, kwargs = ops[operation]
    pipelines = {
        "pandas": PandasPipeline({
            "c": {"masking_operation": pd_op(col_name="c", **kwargs)}
        }),
        "polars": PolarsPipeline({
            "c": {"masking_operation": pl_op(col_name="c", **kwargs)}
        }),
        "spark": SparkPipeline({
            "c": {"masking_operation": spark_op(col_name="c", **kwargs)}
        }),
    }
    rows = [(0, 1000), (1, 2000), (2, None)]

    def run(backend: str) -> list:
        if backend == "pandas":
            return pipelines[backend](pd.DataFrame(rows, columns=["row_id", "c"]))[
                "c"
            ].tolist()
        if backend == "polars":
            out = pipelines[backend](
                pl.DataFrame(rows, schema=["row_id", "c"], orient="row")
            )
            return out.sort("row_id")["c"].to_list()
        out = pipelines[backend](spark.createDataFrame(rows, "row_id long, c long"))
        return [row["c"] for row in out.orderBy("row_id").collect()]

    first = {backend: run(backend) for backend in pipelines}
    second = {backend: run(backend) for backend in pipelines}

    assert first == second
    assert first["pandas"] == first["polars"] == first["spark"]
    assert first["pandas"][2] is None
