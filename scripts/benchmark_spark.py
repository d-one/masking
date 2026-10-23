"""Benchmark the Spark masking pipeline.

Generates a synthetic dataset and times, for each selected operation:
  * build: filter + distinct + mask + collect of the concordance table
  * substitute: writing the masked values back into the full dataframe
  * total: build + substitute

Example:
-------
    python scripts/benchmark_spark.py --rows 2000000 --distinct 200000 --ops hash_secret fake_name
    python scripts/benchmark_spark.py --output results.json

"""

import argparse
import json
import statistics
import time
from collections.abc import Callable
from pathlib import Path

from masking.mask_spark.operations.operation_fake_date import FakeDate
from masking.mask_spark.operations.operation_fake_name import FakeNameOperation
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.operations.operation_string_match import StringMatchOperation
from masking.mask_spark.operations.operation_yyyy_hash import YYYYHashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F  # noqa: N812

OPERATIONS: dict[str, Callable[[], dict]] = {
    "hash_secret": lambda: {
        "name": {"masking_operation": HashOperation(col_name="name", secret="s")}
    },
    "hash_nosecret": lambda: {
        "name": {"masking_operation": HashOperation(col_name="name")}
    },
    "yyyy_hash": lambda: {
        "date": {"masking_operation": YYYYHashOperation(col_name="date", secret="s")}
    },
    "fake_name": lambda: {
        "name": {"masking_operation": FakeNameOperation(col_name="name")}
    },
    "fake_date": lambda: {
        "date": {"masking_operation": FakeDate(col_name="date", preserve=("year",))}
    },
    "string_match": lambda: {
        "text": {
            "masking_operation": StringMatchOperation(
                col_name="text",
                masking_function=lambda _: "<MASKED>",
                pii_cols=["name"],
            )
        }
    },
}


def build_spark(master: str, extra_conf: list[str]) -> SparkSession:
    builder = (
        SparkSession.builder.master(master)
        .appName("masking-benchmark")
        .config("spark.driver.memory", "6g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    for kv in extra_conf:
        k, v = kv.split("=", 1)
        builder = builder.config(k, v)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def build_data(
    spark: SparkSession, rows: int, distinct: int, partitions: int
) -> DataFrame:
    k = F.col("id") % distinct
    sdf = (
        spark.range(rows)
        .select(
            F.col("id").cast("string").alias("id_str"),
            F.concat(F.lit("name_"), k.cast("string")).alias("name"),
            F.date_format(
                F.date_add(
                    F.lit("1900-01-01").cast("date"), ((k % 5000) * 5).cast("int")
                ),
                "yyyy-MM-dd",
            ).alias("date"),
            F.concat(
                F.lit("Patient name_"), k.cast("string"), F.lit(" was admitted.")
            ).alias("text"),
            (k % 997).cast("string").alias("id"),
        )
        .repartition(partitions)
        .cache()
    )
    sdf.count()
    return sdf


def run_once(data: DataFrame, config: dict) -> dict[str, float]:
    pipeline = MaskDataFramePipeline(config, workers=1)

    t0 = time.perf_counter()
    pipeline._run_pipelines(data)  # noqa: SLF001
    t1 = time.perf_counter()
    out = pipeline._substitute_masked_values(  # noqa: SLF001
        data, pipeline.col_pipelines, pipeline.concordance_tables
    )
    out.write.format("noop").mode("overwrite").save()
    t2 = time.perf_counter()

    return {"build": t1 - t0, "substitute": t2 - t1, "total": t2 - t0}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--rows", type=int, default=1_000_000)
    parser.add_argument("--distinct", type=int, default=100_000)
    parser.add_argument("--partitions", type=int, default=8)
    parser.add_argument("--master", default="local[8]")
    parser.add_argument("--repeat", type=int, default=3)
    parser.add_argument(
        "--ops", nargs="+", default=list(OPERATIONS), choices=list(OPERATIONS)
    )
    parser.add_argument(
        "--conf", nargs="*", default=[], help="extra spark conf as key=value"
    )
    parser.add_argument("--output", type=Path, help="write results as JSON")
    args = parser.parse_args()

    spark = build_spark(args.master, args.conf)
    data = build_data(spark, args.rows, args.distinct, args.partitions)

    results: dict[str, dict[str, float]] = {}
    print(
        f"rows={args.rows} distinct={args.distinct} partitions={args.partitions} "
        f"master={args.master} repeat={args.repeat}"
    )
    print(f"{'operation':<24}{'build [s]':>12}{'substitute [s]':>16}{'total [s]':>12}")
    for op in args.ops:
        run_once(data, OPERATIONS[op]())  # warm-up
        runs = [run_once(data, OPERATIONS[op]()) for _ in range(args.repeat)]
        results[op] = {
            k: statistics.median(r[k] for r in runs)
            for k in ("build", "substitute", "total")
        }
        r = results[op]
        print(f"{op:<24}{r['build']:>12.2f}{r['substitute']:>16.2f}{r['total']:>12.2f}")

    if args.output:
        args.output.write_text(
            json.dumps(
                {
                    "args": {k: str(v) for k, v in vars(args).items()},
                    "results": results,
                },
                indent=2,
            )
        )

    spark.stop()


if __name__ == "__main__":
    main()
