# Running the pipeline on Spark

The Spark pipeline (`masking.mask_spark`) has the same API as the pandas and Polars pipelines:

```python
from masking.mask_spark.operations.operation_hash import HashOperation
from masking.mask_spark.pipeline import MaskDataFramePipeline

pipeline = MaskDataFramePipeline({"name": {"masking_operation": HashOperation(col_name="name")}})
masked = pipeline(spark_df)
```

## How it works

For every configured column the pipeline:

1. selects the serving columns, drops nulls and keeps the **distinct** values;
1. masks the distinct values with `mapInPandas` (Arrow), or with native Spark functions when possible (see [hash](operations/operation_hash.md));
1. collects the concordance table `{clear_value: masked_value}` to the driver through Arrow;
1. for operations requiring unique values (`fake_*`), masks again on the driver the values that collide across Spark partitions;
1. replaces the values in the full dataframe with a **broadcast join** against the concordance table. A python UDF is only used for column types whose Spark string representation differs from python's (e.g. booleans, doubles, timestamps).

## Spark configuration

No configuration is required. Settings that can help:

| Setting                                        | Recommendation                                                                                                                                                    |
| ---------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `spark.sql.adaptive.enabled`                   | Keep enabled (default). It coalesces the shuffle partitions created by `distinct()`, otherwise one python task is started per partition.                          |
| `spark.sql.shuffle.partitions`                 | Size it to the data. With small data a low value (e.g. the number of cores) avoids scheduling overhead.                                                           |
| `spark.sql.execution.arrow.maxRecordsPerBatch` | Default (10000) is fine for speed. Lower it (e.g. 1000) for memory-heavy operations such as Presidio.                                                             |
| `spark.sql.execution.pythonUDF.arrow.enabled`  | Not needed by the library. Note that Arrow UDFs convert integer batches containing nulls to floats, so the library explicitly uses plain UDFs where this matters. |

## Running Spark locally

- Install Java 17 or later and set `JAVA_HOME`, e.g. `export JAVA_HOME=/opt/homebrew/opt/openjdk@17`.
- Make sure the Spark workers use the project environment: `export PYSPARK_PYTHON=$PWD/.venv/bin/python`.
- If Spark binds to a VPN interface and fails with `Failed to send RPC` errors, use `export SPARK_LOCAL_IP=127.0.0.1`.

## Benchmark

[`scripts/benchmark_spark.py`](../scripts/benchmark_spark.py) times each operation on synthetic data, split into the build of the concordance table and the substitution:

```sh
python scripts/benchmark_spark.py --rows 2000 --distinct 1000 --repeat 2
python scripts/benchmark_spark.py --rows 1000000 --distinct 200000 --ops hash_secret fake_name \
    --conf spark.sql.shuffle.partitions=8 --output results.json
```

See [spark_performance_plan.md](spark_performance_plan.md) for the optimizations and their measured effect.
