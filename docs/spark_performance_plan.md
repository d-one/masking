# Spark performance improvement plan

Plan for speeding up the Spark masking pipeline (and, where the code is shared, the pandas and Polars pipelines).

## Status and results

All phases are implemented. Measurements on a laptop (`local[8]`), see [spark.md](spark.md#benchmark) for the benchmark script.

**End to end, 2000 rows / 1000 distinct values, default Spark configuration (total seconds per operation):**

| Operation     | Before | After |
| ------------- | -----: | ----: |
| hash_secret   |   3.45 |  0.51 |
| hash_nosecret |   3.59 |  0.44 |
| yyyy_hash     |   3.84 |  0.45 |
| fake_name     |   4.84 |  0.52 |
| fake_date     |   3.86 |  0.43 |
| string_match  |   6.30 |  1.24 |

**Per phase:**

| Phase | Change                                     | Measured effect                                                                                     |
| ----- | ------------------------------------------ | --------------------------------------------------------------------------------------------------- |
| 1     | Set-based uniqueness check                 | 50k unique values: 14.44s → 0.06s                                                                   |
| 1b    | Fake-name sampling, `dateparser` fast path | pandas op, 10k values: fake_name 77.1s → 1.35s, fake_date 13.7s → 1.56s, string_match 52.6s → 5.05s |
| 1b    | Fake dates without timezone round trips    | Spark fake_date batch (~600 rows): 1.6s → 0.017s                                                    |
| 2     | No row-wise `apply(axis=1)`                | pandas hash, 20k rows: 2.69s → 0.075s                                                               |
| 3a    | `useArrow=True` substitution UDF           | 1M rows: 0.44s → 0.40s (superseded by 3b)                                                           |
| 3b    | Broadcast-join substitution                | 1M rows: 1k table 0.36s → 0.24s, 1M table 3.84s → 1.58s (200k table: 0.64s → 0.76s in local mode)   |
| 4     | No `cache()`, Arrow collection             | build with default config: 4.0s → 0.37s; collecting 200k values: 1.76s → 1.49s                      |
| 5     | Native hashing                             | hash without secret, 1M rows / 200k distinct: build 1.02s → 0.84s                                   |
| 6     | Unique fake values across partitions       | correctness fix, no measurable overhead                                                             |
| 7     | Configuration and docs                     | `maxRecordsPerBatch` 10000 vs 50000: no measurable difference                                       |

**Deviations from the plan:**

- Phase 1b was added after profiling: per-row costs in `FakeNameProvider` (`DataFrame.sample` per call, also capping the output to 1001 distinct names), `dateparser.parse` and Faker's timezone conversions dominated before the uniqueness check did. Timezone conversions are especially slow in forked processes on macOS (PySpark workers).
- Phase 2 left Polars unchanged: `iter_rows` was already 12–45× faster than pandas' `apply(axis=1)`.
- Phase 3a's Arrow UDF converts integer batches containing nulls to floats (`'2'` → `'2.0'`). The UDF fallback of Phase 3b therefore uses a plain UDF.
- Phase 4: the `cache()` in `_build_concordance_table` prevented AQE from coalescing the 200 shuffle partitions after `distinct()`, which was the largest fixed cost for small and medium data.
- Phase 7: the library does not set any Spark configuration; [spark.md](spark.md) documents the settings that matter.

## Context

### Current execution flow (Spark)

1. [`MaskDataFramePipeline.__call__`](../src/masking/base_pipeline/pipeline.py) → `_run_pipelines` runs one job per column (optionally in a `ThreadPoolExecutor`).
1. Per column, `_filter_data` selects the serving columns, drops nulls and runs `distinct()`.
1. [`ConcordanceTable._build_concordance_table`](../src/masking/mask_spark/pipeline.py) applies the masking operation, `cache()`s, filters changed values and collects via `.rdd.collectAsMap()` to a driver-side `dict`.
1. The masking operation ([`SparkOperation._mask_data`](../src/masking/mask_spark/operations/operation.py)) already uses **`mapInPandas` (Arrow)** and calls the pandas implementation ([`PandasOperation._mask_data`](../src/masking/mask/operations/operation.py)) per batch, which runs `DataFrame.apply(axis=1)` → `Operation._check_mask_line` for every row.
1. [`_substitute_masked_values`](../src/masking/mask_spark/pipeline.py) writes masked values back to the full dataframe with a **plain Python `udf`** that closes over the concordance `dict`.

### Baseline measurements

Local mode (`local[8]`), 2M rows, 200k distinct values, HMAC-SHA256 concordance table. Script: [`scripts/benchmark_spark.py`](../scripts/benchmark_spark.py) (added in Phase 0).

| Step             | Variant                                                 | Time                                                             |
| ---------------- | ------------------------------------------------------- | ---------------------------------------------------------------- |
| Substitute       | plain `udf` (current)                                   | 2.73s                                                            |
|                  | `udf(useArrow=True)`                                    | 2.00s                                                            |
|                  | `pandas_udf` + `Series.map(dict)`                       | 3.66s                                                            |
|                  | broadcast join (no Python)                              | 2.55s (incl. driver-side table build; scales better on clusters) |
| Mask (hash)      | `mapInPandas` + `apply(axis=1)` (current)               | 0.89s                                                            |
|                  | `mapInPandas` + list comprehension                      | 0.51s                                                            |
|                  | native `sha2` (no secret)                               | 0.59s                                                            |
| Uniqueness check | 2k lookups, `in dict.values()` on 50k entries (current) | 1.14s                                                            |
|                  | 2k lookups, `in set` on 50k entries                     | 0.0005s                                                          |

Key takeaway: the operations already use Arrow. The dominant costs are the Python per-row work and the slow uniqueness check that scans every value.

## Goals & non-goals

### Goals

- Reduce wall-clock time of `MaskDataFramePipeline` on Spark for all operations.
- Keep the masked output **identical** for deterministic operations (hash, yyyy_hash, string_match, presidio) and keep the invariants for random operations (fake\_\*): every changed value differs from its clear value and masked values are unique.
- Fix the cross-partition uniqueness problem for fake operations on Spark.

### Non-goals

- Changing the public API (`MaskDataFramePipeline(config, workers)`, operation constructors).
- Rewriting HMAC / Presidio / Faker logic. These stay in Python.
- Changing the concordance-table output format (`dict[str, str]`).

## Phase overview

| Phase | Topic                                                      | Engines affected      | Expected impact                   | Risk   |
| ----- | ---------------------------------------------------------- | --------------------- | --------------------------------- | ------ |
| 0     | Baseline, benchmark script, Spark test harness             | Spark                 | Required for all later phases     | Low    |
| 1     | Faster uniqueness check (set of masked values)             | all                   | Very high for fake ops            | Low    |
| 1b    | Per-row hotspots: fake-name sampling, `dateparser`         | all                   | Very high for fake/date ops       | Low    |
| 2     | Replace row-wise `apply(axis=1)` / `iter_rows`             | pandas, Spark, Polars | ~1.7× in masking step             | Low    |
| 3     | Substitution: `useArrow=True` → broadcast join             | Spark                 | 25%+, scales with table size      | Medium |
| 4     | Arrow-based concordance collection; drop useless `cache()` | Spark                 | Medium; fixes latent ordering bug | Low    |
| 5     | Native Spark functions for hash ops                        | Spark                 | High for hash without secret      | Medium |
| 6     | Cross-partition uniqueness for fake ops                    | Spark                 | Correctness                       | Medium |
| 7     | Spark config tuning + documentation                        | Spark                 | Low–medium                        | Low    |

Dependencies: 0 → everything. 1 → 6. 4 → 3 (reuse the helper). Phases 1, 2, 4, 5 and 7 are otherwise independent and can be separate PRs.

______________________________________________________________________

## Phase 0 — Baseline, benchmark, test harness

### Tasks

1. Add `scripts/benchmark_spark.py`, a cleaned-up version of the evaluation benchmark. It should:
   - parametrise rows / distinct values / partitions via `argparse`;
   - benchmark each pipeline stage separately (filter+distinct, mask, collect, substitute) and the full `MaskDataFramePipeline` for a hash, a fake and a string-match config;
   - use `df.write.format("noop")` to force execution without I/O.
1. Add a Spark test harness:
   - `tests/mask_spark/__init__.py`, `tests/mask_spark/conftest.py` with a session-scoped `spark` fixture: `master("local[2]")`, `spark.driver.host=127.0.0.1`, `spark.driver.bindAddress=127.0.0.1`, `spark.sql.shuffle.partitions=4`.
   - Document the required env vars in [installation.md](installation.md): `JAVA_HOME` pointing to JDK 17+, `PYSPARK_PYTHON` = venv python.
1. Add **equivalence tests** `tests/mask_spark/test_pipeline_equivalence.py`:
   - For deterministic ops (`HashOperation` with/without secret, `YYYYHashOperation`, `StringMatchOperation`), run the pandas pipeline and the Spark pipeline on the same small dataset (built from `data/MaskingData_en.csv` or a synthetic frame) and assert equal outputs (ordered by a row id).
   - For fake ops, assert the invariants: non-null in → non-null out, `masked != clear` for every changed value, `len(set(masked)) == len(set(clear))` (uniqueness).
   - Include null values, duplicates, non-string columns (int, date) and a multi-partition input (`repartition(4)`).
1. Record baseline numbers from `scripts/run_spark.py` and `scripts/benchmark_spark.py` in the PR description.

**Acceptance criteria**: `pytest tests/mask_spark` passes on `main` (except the fake-op uniqueness test, which is expected to fail until Phase 6; mark it `xfail(strict=True)`).

______________________________________________________________________

## Phase 1 — Faster uniqueness check

**Problem**: [`Operation._check_mask_line`](../src/masking/base_operations/operation.py) runs `masked not in self.concordance_table.values()`. That scans every value on each call, so the total cost grows with the square of the number of values. Fake ops (`_needs_unique_values=True`) also grow the table on every row, so this is the main cost for them. The same pattern is in [`MaskDataFramePipelineBase._run_pipeline`](../src/masking/base_pipeline/pipeline.py) (`k not in output_concordance_table.values()`).

### Tasks

1. In `Operation`, add a private `self._masked_values: set[str]` that mirrors `self.concordance_table.values()`:
   - Rebuild it at the end of `_cast_concordance_table()` (`self._masked_values = set(self.concordance_table.values())`). This covers `__init__`, `__call__` and `update_concordance_table` (which calls `_cast_concordance_table` first).
   - In `update_concordance_table`, after `self.concordance_table.update(...)`, also `self._masked_values.update(concordance_table.values())`.
   - In `_check_mask_line`, replace the check with `masked not in self._masked_values`, and when `_needs_unique_values` add `self._masked_values.add(masked)` next to `self.concordance_table[line] = masked`.
   - Keep `concordance_table` as the source of truth. The set is a derived cache that is rebuilt whenever the table may have been replaced from outside.
1. In `_run_pipeline`, compute `existing_masked = set(output_concordance_table.values())` once per loop iteration and use it in the dict comprehension.
1. Unit tests in `tests/base_operations/`:
   - the set is in sync after `__init__`, `update_concordance_table`, `__call__`;
   - a fake-like stub op that returns colliding values triggers retries and still yields unique outputs;
   - performance check: 50k unique values in under 1s for a trivial `_mask_line`. Mark it `@pytest.mark.slow` if a marker is introduced.

**Acceptance criteria**: all existing tests pass; Phase 0 equivalence tests unchanged; fake-op benchmark improves by orders of magnitude for ≥50k distinct values.

**Result**: 50k unique values: 14.44s → 0.06s. `FakeNameBase._SEEN_INPUTS` also switched to an O(1) `RandomSet` (it used `random.choice(list(set))`).

______________________________________________________________________

## Phase 1b — Per-row hotspots (found by profiling)

Profiling the per-partition code showed costs that dominate before the uniqueness check matters:

- `FakeNameProvider` called `DataFrame.sample(n=1, random_state=randint(0, 1000))` for every name (~7 ms/call). It also capped the output to 1001 distinct last/first names.
- `dateparser.parse` (used by `FakeDate`, `YYYYHashOperation`, `StringMatchOperation`) costs 1–6 ms/call because it tries every locale.

### Tasks

1. `FakeNameProvider`: pre-compute pandas arrays of names once and pick with `generator.random.randrange`. Keep pandas arrays (not lists) because they unpickle ~25× faster, which happens for every Spark task. Exclude the source DataFrames from pickling and reload them lazily.
1. `masking.utils.date_parsing.parse_date`: ISO strings are parsed with `datetime.fromisoformat` (verified identical to `dateparser`), everything else goes through an LRU-cached `dateparser.parse`.

**Result** (pandas op, 10k distinct values): fake_name 77.1s → 1.35s, fake_date 13.7s → 1.56s, string_match with repeating names 52.6s → 5.05s.

______________________________________________________________________

## Phase 2 — Replace row-wise `apply(axis=1)` / `iter_rows`

**Problem**: `DataFrame.apply(axis=1)` builds a `pd.Series` per row and calls `row.drop(...).to_dict()`. This runs inside every Spark `mapInPandas` batch.

### Tasks

1. [`PandasOperation._mask_data`](../src/masking/mask/operations/operation.py), DataFrame branch:

   ```python
   extra_cols = [c for c in self.serving_columns if c != self.col_name]
   values = data[self.col_name].tolist()
   if not extra_cols:
       out = [self._check_mask_line(v) for v in values]
   else:
       extras = [data[c].tolist() for c in extra_cols]
       out = [
           self._check_mask_line(line=v, additional_values=dict(zip(extra_cols, ex)))
           for v, *ex in zip(values, *extras)
       ]
   return pd.Series(out, index=data.index, dtype="object")
   ```

   - Preserve null semantics: `_check_mask_line(None)` already returns `None`. Note that `tolist()` turns `NaN` in object columns into `float('nan')`, not `None`. Normalise with `data[col].astype(object).where(data[col].notna(), None)` before `tolist()`, matching what `apply` passed before.
   - Series branch: `pd.Series([self._check_mask_line(v) for v in ...], index=data.index)` instead of `.map` (minor gain, keeps the code consistent).

1. [`StringMatchDictOperation._mask_data`](../src/masking/mask/operations/operation_string_match_dict.py) (pandas): same transformation, passing `leaf_to_mask` / `leaf_to_deny` from `paths`.

1. Polars ([`PolarsOperation._mask_data`](../src/masking/mask_polars/operations/operation.py), [`operation_string_match_dict.py`](../src/masking/mask_polars/operations/operation_string_match_dict.py)): replace `iter_rows(named=True)` with `zip` over `data[c].to_list()` columns. Replace `map_elements` with a list comprehension + `pl.Series(..., dtype=pl.Utf8)`, which avoids Polars' per-element warning and overhead.

1. Presidio ops already use `Series.map/apply` with a single column. Convert to list comprehensions only if the benchmark shows a gain, since NLP time dominates there.

**Acceptance criteria**: unit + equivalence tests pass; hash masking step ≥1.5× faster in `benchmark_spark.py`.

______________________________________________________________________

## Phase 3 — Substitution step (Spark)

**Problem**: [`_substitute_masked_values`](../src/masking/mask_spark/pipeline.py) uses a plain `udf` that pickles the whole concordance dict into the task closure. That is slow (row-by-row Python serialization) and doesn't scale: a large table bloats every task and can hit size limits.

### Step 3a — quick win (small PR)

- Add `useArrow=True` to the existing `udf(...)` call. Keep the `when(isNotNull)` guard and the `str(x)` fallback semantics.
- Expected: ~25% faster substitution.

### Step 3b — broadcast join (no Python workers)

1. Build a Spark lookup DataFrame per column from the dict using the helper from Phase 4 (`dict_to_spark_df(spark, table, key_type)`):

   - schema `clear_values: <same type as data[c_name]>`, `masked_values: string`, so the join keys match without casts. Build from `pyarrow.Table` / pandas with Arrow enabled.

1. Replace the udf with:

   ```python
   ct = broadcast(lookup).withColumnRenamed("clear_values", key_alias)
   data = (
       data.join(ct, data[c_name].eqNullSafe(ct[key_alias]), "left")
           .withColumn(new_col_name, when(col(c_name).isNotNull(),
                       coalesce(col("masked_values"), col(c_name).cast("string"))))
           .drop(c_name, key_alias, "masked_values")
           .withColumnRenamed(new_col_name, c_name)
   )
   ```

   - Use unique temp column names (`generate_unique_name`) to avoid clashes with user columns.
   - `coalesce(..., cast("string"))` reproduces the current `tab.get(x, str(x))`. Verify that `str(x)` (Python) and `cast("string")` (Spark) match for the supported column types (string, int, date, timestamp, double). Add test cases. If they differ (e.g. timestamps or doubles), keep the original value via the cast and document the change. Values not in the table are unchanged values by construction, so in practice only types matter.

1. **Threshold**: Spark's auto-broadcast limit is 10 MB by default, but the explicit `broadcast()` hint overrides it. For very large tables (> ~1–2 GB serialized), switch to a regular shuffle join. Add a module constant `BROADCAST_MAX_BYTES` (256 MB of Arrow memory; a row count does not bound the size of free-text values) and fall back to a non-hinted join above it.

1. Row order: a join does not guarantee order. Ordering is not guaranteed for Spark DataFrames anyway. Note it in the docstring and make the equivalence tests order-independent (join on row id).

1. `_run_pipeline` (list-of-operations case) also calls `_substitute_masked_values` on the filtered column data, so the same code path benefits automatically.

**Acceptance criteria**: equivalence tests pass; substitution stage faster than 3a on ≥1M rows; no Python worker activity in the substitution stage (check `explain()` for absence of `BatchEvalPython` / `ArrowEvalPython`).

______________________________________________________________________

## Phase 4 — Arrow-based concordance collection

### Problems

- [`ConcordanceTable._build_concordance_table`](../src/masking/mask_spark/pipeline.py) collects via `.rdd.collectAsMap()`, which pickles row by row. It also calls `.cache()` on a dataframe that is only read once (pure overhead).
- [`Operation.cast_concordance_table`](../src/masking/base_operations/operation.py) and [`ConcordanceTableBase._cast_concordance_table`](../src/masking/base_concordance_table/concordance_table.py) collect `clear_values` and `masked_values` in **two separate jobs** and `zip` them. Spark does not guarantee the two jobs return rows in the same order, so this is a latent correctness bug as well as a performance issue.

### Tasks

1. New module `src/masking/utils/spark_utils.py`:
   - `spark_df_to_dict(df, key="clear_values", value="masked_values") -> dict`: `tbl = df.select(key, value).toArrow()` (PySpark ≥ 4.0), then `dict(zip(tbl.column(key).to_pylist(), tbl.column(value).to_pylist()))`.
   - `dict_to_spark_df(spark, mapping, key_field: StructField) -> DataFrame` (used by Phase 3b).
1. Use `spark_df_to_dict` in:
   - `ConcordanceTable._build_concordance_table` (after the filter);
   - `Operation.cast_concordance_table` (Spark branch);
   - `ConcordanceTableBase._cast_concordance_table` (Spark branch). Consider making it delegate to `Operation.cast_concordance_table` to remove the duplication.
1. Remove `.cache()` / `unpersist()` in `_build_concordance_table`. Keep the `try/except` error message.
1. Make sure importing `spark_utils` does not require `pyspark` for pandas/Polars users if it is made optional later (lazy import inside functions).
1. Tests: existing `tests/base_operations` concordance-table tests (they already include Spark DataFrames) must pass. Add a test with a multi-partition Spark concordance DataFrame to cover the ordering bug.

**Acceptance criteria**: tests pass; collection time for 1M-row table measurably lower than `collectAsMap`.

______________________________________________________________________

## Phase 5 — Native Spark functions for hash operations

**Idea**: when no Python logic is needed, skip `mapInPandas` entirely.

### Scope

- `HashOperation` with `secret is None` and `hash_function` in `{sha224, sha256, sha384, sha512}` → `F.sha2(col, bits)`; `md5` → `F.md5`; `sha1` → `F.sha1`.
- Only for `StringType` input columns. Other types fall back to Python, because Python `str(x)` and Spark `cast("string")` can differ (e.g. doubles, timestamps).
- Only when the operation's concordance table is empty. Otherwise the pre-seeded mappings and the uniqueness check in `_check_mask_line` must apply → fall back.
- **Out of scope**: HMAC (`secret` set), because Spark has no HMAC function. `YYYYHashOperation`, because `dateparser.parse` accepts far more formats than Spark's `to_date`.

### Tasks

1. In `SparkOperation`, add a hook `def _native_expression(self, data: DataFrame) -> Column | None` returning `None` by default.
1. In `SparkOperation._mask_data`, if `expr := self._native_expression(data)` is not `None`, return `data.withColumn(self.col_name, expr)`. Otherwise use the existing `mapInPandas` path.
1. Implement `_native_expression` in [`mask_spark/operations/operation_hash.py`](../src/masking/mask_spark/operations/operation_hash.py) with the conditions above. Map hash functions by `hash_function().name` (e.g. `"sha256"`) rather than identity, so `hashlib.sha256` and `hashlib.new("sha256")` behave the same.
1. Tests: for every supported algorithm, native output == Python output on a string column including unicode and empty strings; non-string / seeded-table / secret cases fall back (assert via `explain()` or a spy).

**Acceptance criteria**: equivalence tests pass; hash-without-secret masking stage runs without Python workers.

______________________________________________________________________

## Phase 6 — Cross-partition uniqueness for fake operations (correctness)

**Problem**: on Spark, `mapInPandas` ships a **copy** of the operation to each Python worker. Updates to `concordance_table` / `_masked_values` stay local to that worker, so two different clear values in different partitions can get the same fake value. This breaks the `_needs_unique_values` guarantee that pandas/Polars provide.

**Approach**: resolve collisions on the driver after collection. This is cheap because collisions are rare and the table is already on the driver.

### Tasks

1. In `ConcordanceTable._build_concordance_table` (Spark), after `spark_df_to_dict`, if `self.masking_operation._needs_unique_values`:
   - find masked values that appear for more than one clear value, and masked values that collide with values already in the pre-existing concordance table;
   - keep the first occurrence of each, and for the others call `self.masking_operation._check_mask_line(clear_value)` on the driver. The operation's `_masked_values` set must be seeded with all the accepted masked values first, so retries avoid every existing value.
   - for ops with extra serving columns (`additional_values`), the collected map does not contain them. Either collect the serving columns alongside (`select("clear_values", "masked_values", *extra)`) or restrict driver-side re-masking to `additional_values=None` and document it. Prefer collecting them.
1. Alternative considered: `repartition(1)` before masking fake columns. Rejected because it removes all parallelism.
1. Remove the `xfail` from the Phase 0 uniqueness test.

**Acceptance criteria**: uniqueness invariant holds on multi-partition inputs for all fake ops (`FakeNameOperation`, `FakeDate`, `FakePLZ`, `MedStatsOperation` if applicable).

______________________________________________________________________

## Phase 7 — Spark configuration & documentation

### Tasks

1. Recommended session configs (documented, **not** forced by the library):
   - `spark.sql.execution.arrow.pyspark.enabled=true`: Arrow for `toPandas` / `createDataFrame(pandas)`.
   - `spark.sql.execution.pythonUDF.arrow.enabled=true`: Arrow-optimised plain UDFs (safety net for any remaining `udf`).
   - `spark.sql.execution.arrow.maxRecordsPerBatch`: default 10000. Larger (e.g. 50000) for cheap ops like hash and fake; smaller (e.g. 1000) for Presidio to bound memory per batch.
   - `spark.sql.shuffle.partitions`: size to the data, since `distinct()` in `_filter_data` shuffles.
1. Apply these in [`scripts/run_spark.py`](../scripts/run_spark.py) and `scripts/benchmark_spark.py`.
1. Update docs:
   - [installation.md](installation.md): Java 17+, `JAVA_HOME`, `PYSPARK_PYTHON`, `SPARK_LOCAL_IP=127.0.0.1` for local runs behind VPN.
   - [operations.md](operations/operations.md) / [operation_hash.md](operations/operation_hash.md): mention the native fast path and its conditions.
   - README: short "Performance on Spark" section linking to this plan's outcomes.

______________________________________________________________________

## Validation strategy (all phases)

- **Correctness**: `pytest tests` (existing) + `pytest tests/mask_spark` (new equivalence/invariant tests) on every PR.
- **Performance**: run `scripts/benchmark_spark.py` before/after each phase on the same machine and settings. Report per-stage and total timings in the PR. Target sizes: 200k, 2M, 10M rows.
- **Plan inspection**: `df.explain()` checks for the absence of `BatchEvalPython` / `ArrowEvalPython` / `MapInPandas` where native paths are expected.
- **Lint**: `ruff check` / `ruff format` per repo config.

## Risks & mitigations

| Risk                                                                              | Mitigation                                                                             |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------- |
| `str(x)` vs Spark `cast("string")` mismatch for non-string columns (Phases 3b, 5) | Native/join paths limited to or tested per type; fall back to Python where they differ |
| Broadcasting a very large concordance table OOMs executors                        | `BROADCAST_MAX_BYTES` threshold with shuffle-join fallback                             |
| `_masked_values` set out of sync if `concordance_table` is reassigned externally  | Rebuilt in `_cast_concordance_table`, which runs at the start of every `__call__`      |
| `toArrow()` requires PySpark ≥ 4.0                                                | Already pinned (`pyspark==4.0.0`); fall back to `toPandas()` if the pin is relaxed     |
| Row order changes after join                                                      | Documented; tests compare by row id                                                    |
| NaN vs None handling changes in Phase 2                                           | Explicit normalisation + tests with nulls                                              |

## Suggested PR sequence

1. Phase 0 (benchmark + harness + tests)
1. Phase 1 (set lookup) and Phase 3a (`useArrow=True`), both small
1. Phase 2 (row loops)
1. Phase 4 (Arrow collection, helper module)
1. Phase 3b (broadcast join)
1. Phase 6 (cross-partition uniqueness)
1. Phase 5 (native hash)
1. Phase 7 (config + docs)
