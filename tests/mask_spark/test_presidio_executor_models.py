"""Presidio models given by name are loaded on Spark executors, once per process."""

import os
from collections import defaultdict
from pathlib import Path

import pandas as pd
import spacy
from masking.mask.operations.operation_presidio import MaskPresidio as MaskPresidioPd
from masking.mask_spark.operations.operation_presidio import MaskPresidio
from masking.utils import entity_detection
from masking.utils.entity_detection import (
    MultilingualSpacyNlpEngine,
    PresidioMultilingualAnalyzer,
)
from pyspark.sql import SparkSession

from .conftest import normalize

EMAIL = "john.doe@example.com"


def _model_path(tmp_path: Path) -> str:
    # A blank pipeline saved to disk loads by path like an installed model
    path = tmp_path / "blank_en"
    spacy.blank("en").to_disk(path)
    return str(path)


def test_executors_load_named_models_once_per_process(
    spark: SparkSession, tmp_path: Path
) -> None:
    path = _model_path(tmp_path)
    engine = MultilingualSpacyNlpEngine(models={"en": path})

    def probe(_: object) -> object:
        model = engine.nlp["en"]
        yield os.getpid(), id(model), model is entity_detection._UNPICKLED_MODELS[path]

    tasks = 16
    results = spark.sparkContext.parallelize(range(tasks), tasks).mapPartitions(probe)
    results = results.collect()

    assert len(results) == tasks
    assert all(cached for _, _, cached in results)
    models_by_process = defaultdict(set)
    for pid, model_id, _ in results:
        models_by_process[pid].add(model_id)
    assert all(len(ids) == 1 for ids in models_by_process.values())
    # Several tasks share a process, so the cache is exercised
    assert len(models_by_process) < tasks


def test_named_models_mask_like_pandas(spark: SparkSession, tmp_path: Path) -> None:
    models = {"en": _model_path(tmp_path)}
    lines = [f"Mail {EMAIL}", "hi", None]

    pandas_op = MaskPresidioPd(
        col_name="t", analyzer=PresidioMultilingualAnalyzer(models=models).analyzer
    )
    expected = [normalize(v) for v in pandas_op(pd.DataFrame({"t": lines}))]

    spark_op = MaskPresidio(
        col_name="t", analyzer=PresidioMultilingualAnalyzer(models=models).analyzer
    )
    data = spark.createDataFrame(list(enumerate(lines)), "id int, t string")
    rows = spark_op(data).orderBy("id").collect()

    assert [normalize(row["t"]) for row in rows] == expected
    assert expected == ["Mail <EMAIL_ADDRESS>", "hi", None]
