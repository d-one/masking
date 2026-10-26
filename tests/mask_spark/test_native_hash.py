import hashlib
from dataclasses import dataclass

import pytest
from masking.mask_spark.operations.operation_hash import HashOperation
from pyspark.sql import SparkSession

VALUES = ["abc", "", "Müller Jörg", "日本語", " spaces ", None]


@pytest.mark.parametrize(
    "algorithm", ["md5", "sha1", "sha224", "sha256", "sha384", "sha512"]
)
def test_native_hash_matches_hashlib(spark: SparkSession, algorithm: str) -> None:
    data = spark.createDataFrame([(v,) for v in VALUES], "c string")
    op = HashOperation(col_name="c", hash_function=getattr(hashlib, algorithm))

    assert op._native_mask_expression(data) is not None
    result = [row.c for row in op(data).collect()]
    expected = [
        None if v is None else getattr(hashlib, algorithm)(v.encode()).hexdigest()
        for v in VALUES
    ]
    assert result == expected


@pytest.mark.parametrize(
    ("kwargs", "schema", "values"),
    [
        ({"secret": "s"}, "c string", ["abc"]),
        ({"concordance_table": {"abc": "X"}}, "c string", ["abc", "def"]),
        ({"hash_function": hashlib.sha3_256}, "c string", ["abc"]),
        (
            {"hash_function": lambda data=b"": hashlib.sha256(b"pepper" + data)},
            "c string",
            ["abc"],
        ),
        ({}, "c int", [1, 2]),
    ],
    ids=[
        "secret",
        "concordance_table",
        "unsupported_algorithm",
        "custom_callable",
        "int_column",
    ],
)
def test_python_fallback(
    spark: SparkSession, kwargs: dict, schema: str, values: list
) -> None:
    data = spark.createDataFrame([(v,) for v in values], schema)
    op = HashOperation(col_name="c", **kwargs)
    op._cast_concordance_table()

    assert op._native_mask_expression(data) is None
    result = [row.c for row in op(data).collect()]
    assert result == [op._check_mask_line(v) for v in values]


def test_unhashable_hash_function_falls_back(spark: SparkSession) -> None:
    # Defined locally so that Spark workers receive the class by value
    @dataclass
    class PepperedSha256:
        pepper: bytes = b"pepper"

        def __call__(self, data: bytes = b""):
            return hashlib.sha256(self.pepper + data)

    data = spark.createDataFrame([("abc",)], "c string")
    op = HashOperation(col_name="c", hash_function=PepperedSha256())

    assert op._native_mask_expression(data) is None
    assert [row.c for row in op(data).collect()] == [op._check_mask_line("abc")]
