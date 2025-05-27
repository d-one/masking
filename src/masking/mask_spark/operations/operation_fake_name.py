from masking.mask.operations.operation_fake_name import (
    FakeNameOperation as FakeNamePandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class FakeNameOperation(SparkOperation, FakeNamePandas):
    """Masks a column using a fake date generator."""
