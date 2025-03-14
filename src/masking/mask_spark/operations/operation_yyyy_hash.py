from masking.mask.operations.operation_yyyy_hash import (
    YYYYHashOperation as YYYYHashOperationPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class YYYYHashOperation(SparkOperation, YYYYHashOperationPandas):
    """Hash a datetime column using hash algorithm.

    First extracts the year of the date of birth and then hashee the entire date of birth.
    Example: 1990-01-01 -> 1990_hash(1990-01-01).
    """
