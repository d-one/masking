from masking.mask.operations.operation_dict import (
    HashDictOperation as HashDictOperationPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class HashDictOperation(SparkOperation, HashDictOperationPandas):
    """Hashes a column using entity detection algorithm."""
