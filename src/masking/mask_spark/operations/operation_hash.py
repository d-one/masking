from masking.mask.operations.operation_hash import HashOperation as HashOperationPandas
from masking.mask_spark.operations.operation import SparkOperation


class HashOperation(SparkOperation, HashOperationPandas):
    """Masks a column using hash algorithm."""
