from masking.mask.operations.operation_presidio_dict import (
    MaskDictOperation as MaskDictOperationPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class MaskDictOperation(SparkOperation, MaskDictOperationPandas):
    """Hashes a column using entity detection algorithm."""
