from masking.mask.operations.operation_string_match import (
    StringMatchDictOperation as StringMatchDictOperationPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class StringMatchDictOperation(SparkOperation, StringMatchDictOperationPandas):
    """String matching operation."""
