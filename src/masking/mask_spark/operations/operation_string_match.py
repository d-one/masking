from masking.mask.operations.operation_string_match import (
    StringMatchOperation as StringMatchOperationPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class StringMatchOperation(SparkOperation, StringMatchOperationPandas):
    """String matching operation."""
