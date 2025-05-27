from masking.mask.operations.operation_string_match import (
    StringMatchOperation as StringMatchPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class StringMatchOperation(SparkOperation, StringMatchPandas):
    """Masks a column using a fake date generator."""
