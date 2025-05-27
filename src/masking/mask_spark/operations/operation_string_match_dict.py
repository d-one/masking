from masking.mask.operations.operation_string_match_dict import (
    StringMatchDictOperation as StringMatchDictPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class StringMatchDictOperation(SparkOperation, StringMatchDictPandas):
    """Masks a column using a fake date generator."""
