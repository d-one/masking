from masking.mask.operations.operation_null import NullOperation as NullPandas
from masking.mask_spark.operations.operation import SparkOperation


class NullOperation(SparkOperation, NullPandas):
    """Replaces values of a column with None."""
