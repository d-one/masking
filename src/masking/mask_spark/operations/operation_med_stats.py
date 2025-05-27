from masking.mask.operations.operation_med_stats import (
    MedStatsOperation as MedStatsPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class MedStatsOperation(SparkOperation, MedStatsPandas):
    """Masks a column using a fake date generator."""
