from masking.mask.operations.operation_presidio import (
    MaskPresidio as MaskPresidioPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class MaskPresidio(SparkOperation, MaskPresidioPandas):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""
