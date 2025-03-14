from masking.mask.operations.operation_presidio import (
    HashPresidio as HashPresidioPandas,
)
from masking.mask_spark.operations.operation import SparkOperation


class HashPresidio(SparkOperation, HashPresidioPandas):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""
