from masking.base_operations.operation_hash import HashOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class HashOperation(HashOperationBase, PolarsOperation):
    """Hash a column using SHA256 algorithm."""
