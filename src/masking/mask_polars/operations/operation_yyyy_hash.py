from masking.base_operations.operation_yyyy_hash import YYYYHashOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class YYYYHashOperation(YYYYHashOperationBase, PolarsOperation):
    """Hashes a datetime column using SHA256 algorithm.

    First extracts the year of the date of birth and then hashes the entire date of birth.
    Example: 1990-01-01 -> 1990_hash(1990-01-01).
    """
