from masking.base_operations.operation_yyyy_hash import YYYYHashOperationBase
from masking.mask.operations.operation import PandasOperation


class YYYYHashOperation(YYYYHashOperationBase, PandasOperation):
    """Hashes a datetime column using SHA256 algorithm.

    First extracts the year of the date of birth and then hashee the entire date of birth.
    Example: 1990-01-01 -> 1990_hash(1990-01-01).
    """
