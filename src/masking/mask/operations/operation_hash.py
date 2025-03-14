from masking.base_operations.operation_hash import HashOperationBase
from masking.mask.operations.operation import PandasOperation


class HashOperation(HashOperationBase, PandasOperation):
    """Hashes a column using SHA256 algorithm."""
