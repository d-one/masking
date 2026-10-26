from masking.base_operations.operation_null import NullOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class NullOperation(NullOperationBase, PolarsOperation):
    """Replaces values of a column with None."""
