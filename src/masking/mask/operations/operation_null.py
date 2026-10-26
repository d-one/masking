from masking.base_operations.operation_null import NullOperationBase
from masking.mask.operations.operation import PandasOperation


class NullOperation(NullOperationBase, PandasOperation):
    """Replaces values of a column with None."""
