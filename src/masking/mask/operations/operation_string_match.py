from masking.base_operations.operation_string_match import StringMatchOperationBase
from masking.mask.operations.operation import PandasOperation


class StringMatchOperation(PandasOperation, StringMatchOperationBase):
    """String matching operation."""
