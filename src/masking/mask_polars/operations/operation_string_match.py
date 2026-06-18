from masking.base_operations.operation_string_match import StringMatchOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class StringMatchOperation(PolarsOperation, StringMatchOperationBase):
    """String matching operation."""
