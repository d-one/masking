from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from masking.mask.operations.operation import PandasOperation


class StringMatchDictOperation(PandasOperation, StringMatchDictOperationBase):
    """String matching operation."""
