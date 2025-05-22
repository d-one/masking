from masking.base_operations.operation_fake_name import FakeNameBase
from masking.mask.operations.operation import PandasOperation


class FakeNameOperation(FakeNameBase, PandasOperation):
    """Mask a column with fake PLZ data."""
