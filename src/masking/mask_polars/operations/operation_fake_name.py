from masking.base_operations.operation_fake_name import FakeNameBase
from masking.mask_polars.operations.operation import PolarsOperation


class FakeNameOperation(FakeNameBase, PolarsOperation):
    """Mask a column with fake name data."""
