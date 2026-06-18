from masking.base_operations.operation_fake_date import FakeDateBase
from masking.mask_polars.operations.operation import PolarsOperation


class FakeDate(FakeDateBase, PolarsOperation):
    """Masks a column using a fake date generator."""
