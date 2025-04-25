from masking.base_operations.operation_fake_date import FakeDateBase
from masking.mask.operations.operation import PandasOperation


class FakeDate(FakeDateBase, PandasOperation):
    """Masks a column using a fake date generator."""
