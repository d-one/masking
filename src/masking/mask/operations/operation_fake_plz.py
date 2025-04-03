from masking.base_operations.operation_fake_plz import FakePLZBase
from masking.mask.operations.operation import PandasOperation


class FakePLZ(FakePLZBase, PandasOperation):
    """Mask a column with fake PLZ data."""
