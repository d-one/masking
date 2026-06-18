from masking.base_operations.operation_fake_plz import FakePLZBase
from masking.mask_polars.operations.operation import PolarsOperation


class FakePLZ(FakePLZBase, PolarsOperation):
    """Mask a column with fake PLZ data."""
