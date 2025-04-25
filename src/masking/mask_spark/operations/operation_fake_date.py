from masking.mask.operations.operation_fake_date import FakeDate as FakeDatePandas
from masking.mask_spark.operations.operation import SparkOperation


class FakeDate(SparkOperation, FakeDatePandas):
    """Masks a column using a fake date generator."""
