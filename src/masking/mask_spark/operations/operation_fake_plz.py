from masking.mask.operations.operation_fake_plz import FakePLZ as FakePLZPandas
from masking.mask_spark.operations.operation import SparkOperation


class FakePLZ(SparkOperation, FakePLZPandas):
    """Masks a column using a fake date generator."""
