import hashlib
from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StringType

from masking.mask.operations.operation_hash import HashOperation as HashOperationPandas
from masking.mask_spark.operations.operation import SparkOperation

# Spark functions equivalent to hashlib's hexdigest of the UTF-8 encoded string.
# Matched by identity with the hashlib constructors: custom callables (e.g. wrapping
# a constructor to add a pepper, possibly unhashable) must keep using python.
_NATIVE_HASHES: dict[Callable, Callable[[Column], Column]] = {
    hashlib.md5: F.md5,
    hashlib.sha1: F.sha1,
    hashlib.sha224: lambda c: F.sha2(c, 224),
    hashlib.sha256: lambda c: F.sha2(c, 256),
    hashlib.sha384: lambda c: F.sha2(c, 384),
    hashlib.sha512: lambda c: F.sha2(c, 512),
}


class HashOperation(SparkOperation, HashOperationPandas):
    """Masks a column using hash algorithm."""

    def _native_mask_expression(self, data: DataFrame) -> Column | None:
        """Hash with Spark functions when no secret and no concordance table are used.

        Only string columns are supported: python's str() of other types can differ
        from Spark's cast to string.

        Args:
        ----
            data (DataFrame): input dataframe

        Returns:
        -------
            Column | None: hashed column expression, None to hash with python

        """
        if self.secret is not None or self.concordance_table:
            return None

        if not isinstance(data.schema[self.col_name].dataType, StringType):
            return None

        native = next(
            (f for ctor, f in _NATIVE_HASHES.items() if ctor is self.hash_function),
            None,
        )
        return None if native is None else native(F.col(self.col_name))
