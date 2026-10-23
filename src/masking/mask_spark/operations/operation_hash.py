from collections.abc import Callable

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F  # noqa: N812
from pyspark.sql.types import StringType

from masking.mask.operations.operation_hash import HashOperation as HashOperationPandas
from masking.mask_spark.operations.operation import SparkOperation

# Spark functions equivalent to hashlib's hexdigest of the UTF-8 encoded string
_NATIVE_HASHES: dict[str, Callable[[Column], Column]] = {
    "md5": F.md5,
    "sha1": F.sha1,
    "sha224": lambda c: F.sha2(c, 224),
    "sha256": lambda c: F.sha2(c, 256),
    "sha384": lambda c: F.sha2(c, 384),
    "sha512": lambda c: F.sha2(c, 512),
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

        try:
            name = self.hash_function().name
        except Exception:
            return None

        native = _NATIVE_HASHES.get(name)
        return None if native is None else native(F.col(self.col_name))
