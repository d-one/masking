from collections.abc import Iterator

import pandas as pd
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import IntegralType, StringType, StructField, StructType

from masking.base_operations.operation import Operation


class SparkOperation(Operation):
    """Mask a column in a Spark DataFrame."""

    def _mask_data_pandas(
        self, data: pd.Series | pd.DataFrame
    ) -> pd.Series | pd.DataFrame:
        return super(SparkOperation, self)._mask_data(data)  # noqa: UP008

    def _native_mask_expression(self, data: DataFrame) -> Column | None:  # noqa: ARG002, PLR6301
        """Return a native Spark expression equivalent to the masking, if any.

        Operations that can be expressed with Spark functions override this method
        to avoid running python code on the executors.

        Args:
        ----
            data (DataFrame): input dataframe

        Returns:
        -------
            Column | None: masked column expression, None to mask with python

        """
        return None

    def _mask_data(self, data: DataFrame) -> DataFrame:
        """Mask the data.

        Args:
        ----
            data (DataFrame): input dataframe

        Returns:
        -------
            DataFrame: dataframe with masked column

        """
        native = self._native_mask_expression(data)
        if native is not None:
            return data.withColumn(self.col_name, native)

        # Arrow turns integer columns with nulls into floats (1000 -> 1000.0)
        integral = {
            field.name: "Int64"
            for field in data.schema
            if field.name in self.serving_columns
            and isinstance(field.dataType, IntegralType)
        }

        # Map every partition of the dataframe to the _mask_data_pandas_ function
        def mask_data_partition(
            iterator: Iterator[pd.Series | pd.DataFrame],
        ) -> Iterator[pd.Series | pd.DataFrame]:
            for partition in iterator:
                partition[self.col_name] = self._mask_data_pandas(
                    partition[self.serving_columns].astype(integral)
                )

                yield partition

        return data.mapInPandas(
            mask_data_partition,
            schema=StructType(
                list(
                    {
                        field.name: field
                        if field.name != self.col_name
                        else StructField(self.col_name, StringType(), True)  # noqa: FBT003
                        for field in data.schema
                    }.values()
                )
            ),
        )
