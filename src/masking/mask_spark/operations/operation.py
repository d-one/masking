from collections.abc import Iterator

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from masking.base_operations.operation import Operation


class SparkOperation(Operation):
    """Mask a column in a Spark DataFrame."""

    def _mask_data_pandas(
        self, data: pd.Series | pd.DataFrame
    ) -> pd.Series | pd.DataFrame:
        return super(SparkOperation, self)._mask_data(data)  # noqa: UP008

    def _mask_data(self, data: DataFrame) -> DataFrame:
        """Mask the data.

        Args:
        ----
            data (DataFrame): input dataframe

        Returns:
        -------
            DataFrame: dataframe with masked column

        """

        # Map every partition of the dataframe to the _mask_data_pandas_ function
        def mask_data_partition(
            iterator: Iterator[pd.Series | pd.DataFrame],
        ) -> Iterator[pd.Series | pd.DataFrame]:
            for partition in iterator:
                partition[self.col_name] = self._mask_data_pandas(
                    partition[self.serving_columns]
                )
                yield partition[["clear_values", self.col_name]]

        return data.mapInPandas(
            mask_data_partition,
            StructType([data.schema["clear_values"], data.schema[self.col_name]]),
        )
