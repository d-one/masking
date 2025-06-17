from abc import ABC, abstractmethod
from typing import Any, ClassVar

import pandas as pd
from pyspark.sql import DataFrame

from masking.base_operations.operation import Operation

# Define a new type to pass in typing: pd.DataFrame or DataFrame
AnyDataFrame = pd.DataFrame | DataFrame


class ConcordanceTableBase(ABC):
    config: ClassVar[
        dict[str, Any]
    ] = {}  # Configuration dictionary for the masking operation
    column_name: str  # Column name to be masked
    concordance_table: ClassVar[dict | AnyDataFrame | None] = (
        None  # Concordance table to be used for masking
    )

    def __init__(
        self,
        masking_operation: Operation,
        concordance_table: dict | AnyDataFrame | None = None,
    ) -> None:
        """Initialize the ConcordanceTable.

        Args:
        ----
            masking_operation (Operation): masking operation
            column (str or int): column name (str) or column index (int) to be masked
            concordance_table (pd.DataFrame or None): unique values in the column

        """
        self.masking_operation = masking_operation

        if concordance_table is None:
            concordance_table = {}
        self.concordance_table = concordance_table

    @property
    def column_name(self) -> str:
        """Return the column name."""
        return self.masking_operation.col_name

    @property
    def serving_columns(self) -> list[str]:
        """Return the columns needed for serving the operation."""
        return self.masking_operation.serving_columns

    def _cast_concordance_table(self) -> None:
        """Cast the concordance table to a dictionary."""
        if self.concordance_table is None:
            self.concordance_table = {}

        if isinstance(self.concordance_table, pd.DataFrame):
            # Make sure the dataframe has only two columns: 'clear_values' and 'masked_values'
            try:
                clear_values = self.concordance_table["clear_values"]
                masked_values = self.concordance_table["masked_values"]

                self.concordance_table = dict(
                    zip(clear_values, masked_values, strict=False)
                )
            except Exception as e:
                msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
                raise ValueError(msg) from e

        if isinstance(self.concordance_table, DataFrame):
            # Make sure the dataframe has only two columns: 'clear_values' and 'masked_values'
            try:
                clear_values = (
                    self.concordance_table.select("clear_values")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )
                masked_values = (
                    self.concordance_table.select("masked_values")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )

                self.concordance_table = dict(
                    zip(clear_values, masked_values, strict=False)
                )
            except Exception as e:
                msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
                raise ValueError(msg) from e

    def clear_concordance_table(self) -> None:
        """Clear the concordance table."""
        self.concordance_table = {}

    @abstractmethod
    def _build_concordance_table(self, data: AnyDataFrame) -> AnyDataFrame:
        """Build the concordance table.

        Args:
        ----
            data (DataFrame): input dataframe or series

        Returns:
        -------
            DataFrame: concordance table

        """

    def __call__(self, data: AnyDataFrame) -> AnyDataFrame:
        """Apply the pipeline to the data.

        Args:
        ----
            data (DataFrame): input dataframe or series

        Returns:
        -------
            DataFrame: dataframe or series with masked column

        """
        self._cast_concordance_table()

        # Update the concordance table with the masked values
        self.masking_operation.update_concordance_table(self.concordance_table)

        # Get the new values
        new_values = self._build_concordance_table(data)

        self.concordance_table.update(new_values)
        return self.concordance_table
