from abc import ABC, abstractmethod

import pandas as pd
from pyspark.sql import DataFrame

# Define a new type to pass in typing: pd.DataFrame or DataFrame
AnyDataFrame = pd.DataFrame | DataFrame


class Operation(ABC):
    """Abstract class for masking operations in a pipeline."""

    col_name: str  # column name to be masked
    concordance_table: dict | AnyDataFrame | None = None

    MAX_RETRY = 50

    @property
    def serving_columns(self) -> list[str]:
        """Return the columns needed for serving the operation."""
        return [self.col_name]

    def _cast_concordance_table(self) -> None:
        """Cast the concordance table to a dictionary."""
        if self.concordance_table is None:
            self.concordance_table = {}
            return

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
            return

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
            return

    def update_concordance_table(self, concordance_table: dict) -> None:
        """Update the concordance table.

        Args:
        ----
            concordance_table: dict with masked values

        """
        if not isinstance(concordance_table, dict):
            msg = f"Invalid concordance table, expected a dictionary, got {type(concordance_table)}"
            raise TypeError(msg)

        self._cast_concordance_table()
        self.concordance_table.update(concordance_table)

    def _get_operating_input(self, line: str | pd.Series) -> str:
        """Get the input for the operation.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: input for the operation

        """
        if isinstance(line, pd.Series) and len(self.serving_columns) == 1:
            return line[self.col_name]

        return line

    @abstractmethod
    def _mask_line(self, line: str) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        msg = (
            "Applying operation not implemented: please implement the __call__ method."
        )
        raise NotImplementedError(msg)

    def _check_mask_line(
        self, line: str | pd.Series | None, **kwargs: dict
    ) -> str | None:
        """Check if the line is masked.

        Args:
        ----
            line (str): input line
            kwargs: additional arguments for the masking operation

        Returns:
        -------
            str: masked line

        """
        if line is None:
            return None

        clear_value = line
        if isinstance(line, pd.Series):
            clear_value = line[self.col_name]

        if clear_value in self.concordance_table:
            return self.concordance_table[clear_value]

        masked = self._mask_line(self._get_operating_input(line), **kwargs)
        for _ in range(self.MAX_RETRY):
            if masked not in self.concordance_table.values():
                return masked

            masked = self._mask_line(self._get_operating_input(line), **kwargs)

        # If the maximum number of retries is reached, raise an error
        msg = f"Maximum number of retries reached ({self.MAX_RETRY}) for column {self.col_name}."
        raise ValueError(msg)

    @abstractmethod
    def _mask_data(self, data: AnyDataFrame, **kwargs: dict) -> AnyDataFrame:
        """Mask the data.

        Args:
        ----
            data : input dataframe or series
            kwargs: additional arguments for the masking operation

        Returns:
        -------
            Any: dataframe or series with masked column

        """
        msg = (
            "Applying operation not implemented: please implement the __call__ method."
        )
        raise NotImplementedError(msg)

    def __call__(self, data: AnyDataFrame) -> AnyDataFrame:
        """Generate a masked column.

        Args:
        ----
            data : input dataframe or series

        Returns:
        -------
            Any: dataframe with hashed column

        """
        self._cast_concordance_table()
        return self._mask_data(data)
