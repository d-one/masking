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

    def __init__(
        self,
        col_name: str,
        concordance_table: dict | AnyDataFrame | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the Operation class.

        Args:
        ----
            col_name (str): column name to be masked
            concordance_table (dict | AnyDataFrame | None): concordance table
            **kwargs: additional arguments for the masking operation

        """
        super().__init__(**kwargs)

        if not isinstance(col_name, str):
            msg = f"Invalid column name, expected a string, got {type(col_name)}"
            raise TypeError(msg)

        self.col_name = col_name
        self.concordance_table = concordance_table

        self._cast_concordance_table()

    @property
    def serving_columns(self) -> list[str]:
        """Return the columns needed for serving the operation."""
        return [self.col_name]

    @staticmethod
    def cast_concordance_table(
        concordance_table: pd.DataFrame | DataFrame | dict | None,
    ) -> dict:
        """Cast the concordance table to a dictionary.

        Args:
        ----
            concordance_table (Any): concordance table

        Returns:
        -------
            dict: concordance table as a dictionary

        """
        if concordance_table is None:
            return {}

        if isinstance(concordance_table, pd.DataFrame):
            # Make sure the dataframe has only two columns: 'clear_values' and 'masked_values'
            try:
                return dict(
                    zip(
                        concordance_table["clear_values"],
                        concordance_table["masked_values"],
                        strict=False,
                    )
                )
            except Exception as e:
                msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
                raise TypeError(msg) from e

        if isinstance(concordance_table, DataFrame):
            # Make sure the dataframe has only two columns: 'clear_values' and 'masked_values'
            try:
                clear_values = (
                    concordance_table.select("clear_values")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )
                masked_values = (
                    concordance_table.select("masked_values")
                    .rdd.flatMap(lambda x: x)
                    .collect()
                )

                return dict(zip(clear_values, masked_values, strict=False))
            except Exception as e:
                msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
                raise TypeError(msg) from e

        if isinstance(concordance_table, dict):
            if any([
                len(concordance_table) == 0,
                all(
                    isinstance(value, str) and isinstance(k, str)
                    for k, value in concordance_table.items()
                ),
            ]):
                return concordance_table

            msg = f"Invalid concordance table, expected a dictionary of type dict[str,str], got {type(concordance_table)}"
            raise TypeError(msg)

        msg = f"Invalid concordance table, expected a dictionary, got {type(concordance_table)}"
        raise TypeError(msg)

    def _cast_concordance_table(self) -> None:
        """Cast the concordance table to a dictionary."""
        self.concordance_table = self.cast_concordance_table(self.concordance_table)

    def update_concordance_table(self, concordance_table: dict) -> None:
        """Update the concordance table.

        Args:
        ----
            concordance_table: dict with masked values

        """
        msg = f"Invalid concordance table, expected a dictionary, got {type(concordance_table)}"

        if not isinstance(concordance_table, dict):
            raise TypeError(msg)

        # Check that all values are strings, and all keys are strings
        if not all(
            isinstance(value, str) and isinstance(k, str)
            for k, value in concordance_table.items()
        ):
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
                # Update the concordance table with the new value
                self.concordance_table[clear_value] = masked
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
