from abc import ABC, abstractmethod

import pandas as pd


class Operation(ABC):
    """Abstract class for masking operations in a pipeline."""

    col_name: str  # column name to be masked
    concordance_table: pd.DataFrame | None = None  # unique values in the column

    def _cast_concordance_table(self) -> None:
        """Cast the concordance table as a DataFrame."""
        concordance_table_data = {
            self.col_name: list(self.concordance_table.keys()),
            self.col_name + "_masked": list(self.concordance_table.values()),
        }

        self.concordance_table = pd.DataFrame(concordance_table_data)

    def _prepare_concordance_table(self) -> None:
        """Prepare the concordance table for the pipeline."""
        if self.concordance_table is None:
            self.concordance_table = {}
            return

        msg = "Concordance table is not None, please implement the logic to update the concordance table."
        raise NotImplementedError(msg)

    @abstractmethod
    def _mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        msg = (
            "Applying operation not implemented: please implement the __call__ method."
        )
        raise NotImplementedError(msg)

    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Generate a hashed column and drop the original column.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with hashed column

        """
        self._prepare_concordance_table()
        data_ = self._mask_data(data)
        self._cast_concordance_table()

        return data_
