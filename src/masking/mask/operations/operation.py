from collections.abc import Iterator
from typing import Any

import pandas as pd

from masking.base_operations.operation import Operation


class PandasOperation(Operation):
    """Hashes a column for a pandas dataframe."""

    def _mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        if isinstance(data, pd.Series):
            return data.map(self._check_mask_line)

        # Iterate over plain python lists: DataFrame.apply(axis=1) builds a
        # Series per row, which is orders of magnitude slower.
        return pd.Series(
            [
                self._check_mask_line(line=line, additional_values=additional_values)
                for line, additional_values in self._iter_serving_values(data)
            ],
            index=data.index,
        )

    def _iter_serving_values(self, data: pd.DataFrame) -> Iterator[tuple[Any, dict]]:
        """Iterate over the values of the column with the other serving columns.

        Args:
        ----
            data (pd.DataFrame): input dataframe

        Yields:
        ------
            tuple: value of the column and dictionary of the other serving columns

        """
        other_cols = [c for c in self.serving_columns if c != self.col_name]
        other_values = [data[c].tolist() for c in other_cols]
        for line, *others in zip(
            data[self.col_name].tolist(), *other_values, strict=True
        ):
            yield line, dict(zip(other_cols, others, strict=True))
