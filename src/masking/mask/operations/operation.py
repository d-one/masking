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

        return data[self.serving_columns].apply(
            lambda row: self._check_mask_line(
                line=row[self.col_name],
                additional_values=row.drop(self.col_name).to_dict(),
            ),
            axis=1,
        )
