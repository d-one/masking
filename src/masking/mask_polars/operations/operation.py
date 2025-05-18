import polars as pl

from masking.base_operations.operation import Operation


class PolarsOperation(Operation):
    """Hashes a column for a polars dataframe."""

    def _mask_data(self, data: pl.DataFrame) -> pl.DataFrame:
        """Mask the data.

        Args:
        ----
            data (pl.DataFrame or pl.Series): input dataframe or series

        Returns:
        -------
            pl.DataFrame or pl.Series: dataframe or series with masked column

        """
        if len(self.serving_columns) == 1:
            return (
                data.select(self.serving_columns[0])
                .map_rows(self._check_mask_line, return_dtype=pl.Series)
                .to_series()
            )

        return (
            data.select(self.serving_columns)
            .map_rows(self._check_mask_line, return_dtype=pl.String)
            .to_series()
        )
