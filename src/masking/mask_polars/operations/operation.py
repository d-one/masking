import polars as pl

from masking.base_operations.operation import Operation


class PolarsOperation(Operation):
    """Masking operation for a Polars DataFrame."""

    def _mask_data(self, data: pl.DataFrame | pl.Series) -> pl.DataFrame | pl.Series:
        """Mask the data.

        Args:
        ----
            data (pl.DataFrame or pl.Series): input dataframe or series

        Returns:
        -------
            pl.Series: series with masked column

        """
        if isinstance(data, pl.Series):
            return data.map_elements(self._check_mask_line, return_dtype=pl.Utf8)

        serving_cols = self.serving_columns
        other_cols = [c for c in serving_cols if c != self.col_name]

        if not other_cols:
            return data[self.col_name].map_elements(
                self._check_mask_line, return_dtype=pl.Utf8
            )

        results = []
        for row in data.select(serving_cols).iter_rows(named=True):
            additional_values = {k: v for k, v in row.items() if k != self.col_name}
            results.append(
                self._check_mask_line(
                    line=row[self.col_name], additional_values=additional_values
                )
            )
        return pl.Series(results)
