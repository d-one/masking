import polars as pl

from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from masking.mask_polars.operations.operation import PolarsOperation


class StringMatchDictOperation(PolarsOperation, StringMatchDictOperationBase):
    """String matching operation."""

    def _mask_data(self, data: pl.DataFrame | pl.Series) -> pl.DataFrame | pl.Series:
        """Mask the data.

        Args:
        ----
            data (pl.DataFrame | pl.Series): input data

        Returns:
        -------
            pl.Series: masked data

        """
        if isinstance(data, pl.Series):
            values = data.to_list()
        else:
            values = data[self.col_name].to_list()

        paths = {
            line: self._get_undenied_and_denied_paths(self._parse_line(line))
            for line in values
        }

        if isinstance(data, pl.Series):
            data = data.to_frame(self.col_name)

        results = []
        for row in data.select(self.serving_columns).iter_rows(named=True):
            line = row[self.col_name]
            additional_values = {k: v for k, v in row.items() if k != self.col_name}
            results.append(
                self._check_mask_line(
                    line=line,
                    additional_values=additional_values,
                    leaf_to_mask=paths[line][0],
                    leaf_to_deny=paths[line][1],
                )
            )
        return pl.Series(results)
