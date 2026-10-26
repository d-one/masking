import polars as pl

from masking.base_operations.operation_presidio_dict import MaskDictOperationBase
from masking.mask_polars.operations.operation import PolarsOperation


class MaskDictOperation(PolarsOperation, MaskDictOperationBase):
    """Hashes a column using SHA256 algorithm."""

    def _mask_data(self, data: pl.DataFrame | pl.Series) -> pl.DataFrame | pl.Series:
        """Mask the data.

        Args:
        ----
            data (pl.DataFrame | pl.Series): input data

        Returns:
        -------
            pl.Series: masked data

        """
        series = data if isinstance(data, pl.Series) else data[self.col_name]
        entities = self._get_entities_of_lines(series.to_list())
        return series.map_elements(
            lambda line: self._check_mask_line(line, entities=entities),
            return_dtype=pl.Utf8,
        )
