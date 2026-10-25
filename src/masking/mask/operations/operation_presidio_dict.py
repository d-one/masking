from pandas import DataFrame, Series

from masking.base_operations.operation_presidio_dict import MaskDictOperationBase
from masking.mask.operations.operation import PandasOperation


class MaskDictOperation(PandasOperation, MaskDictOperationBase):
    """Hashes a column using SHA256 algorithm."""

    def _mask_data(self, data: DataFrame | Series) -> DataFrame | Series:
        """Mask the data.

        Args:
        ----
            data (DataFrame | Series): input data

        Returns:
        -------
            DataFrame | Series: masked data

        """
        series = data if isinstance(data, Series) else data[self.col_name]
        entities = self._get_entities_of_lines(series)
        return series.map(lambda line: self._check_mask_line(line, entities=entities))
