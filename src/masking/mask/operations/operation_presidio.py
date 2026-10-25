from pandas import DataFrame, Series

from masking.base_operations.operation_presidio import MaskPresidioBase
from masking.mask.operations.operation import PandasOperation


class MaskPresidio(MaskPresidioBase, PandasOperation):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""

    def _mask_data(self, data: DataFrame | Series) -> DataFrame | Series:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        series = data if isinstance(data, Series) else data[self.col_name]
        entities = self._get_entities_batch(series)
        return series.map(
            lambda line: self._check_mask_line(line, entities=entities[line])
        )
