from pandas import DataFrame, Series

from masking.base_operations.operation_string_match_dict import (
    StringMatchDictOperationBase,
)
from masking.mask.operations.operation import PandasOperation


class StringMatchDictOperation(PandasOperation, StringMatchDictOperationBase):
    """String matching operation."""

    def _mask_data(self, data: DataFrame | Series) -> DataFrame | Series:
        """Mask the data.

        Args:
        ----
            data (DataFrame | Series): input data

        Returns:
        -------
            DataFrame | Series: masked data

        """
        paths = {
            line: self._get_undenied_and_denied_paths(self._parse_line(line))
            for line in (data if isinstance(data, Series) else data[self.col_name])
        }

        if isinstance(data, Series):
            data = data.to_frame(name=self.col_name)

        return data[self.serving_columns].apply(
            lambda row: self._check_mask_line(
                line=row[self.col_name],
                additional_values=row.drop(self.col_name).to_dict(),
                leaf_to_mask=paths[row[self.col_name]][0],
                leaf_to_deny=paths[row[self.col_name]][1],
            ),
            axis=1,
        )
