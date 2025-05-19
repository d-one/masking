from collections import defaultdict

import pandas as pd
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
        entities = defaultdict(set)
        for lang in self.analyzer.supported_languages:
            for line, doc in self.analyzer.nlp_engine.process_batch(
                data if isinstance(data, pd.Series) else data[self.col_name],
                language=lang,
            ):
                entities[line].update(self._get_language_entities(line, lang, doc))

        if isinstance(data, pd.Series):
            return data.map(
                lambda line: self._check_mask_line(line, entities=entities[line])
            )

        return data[self.col_name].apply(
            lambda line: self._check_mask_line(line, entities=entities[line])
        )
