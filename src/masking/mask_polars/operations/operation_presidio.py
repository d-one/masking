from collections import defaultdict

import polars as pl

from masking.base_operations.operation_presidio import MaskPresidioBase
from masking.mask_polars.operations.operation import PolarsOperation


class MaskPresidio(MaskPresidioBase, PolarsOperation):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""

    def _mask_data(self, data: pl.DataFrame | pl.Series) -> pl.DataFrame | pl.Series:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pl.DataFrame or pl.Series): input dataframe or series

        Returns:
        -------
            pl.Series: series with masked column

        """
        series = data if isinstance(data, pl.Series) else data[self.col_name]

        entities = defaultdict(set)
        for lang in self.analyzer.supported_languages:
            for line, doc in self.analyzer.nlp_engine.process_batch(
                series.to_list(), language=lang
            ):
                entities[line].update(self._get_language_entities(line, lang, doc))

        return series.map_elements(
            lambda line: self._check_mask_line(line, entities=entities[line]),
            return_dtype=pl.Utf8,
        )
