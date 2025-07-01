from collections import defaultdict
from itertools import tee

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
        gen_values_to_mask = (
            (self._get_leaf(self._parse_line(line), leaf), line)
            for line in (data if isinstance(data, Series) else data[self.col_name])
            for leaf in self._find_leaf_path(self._parse_line(line))
            if not self._is_denied_path(leaf)
        )

        gen1, gen2 = tee(gen_values_to_mask)
        entities = defaultdict(lambda: defaultdict(set))

        for lang in self.analyzer.supported_languages:
            for line, (leaf, doc) in zip(
                (line for _, line in gen1),
                self.analyzer.nlp_engine.process_batch(
                    (leaf for leaf, _ in gen2), language=lang
                ),
                strict=False,
            ):
                entities[line][leaf].update(
                    self._get_language_entities(leaf, lang, doc)
                )

        return data[self.col_name].apply(
            lambda line: self._check_mask_line(line, entities=entities[line])
        )
