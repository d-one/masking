from collections import defaultdict
from itertools import tee

from masking.base_operations.operation_dict import DictOperationBase
from masking.utils.presidio_handler import PresidioHandler


class MaskDictOperationBase(DictOperationBase, PresidioHandler):
    """Hashes a column using SHA256 algorithm."""

    def _handle_masking_paths(
        self,
        line: dict,
        leaf_to_mask: tuple,
        entities: dict | None = None,
        **kwargs: dict,  # noqa: ARG002
    ) -> dict:
        """Handle paths that need to be masked in the line.

        Args:
        ----
            line (dict): input line as a dictionary
            leaf_to_mask (tuple): paths to mask
            entities (dict | None): additional values to mask
            **kwargs (dict): additional keyword arguments

        Returns:
        -------
            dict: line with masked paths

        """
        if entities is None:
            # Duplicate the leaf_to_mask
            leaf_to_mask, leaf_to_mask_cp = tee(leaf_to_mask)

            entities = defaultdict(set)
            for lang in self.analyzer.supported_languages:
                for value, doc in self.analyzer.nlp_engine.process_batch(
                    [
                        self._get_leaf(line, leaf.split(self.path_separator))
                        for leaf in leaf_to_mask_cp
                    ],
                    language=lang,
                ):
                    entities[value].update(
                        self._get_language_entities(value, lang, doc)
                    )

        for leaf in leaf_to_mask:
            value = self._get_leaf(line, leaf)
            masked = self.anonymizer.anonymize(
                value, analyzer_results=list(entities[value]), operators=self.operators
            ).text
            line = self._set_leaf(line, leaf, masked)

        return line
