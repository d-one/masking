from itertools import tee

from masking.base_operations.operation_dict import DictOperationBase
from masking.utils.presidio_handler import PresidioHandler
from masking.utils.string_match_handler import StringMatchHandler


class StringMatchDictOperationBase(
    DictOperationBase, StringMatchHandler, PresidioHandler
):
    """String Match Operation Base Class."""

    @property
    def serving_columns(self) -> list[str]:
        return [self.col_name, *self.pii_cols]

    def _handle_masking_paths(
        self,
        line: dict,
        leaf_to_mask: tuple,
        analyzer_results: dict | None = None,
        additional_values: dict | None = None,
        **kwargs: dict,  # noqa: ARG002
    ) -> dict:
        """Handle paths that need to be masked in the line.

        Args:
        ----
            line (dict): input line as a dictionary
            leaf_to_mask (tuple): paths to mask
            analyzer_results (dict | None): results from the analyzer
            additional_values (dict | None): additional values to mask
            **kwargs (dict): additional keyword arguments

        Returns:
        -------
            dict: line with masked paths

        """
        if analyzer_results is None:
            analyzer_results = {}
            leaf_to_mask, leaf_to_mask_cp = tee(leaf_to_mask)

            recognizer = self._get_pattern_recognizer(
                self._get_pii_values(additional_values)
            )

            if recognizer:
                analyzer_results = {
                    v: recognizer.analyze(v, entities=list(self._PII_ENTITIES))
                    for leaf in leaf_to_mask_cp
                    if (v := self._get_leaf(line, leaf))
                }

        if analyzer_results:
            for leaf in leaf_to_mask:
                value = self._get_leaf(line, leaf)
                masked = self.anonymizer.anonymize(
                    value,
                    analyzer_results=analyzer_results.get(value, []),
                    operators=self.operators,
                ).text
                line = self._set_leaf(line, leaf, masked)

        return line
