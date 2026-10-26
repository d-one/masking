from collections.abc import Iterable, Iterator

from masking.base_operations.operation_dict import DictOperationBase
from masking.utils.presidio_handler import PresidioHandler


class MaskDictOperationBase(DictOperationBase, PresidioHandler):
    """Hashes a column using SHA256 algorithm."""

    def _iter_values_to_mask(self, lines: Iterable) -> Iterator:
        """Iterate over the values of the leaves to mask of several lines.

        Args:
        ----
            lines (Iterable): input lines, as JSON strings, dictionaries or lists

        Yields:
        ------
            value of each leaf that is not denied

        """
        for line in lines:
            if not isinstance(line, str | dict | list):
                continue
            for path, value in self._find_leaf_path_and_value(self._parse_line(line)):
                if not self._is_denied_path(path):
                    yield value

    def _get_entities_of_lines(self, lines: Iterable) -> dict:
        """Get the entities of the leaves to mask of several lines at once.

        Args:
        ----
            lines (Iterable): input lines

        Returns:
        -------
            dict: entities of each leaf value

        """
        return self._get_entities_batch(self._iter_values_to_mask(lines))

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
            entities (dict | None): entities of each leaf value
            **kwargs (dict): additional keyword arguments

        Returns:
        -------
            dict: line with masked paths

        """
        leaf_values = [(leaf, self._get_leaf(line, leaf)) for leaf in leaf_to_mask]

        if entities is None:
            entities = self._get_entities_batch(value for _, value in leaf_values)

        for leaf, value in leaf_values:
            # Numbers, booleans and nulls cannot be anonymized as text
            if not isinstance(value, str) or not (results := entities.get(value)):
                continue
            masked = self.anonymizer.anonymize(
                value, analyzer_results=list(results), operators=self.operators
            ).text
            line = self._set_leaf(line, leaf, masked)

        return line
