import hashlib
import json
from collections import defaultdict
from collections.abc import Callable, Generator

from presidio_anonymizer import OperatorConfig

from masking.base_operations.operation import Operation
from masking.utils.multi_nested_dict import MultiNestedDictHandler
from masking.utils.presidio_handler import PresidioHandler


class MaskDictOperationBase(Operation, PresidioHandler, MultiNestedDictHandler):
    """Hashes a column using SHA256 algorithm."""

    def __init__(
        self, col_name: str, masking_function: Callable = hashlib.sha256, **kwargs: dict
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            masking_function (Callable): function to hash the line string
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        self.masking_function = masking_function

        if kwargs.get("operators", None) is None:
            self.operators = {
                entity: OperatorConfig("custom", {"lambda": self.masking_function})
                for entity in self._PII_ENTITIES
            }

    def _get_leafs(self, line: dict | str) -> tuple[Generator[str, None, None]]:
        """Get the leafs of a nested dictionary.

        Args:
        ----
            line (dict): input dictionary

        Returns:
        -------
            tuple[Generator[str, None, None]]: generator of leafs

        """
        leafs_to_mask, leafs_to_deny = [], []
        for leaf in self._find_leaf_path(line):
            if self._is_denied_path(leaf):
                leafs_to_deny.append(leaf)
                continue

            leafs_to_mask.append(leaf)

        return leafs_to_mask, leafs_to_deny

    def _mask_line(
        self,
        line: dict | str,
        leaf_to_mask: tuple | None = None,
        leaf_to_deny: tuple | None = None,
        entities: dict | None = None,
    ) -> str:
        """Mask a single line represented by a dictionary.

        Args:
        ----
            line (dict): line line
            leaf_to_mask (tuple): leafs to mask
            leaf_to_deny (tuple): leafs to deny
            entities (dict): entities detected in the line

        Returns:
        -------
            str: masked line

        """
        if isinstance(line, str):
            try:
                line = json.loads(line)
            except json.JSONDecodeError:
                msg = "Failed to parse line, treat it as a string."
                print(msg)  # noqa: T201

        if leaf_to_deny is None and leaf_to_mask is None:
            leaf_to_mask, leaf_to_deny = self._get_leafs(line)

        for leaf in leaf_to_deny:
            value = self._get_leaf(line, self._undeny_path(leaf))
            masked = self.masking_function(
                value
                if isinstance(value, str)
                else json.dumps(value, ensure_ascii=False)
            )
            line = self._set_leaf(line, self._undeny_path(leaf), masked)

        # Process the leaf values
        if entities is None:
            entities = defaultdict(set)
            for lang in self.analyzer.supported_languages:
                for leaf, doc in self.analyzer.nlp_engine.process_batch(
                    [
                        self._get_leaf(line, leaf.split(self.path_separator))
                        for leaf in leaf_to_mask
                    ],
                    language=lang,
                ):
                    entities[leaf].update(self._get_language_entities(leaf, lang, doc))

        for leaf in leaf_to_mask:
            value = self._get_leaf(line, leaf)
            masked = self.anonymizer.anonymize(
                value, analyzer_results=list(entities[value]), operators=self.operators
            ).text
            line = self._set_leaf(line, leaf, masked)

        if isinstance(line, dict):
            return json.dumps(line, ensure_ascii=False)

        return line
