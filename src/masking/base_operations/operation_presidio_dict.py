import json
from collections import defaultdict

from masking.base_operations.operation import Operation
from masking.utils.multi_nested_dict import MultiNestedDictHandler
from masking.utils.presidio_handler import PresidioHandler


class MaskDictOperationBase(Operation, PresidioHandler, MultiNestedDictHandler):
    """Hashes a column using SHA256 algorithm."""

    def _mask_line(
        self,
        line: dict | str,
        leaf_to_mask: tuple | None = None,
        leaf_to_deny: tuple | None = None,
        entities: dict | None = None,
        **kwargs: dict,  # noqa: ARG002
    ) -> str:
        """Mask a single line represented by a dictionary.

        Args:
        ----
            line (dict): line line
            leaf_to_mask (tuple): leafs to mask
            leaf_to_deny (tuple): leafs to deny
            entities (dict): entities detected in the line
            **kwargs (dict): additional arguments for the masking operation

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
            leaf_to_mask, leaf_to_deny = self._get_undenied_and_denied_paths(line)

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
                for value, doc in self.analyzer.nlp_engine.process_batch(
                    [
                        self._get_leaf(line, leaf.split(self.path_separator))
                        for leaf in leaf_to_mask
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

        if isinstance(line, dict):
            return json.dumps(line, ensure_ascii=False)

        return line
