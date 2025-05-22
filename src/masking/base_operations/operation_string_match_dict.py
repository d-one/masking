import json

from masking.base_operations.operation import Operation
from masking.utils.multi_nested_dict import MultiNestedDictHandler
from masking.utils.presidio_handler import PresidioHandler
from masking.utils.string_match_handler import StringMatchHandler


class StringMatchDictOperationBase(
    Operation, StringMatchHandler, PresidioHandler, MultiNestedDictHandler
):
    """String Match Operation Base Class."""

    @property
    def serving_columns(self) -> list[str]:
        return [self.col_name, *self.pii_cols]

    def _mask_line(
        self,
        line: str | dict,
        additional_values: dict | None = None,
        leaf_to_mask: tuple | None = None,
        leaf_to_deny: tuple | None = None,
        **kwargs: dict,  # noqa: ARG002
    ) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line
            additional_values (dict): additional values to mask
            leaf_to_mask (tuple): leafs to mask
            leaf_to_deny (tuple): leafs to deny
            **kwargs (dict): keyword arguments

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

        # Filter all the NaN or NaT values
        recognizer = self._get_pattern_recognizer(
            self._get_pii_values(additional_values)
        )

        for leaf in leaf_to_mask:
            value = self._get_leaf(line, leaf)
            res = recognizer.analyze(value, entities=list(self._PII_ENTITIES))
            masked = self.anonymizer.anonymize(
                value, res, operators=self.operators
            ).text
            line = self._set_leaf(line, leaf, masked)

        if isinstance(line, str):
            line = json.dumps(line, ensure_ascii=False)

        return line
