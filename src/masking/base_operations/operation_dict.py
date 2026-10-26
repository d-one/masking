import json
from abc import abstractmethod

from masking.base_operations.operation import AnyDataFrame, Operation
from masking.utils.multi_nested_dict import MultiNestedDictHandler


class DictOperationBase(Operation, MultiNestedDictHandler):
    """Base class for dictionary operations."""

    masking_function = None  # Should be set in subclasses

    def __init__(
        self,
        col_name: str,
        concordance_table: dict | AnyDataFrame | None = None,
        **kwargs: dict,
    ) -> None:
        """Initialize the operation and check that denied paths can be masked.

        Args:
        ----
            col_name (str): column name to be masked
            concordance_table (dict | AnyDataFrame | None): concordance table
            **kwargs (dict): keyword arguments for the base classes

        Raises:
        ------
            ValueError: if deny keys are set without a masking function

        """
        super().__init__(col_name, concordance_table, **kwargs)

        if (
            self.deny_keys or self.deny_keys_callable
        ) and self.masking_function is None:
            msg = "deny_keys require a masking_function to replace the denied values."
            raise ValueError(msg)

    def _handle_denied_paths(self, line: dict, leaf_to_deny: tuple) -> dict:
        """Handle denied paths in the line.

        Args:
        ----
            line (dict): input line as a dictionary
            leaf_to_deny (tuple): paths to deny

        Returns:
        -------
            dict: line with denied paths masked

        """
        for leaf in leaf_to_deny:
            value = self._get_leaf(line, leaf)
            text = (
                value
                if isinstance(value, str)
                else json.dumps(value, ensure_ascii=False)
            )
            masked = self.masking_function(text)
            line = self._set_leaf(line, leaf, masked)
        return line

    @abstractmethod
    def _handle_masking_paths(
        self, line: dict, leaf_to_mask: tuple, **kwargs: dict
    ) -> dict:
        """Handle paths that need to be masked in the line.

        Args:
        ----
            line (dict): input line as a dictionary
            leaf_to_mask (tuple): paths to mask
            **kwargs (dict): additional keyword arguments

        Returns:
        -------
            dict: line with masked paths

        """

    def _mask_line(
        self,
        line: str | dict,
        leaf_to_mask: tuple | None = None,
        leaf_to_deny: tuple | None = None,
        **kwargs: dict,
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
        # Convert line to dict if it is a string
        if not isinstance(line, (dict | list)):
            line = self._parse_line(line)

        # Get undenied and denied paths if not provided
        if leaf_to_deny is None and leaf_to_mask is None:
            leaf_to_mask, leaf_to_deny = self._get_undenied_and_denied_paths(line)

        # Handle denied paths
        line = self._handle_denied_paths(line, leaf_to_deny)

        # Generate pre-processing values
        self._handle_masking_paths(line, leaf_to_mask, **kwargs)

        # Convert line back to JSON string if it was a dict
        if not isinstance(line, str):
            line = self._dump_line(line)

        return line
