from collections.abc import Callable

from presidio_anonymizer import OperatorConfig

from masking.base_operations.operation import Operation
from masking.utils.presidio_handler import PresidioHandler


class HashPresidioBase(Operation, PresidioHandler):
    """Hashes a text using hashlib algorithm and presidio to detect entities."""

    # Hashing function
    masking_function: Callable[[str], str]  # function to hash the input string

    def __init__(
        self, col_name: str, masking_function: Callable[[str], str], **kwargs: dict
    ) -> None:
        """Initialize the HashTextSHA256 class.

        Args:
        ----
            col_name (str): column name to be hashed
            masking_function (Callable[[str], str]): function to hash the input string
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        self.masking_function = masking_function

        if kwargs.get("operators", None) is None:
            self.operators = {
                entity: OperatorConfig("custom", {"lambda": self.masking_function})
                for entity in self._PII_ENTITIES
            }

    def _mask_line(
        self,
        line: str,
        entities: list[str] | None = None,
        **kwargs: dict,  # noqa: ARG002
    ) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line
            entities (list): list of entities to mask
            **kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """
        # Detect entities in the line
        if entities is None:
            entities = self._get_entities(line)

        # Substitute entities with masked values
        return self.anonymizer.anonymize(
            line, list(entities), operators=self.operators
        ).text
