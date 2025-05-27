from abc import abstractmethod

from masking.base_operations.operation import Operation
from masking.faker.faker import FakeProvider


class FakerOperation(Operation):
    """Class is used to generate fake data using the Faker library.

    It can be used to mask sensitive data in a dataset.
    """

    MAX_RETRY_MASK_LINE = 1000
    faker: FakeProvider

    def __init__(self, col_name: str, provider: FakeProvider, **kwargs: dict) -> None:
        """Initialize the FakerOperation class.

        Args:
        ----
            col_name (str): column name to be masked
            provider (FakeProvider): provider to be used for generating fake data
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)
        self.faker = provider

    @property
    def _needs_unique_values(self) -> bool:
        """Return if the operation needs to produce unique masked values."""
        return True

    @abstractmethod
    def _mask_like_faker(self, line: str, **kwargs: dict) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """
        masked = self._mask_like_faker(line)

        counter = 0
        while masked == line and counter < self.MAX_RETRY_MASK_LINE:
            masked = self._mask_like_faker(line)
            counter += 1

        if masked == line:
            msg = f"Unable to mask the line {line} after {self.MAX_RETRY_MASK_LINE} attempts for column {self.col_name}."
            raise ValueError(msg)

        return masked
