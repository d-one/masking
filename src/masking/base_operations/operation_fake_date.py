from masking.base_operations.operation import Operation
from masking.faker.date import FakeDateProvider


class FakeDateBase(Operation):
    """Hashes a column using SHA256 algorithm."""

    MAX_RETRY_MASK_LINE = 100

    def __init__(
        self, col_name: str, preserve: str | tuple[str] | None = None, **kwargs: dict
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            preserve (str or tuple[str]): part of the date to be preserved. See masking.fake.date.FakeDateProvider for more information.
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)
        self.faker = FakeDateProvider(preserve=preserve)

    @property
    def _needs_unique_values(self) -> bool:
        """Return if the operation needs to produce unique masked values."""
        return True

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): additional arguments for the masking operation

        Returns:
        -------
            str: masked line

        """
        masked = self.faker(line)

        counter = 0
        while masked == line and counter < self.MAX_RETRY_MASK_LINE:
            masked = self.faker(line)
            counter += 1

        if masked == line:
            msg = f"Unable to mask the line {line} after {self.MAX_RETRY} attempts."
            raise ValueError(msg)

        return masked
