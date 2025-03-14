from masking.base_operations.operation import Operation
from masking.faker.date import FakeDateProvider


class FakeDateBase(Operation):
    """Hashes a column using SHA256 algorithm."""

    def __init__(self, col_name: str, preserve: str | tuple[str] = "year") -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            preserve (str or tuple[str]): part of the date to be preserved. See masking.fake.date.FakeDateProvider for more information.

        """
        self.col_name = col_name
        self.faker = FakeDateProvider(preserve=preserve)

    def _mask_line(self, line: str) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        return self.faker(line)
