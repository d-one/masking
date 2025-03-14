from masking.base_operations.operation import Operation
from masking.faker.plz import FakePLZProvider


class FakePLZBase(Operation):
    """Mask a column with fake PLZ data."""

    def __init__(
        self,
        col_name: str,
        preserve: str | tuple[str] | None = None,
        locale: str = "de_CH",
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            preserve (str or tuple[str]): part of the PLZ to be preserved. See masking.fake.plz.FakePLZProvider for more information.
            locale (str, optional): Country initials such ash 'de_CH'.

        """
        self.col_name = col_name
        self.faker = FakePLZProvider(preserve=preserve, locale=locale)

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
