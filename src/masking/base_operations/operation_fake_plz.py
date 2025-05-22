from masking.base_operations.operation_fake import FakerOperation
from masking.faker.plz import FakePLZProvider


class FakePLZBase(FakerOperation):
    """Mask a column with fake PLZ data."""

    def __init__(
        self,
        col_name: str,
        preserve: str | tuple[str] | None = None,
        locale: str = "de_CH",
        **kwargs: dict,
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            preserve (str or tuple[str]): part of the PLZ to be preserved. See masking.fake.plz.FakePLZProvider for more information.
            locale (str, optional): Country initials such ash 'de_CH'.
            **kwargs (dict): keyword arguments

        """
        super().__init__(
            col_name=col_name,
            provider=FakePLZProvider(preserve=preserve, locale=locale),
            **kwargs,
        )

    def _mask_like_faker(self, line: str) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        return self.faker(line)
