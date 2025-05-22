from masking.base_operations.operation_fake import FakerOperation
from masking.faker.date import FakeDateProvider


class FakeDateBase(FakerOperation):
    """Mask a column with fake Date data."""

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
        super().__init__(
            col_name=col_name, provider=FakeDateProvider(preserve=preserve), **kwargs
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
