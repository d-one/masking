import pandas as pd

from masking.mask.fake.plz import FakePLZProvider

from .operation import Operation


class FakePLZ(Operation):
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
        if line not in self.concordance_table:
            faked = self.faker(line)
            while faked in self.concordance_table.values():
                print(  # noqa: T201
                    f"Collision detected: {faked} already exists in the concordance table. Retrying..."
                )
                faked = self.faker(line)

            self.concordance_table.update({line: faked})

        return self.concordance_table.get(line, line)

    def _mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        if isinstance(data, pd.Series):
            return data.apply(lambda x: self._mask_line(x) if pd.notna(x) else x)

        data[self.col_name] = data[self.col_name].apply(
            lambda x: self._mask_line(x) if pd.notna(x) else x
        )
        return data
