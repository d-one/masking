import hashlib
from collections.abc import Callable

import pandas as pd

from .hash import hash_string
from .operation import Operation


class HashOperation(Operation):
    """Hashes a column using SHA256 algorithm."""

    secret: str  # secret key to hash the input string

    def __init__(
        self, col_name: str, secret: str, hash_function: Callable = hashlib.sha256
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            secret (str): secret key to hash the input string
            hash_function (hashlib._Hash): hash function to use

        """
        self.col_name = col_name
        self.secret = secret
        self.hash_function = hash_function

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
            self.concordance_table[line] = hash_string(
                line, self.secret, method=self.hash_function
            )

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
