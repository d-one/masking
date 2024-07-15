import hashlib
from abc import ABC, abstractmethod

import pandas as pd

from .hash import hash_string


class Operation(ABC):
    """Abstract class for masking operations in a pipeline."""

    @abstractmethod
    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Apply the operation to the data."""
        print(  # noqa: T201
            "Applying operation not implemented: please implement the __call__ method."
        )


class HashSHA256(Operation):
    """Hashes a column using SHA256 algorithm."""

    def __init__(self, col_name: str, secret: str) -> None:
        """Initialize the HashSHA256 class.

        Args:
        ----
            col_name (str): column name to be hashed
            secret (str): secret key to hash the input string

        """
        self.secret = secret
        self.col_name = col_name

    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Generate a hashed column and drop the original column.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with hashed column

        """
        data_ = data.copy()

        if isinstance(data_, pd.Series):
            return data_.apply(
                lambda x: hash_string(x, self.secret, method=hashlib.sha256)
            )

        data_.loc[:, self.col_name] = data_.loc[:, self.col_name].apply(
            lambda x: hash_string(x, self.secret, method=hashlib.sha256)
        )
        return data_
