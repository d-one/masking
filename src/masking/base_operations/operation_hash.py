import hashlib
from collections.abc import Callable, Generator

from masking.base_operations.operation import Operation
from masking.utils.hash import hash_string


class HashOperationBase(Operation):
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

    def _mask_line(self, line: str) -> Generator[str, None, None]:
        """Mask a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        if not isinstance(line, str):
            try:
                line = str(line)
            except Exception as e:
                msg = f"Input line must be a string. Tried to convert to string but en error occured: {e}"
                raise ValueError(msg) from e

        return hash_string(line, self.secret, method=self.hash_function)
