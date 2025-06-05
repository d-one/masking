import hashlib
from collections.abc import Callable

from masking.base_operations.operation import Operation
from masking.utils.hash import hash_string


class HashOperationBase(Operation):
    """Hashes a column using SHA256 algorithm."""

    secret: str  # secret key to hash the input string

    def __init__(
        self,
        col_name: str,
        secret: str | None = None,
        hash_function: Callable = hashlib.sha256,
        **kwargs: dict,
    ) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            secret (str): secret key to hash the input string
            hash_function (hashlib._Hash): hash function to use
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        if not any([isinstance(secret, str), secret is None]):
            msg = f"Invalid secret key, expected a string, got {type(secret)}"
            raise TypeError(msg)

        self.secret = secret
        self.hash_function = hash_function

    def _hashing_function(self, line: str) -> str:
        """Hash a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: hashed line

        """
        if self.secret is None:
            return self.hash_function(line.encode()).hexdigest()

        return hash_string(line, self.secret, method=self.hash_function)

    def _mask_line(self, line: str | int, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): keyword arguments

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

        return self._hashing_function(line)
