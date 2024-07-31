import hashlib
import hmac
from collections.abc import Callable


def hash_string(
    data: str | None,
    secret: str,
    method: Callable = hashlib.sha256,
    text_encoding: str = "utf-8",
) -> str:
    """Hashes a string using a secret key with SHA256 as the default hashing algorithm.

    Args:
    ----
        data (str): input string to be hashed
        secret (str): secret key to hash the input string
        method (Callable, optional): Defaults to hashlib.sha256.
        text_encoding (str, optional): Defaults to "utf-8".

    Returns:
    -------
        str: hashed string

    """
    if data is None:
        return None

    assert isinstance(  # noqa: S101
        data, str
    ), f"Provided input is a {type(data)}. Make sure the type is str."

    input_data: bytes = data.encode(encoding=text_encoding)
    secret_key: bytes = secret.encode(encoding=text_encoding)

    signature: str = hmac.new(secret_key, msg=input_data, digestmod=method).hexdigest()

    return signature
