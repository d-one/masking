from abc import ABCMeta
from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest
from masking.base_operations.operation_hash import HashOperationBase, hashlib
from masking.utils.hash import hash_string


@pytest.fixture(
    scope="module",
    params=[
        ("secret1", hashlib.sha256),
        ("secret2", hashlib.sha512),
        ("secret3", hashlib.md5),
        (None, hashlib.sha256),
    ],
)
def operation_class(request: pytest.FixtureRequest) -> type:
    class ConcreteHashOperation(HashOperationBase):
        def __init__(
            self,
            col_name: str,
            secret: str = request.param[0],
            hash_function: Callable[..., Any] = request.param[1],
            **kwargs: dict,
        ) -> None:
            super().__init__(
                col_name=col_name, secret=secret, hash_function=hash_function, **kwargs
            )

        @staticmethod
        def _mask_data() -> str:
            return "masked_data"

    return ConcreteHashOperation


from .operation_inheritance import *  # noqa: E402, F403


def test_operation_is_abstract() -> None:
    """Test if Operation is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if HashOperationBase cannot be instantiated directly
    with pytest.raises(TypeError):
        HashOperationBase("col_name", "secret")

    # Check for abstract methods if needed
    assert hasattr(
        HashOperationBase, "_mask_line"
    ), "HashOperationBase should define _mask_line"
    assert hasattr(
        HashOperationBase, "_mask_data"
    ), "HashOperationBase should define _mask_data"

    # Ensure _mask_data is abstract
    assert isinstance(
        HashOperationBase, ABCMeta
    ), "HashOperationBase should be an abstract base class"


def test_hash_operation_hash(
    operation_class: type, v_input_name_and_line: tuple[str, str | pd.Series]
) -> None:
    """Test if HashOperationBase implements hashlib functions.
    
    The expected behavior is:

    - The hash function should be from hashlib.
    - If no secret is provided, then the mask_line method should return the result of the hash_function applied to the line.
    - If a secret is provided, then the mask_line method should return the result of the hash_string function applied to the line with the secret key.
       
   Args:
    ----
        operation_class (type): HashOperationBase class
        v_input_name_and_line (tuple): tuple with the column name and input line
   """
    col_name, line = v_input_name_and_line
    operation = operation_class(col_name=col_name)

    assert (
        operation.hash_function.__module__ == "_hashlib"
    ), "Hash function should be from hashlib"

    if line is None:
        return

    str_line = line
    if isinstance(line, pd.Series):
        str_line = line[col_name]

    if operation.secret is None:
        # Check if the masked line is the same as the hash of the line
        assert (
            operation._mask_line(str_line)
            == operation.hash_function(str_line.encode()).hexdigest()
        ), "Masked line should be the hash of the line"
        return

    # Check if the masked line is the same as the hash of the line with the secret key
    assert operation._mask_line(str_line) == hash_string(
        str_line, operation.secret, method=operation.hash_function
    ), "Masked line should be the hash of the line with the secret key"
