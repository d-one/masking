import hashlib
from abc import ABCMeta
from collections.abc import Callable
from typing import Any

import pandas as pd
import pytest
from dateparser import parse
from masking.base_operations.operation_yyyy_hash import (
    YYYYHashOperationBase,
    hash_string,
)

from .conftest import get_v_input_name_and_line_date


@pytest.fixture(
    scope="module", params=get_v_input_name_and_line_date()
)  # Valid input name and line
def v_input_name_and_line(
    request: pytest.FixtureRequest,
) -> tuple[str, str | pd.Series]:
    return request.param


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
    class ConcreteYYYYHashOperation(YYYYHashOperationBase):
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

    return ConcreteYYYYHashOperation


from .operation_inheritance import *  # noqa: E402, F403


def test_operation_yyyy_hash_is_abstract() -> None:
    """Test if OperationYYYYHash is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if OperationYYYYHash is an abstract class
    with pytest.raises(TypeError):
        YYYYHashOperationBase("col_name")

    # Check if OperationYYYYHash has abstract methods
    assert hasattr(
        YYYYHashOperationBase, "_mask_line"
    ), "YYYYHashOperationBase should define _mask_line"
    assert hasattr(
        YYYYHashOperationBase, "_mask_data"
    ), "YYYYHashOperationBase should define _mask_data"

    # Ensure _mask_data is abstract
    assert isinstance(
        YYYYHashOperationBase, ABCMeta
    ), "YYYYHashOperationBase should be an abstract base class"


def test_yyyy_hash_operation_hash(
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

    year = str(parse(str_line).year)

    if operation.secret is None:
        # Check if the masked line is the same as the hash of the line
        assert (
            operation._mask_line(str_line)
            == year + "_" + operation.hash_function(str_line.encode()).hexdigest()
        ), "Masked line should be the hash of the line"
        return

    # Check if the masked line is the same as the hash of the line with the secret key
    assert operation._mask_line(str_line) == year + "_" + hash_string(
        str_line, operation.secret, method=operation.hash_function
    ), "Masked line should be the hash of the line with the secret key"
