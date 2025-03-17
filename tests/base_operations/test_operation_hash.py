from abc import ABCMeta
from collections.abc import Callable
from typing import Any

import pytest
from masking.base_operations.operation_hash import HashOperationBase, hashlib


class ConcreteHashOperation(HashOperationBase):
    def __init__(
        self,
        col_name: str,
        secret: str = "my_secret",  # noqa: S107
        hash_function: Callable[..., Any] = hashlib.sha256,
        **kwargs: dict,
    ) -> None:
        super().__init__(col_name, secret, hash_function, **kwargs)

    @staticmethod
    def _mask_data() -> str:
        return "masked_data"


from .test_operation_inheritance import *  # noqa: E402, F403


def test_operation_is_abstract() -> None:
    """Test if Operation is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if HashOperationBase cannot be instantiated directly
    with pytest.raises(TypeError):
        HashOperationBase("col_name", "secret")

    # Check for abstract methods if needed
    assert hasattr(
        HashOperationBase, "_mask_data"
    ), "HashOperationBase should define _mask_data"

    # Ensure _mask_data is abstract
    assert isinstance(
        HashOperationBase, ABCMeta
    ), "HashOperationBase should be an abstract base class"
