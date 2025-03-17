from abc import ABCMeta

import pytest
from masking.base_operations.operation import Operation


class ConcreteOperation(Operation):
    @staticmethod
    def _mask_line(line: str) -> str:
        return "masked_" + line

    @staticmethod
    def _mask_data() -> str:
        return "masked_data"


@pytest.fixture(scope="module")
def operation_class() -> type:
    return ConcreteOperation


from .testsuit_operation_inheritance import *  # noqa: E402, F403


def test_operation_is_abstract() -> None:
    """Test if Operation is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if Operation is an abstract class
    with pytest.raises(TypeError):
        Operation("col_name")

    # Check if Operation has abstract methods
    assert hasattr(Operation, "_mask_line"), "Operation should define _mask_line"
    assert hasattr(Operation, "_mask_data"), "Operation should define _mask_data"

    # Ensure _mask_data is abstract
    assert isinstance(Operation, ABCMeta), "Operation should be an abstract base class"
