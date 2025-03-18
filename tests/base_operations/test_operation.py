from abc import ABCMeta

import pandas as pd
import pytest
from masking.base_operations.operation import Operation

from .conftest import get_v_input_name_and_line_str


@pytest.fixture(
    scope="module", params=get_v_input_name_and_line_str()
)  # Valid input name and line
def v_input_name_and_line(
    request: pytest.FixtureRequest,
) -> tuple[str, str | pd.Series]:
    return request.param


@pytest.fixture(scope="module")
def operation_class() -> type:
    class ConcreteOperation(Operation):
        @staticmethod
        def _mask_line(line: str) -> str:
            return "masked_" + line

        @staticmethod
        def _mask_data() -> str:
            return "masked_data"

    return ConcreteOperation


from .operation_inheritance import *  # noqa: E402, F403


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
