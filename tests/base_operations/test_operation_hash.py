from collections.abc import Callable

import pytest
from masking.base_operations.operation_hash import HashOperationBase
from pandas import DataFrame

from .conftest import CT_TYPE


class ConcreteOperation(HashOperationBase):
    @staticmethod
    def _mask_data() -> str:
        return "masked_data"


def create_concrete_operation(
    name: str, table: dict | None = None, **kwargs: dict
) -> ConcreteOperation:
    """Create a concrete operation.

    Args:
    ----
        name (str): name of the column
        table (dict | None): concordance table
        **kwargs (dict): additional arguments for the masking operation

    Returns:
    -------
        ConcreteOperation: concrete operation

    """
    return ConcreteOperation(col_name=name, concordance_table=table, **kwargs)


def test_operation_is_abstract() -> None:
    """Test if Operation is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if Operation is an abstract class
    with pytest.raises(TypeError):
        HashOperationBase("col_name", "secret")

    # Check if Operation has abstract methods
    with pytest.raises(TypeError):
        HashOperationBase._mask_data("data")


def test_col_name_and_secret_are_input(
    vi_name: str, create_concrete_operation: Callable = create_concrete_operation
) -> None:
    """Test if the col_name attribute is the input for the operation.

    Args:
    ----
        vi_name (str): name of the column
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import test_col_name_is_input as test_col_name_is_input_base

    test_col_name_is_input_base(
        vi_name, lambda n, t=None: create_concrete_operation(n, t, secret=n)
    )

    # Check that secret must be a input
    with pytest.raises(TypeError):
        create_concrete_operation(vi_name)

    # Check that secret must be a string
    with pytest.raises(TypeError):
        create_concrete_operation(vi_name, secret=1)


def test_concordance_table_is_input(
    vi_input_concordance_table: CT_TYPE,
    create_concrete_operation: Callable = create_concrete_operation,
) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        vi_input_concordance_table (CT_TYPE): concordance table
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import (
        test_concordance_table_is_input as test_concordance_table_is_input_base,
    )

    test_concordance_table_is_input_base(
        vi_input_concordance_table,
        lambda n, t=None: create_concrete_operation(n, t, secret=n),
    )


def test_concordance_table_have_columns_clear_and_masked_values(
    invalid_table: DataFrame,
    create_concrete_operation: Callable = create_concrete_operation,
) -> None:
    """Test if the concordance_table attribute has columns clear_values and masked_values.

    Args:
    ----
        invalid_table (DataFrame): concordance table
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import (
        test_concordance_table_have_columns_clear_and_masked_values as test_concordance_table_have_columns_clear_and_masked_values_base,
    )

    test_concordance_table_have_columns_clear_and_masked_values_base(
        invalid_table, lambda n, t=None: create_concrete_operation(n, t, secret=n)
    )


def test_serving_columns_is_list(
    vi_name: str | list | None,
    create_concrete_operation: Callable = create_concrete_operation,
) -> None:
    """Test if the serving_columns method returns a list with the column name.

    Args:
    ----
        vi_name (str): name of the column
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import (
        test_serving_columns_is_list as test_serving_columns_is_list_base,
    )

    test_serving_columns_is_list_base(
        vi_name, lambda n, t=None: create_concrete_operation(n, t, secret=n)
    )


def test_update_concordance_table(
    vi_input_concordance_table: CT_TYPE,
    create_concrete_operation: Callable = create_concrete_operation,
) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        vi_input_concordance_table (dict): concordance table
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import (
        test_update_concordance_table as test_update_concordance_table_base,
    )

    test_update_concordance_table_base(
        vi_input_concordance_table,
        lambda n, t=None: create_concrete_operation(n, t, secret=n),
    )


def test_get_operating_input(
    v_input_name_and_line: tuple,
    create_concrete_operation: Callable = create_concrete_operation,
) -> None:
    """Test if the _get_operating_input method returns the correct input for the operation.

    Args:
    ----
        v_input_name_and_line (tuple): tuple with the column name and input line
        create_concrete_operation (Callable): function to create the concrete operation

    """
    from .test_operation import (
        test_get_operating_input as test_get_operating_input_base,
    )

    test_get_operating_input_base(
        v_input_name_and_line,
        lambda n, t=None: create_concrete_operation(n, t, secret=n),
    )
