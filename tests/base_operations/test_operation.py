import pandas as pd
import pytest
from masking.base_operations.operation import Operation
from pyspark.sql import DataFrame

from .conftest import CT_ALLOWED_TYPES, CT_TYPE


class ConcreteOperation(Operation):
    @staticmethod
    def _mask_line(line: str) -> str:
        return "masked_" + line

    @staticmethod
    def _mask_data() -> str:
        return "masked_data"


def concrete_operation(name: str, table: dict | None = None) -> ConcreteOperation:
    return ConcreteOperation(name, table)


def test_operation_is_abstract() -> None:
    """Test if Operation is an abstract class, with abstract methods _mask_line and _mask_data."""
    # Check if Operation is an abstract class
    with pytest.raises(TypeError):
        Operation("col_name")

    # Check if Operation has abstract methods
    with pytest.raises(TypeError):
        Operation._mask_line("line")

    with pytest.raises(TypeError):
        Operation._mask_data("data")


def test_col_name_is_input(vi_name: str) -> None:
    """Test if the col_name attribute is the input for the operation.

    Args:
    ----
        vi_name (str): name of the column

    """
    # Check that col_name must be a string
    if not isinstance(vi_name, str):
        with pytest.raises(TypeError):
            concrete_operation(vi_name)
        return

    op = concrete_operation(vi_name)
    assert isinstance(op.col_name, str)
    assert op.col_name == vi_name


def test_concordance_table_is_input(vi_input_concordance_table: CT_TYPE) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        vi_input_concordance_table (CT_TYPE): concordance table

    """
    if not any([
        vi_input_concordance_table is None,
        *[isinstance(vi_input_concordance_table, t) for t in CT_ALLOWED_TYPES],
    ]):
        with pytest.raises(TypeError):
            concrete_operation("col_name", vi_input_concordance_table)
        return

    op = concrete_operation("col_name", vi_input_concordance_table)

    # Check that the concordance table must be a dictionary
    assert isinstance(op.concordance_table, dict)

    if isinstance(vi_input_concordance_table, DataFrame):
        vi_input_concordance_table = vi_input_concordance_table.toPandas()

    if isinstance(vi_input_concordance_table, pd.DataFrame):
        vi_input_concordance_table = vi_input_concordance_table.to_dict()
        vi_input_concordance_table = dict(
            zip(
                vi_input_concordance_table["clear_values"].values(),
                vi_input_concordance_table["masked_values"].values(),
                strict=False,
            )
        )

    if vi_input_concordance_table is None:
        assert op.concordance_table == {}
        return

    assert op.concordance_table == vi_input_concordance_table


def test_concordance_table_have_columns_clear_and_masked_values(
    invalid_table: DataFrame,
) -> None:
    """Test if the concordance_table attribute has columns clear_values and masked_values.

    Args:
    ----
        invalid_table (DataFrame): concordance table

    """
    with pytest.raises(TypeError):
        concrete_operation("col_name", invalid_table)


def test_serving_columns_is_list(vi_name: str | list | None) -> None:
    """Test if the serving_columns method returns a list with the column name.

    Args:
    ----
        vi_name (str): name of the column

    """
    if not isinstance(vi_name, str):
        with pytest.raises(TypeError):
            concrete_operation(vi_name)
        return

    op = concrete_operation(vi_name)
    assert isinstance(op.serving_columns, list)
    assert op.serving_columns == [vi_name]


def test_update_concordance_table(vi_input_concordance_table: CT_TYPE) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        vi_input_concordance_table (dict): concordance table

    """
    if not isinstance(vi_input_concordance_table, dict):
        with pytest.raises(TypeError):
            concrete_operation("col_name").update_concordance_table(
                vi_input_concordance_table
            )
        return

    op = concrete_operation("col_name")
    op.update_concordance_table(vi_input_concordance_table)

    assert op.concordance_table == vi_input_concordance_table

    # Make sure that is there exists elements in the concordance table, the method will update the concordance table
    op = concrete_operation("col_name", {"1": "2"})
    op.update_concordance_table(vi_input_concordance_table)

    assert "1" in op.concordance_table
    assert "2" in op.concordance_table.values()

    for k, v in vi_input_concordance_table.items():
        assert k in op.concordance_table
        assert v in op.concordance_table.values()


def test_get_operating_input(v_input_name_and_line: tuple) -> None:
    """Test if the _get_operating_input method returns the correct input for the operation.

    Args:
    ----
        v_input_name_and_line (tuple): tuple with the column name and input line

    """
    name, line = v_input_name_and_line
    op = concrete_operation(name)

    if line is None:
        assert op._get_operating_input(line) is None
        return

    assert op._get_operating_input(line) == "line"


def test_check_mask_line(v_input_name_and_line: tuple) -> None:
    """Test if the _check_mask_line method returns the correct input for the operation.

    If the line is None, the method should return None.
    If the input is a pd.Series, the method should return the value of the column with the same name as the column name.

    Args:
    ----
        v_input_name_and_line (tuple): tuple with the column name and input line

    """
    name, line = v_input_name_and_line
    op = concrete_operation(name)

    if line is None:
        assert op._check_mask_line(line) is None
        return

    expected_line = line
    if isinstance(line, pd.Series):
        expected_line = line[name]

    assert op._check_mask_line(line) == concrete_operation(name)._mask_line(
        expected_line
    )


def test_check_mask_line_with_concordance_table(
    v_input_name_and_line: tuple, vi_input_concordance_table: CT_TYPE
) -> None:
    """Test if the _check_mask_line method returns the correct input for the operation.

    If the value to be masked is in the concordance table, the method should return the masked value in the concordance table.

    Args:
    ----
        v_input_name_and_line (tuple): tuple with the column name and input line
        vi_input_concordance_table (CT_TYPE): concordance table

    """
    name, line = v_input_name_and_line

    # If the concordance table is not in the allowed_types, the method should raise a TypeError
    if not any([
        vi_input_concordance_table is None,
        *[isinstance(vi_input_concordance_table, t) for t in CT_ALLOWED_TYPES],
    ]):
        with pytest.raises(TypeError):
            concrete_operation(name, vi_input_concordance_table)
        return

    op = concrete_operation(name, vi_input_concordance_table)

    if line is None:
        assert op._check_mask_line(line) is None
        return

    expected_line = line
    if isinstance(line, pd.Series):
        expected_line = line[name]

    # Cast the concordance table to a dictionary
    vi_input_concordance_table = op.cast_concordance_table(vi_input_concordance_table)

    if (
        expected_line in vi_input_concordance_table
        and vi_input_concordance_table is not None
    ):
        assert op._check_mask_line(line) == vi_input_concordance_table[expected_line]
        return

    assert op._check_mask_line(line) == concrete_operation(name)._mask_line(
        expected_line
    )
