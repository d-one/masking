import pandas as pd
import pytest
from masking.base_operations.operation import Operation
from pyspark.sql import DataFrame, SparkSession

from .conftest import CT_TYPE


class ConcreteOperation(Operation):
    @staticmethod
    def _mask_line() -> str:
        return "masked_line"

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


@pytest.mark.parametrize("name", ["col_name", "col_name2", "col_name3", None, []])
def test_col_name_is_input(name: str) -> None:
    """Test if the col_name attribute is the input for the operation.

    Args:
    ----
        name (str): name of the column

    """
    # Check that col_name must be a string
    if not isinstance(name, str):
        with pytest.raises(TypeError):
            concrete_operation(name)
        return

    op = concrete_operation(name)
    assert isinstance(op.col_name, str)
    assert op.col_name == name


def test_concordance_table_is_input(input_concordance_table: CT_TYPE) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        input_concordance_table (CT_TYPE): concordance table

    """
    allowed_types = {dict, pd.DataFrame, DataFrame}
    if not any([
        input_concordance_table is None,
        *[isinstance(input_concordance_table, t) for t in allowed_types],
    ]):
        with pytest.raises(TypeError):
            concrete_operation("col_name", input_concordance_table)
        return

    op = concrete_operation("col_name", input_concordance_table)

    # Check that the concordance table must be a dictionary
    assert isinstance(op.concordance_table, dict)

    if isinstance(input_concordance_table, DataFrame):
        input_concordance_table = input_concordance_table.toPandas()

    if isinstance(input_concordance_table, pd.DataFrame):
        input_concordance_table = input_concordance_table.to_dict()
        input_concordance_table = dict(
            zip(
                input_concordance_table["clear_values"].values(),
                input_concordance_table["masked_values"].values(),
                strict=False,
            )
        )

    if input_concordance_table is None:
        assert op.concordance_table == {}
        return

    assert op.concordance_table == input_concordance_table


@pytest.mark.parametrize(
    "table",
    [
        pd.DataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")],
            columns=["clear_values_", "masked_values"],
        ),
        pd.DataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")],
            columns=["clear_values", "_masked_values"],
        ),
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")], ["_clear_values", "masked_values"]
        ),
        SparkSession.builder.getOrCreate().createDataFrame(
            [("a", "b"), ("c", "d"), ("e", "f")], ["clear_values", "masked_values_"]
        ),
    ],
)
def test_concordance_table_have_columns_clear_and_masked_values(
    table: DataFrame,
) -> None:
    """Test if the concordance_table attribute has columns clear_values and masked_values.

    Args:
    ----
        table (DataFrame): concordance table

    """
    with pytest.raises(TypeError):
        concrete_operation("col_name", table)


@pytest.mark.parametrize("name", ["col_name", "col_name2", "col_name3", None, []])
def test_serving_columns_is_list(name: str) -> None:
    """Test if the serving_columns method returns a list with the column name.

    Args:
    ----
        name (str): name of the column

    """
    if not isinstance(name, str):
        with pytest.raises(TypeError):
            concrete_operation(name)
        return

    op = concrete_operation(name)
    assert isinstance(op.serving_columns, list)
    assert op.serving_columns == [name]


def test_update_concordance_table(input_concordance_table: CT_TYPE) -> None:
    """Test if the concordance_table attribute is the input for the operation.

    Args:
    ----
        input_concordance_table (dict): concordance table

    """
    if not isinstance(input_concordance_table, dict):
        with pytest.raises(TypeError):
            concrete_operation("col_name").update_concordance_table(
                input_concordance_table
            )
        return

    op = concrete_operation("col_name")
    op.update_concordance_table(input_concordance_table)

    assert op.concordance_table == input_concordance_table

    # Make sure that is there exists elements in the concordance table, the method will update the concordance table
    op = concrete_operation("col_name", {"1": "2"})
    op.update_concordance_table(input_concordance_table)

    assert "1" in op.concordance_table
    assert "2" in op.concordance_table.values()

    for k, v in input_concordance_table.items():
        assert k in op.concordance_table
        assert v in op.concordance_table.values()
