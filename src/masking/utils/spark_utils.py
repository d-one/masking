from pyspark.sql import DataFrame
from pyspark.sql.types import (
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    FractionalType,
    IntegralType,
    StringType,
)

# Types whose python values are identical when collected via Arrow or via Rows
_ARROW_SAFE_TYPES = (
    BooleanType,
    DateType,
    DecimalType,
    FractionalType,
    IntegralType,
    StringType,
)


def _is_arrow_safe(data_type: DataType) -> bool:
    return isinstance(data_type, _ARROW_SAFE_TYPES)


def spark_df_to_dict(
    data: DataFrame, key: str = "clear_values", value: str = "masked_values"
) -> dict:
    """Collect two columns of a Spark DataFrame into a dictionary {key: value}.

    The columns are collected in a single job, through Arrow when the column
    types allow it (much faster than collecting Rows).

    Args:
    ----
        data (DataFrame): input dataframe
        key (str): name of the column containing the keys
        value (str): name of the column containing the values

    Returns:
    -------
        dict: mapping of the key column to the value column

    """
    data = data.select(key, value)

    if all(_is_arrow_safe(f.dataType) for f in data.schema.fields):
        table = data.toArrow()
        return dict(
            zip(table.column(0).to_pylist(), table.column(1).to_pylist(), strict=True)
        )

    return {row[0]: row[1] for row in data.collect()}
