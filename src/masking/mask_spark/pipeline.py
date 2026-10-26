from datetime import date
from typing import Any

import pyarrow as pa
from pyspark.sql import DataFrame
from pyspark.sql.functions import broadcast, coalesce, col, udf, when
from pyspark.sql.types import (
    ByteType,
    DataType,
    DateType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
)

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase
from masking.utils.spark_utils import spark_df_to_dict
from masking.utils.string_handler import generate_unique_name

# Column types whose Spark cast to string equals python's str() of the collected
# value, so that a join reproduces dict.get(x, str(x)) exactly.
_JOIN_KEY_TYPES: dict[type[DataType], tuple[type, pa.DataType]] = {
    StringType: (str, pa.string()),
    ByteType: (int, pa.int8()),
    ShortType: (int, pa.int16()),
    IntegerType: (int, pa.int32()),
    LongType: (int, pa.int64()),
    DateType: (date, pa.date32()),
}

# Above this size the lookup table is joined with a shuffle instead of broadcast
BROADCAST_MAX_ROWS = 5_000_000


def build_lookup_table(
    data: DataFrame, c_name: str, table: dict, key_name: str, value_name: str
) -> DataFrame | None:
    """Create a Spark DataFrame from the concordance table.

    Args:
    ----
        data (DataFrame): input dataframe
        c_name (str): column to substitute
        table (dict): concordance table {clear_value: masked_value}
        key_name (str): name of the key column of the lookup table
        value_name (str): name of the value column of the lookup table

    Returns:
    -------
        DataFrame | None: lookup table, None if the table cannot be joined with the
            column without changing the result (unsupported column type or keys of
            a different type).

    """
    spec = _JOIN_KEY_TYPES.get(type(data.schema[c_name].dataType))
    if spec is None:
        return None

    key_type, arrow_type = spec
    if not all(type(k) is key_type for k in table):
        return None

    try:
        arrow_table = pa.table({
            key_name: pa.array(list(table), arrow_type),
            value_name: pa.array(list(table.values()), pa.string()),
        })
    except (pa.ArrowException, TypeError, OverflowError):
        return None

    return data.sparkSession.createDataFrame(arrow_table)


def substitute_column(data: DataFrame, c_name: str, table: dict) -> DataFrame:
    """Replace the values of a column with the masked values of the concordance table.

    Values not in the table are cast to string. The substitution is done with a
    (broadcast) join, falling back to a python UDF when a join could give a
    different result.

    Args:
    ----
        data (DataFrame): input dataframe
        c_name (str): column to substitute
        table (dict): concordance table {clear_value: masked_value}

    Returns:
    -------
        DataFrame: dataframe with the substituted column

    """
    # Spark resolves column names case-insensitively by default
    existing = [c.lower() for c in data.columns]
    key_name = generate_unique_name(f"{c_name}_clear".lower(), existing)
    value_name = generate_unique_name(f"{c_name}_masked".lower(), [*existing, key_name])

    lookup = build_lookup_table(data, c_name, table, key_name, value_name)
    if lookup is None:
        # Arrow UDFs go through pandas, which turns integers into floats when a
        # batch contains nulls: keep the plain UDF to get the original values.
        masked = udf(lambda x: table.get(x, str(x)), StringType(), useArrow=False)(
            col(c_name)
        )
        return data.withColumn(c_name, when(col(c_name).isNotNull(), masked))

    if len(table) <= BROADCAST_MAX_ROWS:
        lookup = broadcast(lookup)

    return (
        data.join(lookup, col(c_name) == col(key_name), "left")
        .withColumn(
            c_name,
            when(
                col(c_name).isNotNull(),
                coalesce(col(value_name), col(c_name).cast("string")),
            ),
        )
        .drop(key_name, value_name)
    )


class ConcordanceTable(ConcordanceTableBase):
    def _build_concordance_table(self, data: DataFrame) -> DataFrame:
        """Apply the pipeline to the data.

        Args:
        ----
            data (DataFrame): input dataframe or series

        Returns:
        -------
            DataFrame: dataframe or series with masked column

        """
        try:
            masked = self.masking_operation(
                data.select(*[
                    col(self.column_name).alias("clear_values"),
                    col(self.column_name),
                    *[col(c) for c in self.serving_columns if c != self.column_name],
                ])
            ).withColumnRenamed(self.column_name, "masked_values")

            new_values = spark_df_to_dict(
                masked.filter(
                    col("clear_values").cast("string")
                    != col("masked_values").cast("string")
                )
            )

        except Exception as e:
            msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
            raise ValueError(msg) from e

        if self.masking_operation._needs_unique_values:  # noqa: SLF001
            return self._resolve_collisions(new_values)
        return new_values

    def _resolve_collisions(self, new_values: dict) -> dict:
        """Make the masked values unique across Spark partitions.

        Each Spark task masks its partition with its own copy of the operation, so
        uniqueness is only guaranteed within a partition. Values colliding with
        other partitions (or with the existing concordance table) are masked again
        on the driver.

        Args:
        ----
            new_values (dict): masked values collected from all partitions

        Returns:
        -------
            dict: masked values, unique across all partitions

        """
        operation = self.masking_operation
        accepted = set(operation.concordance_table.values())

        resolved, colliding = {}, []
        for clear, masked in new_values.items():
            if masked in accepted:
                colliding.append(clear)
            else:
                accepted.add(masked)
                resolved[clear] = masked

        if not colliding:
            return resolved

        # Values already in the concordance table get their existing masked value.
        # Operations requiring unique values only use the masked column.
        operation.concordance_table.update(resolved)
        for clear in colliding:
            resolved[clear] = operation._check_mask_line(clear, additional_values={})  # noqa: SLF001
        return resolved


class MaskDataFramePipeline(MaskDataFramePipelineBase):
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    def __init__(
        self,
        configuration: dict[str, dict[str, tuple[dict[str, Any]]]],
        workers: int = 1,
    ) -> None:
        super().__init__(configuration, workers, ConcordanceTable)

    @staticmethod
    def _get_unique_values(data: DataFrame, col_name: str) -> list:
        """Return the distinct non-null values of a column.

        Args:
        ----
            data: input dataframe
            col_name (str): column name

        Returns:
        -------
            list: distinct non-null values

        """
        return [
            row[0]
            for row in data.select(col_name)
            .filter(col(col_name).isNotNull())
            .distinct()
            .collect()
        ]

    @staticmethod
    def _filter_data(
        data: DataFrame, col_name: str, necessary_columns: list[str]
    ) -> DataFrame:
        """Filter out the data where the masked values are the same as the clear values.

        Args:
        ----
            data (DataFrame): input dataframe
            col_name (str): column name
            necessary_columns (list[str]): list of necessary columns

        Returns:
        -------
            DataFrame: dataframe with masked column

        """
        return (
            data.select(necessary_columns).filter(col(col_name).isNotNull()).distinct()
        )

    @staticmethod
    def _substitute_masked_values(
        data: DataFrame,
        col_pipelines: list[ConcordanceTable],
        concordance_tables: dict[str, dict[str, str]],
    ) -> DataFrame:
        """Substitute the masked values in the original dataframe using the concordance table.

        Args:
        ----
            data (DataFrame): input dataframe or series
            col_pipelines (list[ConcordanceTable]): list of concordance tables
            concordance_tables (dict[str, dict[str, str]): dictionary with the concordance tables

        Returns:
        -------
                DataFrame: dataframe or series with masked column.

        """
        for pipeline in col_pipelines:
            c_name = (
                pipeline.column_name
                if not isinstance(pipeline, list)
                else pipeline[0].column_name
            )
            if concordance_tables[c_name]:
                data = substitute_column(data, c_name, concordance_tables[c_name])

        return data

    @staticmethod
    def _copy_column(data: DataFrame, col_name: str, new_col_name: str) -> DataFrame:
        """Copy a column to a new column in the dataframe.

        Args:
        ----
            data (DataFrame): input dataframe
            col_name (str): name of the column to copy
            new_col_name (str): name of the new column

        Returns:
        -------
            DataFrame: dataframe with the new column

        """
        return data.withColumn(new_col_name, col(col_name))

    @staticmethod
    def _rename_columns(data: DataFrame, column_mapping: dict[str, str]) -> DataFrame:
        """Rename columns in the dataframe.

        Args:
        ----
            data (DataFrame): input dataframe
            column_mapping (dict[str, str]): mapping of old column names to new column names

        Returns:
        -------
            DataFrame: dataframe with renamed columns

        """
        existing = [c.lower() for c in data.columns]
        temp_names = {
            k: generate_unique_name(seed=f"___temp_{i}___", existing_names=existing)
            for i, k in enumerate(column_mapping.keys())
        }
        for old_name, new_name in temp_names.items():
            data = data.withColumnRenamed(old_name, new_name)

        for old_name, new_name in column_mapping.items():
            data = data.withColumnRenamed(temp_names[old_name], new_name)
        return data
