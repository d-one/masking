from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase
from masking.utils.string_handler import generate_unique_name


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
            data = (
                self.masking_operation(
                    data.select(*[
                        col(self.column_name).alias("clear_values"),
                        col(self.column_name),
                        *[
                            col(c)
                            for c in self.serving_columns
                            if c != self.column_name
                        ],
                    ])
                )
                .withColumnRenamed(self.column_name, "masked_values")
                .cache()
            )

            return (
                data.filter(
                    col("clear_values").cast("string")
                    != col("masked_values").cast("string")
                )
                .select("clear_values", "masked_values")
                .rdd.collectAsMap()
            )

        except Exception as e:
            msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
            raise ValueError(msg) from e

        finally:
            data.unpersist()


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
                new_col_name = generate_unique_name(
                    seed=f"{c_name}_masked", existing_names=data.columns
                )

                data = (
                    data.withColumn(
                        new_col_name,
                        when(
                            col(c_name).isNotNull(),
                            udf(
                                lambda x, tab=concordance_tables[c_name]: tab.get(
                                    x, str(x)
                                ),
                                StringType(),
                            )(col(c_name)),
                        ).otherwise(None),
                    )
                    .drop(c_name)
                    .withColumnRenamed(new_col_name, c_name)
                )

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
        temp_names = {
            k: generate_unique_name(seed=f"___temp_{i}___", existing_names=data.columns)
            for i, k in enumerate(column_mapping.keys())
        }
        for old_name, new_name in temp_names.items():
            data = data.withColumnRenamed(old_name, new_name)

        for old_name, new_name in column_mapping.items():
            data = data.withColumnRenamed(temp_names[old_name], new_name)
        return data
