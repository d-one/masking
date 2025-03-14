from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, when
from pyspark.sql.types import StringType

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase


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
            masked_values = (
                self.masking_operation(
                    data.withColumn("clear_values", col(self.column_name))
                    .withColumn(self.column_name, col(self.column_name).cast("string"))
                    .filter(col(self.column_name) != "")  # noqa: PLC1901
                    .select(["clear_values", *self.serving_columns])
                )
                .withColumnRenamed(self.column_name, "masked_values")
                .cache()
            )

            return masked_values.filter(
                col("clear_values").cast("string") != col("masked_values")
            ).rdd.collectAsMap()

        except Exception as e:
            msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
            raise ValueError(msg) from e

        finally:
            masked_values.unpersist()


class MaskDataFramePipeline(MaskDataFramePipelineBase):
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    def __init__(
        self,
        configuration: dict[str, dict[str, tuple[dict[str, Any]]]],
        workers: int = 1,
    ) -> None:
        self.config = configuration

        self.col_pipelines: list[ConcordanceTable] = [
            ConcordanceTable(**config) for config in configuration.values()
        ]
        self.workers = workers

    @staticmethod
    def _filter_data(pipeline: ConcordanceTable, data: DataFrame) -> DataFrame:
        """Filter out the data where the masked values are the same as the clear values."""
        return (
            data.select(pipeline.serving_columns)
            .filter(col(pipeline.column_name).isNotNull())
            .distinct()
        )

    def _substitute_masked_values(self, data: DataFrame) -> DataFrame:
        """Substitute the masked values in the original dataframe using the concordance table.

        Args:
        ----
            data (DataFrame): input dataframe or series

        Returns:
        -------
                DataFrame: dataframe or series with masked column.

        """
        for pipeline in self.col_pipelines:
            if self.concordance_tables[pipeline.column_name]:
                new_col_name = f"{pipeline.column_name}_masked"
                while new_col_name in data.columns:
                    new_col_name += "_masked_"

                data = (
                    data.withColumn(
                        new_col_name,
                        when(
                            col(pipeline.column_name).isNotNull(),
                            udf(
                                lambda x,
                                tab=self.concordance_tables[
                                    pipeline.column_name
                                ]: tab.get(x, str(x)),
                                StringType(),
                            )(col(pipeline.column_name)),
                        ).otherwise(None),
                    )
                    .drop(pipeline.column_name)
                    .withColumnRenamed(new_col_name, pipeline.column_name)
                )

        return data

    def __call__(self, data: DataFrame) -> DataFrame:
        """Mask the dataframe."""
        columns_order = {col_name: i for i, col_name in enumerate(data.columns)}

        # Create concordance tables
        if self.workers == 1:
            for pipeline in self.col_pipelines:
                self.concordance_tables[pipeline.column_name] = pipeline(
                    self._filter_data(pipeline, data)
                )
        else:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(
                        pipeline, self._filter_data(pipeline, data)
                    ): pipeline.column_name
                    for pipeline in self.col_pipelines
                }

                for future in as_completed(futures):
                    col_name = futures[future]
                    self.concordance_tables[col_name] = future.result()

        data = self._substitute_masked_values(data)

        # Reorder the columns based on the original order
        return data.select(*sorted(data.columns, key=lambda x: columns_order[x]))
