from typing import Any

import polars as pl

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase
from masking.utils.string_handler import generate_unique_name


class ConcordanceTable(ConcordanceTableBase):
    def _build_concordance_table(self, data: pl.DataFrame) -> dict:
        """Apply the pipeline to the data.

        Args:
        ----
            data (DataFrame): input dataframe or series

        Returns:
        -------
            DataFrame: dataframe or series with masked column

        """
        try:
            data = data.with_columns(
                masked_values=self.masking_operation(data.select(*self.serving_columns))
            ).rename({self.column_name: "clear_values"})

            return {
                v["clear_values"]: v["masked_values"]
                for v in data.select(
                    pl.col("clear_values"), pl.col("masked_values")
                ).to_dicts()
                if v["clear_values"] != v["masked_values"]
            }

        except Exception as e:
            msg = f"Invalid concordance table, expected a Dataframe with columns ['clear_values','masked_values']: {e}"
            raise ValueError(msg) from e


class MaskDataFramePipeline(MaskDataFramePipelineBase):
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    def __init__(
        self,
        configuration: dict[str, dict[str, tuple[dict[str, Any]]]],
        workers: int = 1,
    ) -> None:
        super().__init__(configuration, workers, dtype=ConcordanceTable)

    @staticmethod
    def _filter_data(
        data: pl.DataFrame, col_name: str, necessary_columns: list[str]
    ) -> pl.DataFrame:
        """Filter the data to only include the necessary columns.

        Args:
        ----
            data (DataFrame): input dataframe
            col_name (str): name of the column to mask
            necessary_columns (list[str]): list of columns to keep

        Returns:
        -------
            DataFrame: filtered dataframe

        """
        return (
            data.select(necessary_columns)
            .filter(pl.col(col_name).is_not_null())
            .unique()
        )

    @staticmethod
    def _substitute_masked_values(
        data: pl.DataFrame,
        col_pipelines: list[ConcordanceTable],
        concordance_tables: dict[str, dict[str, str]],
    ) -> pl.DataFrame:
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
                    data.with_columns(
                        (
                            pl.when(pl.col(c_name).is_not_null())
                            .then(
                                pl.col(c_name).map_elements(
                                    lambda x: concordance_tables[c_name].get(x, str(x)),  # noqa: B023
                                    return_dtype=pl.String,
                                )
                            )
                            .otherwise(None)
                        ).alias(new_col_name)
                    )
                    .drop(c_name)
                    .rename({new_col_name: c_name})
                )

        return data

    @staticmethod
    def _impose_ordering(data: pl.DataFrame, columns_order: dict) -> pl.DataFrame:
        """Impose the ordering of the columns.

        Args:
        ----
            data (DataFrame): input dataframe
            columns_order (dict): dictionary with the order of the columns

        Returns:
        -------
        DataFrame: dataframe with the columns ordered

        """
        return data.select(*sorted(data.columns, key=lambda x: columns_order[x]))
