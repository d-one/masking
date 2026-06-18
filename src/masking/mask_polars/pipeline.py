from typing import Any

import polars as pl

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase


class ConcordanceTable(ConcordanceTableBase):
    def _build_concordance_table(self, data: pl.DataFrame) -> dict:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pl.DataFrame): input dataframe

        Returns:
        -------
            dict: concordance table mapping clear values to masked values

        """
        masked_values = self.masking_operation(data)
        data = data.with_columns(masked_values.alias("masked_values"))
        data = data.rename({self.column_name: "clear_values"})

        # Filter out all the values where masked_values == clear_values
        data = data.filter(
            data["clear_values"].cast(pl.Utf8) != data["masked_values"].cast(pl.Utf8)
        )
        return dict(
            zip(
                data["clear_values"].to_list(),
                data["masked_values"].to_list(),
                strict=False,
            )
        )


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
        """Filter out the data where the masked values are the same as the clear values.

        Args:
        ----
            data (pl.DataFrame): input dataframe
            col_name (str): column name
            necessary_columns (list[str]): list of necessary columns

        Returns:
        -------
            pl.DataFrame: dataframe with masked column

        """
        return data.select(necessary_columns).drop_nulls(subset=[col_name]).unique()

    @staticmethod
    def _substitute_masked_values(
        data: pl.DataFrame,
        col_pipelines: list[ConcordanceTable],
        concordance_tables: dict[str, dict[str, str]],
    ) -> pl.DataFrame:
        """Substitute the masked values in the original dataframe using the concordance table.

        Args:
        ----
            data (pl.DataFrame): input dataframe
            col_pipelines (list[ConcordanceTable]): list of concordance tables
            concordance_tables (dict[str, dict[str, str]): dictionary with the concordance tables

        Returns:
        -------
            pl.DataFrame: dataframe with masked column

        """
        for pipeline in col_pipelines:
            c_name = (
                pipeline.column_name
                if not isinstance(pipeline, list)
                else pipeline[0].column_name
            )
            if concordance_tables[c_name]:
                tab = concordance_tables[c_name]
                data = data.with_columns(
                    pl.col(c_name)
                    .cast(pl.Utf8)
                    .map_elements(
                        lambda x, t=tab: t.get(x, x) if x is not None else x,
                        return_dtype=pl.Utf8,
                    )
                )

        return data

    @staticmethod
    def _copy_column(
        data: pl.DataFrame, col_name: str, new_col_name: str
    ) -> pl.DataFrame:
        """Copy a column to a new column in the dataframe.

        Args:
        ----
            data (pl.DataFrame): input dataframe
            col_name (str): name of the column to copy
            new_col_name (str): name of the new column

        Returns:
        -------
            pl.DataFrame: dataframe with the new column

        """
        return data.with_columns(pl.col(col_name).alias(new_col_name))

    @staticmethod
    def _rename_columns(
        data: pl.DataFrame, column_mapping: dict[str, str]
    ) -> pl.DataFrame:
        """Rename columns in the dataframe.

        Args:
        ----
            data (pl.DataFrame): input dataframe
            column_mapping (dict[str, str]): mapping of old column names to new column names

        Returns:
        -------
            pl.DataFrame: dataframe with renamed columns

        """
        return data.rename(column_mapping)
