from typing import Any

import pandas as pd

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_pipeline.pipeline import MaskDataFramePipelineBase


class ConcordanceTable(ConcordanceTableBase):
    def _build_concordance_table(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        data["masked_values"] = self.masking_operation(data)
        data = data.rename(columns={self.column_name: "clear_values"})

        # Filter out all the values where masked_values == clear_values
        data = data[
            data["clear_values"].astype(str) != data["masked_values"].astype(str)
        ]
        return dict(
            zip(
                data["clear_values"].tolist(),
                data["masked_values"].tolist(),
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
        data: pd.DataFrame, col_name: str, necessary_columns: list[str]
    ) -> pd.DataFrame:
        """Filter out the data where the masked values are the same as the clear values.

        Args:
        ----
            data (pd.DataFrame): input dataframe
            col_name (str): column name
            necessary_columns (list[str]): list of necessary columns

        Returns:
        -------
            pd.DataFrame: dataframe with masked column

        """
        # Filter out all the row which have null valus on the col_name
        return (
            data[necessary_columns]
            .dropna(subset=[col_name])
            .drop_duplicates(keep="first")
        )

    @staticmethod
    def _substitute_masked_values(
        data: pd.DataFrame,
        col_pipelines: list[ConcordanceTable],
        concordance_tables: dict[str, dict[str, str]],
    ) -> pd.DataFrame:
        """Substitute the masked values in the original dataframe using the concordance table.

        Args:
        ----
            data (pd.DataFrame): input dataframe
            col_pipelines (list[ConcordanceTable]): list of concordance tables
            concordance_tables (dict[str, dict[str, str]): dictionary with the concordance tables

        Returns:
        -------
        pd.DataFrame: dataframe with masked column

        """
        for pipeline in col_pipelines:
            c_name = (
                pipeline.column_name
                if not isinstance(pipeline, list)
                else pipeline[0].column_name
            )
            if concordance_tables[c_name]:
                data[c_name] = data[c_name].map(
                    lambda x, col_name=c_name: concordance_tables[col_name].get(x, x)
                    if not pd.isna(x)
                    else x
                )

        return data

    @staticmethod
    def _impose_ordering(data: pd.DataFrame, columns_order: dict) -> pd.DataFrame:
        """Impose the ordering of the columns.

        Args:
        ----
            data (pd.DataFrame): input dataframe
            columns_order (dict): dictionary with the columns order {<col_name>:<position>}

        Returns:
        -------
        pd.DataFrame: dataframe with the columns ordered

        """
        return data[sorted(data.columns, key=lambda x: columns_order[x])]
