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
        if isinstance(data, pd.Series):
            data = data.dropna()
            masked = self.masking_operation(data)

            # Drop all the key-value pairs where key==value
            index_to_drop = data == masked
            return dict(
                zip(
                    data[~index_to_drop].tolist(),
                    masked[~index_to_drop].tolist(),
                    strict=False,
                )
            )

        # If we reached this point, we have a DataFrame
        data = data[self.serving_columns].dropna().reset_index(drop=True)

        # Filter out the values that are already in the concordance table
        masked = self.masking_operation(data)

        data = data[self.column_name]
        if isinstance(masked, pd.DataFrame):
            masked = masked[self.column_name]

        # Drop all the key-value pairs where key==value
        index_to_drop = data == masked
        return dict(
            zip(
                data[~index_to_drop].tolist(),
                masked[~index_to_drop].tolist(),
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
        self.config = configuration

        self.col_pipelines: list[ConcordanceTable] = [
            ConcordanceTable(**config) for config in configuration.values()
        ]
        self.workers = workers

    @staticmethod
    def _filter_data(pipeline: ConcordanceTable, data: pd.DataFrame) -> pd.DataFrame:
        """Filter out the data where the masked values are the same as the clear values.

        Args:
        ----
            pipeline (ConcordanceTable): pipeline object
            data (pd.DataFrame): input dataframe

        Returns:
        -------
            pd.DataFrame: dataframe with masked column

        """
        return (
            data[pipeline.serving_columns]
            .dropna(how="all")
            .drop_duplicates(keep="first")
        )

    def _substitute_masked_values(self, data: pd.DataFrame) -> pd.DataFrame:
        """Substitute the masked values in the original dataframe using the concordance table.

        Args:
        ----
            data (pd.DataFrame): input dataframe

        Returns:
        -------
        pd.DataFrame: dataframe with masked column

        """
        for pipeline in self.col_pipelines:
            if self.concordance_tables[pipeline.column_name]:
                data[pipeline.column_name] = data[pipeline.column_name].map(
                    lambda x, col_name=pipeline.column_name: self.concordance_tables[
                        col_name
                    ].get(x, x)
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
            columns_order (dict): dictionary with the columns order

        Returns:
        -------
        pd.DataFrame: dataframe with the columns ordered

        """
        return data[sorted(data.columns, key=lambda x: columns_order[x])]
