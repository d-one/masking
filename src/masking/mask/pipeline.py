from concurrent.futures import ThreadPoolExecutor, as_completed
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
    def _run_pipeline(
        pipeline: ConcordanceTable, col_data: pd.Series | pd.DataFrame
    ) -> tuple:
        return pipeline(col_data)

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """Mask the dataframe."""
        # Create concordance tables
        if self.workers == 1:
            for pipeline in self.col_pipelines:
                self.concordance_tables[pipeline.column_name] = self._run_pipeline(
                    pipeline, data[pipeline.serving_columns]
                )

                data[pipeline.column_name] = data[pipeline.column_name].map(
                    lambda x, col_name=pipeline.column_name: self.concordance_tables[
                        col_name
                    ].get(x, x)
                    if not pd.isna(x)
                    else x
                )
        else:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                futures = {
                    executor.submit(
                        self._run_pipeline, pipeline, data[pipeline.serving_columns]
                    ): pipeline.column_name
                    for pipeline in self.col_pipelines
                }

                for future in as_completed(futures):
                    col_name = futures[future]
                    self.concordance_tables[col_name] = future.result()

                    data[col_name] = data[col_name].map(
                        lambda x, col_name=col_name: self.concordance_tables[
                            col_name
                        ].get(x, x)
                        if not pd.isna(x)
                        else x
                    )

        return data
