from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, ClassVar

from masking.base_concordance_table.concordance_table import ConcordanceTableBase
from masking.base_operations.operation import AnyDataFrame


class MaskDataFramePipelineBase(ABC):
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    config: dict[str, Any]
    col_pipelines: list[ConcordanceTableBase]
    workers: ClassVar[int] = 1

    concordance_tables: ClassVar[dict[str, dict]] = {}  # unique values in the columns

    def __init__(
        self,
        configuration: dict[str, dict[str, tuple[dict[str, Any]]]],
        workers: int = 1,
        dtype: ConcordanceTableBase = ConcordanceTableBase,
    ) -> None:
        self.config = configuration

        self.col_pipelines = [dtype(**config) for config in configuration.values()]
        self.workers = workers

    def clear_concordance_tables(self) -> None:
        """Clear the concordance tables."""
        for pipeline in self.col_pipelines:
            pipeline.clear_concordance_table()

    @staticmethod
    def _get_data_columns_order(data: AnyDataFrame) -> dict:
        """Get the order of the columns in the dataframe."""
        return {col_name: i for i, col_name in enumerate(data.columns)}

    @abstractmethod
    def _substitute_masked_values(self, data: AnyDataFrame) -> AnyDataFrame:
        """Substitute the masked values in the original dataframe using the concordance table."""

    @staticmethod
    @abstractmethod
    def _filter_data(
        pipeline: ConcordanceTableBase, data: AnyDataFrame
    ) -> AnyDataFrame:
        """Filter only the relevant columns for the masking."""

    @staticmethod
    @abstractmethod
    def _impose_ordering(data: AnyDataFrame, columns_order: dict) -> AnyDataFrame:
        """Impose the ordering of the columns."""

    @staticmethod
    def _run_pipeline(pipeline: ConcordanceTableBase, col_data: AnyDataFrame) -> tuple:
        return pipeline(col_data)

    def _run_pipelines_serial(self, data: AnyDataFrame) -> None:
        """Run the pipelines for each column serially.

        Args:
        ----
            data (AnyDataFrame): input dataframe

        """
        for pipeline in self.col_pipelines:
            self.concordance_tables[pipeline.column_name] = self._run_pipeline(
                pipeline, self._filter_data(pipeline, data)
            )

    def _run_pipelines_parallel(self, data: AnyDataFrame) -> None:
        """Run the pipelines for each column in parallel.

        Args:
        ----
            data (AnyDataFrame): input dataframe

        """
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            futures = {
                executor.submit(
                    self._run_pipeline, pipeline, self._filter_data(pipeline, data)
                ): pipeline.column_name
                for pipeline in self.col_pipelines
            }

            for future in as_completed(futures):
                col_name = futures[future]
                self.concordance_tables[col_name] = future.result()

    def _run_pipelines(self, data: AnyDataFrame) -> None:
        """Run the pipelines for each column.

        Args:
        ----
            data (AnyDataFrame): input dataframe

        """
        if self.workers == 1:
            self._run_pipelines_serial(data)
            return

        self._run_pipelines_parallel(data)

    def __call__(self, data: AnyDataFrame) -> AnyDataFrame:
        """Mask the dataframe."""
        # Get the ordering of the columns
        columns_order = self._get_data_columns_order(data)

        # Create concordance tables
        self._run_pipelines(data)

        # Substitute the masked values in the original dataframe
        data = self._substitute_masked_values(data)

        # Impose the ordering of the columns
        return self._impose_ordering(data, columns_order)
