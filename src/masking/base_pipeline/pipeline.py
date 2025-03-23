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

        self.col_pipelines = []
        for col_name, config in configuration.items():
            if isinstance(config, dict):
                self.col_pipelines.append(dtype(**config))
                continue

            if isinstance(config, list):
                self.col_pipelines.append([dtype(**c) for c in config])
                continue

            msg = f"Invalid configuration for column {col_name}: {config}"
            raise ValueError(msg)

        self.workers = workers

    def clear_concordance_tables(self) -> None:
        """Clear the concordance tables."""
        for pipeline in self.col_pipelines:
            pipeline.clear_concordance_table()

    @staticmethod
    def _get_data_columns_order(data: AnyDataFrame) -> dict:
        """Get the order of the columns in the dataframe."""
        return {col_name: i for i, col_name in enumerate(data.columns)}

    @staticmethod
    @abstractmethod
    def _substitute_masked_values(
        data: AnyDataFrame,
        col_pipelines: list[ConcordanceTableBase],
        concordance_tables: dict[str, dict[str, str]],
    ) -> AnyDataFrame:
        """Substitute the masked values in the original dataframe using the concordance table."""

    @staticmethod
    @abstractmethod
    def _filter_data(
        data: AnyDataFrame, col_name: str, necessary_columns: list[str]
    ) -> AnyDataFrame:
        """Filter only the relevant columns for the masking."""

    def filter_data(
        self,
        pipeline: ConcordanceTableBase | list[ConcordanceTableBase],
        data: AnyDataFrame,
    ) -> AnyDataFrame:
        """Filter the data for the pipeline.

        Args:
        ----
            pipeline (ConcordanceTableBase): pipeline object
            data (AnyDataFrame): input dataframe

        Returns:
        -------
            AnyDataFrame: filtered dataframe

        """
        if not isinstance(pipeline, list):
            return self._filter_data(
                data,
                col_name=pipeline.column_name,
                necessary_columns=pipeline.serving_columns,
            )

        necessary_columns = {col for p in pipeline for col in p.serving_columns}
        return self._filter_data(data, pipeline[0].column_name, list(necessary_columns))

    @staticmethod
    @abstractmethod
    def _impose_ordering(data: AnyDataFrame, columns_order: dict) -> AnyDataFrame:
        """Impose the ordering of the columns."""

    def _run_pipeline(
        self,
        pipeline: ConcordanceTableBase | list[ConcordanceTableBase],
        col_data: AnyDataFrame,
    ) -> tuple:
        """Run the pipeline for a column.

        Args:
        ----
            pipeline (ConcordanceTableBase): pipeline object
            col_data (AnyDataFrame): input dataframe

        Returns:
        -------
            tuple: concordance table

        """
        if not isinstance(pipeline, list):
            return pipeline(col_data)

        output_concordance_table = {}
        for pipe in pipeline:
            # Generate the new concordance table
            new_table = pipe(col_data)

            # Substitute the masked values in the original dataframe
            col_data = self._substitute_masked_values(
                col_data, [pipe], {pipe.column_name: new_table}
            )

            # Merge the new concordance table with the previous one
            new_concordance_values = {
                k: v
                for k, v in new_table.items()
                if k not in output_concordance_table.values()
            }
            old_concordance_values = {
                k: v for k, v in new_table.items() if k not in new_concordance_values
            }

            if old_concordance_values:
                output_concordance_table = {
                    k: old_concordance_values.get(v, v)
                    for k, v in output_concordance_table.items()
                }

            output_concordance_table = {
                **output_concordance_table,
                **new_concordance_values,
            }

        return output_concordance_table

    def _run_pipelines_serial(self, data: AnyDataFrame) -> None:
        """Run the pipelines for each column serially.

        Args:
        ----
            data (AnyDataFrame): input dataframe

        """
        for pipeline in self.col_pipelines:
            c_name = (
                pipeline.column_name
                if not isinstance(pipeline, list)
                else pipeline[0].column_name
            )
            self.concordance_tables[c_name] = self._run_pipeline(
                pipeline, self.filter_data(pipeline, data)
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
                    self._run_pipeline, pipeline, self.filter_data(pipeline, data)
                ): (
                    pipeline.column_name
                    if not isinstance(pipeline, list)
                    else pipeline[0].column_name
                )
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
        data = self._substitute_masked_values(
            data, self.col_pipelines, self.concordance_tables
        )

        # Impose the ordering of the columns
        return self._impose_ordering(data, columns_order)
