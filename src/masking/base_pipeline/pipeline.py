from abc import ABC, abstractmethod
from collections import defaultdict
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
        # Mapping of output columns to their input columns and serving columns
        self.output_columns_mapping = defaultdict(
            lambda: {"col_name": "", "serving_columns": [], "col_pipeline_index": 0}
        )

        self.col_pipelines = []
        for c, (col_name, config) in enumerate(configuration.items()):
            self.output_columns_mapping[col_name]["col_pipeline_index"] = c

            if isinstance(config, dict):
                self.col_pipelines.append(dtype(**config))
                self.output_columns_mapping[col_name]["col_name"] = self.col_pipelines[
                    -1
                ].masking_operation.col_name
                self.output_columns_mapping[col_name]["serving_columns"] = (
                    self.col_pipelines[-1].masking_operation.serving_columns
                )
                continue

            if isinstance(config, list):
                pipline_list = []
                pipe_col_name = config[0]["masking_operation"].col_name
                for cf in config:
                    cf_col_name = cf["masking_operation"].col_name

                    if cf_col_name != pipe_col_name:
                        msg = f"All masking operations in a list must have the same column name. Expected {pipe_col_name}, got {cf_col_name}"
                        raise ValueError(msg)

                    if isinstance(cf, dict):
                        pipline_list.append(dtype(**cf))
                        continue

                    msg = f"Invalid configuration for column {col_name}: {cf}"
                    raise ValueError(msg)

                self.col_pipelines.append(pipline_list)

                self.output_columns_mapping[col_name]["col_name"] = pipline_list[
                    0
                ].masking_operation.col_name
                self.output_columns_mapping[col_name]["serving_columns"] = list({
                    col
                    for p in pipline_list
                    for col in p.masking_operation.serving_columns
                })
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
    def _impose_ordering(data: AnyDataFrame, columns_order: dict) -> AnyDataFrame:
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

    @staticmethod
    @abstractmethod
    def _copy_column(
        data: AnyDataFrame, col_name: str, new_col_name: str
    ) -> AnyDataFrame:
        """Copy a column to a new column in the dataframe.

        Args:
        ----
            data (AnyDataFrame): input dataframe
            col_name (str): name of the column to copy
            new_col_name (str): name of the new column

        Returns:
        -------
            AnyDataFrame: dataframe with the new column

        """

    def _preprocess_data(self, data: AnyDataFrame) -> AnyDataFrame:
        """Preprocess the data to ensure two things.

        1. Each masking operation is not changing the input of other masking operations.
        2. If the masking is supposed to appear on a new column, the input dataframe is copied.

        Args:
        ----
            data (AnyDataFrame): input dataframe

        Returns:
        -------
            AnyDataFrame: preprocessed dataframe

        """
        for output_column, input_params in self.output_columns_mapping.items():
            if output_column == input_params["col_name"]:
                continue

            # If we reached this point it means that the masking operation is supposed to appear on a new column

            # 1. Make sure that the output column is not already in the dataframe
            if output_column in {
                c
                for cols in self.output_columns_mapping.values()
                for c in cols["serving_columns"]
            }:
                msg = f"Output column name '{output_column}' cannot be used: this column is already used by some masking operation."
                raise ValueError(msg)

            # 2. If the output column is not in the dataframe, we need to copy the input column to the output column
            if output_column not in data.columns:
                data = self._copy_column(
                    data=data,
                    col_name=input_params["col_name"],
                    new_col_name=output_column,
                )

        return data

    @staticmethod
    @abstractmethod
    def _rename_columns(
        data: AnyDataFrame, column_mapping: dict[str, str]
    ) -> AnyDataFrame:
        """Rename columns in the dataframe.

        Args:
        ----
            data (AnyDataFrame): input dataframe
            column_mapping (dict[str, str]): mapping of old column names to new column names

        Returns:
        -------
            AnyDataFrame: dataframe with renamed columns

        """

    def _post_process_data(self, data: AnyDataFrame) -> AnyDataFrame:
        """Post-process the data after masking.

        This method is used only if

        Args:
        ----
            data (AnyDataFrame): masked dataframe

        Returns:
        -------
            AnyDataFrame: post-processed dataframe

        """
        for output_column, input_params in self.output_columns_mapping.items():
            # If output_column is not input_params["col_name"], it means that the masking operation is supposed to appear on a new column
            if output_column == input_params["col_name"]:
                continue

            # We need to rename the output column to the input column name
            if output_column in data.columns:
                # 1. First swap the order of the columns output_column and input_params["col_name"]
                columns = list(data.columns)
                i_output, i_input = (
                    columns.index(output_column),
                    columns.index(input_params["col_name"]),
                )
                columns[i_output], columns[i_input] = (
                    columns[i_input],
                    columns[i_output],
                )

                data = self._rename_columns(
                    data=data[columns],
                    column_mapping={
                        output_column: input_params["col_name"],
                        input_params["col_name"]: output_column,
                    },
                )

        return data

    def __call__(self, data: AnyDataFrame) -> AnyDataFrame:
        """Mask the dataframe."""
        # Find if copy is necessary
        data = self._preprocess_data(data)

        # Get the ordering of the columns
        columns_order = self._get_data_columns_order(data)

        # Create concordance tables
        self._run_pipelines(data)

        # Substitute the masked values in the original dataframe
        data = self._substitute_masked_values(
            data, self.col_pipelines, self.concordance_tables
        )

        # Impose the ordering of the columns
        data = self._impose_ordering(data, columns_order)

        return self._post_process_data(data)
