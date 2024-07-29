from concurrent.futures import ThreadPoolExecutor
from itertools import starmap
from typing import Any

import pandas as pd

from .operations import MASKING_OPERATIONS, Operation


class MaskColumnPipeline:
    """Mask columns in a dataframe.

    The pipeline applies a series of operations to a column in a dataframe.
    The operations are defined in the configuration.
    The following steps are performed:
    1. Extract the column to be masked
    2. Extract the unique values in the column
    3. Apply the operations in the unique values
    4. Populate the masked column with the transformed values
    """

    config: dict[str, Any]  # masking operation and configuration
    pipeline: list[Operation]  # list of operations to apply
    column_name: str  # column name to be masked
    concordance_table: pd.DataFrame | None = None  # unique values in the column

    def __init__(
        self,
        column: str,
        config: dict[str, dict[Any]],
        concordance_table: pd.DataFrame | None = None,
    ) -> None:
        """Initialize the MaskColumnPipeline.

        Args:
        ----
            column (str or int): column name (str) or column index (int) to be masked
            config (tuple): masking operation and configuration
            concordance_table (pd.DataFrame or None): unique values in the column

        """

        self.config = config
        self.column_name = column
        self.concordance_table = concordance_table

        self.pipeline = self._build_pipeline()

    @classmethod
    def from_dict(cls, configuration: dict[str, dict[dict[str, Any]]]) -> None:
        """Create a MaskColumnPipeline from a dictionary."""
        col_name, config = next(iter(configuration.items()))
        return cls(col_name, config)

    def _build_pipeline(self) -> list[Operation]:
        """Build the pipeline to mask the column."""
        masking = self.config["masking"] 
        config_ = self.config.get("config", {})

        if "col_name" not in config_:
            config_["col_name"] = self.column_name

        operation_class = MASKING_OPERATIONS.get(masking, Operation)
        return [operation_class(**config_)]

    def _prepare_concordance_table(self) -> None:
        """Prepare the concordance table for the pipeline.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        """
        # Extract the column to be masked
        if self.concordance_table is None:
            self.concordance_table = {}

        # Cast the existing concordance table as a dict
        if isinstance(self.concordance_table, pd.DataFrame):
            keys = self.concordance_table[self.column_name].to_numpy()
            values = self.concordance_table[self.column_name + "_masked"].to_numpy()

            self.concordance_table = dict(zip(keys, values, strict=False))

    def _mask_data(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        # Prepare the data: set index/column name and prepare concordance table
        self._prepare_concordance_table()

        # Check if the concordance table is set
        if not isinstance(self.concordance_table, dict):
            msg = "Concordance table is not set. Please execute function _prepare_data first."
            raise TypeError(msg)

        for operation in self.pipeline:
            data = operation(data)

            # extend concordance table
            keys = list(operation.concordance_table[self.column_name])
            values = list(operation.concordance_table[self.column_name + "_masked"])
            self.concordance_table.update(dict(zip(keys, values, strict=False)))

        return data

    def _cast_concordance_table(self) -> None:
        """Cast the concordance table as a DataFrame."""
        concordance_table_data = {
            self.column_name: list(self.concordance_table.keys()),
            self.column_name + "_masked": list(self.concordance_table.values()),
        }

        self.concordance_table = pd.DataFrame(concordance_table_data)

    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        Returns:
        -------
            pd.DataFrame or pd.Series: dataframe or series with masked column

        """
        # Prepare the data: set index/column name and prepare concordance table
        data = self._mask_data(data)
        self._cast_concordance_table()

        return data


class MaskDataFramePipeline:
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    config: dict[str, dict[str, dict[dict[str, Any]]]]  # configuration
    col_pipelines: list[MaskColumnPipeline]  # list of column pipelines

    concordance_tables: dict[str, pd.DataFrame]  # unique values in the columns
    masked_data: dict[str, pd.DataFrame]  # masked data

    def __init__(
        self,
        configuration: dict[str, dict[str, tuple[dict[str, Any]]]],
        workers: int = 1,
    ) -> None:
        self.config = configuration
        self.col_pipelines: list[MaskColumnPipeline] = list(
            starmap(MaskColumnPipeline, configuration.items())
        )
        self.concordance_tables: dict[str, pd.DataFrame] = {}
        self.masked_data: dict[str, pd.DataFrame] = {}
        self.workers = workers

    @staticmethod
    def _process_pipeline(
        pipeline: MaskColumnPipeline, col_data: pd.Series
    ) -> tuple[str, pd.Series, pd.DataFrame]:
        masked_data = pipeline(col_data)
        return pipeline.column_name, masked_data, pipeline.concordance_table

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """Mask the dataframe."""
        data_ = data

        if self.workers == 1:
            for pipeline in self.col_pipelines:
                (
                    _,
                    self.masked_data[pipeline.column_name],
                    self.concordance_tables[pipeline.column_name],
                ) = self._process_pipeline(pipeline, data_[pipeline.column_name])
        else:
            with ThreadPoolExecutor(max_workers=self.workers) as executor:
                results = list(
                    executor.map(
                        self._process_pipeline,
                        self.col_pipelines,
                        [data[p.column_name].copy() for p in self.col_pipelines],
                    )
                )

                for col_name, masked_data, concordance_table in results:
                    self.masked_data[col_name] = masked_data
                    self.concordance_tables[col_name] = concordance_table

        # Implement parallel execution of pipeilnes

        for col, vals in self.masked_data.items():
            data_[col] = vals

        return data_
