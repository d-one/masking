from itertools import starmap
from typing import Any

import pandas as pd

from .operation import DeMasking, DeMaskingEntities, Operation

DEMASKING_OPERATIONS: dict[str, Operation] = {
    "hash": DeMasking,
    "fake_date": DeMasking,
    "fake_plz": DeMasking,
    "entities": DeMaskingEntities,
}


class DeMaskColumnPipeline:
    """DeMask column in a dataframe."""

    config: dict[dict[str, Any]]  # demasking operation and configuration
    pipeline: list[Operation]  # list of operations to apply
    column_name: str  # column name to be masked
    concordance_table: pd.DataFrame | None = None  # unique values in the column

    def __init__(self, column: str, config: dict[str, dict[Any]]) -> None:
        """Initialize the DeMaskColumnPipeline.

        Args:
        ----
            column (str or int): column name (str) or column index (int) to be masked
            config (tuple): masking operation and configuration

        """
        self.column_name = column
        self.pipeline = self._build_pipeline(config)

    def _build_pipeline(self, configuration: dict[str, dict[Any]]) -> list[Operation]:
        """Build the pipeline to mask the column."""
        masking = configuration["masking"]
        config = configuration.get("config", {})

        if "col_name" not in config:
            config["col_name"] = self.column_name

        operation_class = DEMASKING_OPERATIONS.get(masking, Operation)
        return [operation_class(**config)]

    def __call__(
        self, data: pd.DataFrame, concordance_table: pd.DataFrame
    ) -> pd.DataFrame:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pd.DataFrame): input data
            concordance_table (pd.DataFrame): concordance table

        Returns:
        -------
            pd.DataFrame: masked data

        """
        #  Extract not-na values from the column
        for operation in self.pipeline:
            data = operation(data, concordance_table)

        return data


class DeMaskTablePipeline:
    """DeMask table in a dataframe."""

    config: dict[dict[str, Any]]  # demasking operation and configuration
    pipeline: list[Operation]  # list of operations to apply
    concordance_tables: dict[str, pd.DataFrame]  # unique values in the column

    def __init__(
        self, config: dict[str, dict[Any]], concordance_tables: dict[str, pd.DataFrame]
    ) -> None:
        """Initialize the DeMaskTablePipeline.

        Args:
        ----
            config (dict): demasking operation and configuration
            concordance_tables (dict): concordance tables

        """
        self.pipeline: list[DeMaskColumnPipeline] = list(
            starmap(DeMaskColumnPipeline, config.items())
        )
        self.concordance_tables = concordance_tables

    def __call__(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply the pipeline to the data.

        Args:
        ----
            data (pd.DataFrame): input data

        Returns:
        -------
            pd.DataFrame: masked data

        """
        #  Extract not-na values from the column
        for pipeline in self.pipeline:
            data = pipeline(data, self.concordance_tables[pipeline.column_name])

        return data
