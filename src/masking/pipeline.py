from itertools import starmap
from typing import Any

import pandas as pd

from .operation import HashSHA256, Operation

MASKING_OPERATIONS: dict[str, Operation] = {"hash": HashSHA256}


class MaskColumnPipeline:
    """Mask columns in a dataframe."""

    def __init__(self, column: str, config: tuple[dict[str, Any]]) -> None:
        """Initialize the MaskColumnPipeline.

        Args:
        ----
            column (str): column name to be masked
            config (tuple): masking operation and configuration

        """
        self.config = config
        self.pipeline = self._build_pipeline()
        self.column = column

    @classmethod
    def from_dict(cls, configuration: dict[str, tuple[dict[str, Any]]]) -> None:
        """Create a MaskColumnPipeline from a dictionary."""
        col_name, config = next(iter(configuration.items()))
        return cls(col_name, config)

    def _build_pipeline(self) -> list[Operation]:
        """Build the pipeline to mask the column."""
        masking, config = self.config
        operation_class = MASKING_OPERATIONS.get(masking, Operation)
        return [operation_class(**config)]

    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Apply the pipeline to the data."""
        df_iter = data

        # Cast non-str data to None
        df_iter[self.column][df_iter[self.column].isna()] = None

        for operation in self.pipeline[:-1]:
            df_iter = operation(df_iter)

        return self.pipeline[-1](df_iter)


class MaskDataFramePipeline(MaskColumnPipeline):
    """Pipeline to mask a dataframe."""

    def __init__(
        self, configuration: dict[str, dict[str, tuple[dict[str, Any]]]]
    ) -> None:
        self.config = configuration
        self.col_pipelines: list[MaskColumnPipeline] = list(
            starmap(MaskColumnPipeline, configuration.items())
        )

    def __call__(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Mask the dataframe."""
        data_ = data

        for pipeline in self.col_pipelines:
            data_ = pipeline(data_)
        return data_
