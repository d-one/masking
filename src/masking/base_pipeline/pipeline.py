from abc import ABC
from typing import Any, ClassVar

from masking.base_concordance_table.concordance_table import ConcordanceTableBase


class MaskDataFramePipelineBase(ABC):
    """Pipeline to mask a dataframe.

    The pipeline applies a series of operations to a dataframe.
    """

    config: dict[str, Any]
    col_pipelines: list[ConcordanceTableBase]

    concordance_tables: ClassVar[dict[str, dict]] = {}  # unique values in the columns

    def clear_concordance_tables(self) -> None:
        """Clear the concordance tables."""
        for pipeline in self.col_pipelines:
            pipeline.clear_concordance_table()
