import re
from abc import ABC, abstractmethod

import pandas as pd


class Operation(ABC):
    """Abstract class for masking operations in a pipeline."""

    def _prepare_concordance_table(
        self, data: pd.DataFrame | pd.Series
    ) -> dict[str, str]:
        """Caste concordance table to a dictionary.

        Args:
        ----
            data (pd.DataFrame or pd.Series): input dataframe or series

        """
        # Define a dict where the keys are the self.col_name and the values are values in the column self.col_name + "_masked"
        values = data[self.col_name].to_numpy()
        keys = data[self.col_name + "_masked"].to_numpy()
        return dict(zip(keys, values, strict=False))

    @abstractmethod
    def _demask_row(self, data: pd.DataFrame | pd.Series) -> pd.DataFrame | pd.Series:
        """Apply the operation to the data."""
        print(  # noqa: T201
            "Applying operation not implemented: please implement the __call__ method."
        )

    def __call__(
        self, data: pd.DataFrame | pd.Series, concordance_table: pd.DataFrame
    ) -> pd.DataFrame | pd.Series:
        # Prepare the concordance table
        concordance_table = self._prepare_concordance_table(concordance_table)

        if isinstance(data, pd.Series):
            # Replace the masked values with the original values
            return data.apply(
                lambda x: self._demask_row(x, concordance_table=concordance_table)
                if pd.notna(x)
                else x
            )

        # Replace the masked values with the original values
        data[self.col_name] = data[self.col_name].apply(
            lambda x: self._demask_row(x, concordance_table=concordance_table)
            if pd.notna(x)
            else x
        )
        return data


class DeMasking(Operation):
    """Class for removing the mask from the data."""

    col_name: str  # column name to be masked

    def __init__(self, col_name: str) -> None:
        """Initialize the DeMasking object."""
        self.col_name = col_name

    @staticmethod
    def _demask_row(line: str, concordance_table: dict[str, str]) -> pd.Series:
        """Demask a row in a dataframe.

        Args:
        ----
            line (str): row to demask
            concordance_table (dict): concordance table

        Returns:
        -------
            pd.Series: demasked row

        """
        return concordance_table.get(line, line)


class DeMaskingEntities(Operation):
    """Class for removing the mask from the data."""

    col_name: str  # column name to be masked

    def __init__(self, col_name: str) -> None:
        """Initialize the DeMasking object."""
        self.col_name = col_name

    @staticmethod
    def _demask_row(line: str, concordance_table: dict[str, str]) -> pd.Series:
        """Demask a row in a dataframe.

        Args:
        ----
            line (str): row to demask
            concordance_table (dict): concordance table

        Returns:
        -------
            pd.Series: demasked row

        """
        # Find all the entities in the line between the delimiters "<< >>" using regex
        entities = re.findall(r"<<(.*?)>>", line)

        # Replace the entities found in the line with the original values
        for entity in entities:
            line = line.replace(
                f"<<{entity}>>", concordance_table.get(entity, f"<<{entity}>>")
            )

        return line
