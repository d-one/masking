from pathlib import Path

import pandas as pd

from masking.base_operations.operation import Operation


class MedStatsOperationBase(Operation):
    """Anonymize a column using MedStat regions.

    Definition and source:
    https://dam-api.bfs.admin.ch/hub/api/dam/assets/33347910/master
    """

    _MEDSTATS_LOOKUP_TABLE: dict[str, str] | None = None

    def __init__(self, col_name: str, **kwargs: dict) -> None:
        """Initialize the HashOperation class.

        Args:
        ----
            col_name (str): column name to be hashed
            secret (str): secret key to hash the input string
            hash_function (hashlib._Hash): hash function to use
            **kwargs (dict): keyword arguments

        """
        super().__init__(col_name=col_name, **kwargs)

        # Initialize the med_stats lookup table
        if self._MEDSTATS_LOOKUP_TABLE is None:
            self._MEDSTATS_LOOKUP_TABLE = self._get_medstats_lookup_table()

    @staticmethod
    def _get_medstats_lookup_table() -> dict[str, str]:
        """Get the MedStat lookup table from the given URL, by parsing the XLSX file and returning a dictionary.

        Args:
        ----
            url (str): URL to the XLSX file containing MedStat regions.

        Returns:
        -------
            dict[str, str]: A dictionary mapping region names to their corresponding codes.

        """
        # Read the MedStat regions from the provided Excel file.
        med_stat_file = Path(__file__).parent / "sources" / "med_stats_2024.xlsx"
        medstats = pd.read_excel(
            med_stat_file, sheet_name="REGION=CH", engine="openpyxl"
        )

        # Create a dictionary mapping region names to their corresponding codes.
        # The 'NPA/PLZ' column contains the region codes, and the 'MedStat' column contains the region names.
        # Both columns should be converted to strings to ensure compatibility.
        return {
            str(row["NPA/PLZ"]).strip(): str(row["MedStat"]).strip()
            for _, row in medstats.iterrows()
            if pd.notna(row["MedStat"]) and pd.notna(row["NPA/PLZ"])
        }

    def _mask_line(self, line: str, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): keyword arguments

        Returns:
        -------
            str: masked line

        """
        try:
            # Convert the line to a string
            line = str(line).strip()
        except Exception as e:
            msg = f"Error converting line to string: {e}"
            raise ValueError(msg) from e

        # Return the masked line using the lookup table
        return self._MEDSTATS_LOOKUP_TABLE.get(line, line)
