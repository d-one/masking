from datetime import datetime

from dateparser import parse

from masking.base_operations.operation_hash import HashOperationBase


class YYYYHashOperationBase(HashOperationBase):
    """Hashes a datetime column using SHA256 algorithm.

    First extracts the year of the date of birth and then hashee the entire date of birth.
    Example: 1990-01-01 -> 1990_hash(1990-01-01).
    """

    def _mask_line(self, line: str | datetime, **kwargs: dict) -> str:  # noqa: ARG002
        """Mask a single line.

        Args:
        ----
            line (str): input line
            **kwargs (dict): additional arguments for the masking operation

        Returns:
        -------
            str: masked line

        """
        if not isinstance(line, str):
            line = str(line)

        # Extract the year from the date
        try:
            line_date = parse(line)
            year = line_date.year
        except Exception as e:
            msg = f"Could not parse date from line: {line}. Error: {e}"
            raise ValueError(msg) from e

        signature = self._hashing_function(line)
        return "_".join([str(year).zfill(4), signature])
