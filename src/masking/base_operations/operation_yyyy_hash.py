from datetime import date, datetime

from dateparser import parse

from masking.base_operations.operation_hash import HashOperationBase
from masking.utils.hash import hash_string


class YYYYHashOperationBase(HashOperationBase):
    """Hashes a datetime column using SHA256 algorithm.

    First extracts the year of the date of birth and then hashee the entire date of birth.
    Example: 1990-01-01 -> 1990_hash(1990-01-01).
    """

    def _mask_line(self, line: str | datetime) -> str:
        """Mask a single line.

        Args:
        ----
            line (str): input line

        Returns:
        -------
            str: masked line

        """
        # Extract the year from the date
        if isinstance(line, str):
            try:
                line_date = parse(line)
                year = line_date.year
            except Exception as e:
                msg = f"Failed to parse date {line}: {e}"
                raise ValueError(msg) from e

        if any(isinstance(line, t) for t in (datetime, date)):
            year = line.year

        signature = hash_string(str(line), self.secret, method=self.hash_function)
        return "_".join([str(year), signature])
